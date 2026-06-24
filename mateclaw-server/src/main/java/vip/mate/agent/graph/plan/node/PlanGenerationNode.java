package vip.mate.agent.graph.plan.node;

import com.alibaba.cloud.ai.graph.OverAllState;
import com.alibaba.cloud.ai.graph.action.NodeAction;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.extern.slf4j.Slf4j;
import org.springframework.ai.chat.messages.Message;
import org.springframework.ai.chat.messages.SystemMessage;
import org.springframework.ai.chat.messages.UserMessage;
import org.springframework.ai.chat.model.ChatModel;
import org.springframework.ai.chat.prompt.Prompt;
import org.springframework.ai.converter.BeanOutputConverter;
import vip.mate.agent.AgentToolSet;
import vip.mate.agent.GraphEventPublisher;
import vip.mate.agent.graph.NodeStreamingChatHelper;
import vip.mate.agent.graph.plan.state.PlanStateAccessor;
import vip.mate.agent.graph.plan.state.PlanStateKeys;
import vip.mate.agent.graph.state.MateClawStateKeys;
import vip.mate.agent.context.ConversationWindowManager;
import vip.mate.agent.context.RuntimeContextInjector;
import vip.mate.planning.service.PlanningService;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * ============================================================
 * 【Plan-Execute 第1阶段：Triage】任务分流 + 规划生成节点
 * ============================================================
 * 这是 Plan-Execute 流程的第一个节点（所有请求必经之路），负责将用户请求
 * 分到三条执行路径之一：
 *
 *   (A) 直接回答: needs_planning=false → DirectAnswerNode → END
 *       纯知识问答，不需要工具，LLM 直接生成回答
 *
 *   (B) 单步任务: needs_planning=true, steps=[一条指令] → StepExecutionNode
 *       需要工具但本质是一个连贯动作，执行器内部迭代调用工具（不提前拆分为多步）
 *
 *   (C) 多步任务: needs_planning=true, steps=[2~6条] → StepExecutionNode（循环）
 *       多个明显独立的子任务，逐步执行 → 汇总
 *
 * 核心 Prompt 设计（PLANNING_PROMPT）：
 * - 角色定义："你是任务分流器，不是聊天助手"
 * - 输出格式：结构化 JSON（needs_planning, direct_answer, steps）
 * - 关键原则：单工具调用不拆成多步、默认不读取 MEMORY.md
 * - 工具公示：告知 LLM 可用工具名称，帮助判断类别
 *
 * 为什么不用 agent 的 systemPrompt 做分流？
 * - agent 的 systemPrompt 包含 wiki、skill 指南、记忆等，会稀释分流指令
 * - 分流只需要判断"是否需要工具、是否需要拆解步骤"，不需要全部上下文
 *
 * 降级策略：
 * - LLM 返回异常 → fallback 为单步计划（保证工具可用，不降级为空文本）
 * - 返回 needs_planning=true 但 steps 为空 → 以用户目标作为单步指令兜底
 * - 持久化失败 → 最终降级为 direct_answer 错误提示
 *
 * 审批重放路径（Replay）：
 * - 如果 state 中已有 PLAN_ID（由 chatWithReplayStream 注入），跳过 LLM 分流
 * - 直接复用已存在的计划，从暂停步骤继续执行
 *
 * @author MateClaw Team
 */
@Slf4j
public class PlanGenerationNode implements NodeAction {

    private final ChatModel chatModel;
    private final PlanningService planningService;
    private final NodeStreamingChatHelper streamingHelper;
    private final ConversationWindowManager conversationWindowManager;
    private final AgentToolSet toolSet;

    /**
     * Structured triage result — field names use @JsonProperty to match the
     * snake_case keys the LLM is instructed to produce, so no prompt changes needed.
     */
    record TriageResult(
            @JsonProperty("needs_planning") boolean needsPlanning,
            @JsonProperty("direct_answer")  String directAnswer,
            @JsonProperty("plan_type")      String planType,
            @JsonProperty("steps")          List<String> steps
    ) {}

    private static final String PLANNING_PROMPT = """
            你是任务分流器，不是聊天助手。根据用户目标把请求分到三类之一，并只输出一个 JSON 对象。

            硬性规则：
            1. 只返回一个 JSON 对象；不允许 markdown 代码块、不允许任何 JSON 以外的文字。
            2. 不要解释，不要寒暄，不要说"我来...""我先..."。
            3. 判断依据是"目标是否由多个明显独立的子任务/交付物组成"，而不是难度高低：
               单个连贯动作不要拆，但目标确实分成多个部分时也不要硬压成一步。

            三类分流：

            (A) 直接回答 — 简单的纯知识问答：凭自身知识用一两段话即可答完，不需要任何工具、不需要读文件、
                不需要查询当前状态，且目标本身不包含多个需要分别完成的子任务。
                （注意：成段的分析、对比、方案、规划、教程等通常不属于此类，应走 B 或 C。）
                输出：{"needs_planning": false, "direct_answer": "<你的回答>"}

            (B) 单步任务 — 本质是一个连贯动作（一次文件读取 / 一次搜索 / 一次命令 / 一次记忆读写 / 一次计算 /
                一段集中产出）。执行器会在这一步内部迭代调用多次工具，你**不要**提前拆分。
                输出：{"needs_planning": true, "steps": ["<将用户目标复述为一句清晰可执行的指令>"]}

            (C) 多步任务 — 用户目标包含 2 个及以上明显独立、需要先后完成的子任务或交付物（例如"先调研 A 再调研 B
                然后对比"、"读配置、迁移数据、验证结果"、"分阶段制定计划"、"产出由若干独立部分组成的方案"）。
                这是规划型智能体的主路径——当目标确实由多个部分组成时就走这里。
                输出：{"needs_planning": true, "steps": ["步骤1", "步骤2", ...]}（2 到 6 个步骤）

            关键原则：
            - 单工具调用绝对不拆成多步。例："读 A 文件并总结" 是单步（B），不是两步。
            - 默认不要把 MEMORY.md / PROFILE.md / 技能文件读取当成独立步骤；仅当用户明确询问偏好、历史决策或长期约束时才加入。
            - 每个步骤必须是可执行动作，不写"思考一下""确认一下"之类的空话。
            - 多部分、多阶段、需要逐步推进的目标走(C)；真正单一原子动作走(B)；只有简单一问一答才用(A)。
            """;

    public PlanGenerationNode(ChatModel chatModel, PlanningService planningService,
                              NodeStreamingChatHelper streamingHelper,
                              ConversationWindowManager conversationWindowManager,
                              AgentToolSet toolSet) {
        this.chatModel = chatModel;
        this.planningService = planningService;
        this.streamingHelper = streamingHelper;
        this.conversationWindowManager = conversationWindowManager;
        this.toolSet = toolSet;
    }

    /**
     * @deprecated use the full-parameter constructor instead
     */
    @Deprecated
    public PlanGenerationNode(ChatModel chatModel, PlanningService planningService) {
        this(chatModel, planningService, null, null, null);
    }

    /**
     * ★ Plan-Execute 图的第一站 — 所有请求必经此门。
     *
     * 输入（从 OverAllState 读取）：
     *   GOAL — 用户原始请求，buildInitialState 注入
     *   WORKING_CONTEXT — 对话历史压缩摘要，buildInitialState 或上一步 StepExecutionNode 写入
     *   SYSTEM_PROMPT — Agent 的系统提示词
     *   GOAL_FOLLOWUP_PROMPT — GoalEvaluationNode 回传的跟进提示（重新规划时才有）
     *   PLAN_ID — 审批重放时由 chatWithReplayStream 注入（有则跳过 LLM 分流）
     *
     * 内部处理三步走：
     *   ① Goal 跟进注入：如果有 followupPrompt，追加到 goal 尾部（原目标 + 评估建议）
     *   ② 审批重放检查：如果 state 已有 PLAN_ID → 跳过 LLM，直接复用已有计划返回 needsPlanning(true)
     *   ③ LLM 分流：构建5条消息的 Prompt → streamCallSilent 调 LLM → 解析 TriageResult → 三种输出
     *
     * 输出及后续流转：
     *   (A) needsPlanning=false  → DIRECT_ANSWER, phase="direct_answer"
     *       → PlanGenerationDispatcher → DirectAnswerNode
     *       → DirectAnswerNode 搬运 DIRECT_ANSWER → FINAL_SUMMARY → (goal?) GoalEvaluation → END
     *
     *   (B/C) needsPlanning=true → PLAN_ID, PLAN_STEPS, phase="plan_generated"
     *       → PlanGenerationDispatcher → StepExecutionNode（进入逐步执行，内部 while 循环）
     *       → 完成 → StepProgressDispatcher → PlanSummaryNode（汇总）
     *
     * 异常处理（降级链）：
     *   LLM 异常 → fallback 单步 plan(goal, [goal]) → 持久化失败 → direct_answer 错误提示
     */
    @Override
    public Map<String, Object> apply(OverAllState state) throws Exception {
        PlanStateAccessor accessor = new PlanStateAccessor(state);
        String goal = accessor.goal();

        // ★ Goal 跟进注入：
        //    上游: GoalEvaluationNode 评估后认为目标未完成 → 设置 GOAL_FOLLOWUP_PROMPT
        //          并以 followup 路由回本节点（PlanGenerationNode）
        //    此时步骤状态已被 GoalEvaluationNode 清除，所以重新走分流流程
        //    原目标 + "Follow-up guidance" = 原始目标 + 评估器的建议作为下一轮目标
        //    后续: goal 被传给 LLM 做分流 → 新一轮的 PlanGeneration → StepExecution
        String followupPrompt = state.value(MateClawStateKeys.GOAL_FOLLOWUP_PROMPT, "");
        if (!followupPrompt.isEmpty()) {
            log.info("[PlanGeneration] Goal follow-up active, augmenting goal with {} chars of guidance",
                    followupPrompt.length());
            goal = goal + "\n\n[Follow-up guidance]\n" + followupPrompt;
        }

        String systemPrompt = accessor.systemPrompt();
        String agentId = state.value(MateClawStateKeys.TRACE_ID, "unknown");
        String conversationId = accessor.conversationId();

        log.info("[PlanGeneration] Evaluating goal: {}", goal.length() > 100 ? goal.substring(0, 100) + "..." : goal);

        List<GraphEventPublisher.GraphEvent> events = new ArrayList<>();
        events.add(GraphEventPublisher.phase("planning", Map.of("goal", goal)));

        // ---- 分支0：审批重放 — state 中已有 PLAN_ID，跳过 LLM 分流 ----
        // 来源: chatWithReplayStream 提前注入 PLAN_ID + PLAN_STEPS + CURRENT_STEP_INDEX 到 state
        // 后续: PlanGenerationDispatcher(needsPlanning=true) → StepExecutionNode，从 resumeIndex 继续
        Long existingPlanId = state.<Long>value(PlanStateKeys.PLAN_ID).orElse(null);
        if (existingPlanId != null) {
            List<String> existingSteps = accessor.planSteps();
            int resumeIndex = accessor.currentStepIndex();
            log.info("[PlanGeneration] Replay mode — reusing plan {} at step {}/{}", existingPlanId, resumeIndex, existingSteps.size());
            return PlanStateAccessor.output()
                    .needsPlanning(true)
                    .planId(existingPlanId)
                    .planSteps(existingSteps)
                    .planValid(true)
                    .currentStepIndex(resumeIndex)     // 从暂停的步骤继续
                    .currentPhase("plan_generated")
                    .events(events)
                    .build();
        }

        try {
            // ---- 构建分流 Prompt（5层消息结构，不含 agent 的 systemPrompt）----
            // L1. System — 分流器角色定义 + 分类规则（PLANNING_PROMPT）
            // L2. User  — 运行时上下文（当前时间、工作目录、渠道/发起者）
            //     ↳ RuntimeContextInjector.buildContextMessage(workspaceBasePath, null, chatOrigin)
            // L3. User  — 可用工具列表（"可用工具：search, file, browser..."）
            //     ↳ 让 LLM 知道有什么工具可用，从而判断类别：无工具→简单问答，单工具→单步，多工具/独立→多步
            // L4. User  — Working Context（对话历史压缩摘要）
            //     ↳ 让分流感知用户此前提过的要求（"用中文回复"、"格式要求"等）
            // L5. User  — 用户目标 + JSON schema 提示（BeanOutputConverter.getFormat()）
            //     ↳ 告诉 LLM 输出格式：{"needs_planning": bool, "direct_answer": "...", "steps": [...]}
            List<Message> promptMessages = new ArrayList<>();
            promptMessages.add(new SystemMessage(PLANNING_PROMPT));
            String workspaceBasePath = state.value(MateClawStateKeys.WORKSPACE_BASE_PATH, "");
            vip.mate.agent.context.ChatOrigin chatOrigin =
                    state.<vip.mate.agent.context.ChatOrigin>value(MateClawStateKeys.CHAT_ORIGIN)
                            .orElse(vip.mate.agent.context.ChatOrigin.EMPTY);
            promptMessages.add(new UserMessage(
                    RuntimeContextInjector.buildContextMessage(workspaceBasePath, null, chatOrigin)));

            // Advertise available tools so the LLM can recognize when an action is possible,
            // but do NOT force "any tool usage implies multi-step" — single-hop tool use
            // should resolve to a 1-step plan, not a multi-step decomposition.
            if (toolSet != null && !toolSet.callbacks().isEmpty()) {
                String toolNames = toolSet.callbacks().stream()
                        .map(cb -> cb.getToolDefinition().name())
                        .collect(Collectors.joining(", "));
                promptMessages.add(new UserMessage(
                        "可用工具：" + toolNames
                                + "\n单次工具调用应归为单步（B），不要拆成多步。"));
            }

            // Inject working context (rolling conversation summary) so triage respects
            // prior constraints without re-reading full history.
            String workingContext = accessor.workingContext();
            if (!workingContext.isEmpty()) {
                promptMessages.add(new UserMessage(
                        "以下是此前对话中用户提出的约束、说明和上下文，请在分流时参考：\n\n"
                                + workingContext));
            }

            promptMessages.add(new UserMessage("用户目标：" + goal));

            // Append JSON schema hint generated by BeanOutputConverter so the LLM
            // knows the exact expected structure (replaces hand-written schema in PLANNING_PROMPT).
            BeanOutputConverter<TriageResult> converter = new BeanOutputConverter<>(TriageResult.class);
            promptMessages.add(new UserMessage(converter.getFormat()));

            Prompt prompt = new Prompt(promptMessages);

            // Broadcast a lightweight progress token so the frontend shows activity
            // during the silent triage call (typically 1-3 s).
            if (streamingHelper != null) {
                streamingHelper.broadcastProgress(conversationId, "分析中...");
            }

            // Silent streaming call — structured JSON is parsed below; tokens are not forwarded to the client.
            long triageStartMs = System.currentTimeMillis();
            NodeStreamingChatHelper.StreamResult result = streamingHelper.streamCallSilent(
                    chatModel, prompt, conversationId, "plan_generation");

            // Prompt-too-long handling: compact the conversation window and retry once.
            if (result.isPromptTooLong() && conversationWindowManager != null) {
                log.warn("[PlanGeneration] Prompt too long, attempting compaction and retry");
                List<Message> compactedMessages = conversationWindowManager.compactForRetry(
                        promptMessages.subList(1, promptMessages.size()));
                if (compactedMessages != null) {
                    List<Message> retryMessages = new ArrayList<>();
                    retryMessages.add(promptMessages.get(0));
                    retryMessages.addAll(compactedMessages);
                    result = streamingHelper.streamCallSilent(
                            chatModel, new Prompt(retryMessages), conversationId, "plan_generation_compact_retry");
                }
            }

            long triageMs = System.currentTimeMillis() - triageStartMs;

            String llmResponse = result.text();
            log.info("[PlanGeneration] Triage completed in {}ms", triageMs);
            log.debug("[PlanGeneration] LLM response: {}", llmResponse);

            // D-6: emit triage perf summary
            events.add(GraphEventPublisher.perfSummary("triage", Map.of(
                    "triage_ms", triageMs,
                    "prompt_tokens", result.promptTokens(),
                    "completion_tokens", result.completionTokens()
            )));

            TriageResult triage = converter.convert(llmResponse);
            boolean needsPlanning = triage != null && triage.needsPlanning();

            if (!needsPlanning) {
                // ---- 类别 (A) 直接回答：纯知识问答，无需工具 ----
                // 输出: NEEDS_PLANNING=false, DIRECT_ANSWER, CONTENT_STREAMED=true
                // 后续: PlanGenerationDispatcher(false) → DirectAnswerNode
                //       → DirectAnswerNode 搬运 DIRECT_ANSWER → FINAL_SUMMARY
                //       → (有 active goal?) → GoalEvaluationNode → END
                // 注意: broadcastContent 已将答案直推 SSE，FINAL_SUMMARY 由 executeStream
                //       标记 persistOnly 做 DB 持久化（防止 SSE 重复推送）
                String directAnswer = triage != null && triage.directAnswer() != null
                        ? triage.directAnswer() : llmResponse;
                log.info("[PlanGeneration] Direct-answer route taken (no tools, no planning)");

                streamingHelper.broadcastContent(conversationId, directAnswer);

                return PlanStateAccessor.output()
                        .needsPlanning(false)
                        .directAnswer(directAnswer)
                        .currentPhase("direct_answer")
                        .contentStreamed(true)       // ← executeStream 据此标记 persistOnly
                        .thinkingStreamed(!result.thinking().isEmpty())
                        .mergeUsage(state, result)
                        .events(events)
                        .build();
            }

            // ---- 类别 (B) 单步 or (C) 多步：提取步骤列表 ----
            // 输出: NEEDS_PLANNING=true, PLAN_ID, PLAN_STEPS, CURRENT_STEP_INDEX=0
            // 后续: PlanGenerationDispatcher(true) → StepExecutionNode（逐步执行）
            List<String> steps = triage != null ? triage.steps() : null;
            if (steps == null || steps.isEmpty()) {
                // LLM 判定 needsPlanning=true 但没给步骤 → 用用户目标兜底为单步计划
                // 之前的做法是降级为 direct_answer，但那样会丢失工具调用能力
                log.warn("[PlanGeneration] needs_planning=true with empty steps; falling back to single-step plan");
                steps = List.of(goal);
            }

            // ★ 持久化计划到 DB（mate_plan 表 + mate_plan_step 子表）
            //    planningService.createPlan 返回的 PlanEntity 包含自动生成的 id
            var plan = planningService.createPlan(agentId, goal, steps);
            log.info("[PlanGeneration] Plan created: id={}, steps={} ({})",
                    plan.getId(), steps.size(), steps.size() == 1 ? "single-step" : "multi-step");

            events.add(GraphEventPublisher.planCreated(plan.getId(), steps));

            return PlanStateAccessor.output()
                    .needsPlanning(true)
                    .planId(plan.getId())             // → StepExecutionNode 用此 id 更新 DB
                    .planSteps(steps)                  // → StepExecutionNode 遍历 steps
                    .planValid(true)
                    .currentStepIndex(0)               // → StepExecutionNode 从第0步开始
                    .currentPhase("plan_generated")    // → 前端状态栏展示
                    .contentStreamed(true)
                    .thinkingStreamed(!result.thinking().isEmpty())
                    .mergeUsage(state, result)
                    .events(events)
                    .build();

        } catch (Exception e) {
            log.error("[PlanGeneration] Triage failed, falling back to single-step plan: {}", e.getMessage(), e);
            // When the triage LLM fails or returns unparseable output we now fall back to
            // a single-step plan (the user's goal verbatim) instead of a direct text
            // answer. This preserves tool access on the failure path; the previous
            // "direct answer" fallback silently degraded tool-requiring tasks.
            try {
                var plan = planningService.createPlan(agentId, goal, List.of(goal));
                events.add(GraphEventPublisher.planCreated(plan.getId(), List.of(goal)));
                return PlanStateAccessor.output()
                        .needsPlanning(true)
                        .planId(plan.getId())
                        .planSteps(List.of(goal))
                        .planValid(true)
                        .currentStepIndex(0)
                        .currentPhase("plan_generated")
                        .events(events)
                        .build();
            } catch (Exception persistErr) {
                log.error("[PlanGeneration] Single-step fallback persistence also failed: {}", persistErr.getMessage());
                return PlanStateAccessor.output()
                        .needsPlanning(false)
                        .directAnswer("抱歉，我暂时无法完成任务分流，请重试或换一种方式描述任务。")
                        .currentPhase("direct_answer")
                        .events(events)
                        .build();
            }
        }
    }

}
