package vip.mate.agent.graph.plan;

import com.alibaba.cloud.ai.graph.CompiledGraph;
import com.alibaba.cloud.ai.graph.RunnableConfig;
import lombok.extern.slf4j.Slf4j;
import org.springframework.ai.chat.client.ChatClient;
import org.springframework.ai.chat.messages.Message;
import org.springframework.ai.chat.messages.UserMessage;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import vip.mate.agent.AgentService;
import vip.mate.agent.AgentState;
import vip.mate.agent.BaseAgent;
import vip.mate.agent.GraphEventPublisher;
import vip.mate.agent.StructuredStreamCapable;
import vip.mate.agent.graph.plan.state.PlanStateKeys;
import vip.mate.agent.graph.state.MateClawStateKeys;
import vip.mate.agent.context.ConversationWindowManager;
import vip.mate.planning.service.PlanningService;
import vip.mate.workspace.conversation.ConversationService;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * ============================================================
 * 【Plan-Execute 主执行体】— 基于 StateGraph 的先规划后执行 Agent
 * ============================================================
 *
 * 角色：Plan-Execute 模式的"驱动层"—— 负责构建初始状态、启动图执行、处理结果流。
 * 与 StateGraphReActAgent 同一层级，共同继承 BaseAgent 并实现 StructuredStreamCapable。
 *
 * 为什么需要这个类（而不在 AgentGraphBuilder 里直接调 compiledGraph.stream()）？
 * 1. 封装初始状态构建（buildInitialState）：加载对话历史、裁剪窗口、压缩 working context、
 *    注入 active goal 快照 —— 这些逻辑 ReAct 也有但细节不同，需要在各自的 Agent 实现中处理
 * 2. 封装流执行逻辑（executeStream）：处理 NodeOutput → 提取事件/步骤结果/汇总/去重 → StreamDelta
 * 3. 支持审批重放（chatWithReplayStream）：从 DB 恢复上下文，注入预批准工具调用，重新启动图
 * 4. 实现 AgentService 的统一接口（chat/chatStream/chatStructuredStream），供 Controller 层调用
 *
 * 完整调用链路（从 HTTP 请求到 SSE 响应）：
 *
 *   AgentController → AgentService.chatStructured(agentId, msg)
 *     → getOrCreateAgent() 从缓存获取 this(StateGraphPlanExecuteAgent)
 *     → this.chatStructuredStream(userMessage, conversationId, requesterId)
 *       → buildInitialState(userMessage, conversationId)
 *         → buildConversationHistory() 加载历史消息
 *         → conversationWindowManager.fitToWindow() 裁剪窗口
 *         → buildCurrentUserMessageWithRouting() 构建当前消息（含路由决策）
 *         → buildWorkingContext(history, List.of()) 压缩对话历史摘要
 *         → 注入 GOAL, SYSTEM_PROMPT, MESSAGES, WORKING_CONTEXT, ChatOrigin, active goal 快照...
 *       → executeStream(inputs)
 *         → routingStartupDelta() 前置"正在分析..."提示
 *         → compiledGraph.stream(inputs, config) 启动 StateGraph 流式执行
 *           → START → PlanGenerationNode(分流) → StepExecutionNode(执行)循环 → PlanSummaryNode(汇总) → END
 *         → 逐节点处理 NodeOutput:
 *            - 提取 PENDING_EVENTS → SSE 结构化事件（planCreated/stepStarted/phase/perfSummary...）
 *            - 提取 CURRENT_STEP_RESULT → persistOnly StreamDelta（已流式推送的不重复推）
 *            - 提取 FINAL_SUMMARY → StreamDelta（ChatController 收集后写入 mate_message 表）
 *            - 提取 token usage → _usage_final 事件
 *         → doOnComplete: setState(IDLE)
 *         → doOnError: setState(ERROR)
 *
 * ReAct vs Plan-Execute 对比：
 * ┌──────────────────────────────────────────────────────────────┐
 * │ Plan-Execute (本类)：先做一次 LLM 调用做任务分流（是否需要规划?）│
 * │                                                    │          │
 * │  ┌─ 不需要 → 直接回答出口                           │          │
 * │  └─ 需要   → 拆解为1-6步 → 逐步执行 → 汇总回答     │          │
 * └──────────────────────────────────────────────────────────────┘
 *
 * ┌──────────────────────────────────────────────────────────────┐
 * │ ReAct：进入"思考→行动→观察"循环，每一步都是一次完整推理      │
 * │    ReasoningNode → ActionNode → ObservationNode → (循环)     │
 * └──────────────────────────────────────────────────────────────┘
 *
 * @author MateClaw Team
 */
@Slf4j
public class StateGraphPlanExecuteAgent extends BaseAgent implements StructuredStreamCapable {

    private final CompiledGraph compiledGraph;
    private final PlanningService planningService;
    private final org.springframework.ai.chat.model.ChatModel chatModel;
    private final ConversationWindowManager conversationWindowManager;
    /** Held only so context-window budget includes the tools schema. Nullable for legacy constructor. */
    private final vip.mate.agent.AgentToolSet toolSet;

    public StateGraphPlanExecuteAgent(ChatClient chatClient, ConversationService conversationService,
                                      CompiledGraph compiledGraph, PlanningService planningService,
                                      org.springframework.ai.chat.model.ChatModel chatModel,
                                      ConversationWindowManager conversationWindowManager) {
        this(chatClient, conversationService, compiledGraph, planningService,
                chatModel, conversationWindowManager, null);
    }

    public StateGraphPlanExecuteAgent(ChatClient chatClient, ConversationService conversationService,
                                      CompiledGraph compiledGraph, PlanningService planningService,
                                      org.springframework.ai.chat.model.ChatModel chatModel,
                                      ConversationWindowManager conversationWindowManager,
                                      vip.mate.agent.AgentToolSet toolSet) {
        super(chatClient, conversationService);
        this.compiledGraph = compiledGraph;
        this.planningService = planningService;
        this.chatModel = chatModel;
        this.conversationWindowManager = conversationWindowManager;
        this.toolSet = toolSet;
    }

    @Override
    public Flux<AgentService.StreamDelta> chatStructuredStream(String userMessage, String conversationId) {
        return chatStructuredStream(userMessage, conversationId, "");
    }

    /**
     * 【主入口：新对话 / 新消息】Plan-Execute 流式对话。
     *
     * 被谁调用：
     *   AgentController → AgentService.chatStructured(agentId, msg, convId, requesterId)
     *     → 从 agentInstances 缓存中获取 this(StateGraphPlanExecuteAgent)
     *     → this.chatStructuredStream(msg, convId, requesterId)
     *
     * 内部调用链：
     *   1. setState(RUNNING) — 状态机切换到运行中
     *   2. buildInitialState(userMessage, conversationId) — 构建初始状态 Map
     *      → 内部调用 buildConversationHistory, fitToWindow, buildWorkingContext,
     *        buildCurrentUserMessageWithRouting, 注入 active goal 等
     *   3. 注入 REQUESTER_ID — 标识发起者（IM 渠道用户名 / Web 登录名）
     *   4. executeStream(inputs) — 驱动 compiledGraph 流式执行
     *      → 内部 routingStartupDelta → compiledGraph.stream → 逐节点提取 StreamDelta
     *   5. 异常时 setState(ERROR) + Flux.error
     *
     * 与 ReAct 的 chatStructuredStream 相比：
     *   多了 buildWorkingContext 步骤（对话历史压缩成摘要，避免 prompt 膨胀）
     *   没有 maxIterations / currentIteration（步骤循环由 StepProgressDispatcher 自驱动）
     */
    @Override
    public Flux<AgentService.StreamDelta> chatStructuredStream(String userMessage, String conversationId,
                                                                String requesterId) {
        setState(AgentState.RUNNING);
        try {
            log.info("[{}] Plan-Execute structured stream: conversationId={}", agentName, conversationId);
            Map<String, Object> inputs = buildInitialState(userMessage, conversationId);
            inputs.put(MateClawStateKeys.REQUESTER_ID, requesterId != null ? requesterId : "");
            return executeStream(inputs);
        } catch (Exception e) {
            setState(AgentState.ERROR);
            return Flux.error(e);
        }
    }

    /**
     * 【审批重放入口】工具调用被 ToolGuard 拦截 → 用户在前端确认 → 通过此方法重新执行。
     *
     * 被谁调用：
     *   AgentController（审批确认 API）→ AgentService.chatWithReplay(agentId, msg, convId, toolCallPayload)
     *     → 从缓存获取 this → this.chatWithReplayStream(msg, convId, payload)
     *
     * 与 chatStructuredStream 的关键区别：
     * 1. 从 DB 恢复 awaiting_approval 状态（planningService.findAwaitingApprovalContext()）:
     *    → 取出 planId, steps, awaitingStepIndex, completedResults
     * 2. 重建 working context（历史消息 + 已完成步骤摘要）
     * 3. 注入 PRE_APPROVED_TOOL_CALL → StepExecutionNode 匹配工具名后跳过 ToolGuard 直接调用 executePreApproved()
     *
     * 后续调用链（注入 state 后进入图）：
     *   → executeStream → compiledGraph.stream → START → PlanGenerationNode
     *     → 检测到 PLAN_ID 已存在 → 跳过 LLM 分流，直接返回 needsPlanning(true) + 复用已有计划
     *     → PlanGenerationDispatcher → StepExecutionNode
     *     → 检测到 PRE_APPROVED_TOOL_CALL → 匹配工具名 → 调用 executePreApproved → 继续执行
     *
     * ★ 关键设计：currentStepIndex 不递增 — 从暂停的同一步继续执行，不跳步
     */
    @Override
    public Flux<AgentService.StreamDelta> chatWithReplayStream(String userMessage, String conversationId,
                                                                String toolCallPayload) {
        setState(AgentState.RUNNING);
        try {
            log.info("[{}] Plan-Execute replay stream: conversationId={}", agentName, conversationId);
            Map<String, Object> inputs = buildInitialState(userMessage, conversationId);

            // 从 DB 恢复 awaiting_approval 状态的计划上下文
            PlanningService.PlanResumeContext ctx = planningService.findAwaitingApprovalContext();
            if (ctx != null) {
                inputs.put(PlanStateKeys.PLAN_ID, ctx.planId());
                inputs.put(PlanStateKeys.PLAN_STEPS, ctx.steps());
                inputs.put(PlanStateKeys.NEEDS_PLANNING, true);
                inputs.put(PlanStateKeys.PLAN_VALID, true);
                inputs.put(PlanStateKeys.CURRENT_STEP_INDEX, ctx.awaitingStepIndex());
                if (!ctx.completedResults().isEmpty()) {
                    inputs.put(PlanStateKeys.COMPLETED_RESULTS, ctx.completedResults());
                    // 重建 working context，包含历史消息和已完成步骤结果
                    @SuppressWarnings("unchecked")
                    List<Message> messages = (List<Message>) inputs.get(MateClawStateKeys.MESSAGES);
                    // messages 中最后一条是当前 UserMessage，去掉再算历史
                    List<Message> history = messages.size() > 1
                            ? messages.subList(0, messages.size() - 1) : List.of();
                    inputs.put(PlanStateKeys.WORKING_CONTEXT,
                            buildWorkingContext(history, ctx.completedResults()));
                }
                log.info("[{}] Replay: restored plan {} at step {}/{}", agentName,
                        ctx.planId(), ctx.awaitingStepIndex(), ctx.steps().size());
            } else {
                log.warn("[{}] Replay: no awaiting-approval plan found, falling back to fresh run", agentName);
            }

            // 注入预批准的工具调用，StepExecutionNode 匹配后跳过 ToolGuard
            if (toolCallPayload != null && !toolCallPayload.isEmpty()) {
                inputs.put(MateClawStateKeys.PRE_APPROVED_TOOL_CALL, toolCallPayload);
            }

            return executeStream(inputs);
        } catch (Exception e) {
            setState(AgentState.ERROR);
            return Flux.error(e);
        }
    }

    /**
     * 【核心执行引擎】被 chatStructuredStream 和 chatWithReplayStream 共用。
     *
     * 执行过程（时间线）：
     * 1. 生成独立 threadId（UUID）→ 确保图状态不跨请求泄漏（每次对话独立开辟 StateGraph session）
     * 2. 前置 routingStartupDelta（"正在分析..."）→ 前端在分流阶段（1-3秒静默）有进度展示
     * 3. compiledGraph.stream(inputs, config) → 启动 StateGraph 流式执行
     *    → START → [PlanGenerationNode] → (dispatch) → [StepExecutionNode] ↩ → [PlanSummaryNode] → END
     *    → 每个节点执行完后触发 NodeOutput 回调（flatMapIterable 中的 lambda）
     * 4. 逐节点处理 NodeOutput → 产出 StreamDelta Flux：
     *    a. 提取 PENDING_EVENTS → 增量发送结构化事件（只发新增部分）
     *    b. CURRENT_STEP_RESULT / CURRENT_STEP_THINKING → persistOnly（已流式推送过，只持久化不重复推）
     *    c. FINAL_SUMMARY / FINAL_SUMMARY_THINKING → persistOnly or new StreamDelta（取决于是否已流式推送）
     *    d. 累加 token usage（PROMPT_TOKENS / COMPLETION_TOKENS / RUNTIME_MODEL_NAME）
     * 5. .concatWith(_usage_final 事件) → 流结束后的最终 token 统计事件
     * 6. .doOnComplete: setState(IDLE) | .doOnError: setState(ERROR)
     *
     * 去重机制（Plan-Execute 特有）：
     * - StepExecutionNode 的 while 循环中每次 LLM 调用都被 NodeStreamingChatHelper.streamCall()
     *   实时推送到 SSE（属于"已流式推送"内容），但 NodeOutput 回调中可能同样 emit 这些结果
     * - lastPersistedStepResult / lastPersistedStepThinking：内容级去重
     *   因为 PlanSummaryNode 输出时 state 中可能残留上一步的值被重复提取
     *
     * 与 ReAct 的 executeStream 对比：
     * - ReAct 只提取 FINAL_ANSWER，Plan-Execute 多了 CURRENT_STEP_RESULT / CURRENT_STEP_THINKING
     *   和 FINAL_SUMMARY_THINKING 的提取逻辑
     * - ReAct 没有 persistOnly 概念（所有内容都通过 StreamDelta 下发），
     *   Plan-Execute 的步骤内容由 StepExecutionNode 直推 SSE，此处只做持久化
     */
    private Flux<AgentService.StreamDelta> executeStream(Map<String, Object> inputs) {
        String threadId = UUID.randomUUID().toString();
        RunnableConfig config = RunnableConfig.builder().threadId(threadId).build();

        AtomicInteger sentEventCount = new AtomicInteger(0);
        AtomicInteger finalPromptTokens = new AtomicInteger(0);
        AtomicInteger finalCompletionTokens = new AtomicInteger(0);
        AtomicReference<String> finalModelName = new AtomicReference<>("");
        AtomicReference<String> finalProviderId = new AtomicReference<>("");
        // 去重：记录上一次已持久化的 step 结果和 thinking，防止 PlanSummaryNode 重复 emit 上一步内容
        AtomicReference<String> lastPersistedStepResult = new AtomicReference<>("");
        AtomicReference<String> lastPersistedStepThinking = new AtomicReference<>("");

        return BaseAgent.routingStartupDelta(inputs).concatWith(compiledGraph.stream(inputs, config)
                .flatMapIterable(output -> {
                    List<AgentService.StreamDelta> deltas = new ArrayList<>();
                    // 1. 提取事件（只发送新增部分）
                    List<GraphEventPublisher.GraphEvent> allEvents = GraphEventPublisher.extractEvents(output);
                    int newStart = sentEventCount.get();
                    if (newStart < allEvents.size()) {
                        for (int i = newStart; i < allEvents.size(); i++) {
                            var event = allEvents.get(i);
                            deltas.add(AgentService.StreamDelta.event(event.type(), event.data()));
                        }
                        sentEventCount.set(allEvents.size());
                    }

                    // 2. 内容始终通过 StreamDelta 返回（用于持久化），已广播过的标记 persistOnly 避免重复推送
                    boolean contentAlreadyStreamed = output.state()
                            .value(MateClawStateKeys.CONTENT_STREAMED, false);
                    boolean thinkingAlreadyStreamed = output.state()
                            .value(MateClawStateKeys.THINKING_STREAMED, false);

                    // 2a. 各步骤执行结果（StepExecutionNode 已通过 NodeStreamingChatHelper 直推 SSE，
                    //     这里仅作为 persistOnly 送入 Accumulator，确保写入 mate_message）
                    //     利用内容本身去重，避免 PlanSummaryNode 输出时重复 emit 上一步残留在 state 的值
                    output.state().<String>value(PlanStateKeys.CURRENT_STEP_RESULT)
                            .filter(s -> !s.isEmpty())
                            .filter(s -> !s.equals(lastPersistedStepResult.get()))
                            .ifPresent(stepContent -> {
                                deltas.add(AgentService.StreamDelta.persistOnly(stepContent, null));
                                lastPersistedStepResult.set(stepContent);
                            });

                    output.state().<String>value(PlanStateKeys.CURRENT_STEP_THINKING)
                            .filter(s -> !s.isEmpty())
                            .filter(s -> !s.equals(lastPersistedStepThinking.get()))
                            .ifPresent(stepThinking -> {
                                deltas.add(AgentService.StreamDelta.persistOnly(null, stepThinking));
                                lastPersistedStepThinking.set(stepThinking);
                            });

                    // 2b. 最终汇总
                    output.state().<String>value(PlanStateKeys.FINAL_SUMMARY)
                            .filter(s -> !s.isEmpty())
                            .ifPresent(summary -> deltas.add(contentAlreadyStreamed
                                    ? AgentService.StreamDelta.persistOnly(summary, null)
                                    : new AgentService.StreamDelta(summary, null)));

                    output.state().<String>value(PlanStateKeys.FINAL_SUMMARY_THINKING)
                            .filter(s -> !s.isEmpty())
                            .ifPresent(thinking -> deltas.add(thinkingAlreadyStreamed
                                    ? AgentService.StreamDelta.persistOnly(null, thinking)
                                    : new AgentService.StreamDelta(null, thinking)));

                    // 3. 更新最新累计 token usage
                    finalPromptTokens.set(output.state().value(MateClawStateKeys.PROMPT_TOKENS, 0));
                    finalCompletionTokens.set(output.state().value(MateClawStateKeys.COMPLETION_TOKENS, 0));
                    finalModelName.set(output.state().value(MateClawStateKeys.RUNTIME_MODEL_NAME, ""));
                    finalProviderId.set(output.state().value(MateClawStateKeys.RUNTIME_PROVIDER_ID, ""));

                    return deltas;
                })
                .concatWith(Mono.fromSupplier(() -> {
                    if (finalPromptTokens.get() > 0 || finalCompletionTokens.get() > 0) {
                        return AgentService.StreamDelta.event("_usage_final", Map.of(
                                "promptTokens", finalPromptTokens.get(),
                                "completionTokens", finalCompletionTokens.get(),
                                "runtimeModelName", finalModelName.get(),
                                "runtimeProviderId", finalProviderId.get()
                        ));
                    }
                    return null;
                }).flatMapMany(d -> d != null ? Flux.just(d) : Flux.empty())))
                .doOnComplete(() -> setState(AgentState.IDLE))
                .doOnError(e -> {
                    log.error("[{}] Plan-Execute stream error: {}", agentName, e.getMessage());
                    setState(AgentState.ERROR);
                });
    }

    @Override
    public String chat(String userMessage, String conversationId) {
        // 委托到 chatStructuredStream，过滤事件，拼接内容
        return chatStructuredStream(userMessage, conversationId)
                .filter(delta -> !delta.isEvent() && delta.content() != null)
                .map(AgentService.StreamDelta::content)
                .collectList()
                .map(chunks -> String.join("", chunks))
                .block();
    }

    @Override
    public Flux<String> chatStream(String userMessage, String conversationId) {
        // 委托到 chatStructuredStream，过滤事件，只保留内容
        return chatStructuredStream(userMessage, conversationId)
                .filter(delta -> !delta.isEvent() && delta.content() != null)
                .map(AgentService.StreamDelta::content);
    }

    @Override
    public String execute(String goal, String conversationId) {
        // 同 chat()，走同一套 Plan-Execute Graph
        return chat(goal, conversationId);
    }

    /**
     * 【初始状态构建】在图执行前将 Agent 配置、历史、约束打包成 StateGraph 输入 Map。
     *
     * 这个方法产出的 Map 就是图的"初始全局状态"，引擎会将此 Map 作为 OverAllState 传给
     * 第一个节点（PlanGenerationNode）。
     *
     * 写入哪些键（"谁消费"列说明这些值最终被哪个节点/方法读取）：
     *
     *   GOAL = userMessage                        → PlanGenerationNode 分流判断 + StepExecutionNode 展示总目标
     *   SYSTEM_PROMPT                             → PlanGenerationNode / StepExecutionNode 构建系统消息
     *   CONVERSATION_ID                           → streamingHelper 的 SSE 广播路径
     *   AGENT_ID                                  → 日志关联
     *   MESSAGES（对话历史 + 当前UserMessage）     → StepExecutionNode 的 rebuildWorkingContext 做压缩
     *   WORKING_CONTEXT（buildWorkingContext 产出）→ PlanGenerationNode / StepExecutionNode 避免反复读完整历史
     *   CURRENT_STEP_INDEX = 0                    → StepExecutionNode 从第 0 步开始
     *   CONTENT_STREAMED / THINKING_STREAMED = false → executeStream 的去重标记
     *   PROMPT_TOKENS / COMPLETION_TOKENS = 0     → mergeUsage 累加的起始值
     *   RUNTIME_MODEL_NAME / RUNTIME_PROVIDER_ID  → _usage_final 事件上报
     *   TRACE_ID（UUID 前 8 位）                  → 日志链路追踪
     *   ROUTING_DECISION（多模态路由结果）          → PlanGenerationNode 考虑路由偏好
     *   CHAT_ORIGIN                               → DelegateAgentTool 子图继承渠道信息
     *   ACTIVE_GOAL                               → GoalEvaluationNode 目标进度评估
     *   GOAL_EVALUATED_THIS_RUN = false           → 避免同一图执行重复评估
     *   GOAL_FOLLOWUP_PROMPT = ""                 → PlanGenerationNode 跟进提示注入
     *
     * 与 ReAct 的 buildInitialState 对比：
     *   相同：对话历史加载、窗口裁剪、当前消息构建、active goal 注入
     *   不同：没有 maxIterations/currentIteration（步骤循环由 StepProgressDispatcher 自驱动）、
     *        多了 GOAL + WORKING_CONTEXT + 预初始 Plan-Execute 键、多了 buildWorkingContext 调用
     */
    private Map<String, Object> buildInitialState(String userMessage, String conversationId) {
        // 加载会话历史（复用 BaseAgent.buildConversationHistory，与 ReAct 对齐）
        List<Message> historyMessages = buildConversationHistory(conversationId, userMessage);

        // 上下文窗口管理：裁剪超出模型 context window 的历史（含当前消息预算）
        if (conversationWindowManager != null) {
            Long parsedAgentId = null;
            try { parsedAgentId = Long.valueOf(agentId); } catch (Exception ignored) {}
            historyMessages = conversationWindowManager.fitToWindow(
                    historyMessages,
                    systemPrompt != null ? systemPrompt : "",
                    userMessage,
                    maxInputTokens,
                    chatModel,
                    conversationId,
                    parsedAgentId,
                    toolSet != null ? toolSet.callbacks() : null,
                    workspaceBasePath);
        }

        List<Message> messages = new ArrayList<>(historyMessages);
        BaseAgent.CurrentTurnUserMessage currentTurn = buildCurrentUserMessageWithRouting(conversationId, userMessage);
        messages.add(currentTurn.userMessage());

        // 构建 working context：对历史消息做受控长度摘要
        String workingContext = buildWorkingContext(historyMessages, List.of());

        Map<String, Object> inputs = new HashMap<>();
        inputs.put(PlanStateKeys.GOAL, userMessage);
        inputs.put(MateClawStateKeys.SYSTEM_PROMPT,
                systemPrompt != null ? systemPrompt : "你是一个有帮助的AI助手。");
        inputs.put(MateClawStateKeys.CONVERSATION_ID, conversationId);
        inputs.put(MateClawStateKeys.AGENT_ID, agentId != null ? agentId : "");
        inputs.put(MateClawStateKeys.WORKSPACE_BASE_PATH, workspaceBasePath != null ? workspaceBasePath : "");
        // 注入会话消息（复用 MateClawStateKeys.MESSAGES，与 ReAct 一致）
        inputs.put(MateClawStateKeys.MESSAGES, messages);
        // 注入 working context
        inputs.put(PlanStateKeys.WORKING_CONTEXT, workingContext);
        inputs.put(PlanStateKeys.CURRENT_STEP_INDEX, 0);
        inputs.put(MateClawStateKeys.CONTENT_STREAMED, false);
        inputs.put(MateClawStateKeys.THINKING_STREAMED, false);
        inputs.put(MateClawStateKeys.STREAMED_CONTENT, "");
        inputs.put(MateClawStateKeys.STREAMED_THINKING, "");
        inputs.put(MateClawStateKeys.REQUESTER_ID, "");
        inputs.put(MateClawStateKeys.PROMPT_TOKENS, 0);
        inputs.put(MateClawStateKeys.COMPLETION_TOKENS, 0);
        inputs.put(MateClawStateKeys.RUNTIME_MODEL_NAME, modelName != null ? modelName : "");
        inputs.put(MateClawStateKeys.RUNTIME_PROVIDER_ID, runtimeProviderId != null ? runtimeProviderId : "");
        inputs.put(MateClawStateKeys.TRACE_ID, UUID.randomUUID().toString().substring(0, 8));

        if (currentTurn.routingDecision() != null
                && (currentTurn.routingDecision().strategy() != vip.mate.llm.routing.model.MultimodalRoutingDecision.Strategy.NONE
                        || !currentTurn.routingDecision().skipped().isEmpty())) {
            inputs.put(MateClawStateKeys.ROUTING_DECISION, currentTurn.routingDecision().toMap());
        }

        // RFC-063r §2.5: same as ReAct path — enrich and store the ChatOrigin
        // so StepExecutionNode (and any sub-graphs spawned via DelegateAgentTool)
        // can read it back from state.
        vip.mate.agent.context.ChatOrigin origin = vip.mate.agent.context.ChatOriginHolder.get();
        Long parsedAgentIdForOrigin = null;
        try { parsedAgentIdForOrigin = agentId != null ? Long.valueOf(agentId) : null; } catch (Exception ignored) {}
        if (parsedAgentIdForOrigin != null) {
            origin = origin.withAgent(parsedAgentIdForOrigin);
        }
        origin = origin.withConversationId(conversationId)
                .withWorkspace(origin.workspaceId(), workspaceBasePath);
        inputs.put(MateClawStateKeys.CHAT_ORIGIN, origin);

        // RFC 48 — inject active goal snapshot for GoalEvaluationNode.
        // Mirrors StateGraphReActAgent.buildInitialState exactly.
        if (goalService != null && conversationId != null && !conversationId.isBlank()) {
            try {
                vip.mate.goal.model.GoalEntity active =
                        goalService.findActiveByConversation(conversationId);
                if (active != null) {
                    inputs.put(MateClawStateKeys.ACTIVE_GOAL, active);
                }
            } catch (Exception e) {
                log.warn("[{}] findActiveByConversation failed: {}", agentName, e.getMessage());
            }
        }
        inputs.put(MateClawStateKeys.GOAL_EVALUATED_THIS_RUN, false);
        inputs.put(MateClawStateKeys.GOAL_FOLLOWUP_INJECTED, false);
        inputs.put(MateClawStateKeys.GOAL_FOLLOWUP_PROMPT, "");

        return inputs;
    }

    /**
     * 【上下文压缩】将对话历史 + 已完成步骤结果压缩为受控长度（≤6000 字符）的摘要。
     *
     * 被谁调用：
     *   - buildInitialState()：图执行前压缩对话历史（步骤结果为空 List）
     *   - chatWithReplayStream()：审批重放时重建 working context（含已完成步骤结果）
     *
     * 为什么需要这个机制（而不直接把 MESSAGES 传给每个节点的 Prompt）？
     *   多步执行中，每步的 Prompt 都包含"完整对话历史 + 所有已完成步骤结果"会导致：
     *   - 第 1 步：Prompt 大小 = 对话历史 + 1 步结果 → 还行
     *   - 第 4 步：Prompt 大小 = 对话历史 + 4 步结果 → 开始变大
     *   - 第 6 步：Prompt 大小 = 对话历史 + 6 步结果 → 可能超过模型 context window
     *   通过 WORKING_CONTEXT 压缩传递，每步的 Prompt 只追加 ≤6000 字符的摘要，
     *   不随步骤数线性膨胀。
     *
     * 压缩规则：
     *   - 历史消息：保留最近 10 条，每条截断至 500 字符
     *   - 步骤结果：保留最近 5 条，每条截断至 800 字符
     *   - 总体截断至 6000 字符
     *
     * 与 ReAct 的 SummarizingNode 对比：
     *   - ReAct：SummarizingNode 将压缩后的摘要追加到 MESSAGES，和原始消息混在一起
     *   - Plan-Execute：WORKING_CONTEXT 独立一个键，不修改 MESSAGES，职责分离更清晰
     */
    static String buildWorkingContext(List<Message> historyMessages, List<String> completedResults) {
        StringBuilder sb = new StringBuilder();

        // 历史消息摘要
        if (historyMessages != null && !historyMessages.isEmpty()) {
            sb.append("=== 对话历史摘要 ===\n");
            int startIdx = Math.max(0, historyMessages.size() - MAX_HISTORY_MESSAGES);
            for (int i = startIdx; i < historyMessages.size(); i++) {
                Message msg = historyMessages.get(i);
                String role = msg.getMessageType().name().toLowerCase();
                String content = msg.getText();
                if (content != null && !content.isEmpty()) {
                    String truncated = content.length() > MAX_MSG_CHARS
                            ? content.substring(0, MAX_MSG_CHARS) + "…" : content;
                    sb.append("[").append(role).append("] ").append(truncated).append("\n");
                }
            }
            sb.append("\n");
        }

        // 已完成步骤结果摘要
        if (completedResults != null && !completedResults.isEmpty()) {
            sb.append("=== 已完成步骤结果 ===\n");
            int startIdx = Math.max(0, completedResults.size() - MAX_STEP_RESULTS);
            for (int i = startIdx; i < completedResults.size(); i++) {
                String result = completedResults.get(i);
                String truncated = result.length() > MAX_STEP_CHARS
                        ? result.substring(0, MAX_STEP_CHARS) + "…" : result;
                sb.append(truncated).append("\n");
            }
        }

        // 总体截断
        String context = sb.toString();
        if (context.length() > MAX_CONTEXT_CHARS) {
            context = context.substring(0, MAX_CONTEXT_CHARS) + "\n…（上下文已截断）";
        }
        return context;
    }

    // Working context 长度控制参数
    private static final int MAX_HISTORY_MESSAGES = 10;
    private static final int MAX_MSG_CHARS = 500;
    private static final int MAX_STEP_RESULTS = 5;
    private static final int MAX_STEP_CHARS = 800;
    private static final int MAX_CONTEXT_CHARS = 6000;
}
