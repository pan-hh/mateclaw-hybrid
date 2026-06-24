package vip.mate.agent.graph.plan.node;

import com.alibaba.cloud.ai.graph.OverAllState;
import com.alibaba.cloud.ai.graph.action.NodeAction;
import lombok.extern.slf4j.Slf4j;
import org.springframework.ai.chat.messages.SystemMessage;
import org.springframework.ai.chat.messages.UserMessage;
import org.springframework.ai.chat.model.ChatModel;
import org.springframework.ai.chat.prompt.Prompt;
import vip.mate.agent.graph.NodeStreamingChatHelper;
import vip.mate.agent.graph.plan.state.PlanStateAccessor;
import vip.mate.agent.graph.plan.state.PlanStateKeys;
import vip.mate.agent.graph.state.MateClawStateKeys;
import vip.mate.planning.service.PlanningService;

import java.util.List;
import java.util.Map;

/**
 * ============================================================
 * 【Plan-Execute 第4阶段：Summarize】计划汇总节点 — 生成最终回答
 * ============================================================
 * 角色：所有步骤执行完毕后，由本节点汇总步骤结果，调用 LLM 生成最终的用户回答。
 *
 * 工作流程：
 * 1. 从 state 读取 GOAL（用户目标）+ COMPLETED_RESULTS（各步结果）+ WORKING_CONTEXT
 * 2. 组装总结 Prompt：原始目标 + 对话上下文 + 各步骤执行结果
 * 3. 调 LLM 生成简洁完整的最终回答（而不是逐步骤罗列）
 * 4. 调 planningService.completePlan() 标记计划完成
 *
 * 降级策略：
 * LLM 调用失败 → buildFallbackSummary() 本地生成降级摘要（直接拼接步骤原始结果）
 *
 * 与 ReAct 的 FinalAnswerNode 对比：
 *  - FinalAnswerNode 处理的是单轮推理的最终答案（含证据校验、虚假链接清洗）
 *  - PlanSummaryNode 处理的是多步汇总，更注重"不要罗列步骤，给一个完整回答"
 *  - FinalAnswerNode 有证据校验功能，PlanSummaryNode 没有（步骤结果本身是"证据"）
 *
 * @author MateClaw Team
 */
@Slf4j
public class PlanSummaryNode implements NodeAction {

    private final ChatModel chatModel;
    private final PlanningService planningService;
    private final NodeStreamingChatHelper streamingHelper;

    public PlanSummaryNode(ChatModel chatModel, PlanningService planningService,
                           NodeStreamingChatHelper streamingHelper) {
        this.chatModel = chatModel;
        this.planningService = planningService;
        this.streamingHelper = streamingHelper;
    }

    /**
     * @deprecated Use constructor with NodeStreamingChatHelper
     */
    @Deprecated
    public PlanSummaryNode(ChatModel chatModel, PlanningService planningService) {
        this(chatModel, planningService, null);
    }

    /**
     * ★ Plan-Execute 最后一站 — 汇总步骤结果生成最终回答。
     * 输入: GOAL + COMPLETED_RESULTS(累积,APPEND) + WORKING_CONTEXT
     * 输出: FINAL_SUMMARY → executeStream → ChatController → mate_message
     * 降级: LLM失败 → buildFallbackSummary() 本地拼接
     */
    @Override
    public Map<String, Object> apply(OverAllState state) throws Exception {
        PlanStateAccessor accessor = new PlanStateAccessor(state);
        Long planId = accessor.planId();
        String goal = accessor.goal();
        List<String> completedResults = accessor.completedResults();
        String conversationId = accessor.conversationId();
        String workingContext = accessor.workingContext();

        log.info("[PlanSummary] Summarizing plan {}: {} completed results", planId, completedResults.size());

        try {
            // 构建汇总 prompt：结合 working context 和步骤结果
            StringBuilder userContent = new StringBuilder();
            userContent.append("原始目标：").append(goal).append("\n\n");

            // 注入 working context（包含对话历史摘要），让汇总感知用户此前提过的要求
            if (workingContext != null && !workingContext.isEmpty()) {
                userContent.append("对话上下文：\n").append(workingContext).append("\n\n");
            }

            userContent.append("执行结果：\n").append(String.join("\n", completedResults));

            Prompt prompt = new Prompt(List.of(
                    new SystemMessage("请根据以下各步骤的执行结果，给出一个简洁完整的总结回答。"
                            + "直接回答用户的原始问题，不要罗列步骤。"
                            + "如果对话上下文中包含用户的特殊要求（如风格、语言、格式等），请在总结中体现。"),
                    new UserMessage(userContent.toString())
            ));

            // 流式调用 LLM，实时推送 content/thinking
            NodeStreamingChatHelper.StreamResult result = streamingHelper.streamCall(
                    chatModel, prompt, conversationId, "plan_summary");

            String summary = result.text();
            planningService.completePlan(planId, summary);
            log.info("[PlanSummary] Plan {} completed with summary: {}",
                    planId, summary.length() > 100 ? summary.substring(0, 100) + "..." : summary);

            return PlanStateAccessor.output()
                    .finalSummary(summary)
                    .finalSummaryThinking(result.thinking())
                    .contentStreamed(true)
                    .thinkingStreamed(!result.thinking().isEmpty())
                    .mergeUsage(state, result)
                    .build();

        } catch (Exception e) {
            log.error("[PlanSummary] Failed to summarize plan {}: {}", planId, e.getMessage(), e);
            String fallbackSummary = buildFallbackSummary(goal, completedResults);
            planningService.markPlanFailed(planId, "汇总阶段失败：" + truncate(e.getMessage(), 100));
            return Map.of(PlanStateKeys.FINAL_SUMMARY, fallbackSummary);
        }
    }

    /**
     * 在 LLM 汇总调用失败时生成本地 fallback 摘要。
     * 每条步骤结果截断至 300 字，避免把过长内容（包括错误体）直接暴露给用户。
     */
    private static String buildFallbackSummary(String goal, List<String> completedResults) {
        StringBuilder sb = new StringBuilder("目标：").append(goal).append("\n\n执行摘要（LLM 汇总失败，以下为步骤原始结果）：\n");
        for (String r : completedResults) {
            sb.append(truncate(r, 300)).append("\n");
        }
        return sb.toString();
    }

    private static String truncate(String s, int maxLen) {
        if (s == null) return "";
        return s.length() > maxLen ? s.substring(0, maxLen) + "…" : s;
    }
}
