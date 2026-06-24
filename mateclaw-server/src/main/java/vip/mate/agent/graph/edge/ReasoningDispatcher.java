package vip.mate.agent.graph.edge;

import com.alibaba.cloud.ai.graph.OverAllState;
import com.alibaba.cloud.ai.graph.action.EdgeAction;
import lombok.extern.slf4j.Slf4j;
import vip.mate.agent.graph.state.MateClawStateAccessor;

import static vip.mate.agent.graph.state.MateClawStateKeys.*;

/**
 * ============================================================
 * 【ReAct 路由1】ReasoningDispatcher — 推理节点后的分支决策
 * ============================================================
 * 在 ReasoningNode 完成 LLM 调用后，根据结果决定下一步走向。
 *
 * 分支优先级（按顺序检查，命中即返回）：
 * 1. 迭代超限？         → LimitExceededNode（强制终止循环）
 * 2. 可直接回答？       → FinalAnswerNode（LLM 直接给出了答案）
 * 3. LLM 调用次数超限？→ LimitExceededNode（安全兜底）
 * 4. 需要工具调用？     → ActionNode（进入工具执行阶段）
 * 5. 需要总结压缩？     → SummarizingNode（上下文过长，先压缩再继续）
 * 6. 兜底              → FinalAnswerNode
 *
 * 这是整个 ReAct 循环中最重要的路由决策点。
 *
 * @author MateClaw Team
 */
@Slf4j
public class ReasoningDispatcher implements EdgeAction {

    /** LLM 调用次数的安全倍数上限（相对于 maxIterations） */
    private static final int LLM_CALL_MULTIPLIER = 5;

    @Override
    public String apply(OverAllState state) throws Exception {
        MateClawStateAccessor accessor = new MateClawStateAccessor(state);

        // 1. 迭代超限检查
        if (accessor.isLimitReached()) {
            log.warn("[ReasoningDispatcher] Iteration limit reached ({}/{}), routing to limitExceededNode",
                    accessor.iterationCount(), accessor.maxIterations());
            return LIMIT_EXCEEDED_NODE;
        }

        // 2. 可直接回答 → finalAnswerNode
        //    覆盖以下场景：
        //    - LLM 正常产出最终回答 (needsToolCall=false, finalAnswer 非空)
        //    - fatal error (ReasoningNode 设置了 finalAnswer=错误文案 + finishReason=ERROR_FALLBACK)
        //    - 用户停止无内容 (finalAnswer="" + finishReason=STOPPED)
        //    不受 llm_call_count 限制 — 已有结果不应丢弃。
        if (!accessor.needsToolCall() && !accessor.shouldSummarize()) {
            log.debug("[ReasoningDispatcher] Routing to finalAnswerNode (direct answer)");
            return FINAL_ANSWER_NODE;
        }

        // 3. LLM 调用次数超限 — 仅拦截继续循环（工具调用/总结）的路径（maxIterations=0 不限制）
        int llmCallCount = accessor.llmCallCount();
        int maxIter = accessor.maxIterations();
        int llmCallLimit = maxIter > 0 ? maxIter * LLM_CALL_MULTIPLIER : Integer.MAX_VALUE;
        if (llmCallCount >= llmCallLimit) {
            log.warn("[ReasoningDispatcher] LLM call count limit reached ({}/{}), " +
                            "routing to limitExceededNode instead of continuing loop",
                    llmCallCount, llmCallLimit);
            return LIMIT_EXCEEDED_NODE;
        }

        // 4. 工具调用
        if (accessor.needsToolCall()) {
            log.debug("[ReasoningDispatcher] Routing to actionNode (tool call needed)");
            return ACTION_NODE;
        }

        // 5. 需要总结
        if (accessor.shouldSummarize()) {
            log.info("[ReasoningDispatcher] Routing to summarizingNode (observation context too large)");
            return SUMMARIZING_NODE;
        }

        // 6. 兜底
        log.debug("[ReasoningDispatcher] Routing to finalAnswerNode (fallback)");
        return FINAL_ANSWER_NODE;
    }
}
