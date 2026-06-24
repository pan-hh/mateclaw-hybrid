package vip.mate.agent.graph.edge;

import com.alibaba.cloud.ai.graph.OverAllState;
import com.alibaba.cloud.ai.graph.action.EdgeAction;
import lombok.extern.slf4j.Slf4j;
import vip.mate.agent.graph.state.MateClawStateAccessor;

import static vip.mate.agent.graph.state.MateClawStateKeys.*;

/**
 * ============================================================
 * 【ReAct 路由2】ObservationDispatcher — 迭代控制的核心执行点
 * ============================================================
 * 在 ObservationNode 处理完工具结果后，决定下一步走向。
 *
 * 分支优先级：
 * 0. 审批等待？         → FinalAnswerNode（图暂停，等待用户审批后重放）
 * 0b. returnDirect？    → FinalAnswerNode（跳过后续 LLM 调用，直接用工具结果回答）
 * 1. 迭代超限？         → LimitExceededNode（currentIteration >= maxIterations → 强制终止）
 * 2. 有错误？           → LimitExceededNode
 * 3. 需要总结？         → SummarizingNode（观察历史太长需要压缩）
 * 4. 否则               → ReasoningNode（继续下一轮 ReAct 循环）
 *
 * 这是 maxIterations 的核心执行点 — 每次观察后都检查迭代上限，
 * 确保 Agent 不会无限循环。
 *
 * @author MateClaw Team
 */
@Slf4j
public class ObservationDispatcher implements EdgeAction {

    @Override
    public String apply(OverAllState state) throws Exception {
        MateClawStateAccessor accessor = new MateClawStateAccessor(state);

        int currentIteration = accessor.iterationCount();
        int maxIterations = accessor.maxIterations();

        // 0. 审批等待检查 — Graph 必须立即终止，由 Replay 继续
        if (accessor.awaitingApproval()) {
            log.info("[ObservationDispatcher] AWAITING_APPROVAL=true, terminating graph " +
                    "(replay will continue after user decision), iteration {}/{}", currentIteration, maxIterations);
            return FINAL_ANSWER_NODE;
        }

        // RFC-052: returnDirect short-circuit — highest priority after approval.
        // Any tool in the latest batch declared returnDirect=true: skip the next
        // LLM call entirely and route straight to FinalAnswerNode, which will
        // assemble the final answer from DIRECT_TOOL_OUTPUTS.
        if (accessor.returnDirectTriggered()) {
            log.info("[ObservationDispatcher] RETURN_DIRECT_TRIGGERED=true, " +
                    "routing to finalAnswerNode (skipping next LLM call), iteration {}/{}",
                    currentIteration, maxIterations);
            return FINAL_ANSWER_NODE;
        }

        // 1. 迭代超限检查（maxIterations=0 表示不限制）
        if (maxIterations > 0 && currentIteration >= maxIterations) {
            log.warn("[ObservationDispatcher] Max iterations ({}) reached at iteration {}, " +
                    "routing to limitExceededNode", maxIterations, currentIteration);
            return LIMIT_EXCEEDED_NODE;
        }

        // 2. 错误检查
        if (accessor.hasError()) {
            log.warn("[ObservationDispatcher] Error detected, routing to limitExceededNode: {}",
                    accessor.error());
            return LIMIT_EXCEEDED_NODE;
        }

        // 3. 需要总结（ObservationNode 已判断并设置 shouldSummarize）
        if (accessor.shouldSummarize()) {
            log.info("[ObservationDispatcher] shouldSummarize=true, routing to summarizingNode " +
                            "(iteration {}/{}, observations={} entries)",
                    currentIteration, maxIterations, accessor.observationHistory().size());
            return SUMMARIZING_NODE;
        }

        // 4. 继续循环
        log.debug("[ObservationDispatcher] Continuing loop, iteration {}/{}", currentIteration, maxIterations);
        return REASONING_NODE;
    }
}
