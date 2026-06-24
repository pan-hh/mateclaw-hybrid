package vip.mate.agent.graph.plan.edge;

import com.alibaba.cloud.ai.graph.OverAllState;
import com.alibaba.cloud.ai.graph.action.EdgeAction;
import vip.mate.agent.graph.plan.state.PlanStateKeys;

/**
 * ============================================================
 * 【Plan-Execute 第1个路由】PlanGenerationDispatcher — 规划后的两路分支
 * ============================================================
 * 在 PlanGenerationNode 完成分流后，根据 NEEDS_PLANNING 决定走向：
 *
 * - needs_planning = false → DIRECT_ANSWER_NODE（简单问答，直接回答后结束）
 * - needs_planning = true  → STEP_EXECUTION_NODE（进入逐步执行流程）
 *
 * 默认策略（needs_planning 未设置时）：
 *   默认为 false → 走直接回答路径。
 *   原因：未设置意味着分流没跑完，保守起见不应该把未分类的请求
 *         强推进多步计划（之前默认 true 导致"每个请求都被拆成多步"的问题，
 *         参见 RFC-008）。
 *
 * 与 ReAct 的 ReasoningDispatcher 对比：
 *   ReAct 有6个分支（工具调用/总结/直接回答/超限...），
 *   PlanExecute 只有2个分支，更简洁 — 复杂度的处理完全交给 StepExecutionNode
 *
 * @author MateClaw Team
 */
public class PlanGenerationDispatcher implements EdgeAction {

    @Override
    public String apply(OverAllState state) {
        boolean needsPlanning = state.value(PlanStateKeys.NEEDS_PLANNING, false);
        if (!needsPlanning) {
            return PlanStateKeys.DIRECT_ANSWER_NODE;
        }
        return PlanStateKeys.STEP_EXECUTION_NODE;
    }
}
