package vip.mate.agent.graph.plan.edge;

import com.alibaba.cloud.ai.graph.OverAllState;
import com.alibaba.cloud.ai.graph.StateGraph;
import com.alibaba.cloud.ai.graph.action.EdgeAction;
import vip.mate.agent.graph.plan.state.PlanStateKeys;
import vip.mate.agent.graph.state.MateClawStateKeys;

import java.util.List;

/**
 * ============================================================
 * 【Plan-Execute 第2个路由】StepProgressDispatcher — 步骤完成后的三路分支
 * ============================================================
 * 在 StepExecutionNode 完成一个步骤后，根据 current_phase 和 current_step_index
 * 决定下一步走向。这是 Plan-Execute 循环控制的核心路由点。
 *
 * 三路分支（按优先级）：
 * 1. current_phase == "awaiting_approval" → END
 *    工具审批暂停 — 图暂停，等用户确认后通过 replay 重新执行
 * 2. current_phase == "plan_aborted" → END
 *    计划异常中止（returnDirect 短路 / 步骤执行异常）— 图终止
 * 3. current_step_index >= plan_steps.size() → PLAN_SUMMARY_NODE
 *    所有步骤已完成 — 进入汇总阶段
 * 4. 否则 → STEP_EXECUTION_NODE
 *    还有步骤未完成 — 继续执行下一步（循环回自身）
 *
 * 与 ReAct 的 ObservationDispatcher 对比：
 *  - ObservationDispatcher 多了"迭代超限 → LimitExceededNode"分支
 *  - StepProgressDispatcher 没有超限 — 步骤数量固定，由规划决定
 *  - 两者都有审批暂停处理（awaiting_approval → END）
 *
 * @author MateClaw Team
 */
public class StepProgressDispatcher implements EdgeAction {

    @Override
    @SuppressWarnings("unchecked")
    public String apply(OverAllState state) {
        // ★ 审批暂停 or 计划中止 → 直接结束图 tick
        //    awaiting_approval: 工具需要审批，等 replay 重新注入 PRE_APPROVED_TOOL_CALL
        //    plan_aborted: returnDirect 短路 or 步骤异常 → 计划终止
        String currentPhase = state.value(MateClawStateKeys.CURRENT_PHASE, "");
        if ("awaiting_approval".equals(currentPhase) || "plan_aborted".equals(currentPhase)) {
            return StateGraph.END;
        }

        int currentIndex = state.value(PlanStateKeys.CURRENT_STEP_INDEX, 0);
        List<String> steps = state.<List<String>>value(PlanStateKeys.PLAN_STEPS).orElse(List.of());
        if (currentIndex >= steps.size()) {
            return PlanStateKeys.PLAN_SUMMARY_NODE;
        }
        return PlanStateKeys.STEP_EXECUTION_NODE;
    }
}
