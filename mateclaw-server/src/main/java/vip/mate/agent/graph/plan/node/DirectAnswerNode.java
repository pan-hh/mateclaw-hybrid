package vip.mate.agent.graph.plan.node;

import com.alibaba.cloud.ai.graph.OverAllState;
import com.alibaba.cloud.ai.graph.action.NodeAction;
import vip.mate.agent.graph.plan.state.PlanStateKeys;

import java.util.Map;

/**
 * ============================================================
 * 【Plan-Execute 快速出口】直接回答节点 — 搬运 DIRECT_ANSWER → FINAL_SUMMARY
 * ============================================================
 *
 * 角色：PlanGenerationNode 判定为类别(A)直接回答时，由 PlanGenerationDispatcher 路由到此节点。
 * 本节点只做一件事: 读取 DIRECT_ANSWER，写入 FINAL_SUMMARY，图即可终止。
 *
 * 为什么需要独立节点（而不是在 PlanGenerationNode 直接写 FINAL_SUMMARY）？
 * 1. 【架构一致性】所有终止路径统一通过 FINAL_SUMMARY 输出，ChatController 只看这个键
 * 2. 【Goal 评估链路】即使简单问答也可能属于 active goal，需要经过 GoalEvaluationNode 评估进度。
 *    DirectAnswerNode → (active goal && !evaluated?) → GoalEvaluationNode → END
 *    如果 PlanGenerationNode 直接写 FINAL_SUMMARY 并加 END 边，Goal 评估就无法触发
 * 3. 【流式防重】DIRECT_ANSWER 已通过 broadcastContent 推 SSE，
 *    executeStream 将 FINAL_SUMMARY 标记 persistOnly 做 DB 持久化不重复推送
 *
 * 数据流:
 *   上游: PlanGenerationNode → DIRECT_ANSWER + PlanGenerationDispatcher(false)
 *   本节点: DIRECT_ANSWER → FINAL_SUMMARY
 *   下游: executeStream → StreamDelta → ChatController → mate_message(DB+IM)
 *   路由: (active goal && !evaluated?) → GoalEvaluationNode → END
 *
 * @author MateClaw Team
 */
public class DirectAnswerNode implements NodeAction {

    @Override
    public Map<String, Object> apply(OverAllState state) {
        String directAnswer = state.value(PlanStateKeys.DIRECT_ANSWER, "");
        return Map.of(PlanStateKeys.FINAL_SUMMARY, directAnswer);
    }
}
