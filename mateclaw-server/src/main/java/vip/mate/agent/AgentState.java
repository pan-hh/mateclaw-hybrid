package vip.mate.agent;

/**
 * ============================================================
 * 【调用链路第4步之A】Agent 运行状态枚举
 * ============================================================
 * 定义了 Agent 的生命周期状态机：
 *   IDLE → RUNNING/PLANNING/EXECUTING → DONE/FAILED/ERROR
 * 或 IDLE → RUNNING → WAITING_USER_INPUT（审批等待）
 *
 * 状态转换由 BaseAgent.setState() 管理，用 AtomicReference 保证线程安全
 *
 * @author MateClaw Team
 */
public enum AgentState {

    /** 空闲，等待任务 */
    IDLE,

    /** 规划中，正在生成执行计划 */
    PLANNING,

    /** 执行中，正在执行工具调用或子任务 */
    EXECUTING,

    /** 运行中（ReAct / PlanExecute 使用） */
    RUNNING,

    /** 等待用户输入（如审批确认） */
    WAITING_USER_INPUT,

    /** 已完成 */
    DONE,

    /** 执行失败 */
    FAILED,

    /** 错误状态（流式调用异常） */
    ERROR
}
