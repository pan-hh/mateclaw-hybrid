package vip.mate.agent.graph.plan.state;

/**
 * ============================================================
 * 【Plan-Execute 专用状态键】— Plan-Execute 图节点之间传递数据的"数据总线"
 * ============================================================
 *
 * 背景：StateGraph 引擎的每个节点执行完后产出一个 Map&lt;String, Object&gt;，引擎自动把这些
 *       key-value 合并到全局状态（OverAllState）中，下一个节点再从中读取。这些 key 就是"状态键"。
 *       —— 也就是说，节点之间不是通过方法参数通信，而是通过这个全局 Map 通信。
 *
 * 如果状态键缺失会怎样？
 * - 读方通过 PlanStateAccessor 的默认值兜底（如 goal() 缺省 ""，planSteps() 缺省 List.of()）
 * - 但这只是"不崩"，语义上取不到正确值会导致图行为异常（如分流器看不到 goal 就无从分流）
 *
 * 与 MateClawStateKeys 的分工：
 * - MateClawStateKeys：ReAct / Plan-Execute 两种模式共享的通用键（MESSAGES, SYSTEM_PROMPT, CURRENT_PHASE...）
 * - PlanStateKeys：Plan-Execute 模式独有的业务键（PLAN_STEPS, WORKING_CONTEXT, CURRENT_STEP_INDEX...）
 *
 * 写入策略（在 AgentGraphBuilder.buildPlanExecuteAgent 里注册 KeyStrategy）：
 * - REPLACE 策略（默认）：新值直接覆盖旧值。用于单值键（goal, current_step_result, final_summary）
 * - APPEND 策略：新值追加到已有列表尾部。用于累积键（completed_results — 每步一条，PlanSummaryNode 汇总用）
 *
 * ★ 完整数据流转（跟着这个看，就知道每个键在哪个节点被写入、被哪个节点读取）：
 *
 *  StateGraphPlanExecuteAgent.buildInitialState()
 *    │  写入: GOAL(用户消息), WORKING_CONTEXT(对话历史压缩摘要), CURRENT_STEP_INDEX=0
 *    │
 *    ▼
 * [PlanGenerationNode]  ← 读取 GOAL, WORKING_CONTEXT  ── 写入 NEEDS_PLANNING, PLAN_ID, PLAN_STEPS 或 DIRECT_ANSWER
 *    │
 *    ├─ needsPlanning=false → [DirectAnswerNode]  ← 读取 DIRECT_ANSWER ── 写入 FINAL_SUMMARY → END
 *    │
 *    └─ needsPlanning=true  → [StepExecutionNode]  ← 读取 CURRENT_STEP_INDEX, PLAN_STEPS, WORKING_CONTEXT
 *                                                      写入 CURRENT_STEP_RESULT, CURRENT_STEP_THINKING,
 *                                                      COMPLETED_RESULTS(APPEND), WORKING_CONTEXT(更新)
 *         │
 *         └─ 所有步骤完成 → [PlanSummaryNode]  ← 读取 GOAL, COMPLETED_RESULTS, WORKING_CONTEXT
 *                                                  写入 FINAL_SUMMARY, FINAL_SUMMARY_THINKING → END
 *
 * @author MateClaw Team
 */
public final class PlanStateKeys {

    private PlanStateKeys() {}

    // ========================================================================
    // 输入键（由 buildInitialState 一次性写入，整个图执行过程中只有 PlanGenerationNode 读取）
    // ========================================================================

    /**
     * 用户原始目标/请求，由 StateGraphPlanExecuteAgent.buildInitialState() 在每次对话开始时注入。
     *
     * 写入者：仅 buildInitialState（上游，图执行前）
     * 读取者：PlanGenerationNode（分流判断依据）、StepExecutionNode（展示总目标给执行器）、
     *        PlanSummaryNode（汇总时作为"原始目标"标题）
     * 特点：整个图执行过程中不变（没有节点修改它）
     * 缺省值：""
     */
    public static final String GOAL = "goal";

    // ========================================================================
    // 计划键（PlanGenerationNode 产出，StepExecutionNode 和 PlanSummaryNode 消费）
    // ========================================================================

    /**
     * 持久化后的计划 ID（planningService.createPlan() 返回的 mate_plan 表主键）。
     *
     * 写入者：PlanGenerationNode（分流判定为 needsPlanning=true 后调用 planningService.createPlan）
     * 读取者：StepExecutionNode（调用 planningService.updateSubPlanResult/Status 需要 planId 定位记录）、
     *        PlanSummaryNode（调用 planningService.completePlan 标记计划完成）
     * 审批重放路径：chatWithReplayStream 先从 DB 查到 planId 再注入 state，PlanGenerationNode 检测到已有 PLAN_ID 则跳过 LLM 分流
     * 如果缺失：审批重放恢复不了原有计划，步骤执行和汇总无法更新 DB 记录
     */
    public static final String PLAN_ID = "plan_id";

    /**
     * 计划步骤列表 List&lt;String&gt;，由 LLM 分流生成。
     *
     * 写入者：PlanGenerationNode（从 TriageResult.steps() 提取，单步任务也是 List.of(step)，不为 null）
     * 读取者：StepExecutionNode（循环遍历 steps.get(currentStepIndex) 获取当前步骤指令）、
     *        StepProgressDispatcher（比较 currentStepIndex >= planSteps.size() 判断是否全部完成）
     * 如果缺失（空列表）：StepExecutionNode 的 stepIndex >= 0 >= 0 条件为 true，越界跳过陷入死循环
     * 缺省值：List.of()
     */
    public static final String PLAN_STEPS = "plan_steps";

    /**
     * 计划是否有效（分流成功 → true，分流异常降级 → false）。
     *
     * 写入者：PlanGenerationNode
     * 读取者：目前无直接消费者（保留字段，未来可用于图路由判断）
     */
    public static final String PLAN_VALID = "plan_valid";

    /**
     * ★ 核心分流标记 — 控制图走哪条路径。
     *
     * 写入者：PlanGenerationNode（根据 LLM 分流结果设置）
     * 读取者：PlanGenerationDispatcher（true→StepExecutionNode, false→DirectAnswerNode）
     *
     * 这个键是整个 Plan-Execute 图的一级路由开关。如果没有它或者默认 false，
     * 所有请求都会走直接回答路径——即使用户请求需要工具调用，也会被当简单问答处理。
     * 之前的默认值曾为 true，导致"所有请求都被拆成多步"（参见 RFC-008）。
     */
    public static final String NEEDS_PLANNING = "needs_planning";

    // ========================================================================
    // 步骤控制键（StepExecutionNode 循环更新，StepProgressDispatcher 消费）
    // ========================================================================

    /**
     * 当前正在执行的步骤索引（从 0 开始）。
     *
     * 写入者：buildInitialState（初始化为 0）、StepExecutionNode（每步完成后 +1）、
     *        PlanGenerationNode（重放时恢复到暂停步骤）
     * 读取者：StepExecutionNode（steps.get(currentStepIndex) 获取当前步骤指令）、
     *        StepProgressDispatcher（判断 currentStepIndex >= planSteps.size() ？→ 汇总 : 继续循环）
     *
     * ★ 审批暂停时 currentStepIndex 不递增 — 这是保证重放能恢复的关键设计：
     *   如果递增了，重放时会跳到下一步，当前步骤的审批就被跳过了。
     */
    public static final String CURRENT_STEP_INDEX = "current_step_index";

    /**
     * 当前步骤的标题/描述，供前端展示。
     *
     * 写入者：StepExecutionNode（进入步骤时设置）
     * 读取者：前端 SSE 事件流（stepStarted 事件携带）
     */
    public static final String CURRENT_STEP_TITLE = "current_step_title";

    /**
     * 最后一步的执行结果（当前步骤完成后写入）。
     *
     * 写入者：StepExecutionNode（每步完成后写入）
     * 读取者：StateGraphPlanExecuteAgent.executeStream()（流式推送步骤结果给前端）
     *
     * 注意：这个值在 executeStream 中做内容级去重 — lastPersistedStepResult 记录上次已发送的值，
     *       防止 PlanSummaryNode 输出时把上一步残留在 state 的值重复推送给前端。
     */
    public static final String CURRENT_STEP_RESULT = "current_step_result";

    /**
     * ★ APPEND 策略 — 所有已完成步骤的累积结果列表。
     *
     * 写入者：StepExecutionNode（每步完成后追加一条 `formatStepResult(stepIndex, finalResult)`）
     * 读取者：PlanSummaryNode（汇总时遍历所有步骤结果生成最终回答）、
     *        StepExecutionNode（buildStepMessages 中展示最近完成步骤结果给执行器）
     *
     * 为什么用 APPEND 而不是 REPLACE：如果每步 REPLACE，后一步会覆盖前一步的结果，
     * PlanSummaryNode 只能看到最后一步的结果，无法生成完整汇总。
     *
     * 写入格式：`步骤1结果：xxx`、`步骤2结果：yyy`
     */
    public static final String COMPLETED_RESULTS = "completed_results";

    // ========================================================================
    // 终止键（PlanSummaryNode / DirectAnswerNode 产出，ChatController 消费）
    // ========================================================================

    /**
     * 最终回答，PlanSummaryNode 或 DirectAnswerNode 产出。
     *
     * 写入者：PlanSummaryNode（汇总所有步骤结果后调 LLM 生成）、DirectAnswerNode（直接回答写入此键）
     * 读取者：ChatController（从结构化流中收集 FINAL_SUMMARY 的 StreamDelta，写入 mate_message 持久化）、
     *        IM 渠道（非 SSE 渠道通过 AgentService.chat() 同步获取此值）
     *
     * ★ 这是一个关键的"契约键"：ChatController 约定从 FINAL_SUMMARY 取答案做 DB 持久化。
     *   如果 FINAL_SUMMARY 没写入，IM 渠道（钉钉/企微/Slack）会收到空回复。
     */
    public static final String FINAL_SUMMARY = "final_summary";

    /**
     * PlanGenerationNode 在直接回答路径（needsPlanning=false）时生成的答案。
     *
     * 写入者：PlanGenerationNode（分流为类别A时写入）
     * 读取者：DirectAnswerNode（读取此值并传播到 FINAL_SUMMARY）
     *
     * 为什么中间需要 DirectAnswerNode 做一次搬运？架构一致性——
     * 所有终止路径最终都通过 FINAL_SUMMARY 键输出，ChatController 只看 FINAL_SUMMARY。
     * 另外 DirectAnswerNode 之后还要连接 GoalEvaluationNode（目标评估），必须作为独立节点存在。
     */
    public static final String DIRECT_ANSWER = "direct_answer";

    // ========================================================================
    // 上下文键（Plan-Execute 与 ReAct 最核心的区别之一）
    // ========================================================================

    /**
     ★ 工作上下文 / 摘要上下文（REPLACE 策略）— Plan-Execute 的"压缩记忆"
     *
     * 这是 Plan-Execute 模式与 ReAct 模式最关键的区别之一：
     * - ReAct：消息历史完整保留在 MESSAGES（MateClawStateKeys）中，SummarizingNode 对其压缩
     * - Plan-Execute：MESSAGES 保留原始消息但每个节点的 Prompt 不直接拼接数百条历史，
     *                 而是通过 WORKING_CONTEXT 传递一个受控长度（≤6000字符）的压缩摘要
     *
     * 写入者：
     *   - buildInitialState：buildWorkingContext(historyMessages, List.of()) 压缩对话历史
     *   - StepExecutionNode：每步完成后增量更新（首步完整重建 → 后续追加，O(1) 非 O(N)）
     * 读取者：
     *   - PlanGenerationNode（分流时感知对话历史约束）
     *   - StepExecutionNode（构建步骤 Prompt 的 Layer 4，让执行器知道前序上下文）
     *   - PlanSummaryNode（汇总时感知对话要求）
     *
     * 如果没有 WORKING_CONTEXT：StepExecutionNode 的 prompt 在长计划中会无限膨胀
     *   （每步都把对话历史+所有步骤结果全量拼接），最终超过模型 context window，步骤执行失败。
     *
     * 内容结构：
     *   === 对话历史摘要 ===（最近10条消息，每条截断至500字符）
     *   === 已完成步骤结果 ===（最近5条，每条截断至800字符）
     * 更新方式：
     *   - 首步（prevWorkingContext 为空）→ rebuildWorkingContext() 完整重建
     *   - 后续步骤 → appendStepIncremental() O(1) 追加
     * 大小上限：6000 字符
     */
    public static final String WORKING_CONTEXT = "working_context";

    // ========================================================================
    // Thinking 键（LLM 思维链，供前端"思考过程"面板展示）
    // ========================================================================

    /**
     * PlanSummaryNode 生成的 thinking（LLM 在汇总时的思维链）。
     *
     * 写入者：PlanSummaryNode（streamCall 的 result.thinking()）
     * 读取者：StateGraphPlanExecuteAgent.executeStream()（流式推送给前端 SSE）
     */
    public static final String FINAL_SUMMARY_THINKING = "final_summary_thinking";

    /**
     * 当前步骤的 thinking，由 StepExecutionNode 在每一步的 while 循环中产出。
     *
     * 写入者：StepExecutionNode（while 循环中每轮 LLM 调用的 result.thinking()）
     * 读取者：StateGraphPlanExecuteAgent.executeStream()（流式推送给前端 SSE + 内容级去重）
     */
    public static final String CURRENT_STEP_THINKING = "current_step_thinking";

    // ========================================================================
    // 节点名称（用于 StateGraph.addNode/addEdge 注册）
    // ========================================================================

    /** PlanGenerationNode 在图中的注册名 — 任务分流 + 计划生成 */
    public static final String PLAN_GENERATION_NODE = "plan_generation";
    /** StepExecutionNode 在图中的注册名 — 逐步执行（含内部工具调用循环） */
    public static final String STEP_EXECUTION_NODE = "step_execution";
    /** PlanSummaryNode 在图中的注册名 — 汇总所有步骤结果 */
    public static final String PLAN_SUMMARY_NODE = "plan_summary";
    /** DirectAnswerNode 在图中的注册名 — 简单问答直出（不产生计划） */
    public static final String DIRECT_ANSWER_NODE = "direct_answer_node";
}
