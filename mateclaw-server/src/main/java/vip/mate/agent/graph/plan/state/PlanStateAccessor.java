package vip.mate.agent.graph.plan.state;

import com.alibaba.cloud.ai.graph.OverAllState;
import org.springframework.ai.chat.messages.Message;
import vip.mate.agent.GraphEventPublisher;
import vip.mate.agent.graph.NodeStreamingChatHelper;
import vip.mate.agent.graph.state.MateClawStateKeys;

import java.util.*;

import static vip.mate.agent.graph.plan.state.PlanStateKeys.*;

/**
 * ============================================================
 * 【Plan-Execute 类型安全状态访问器】— OverAllState 的强类型封装层
 * ============================================================
 *
 * 为什么需要这个类（而不用裸 OverAllState）？三个核心原因：
 *
 * 1. 【泛型擦除陷阱】
 *    OverAllState 底层是 Map&lt;String, byte[]&gt;（每个键的值被序列化为二进制 blob），
 *    value("plan_steps") 返回 Optional&lt;Object&gt;，需要手动强转：
 *      state.&lt;List&lt;String&gt;&gt;value("plan_steps").orElse(List.of())  ← 容易写出 ClassCastException
 *    而 PlanStateAccessor 封装了这些类型转换：
 *      accessor.planSteps() → 直接返回 List&lt;String&gt;，不会出错
 *
 * 2. 【默认值集中管理】
 *    如果用裸 Map，每个节点的代码都要写 .orElse("") .orElse(List.of()) .orElse(false)。
 *    默认策略分散在几十处，改一处默认值要翻遍所有节点。PlanStateAccessor 把默认值集中在这里。
 *
 * 3. 【Fluent Builder 构建输出 Map】
 *    PlanStateAccessor.output().goal(x).planSteps(y).build() 比手动 new HashMap() + put() 安全——
 *    Builder 方法接受强类型参数（goal(String) 不接受 int），编译期就能发现类型错误。
 *    而且不会拼错 key（手写 Map 可能写 "plan_stpes" 这种 typo）。
 *
 * 使用模式：每个 Plan-Execute 节点的 apply(OverAllState state) 方法第一行就是
 *   PlanStateAccessor accessor = new PlanStateAccessor(state);
 * 然后通过 accessor.xxx() 读、PlanStateAccessor.output().xxx().build() 写。
 *
 * 与 MateClawStateAccessor 对比：
 *  - MateClawStateAccessor：两种模式共享的状态（MESSAGES, systemPrompt, goalSnapshot 等）
 *  - PlanStateAccessor：Plan-Execute 独有的状态（planSteps, currentStepIndex, workingContext 等）
 *  - 两者都可以读对方的键（如 accessor.messages() 读的是 MateClawStateKeys.MESSAGES），
 *    但各自负责"自己领域"的 key 的写入
 *
 * 在调用链中的位置：
 *   OverAllState（引擎管理的数据总线）
 *     ↓ 被包装
 *   PlanStateAccessor / MateClawStateAccessor（类型安全层）
 *     ↓ 被调用
 *   PlanGenerationNode / StepExecutionNode / PlanSummaryNode / DirectAnswerNode（业务节点）
 *
 * @author MateClaw Team
 */
public final class PlanStateAccessor {

    private final OverAllState state;

    public PlanStateAccessor(OverAllState state) {
        this.state = Objects.requireNonNull(state, "state must not be null");
    }

    // ===== 输入 =====

    /**
     * 用户原始目标，被以下节点消费：
     * - PlanGenerationNode：分流判断的核心依据（"这个目标需要工具吗？需要拆步吗？"）
     * - StepExecutionNode：展示总目标给执行器，告诉它"我们正在做这件事"
     * - PlanSummaryNode：汇总时作为标题"原始目标：xxx"
     */
    public String goal() {
        return state.value(GOAL, "");
    }

    // ===== 计划 =====

    /**
     * 持久化的计划 ID，用于 planningService 更新 DB（updateSubPlanResult/Status/completePlan）。
     * 审批重放时，PlanGenerationNode 先检查 planId != null → 有则跳过 LLM 分流直接复用已有计划。
     */
    public Long planId() {
        return state.value(PLAN_ID, 0L);
    }

    /**
     * 计划步骤列表，单步/多步都存为 List&lt;String&gt;（单步是 List.of(step)，不为 null）。
     * StepExecutionNode 每次进入都取 steps.get(currentStepIndex) 作为当前步骤指令。
     * StepProgressDispatcher 比较 currentStepIndex >= planSteps.size() 判断是否全部完成。
     */
    @SuppressWarnings("unchecked")
    public List<String> planSteps() {
        return state.<List<String>>value(PLAN_STEPS).orElse(List.of());
    }

    public boolean planValid() {
        return state.value(PLAN_VALID, false);
    }

    /**
     * ★ 是否需要规划（核心分流标记）
     * - true  → PlanGenerationDispatcher 路由到 StepExecutionNode，进入逐步执行流程
     * - false → PlanGenerationDispatcher 路由到 DirectAnswerNode，直接回答后结束
     *
     * 默认 false 的原因：未设置意味着分流没跑完，保守起见不应该把未分类的请求
     * 强推进多步计划（之前默认 true 导致"每个请求都被拆成多步"的问题，参见 RFC-008）。
     */
    public boolean needsPlanning() {
        return state.value(NEEDS_PLANNING, false);
    }

    // ===== 步骤控制 =====

    /**
     * ★ 当前正在执行第几步（从 0 开始）。
     *
     * 生命周期：
     *   buildInitialState → 初始化为 0
     *   PlanGenerationNode（重放路径）→ 恢复到 DB 中的 awaitingStepIndex
     *   StepExecutionNode（正常完成）→ +1（currentStepIndex + 1）
     *   StepExecutionNode（审批暂停）→ 不递增！（下次重放从同一步继续）
     *   StepExecutionNode（returnDirect 短路）→ 直接设为 steps.size()（跳过后续步骤）
     *
     * ★ currentStepIndex >= planSteps().size() 是 StepProgressDispatcher 判断
     *   "所有步骤完成 → 进入 PlanSummaryNode" 的唯一条件。
     */
    public int currentStepIndex() {
        return state.value(CURRENT_STEP_INDEX, 0);
    }

    /** 当前步骤标题，供前端 SSE stepStarted 事件展示 */
    public String currentStepTitle() {
        return state.value(CURRENT_STEP_TITLE, "");
    }

    /**
     * 最后一步的执行结果文本，由 StepExecutionNode 写入，
     * 被 StateGraphPlanExecuteAgent.executeStream() 读取后流式推送给前端（内容级去重）。
     */
    public String currentStepResult() {
        return state.value(CURRENT_STEP_RESULT, "");
    }

    /**
     * 所有已完成步骤的累积结果列表（APPEND 策略，每步追加一条）。
     * PlanSummaryNode 汇总时遍历此列表生成最终回答。
     * StepExecutionNode 构建步骤 Prompt 时展示最近 3 条已完成结果给执行器参考。
     */
    @SuppressWarnings("unchecked")
    public List<String> completedResults() {
        return state.<List<String>>value(COMPLETED_RESULTS).orElse(List.of());
    }

    // ===== 终止 =====

    /**
     * 最终回答，被 ChatController 读取后写入 mate_message 表做 DB 持久化，
     * 同时供 IM 渠道（钉钉/企微/Slack/Telegram 等）同步获取。
     * ★ 如果 FINAL_SUMMARY 没写入，IM 渠道收到空回复，用户看不到答案。
     */
    public String finalSummary() {
        return state.value(FINAL_SUMMARY, "");
    }

    /**
     * 直接回答文本（needsPlanning=false 时的产出），
     * PlanGenerationNode 写入 → DirectAnswerNode 读取后搬运到 FINAL_SUMMARY。
     */
    public String directAnswer() {
        return state.value(DIRECT_ANSWER, "");
    }

    // ===== Thinking =====

    /** PlanSummaryNode 汇总时的 LLM 思维链，供前端"思考过程"面板展示 */
    public String finalSummaryThinking() {
        return state.value(FINAL_SUMMARY_THINKING, "");
    }

    /** 当前步骤的 LLM 思维链，由 StepExecutionNode 在 while 循环中产出，经 executeStream 做内容级去重后推送给前端 */
    public String currentStepThinking() {
        return state.value(CURRENT_STEP_THINKING, "");
    }

    // ===== 共享键（读取 MateClawStateKeys 的值，Plan-Execute 节点也需要这些信息）=====

    /** Agent 的 systemPrompt，StepExecutionNode 用来构建增强的系统消息（systemPrompt + 硬性规则） */
    public String systemPrompt() {
        return state.value(MateClawStateKeys.SYSTEM_PROMPT, "你是一个有帮助的AI助手。");
    }

    /** 会话 ID，用于 planningService 定位记录和 streamingHelper 的 SSE 广播 */
    public String conversationId() {
        return state.value(MateClawStateKeys.CONVERSATION_ID, "");
    }

    /** 追踪 ID（UUID 前 8 位），用于日志关联 */
    public String traceId() {
        return state.value(MateClawStateKeys.TRACE_ID, "");
    }

    /**
     * 聊天来源（Web / API / DingTalk / Slack...），
     * 从 state 读取后传递给 DelegateAgentTool 的子图，确保子图继承父图的渠道信息。
     */
    public vip.mate.agent.context.ChatOrigin chatOrigin() {
        return state.<vip.mate.agent.context.ChatOrigin>value(MateClawStateKeys.CHAT_ORIGIN)
                .orElse(vip.mate.agent.context.ChatOrigin.EMPTY);
    }

    // ===== 会话消息（复用了 MateClawStateKeys.MESSAGES，但为了方便 Plan-Execute 节点使用也在此暴露）=====

    /**
     * 完整会话消息列表，包含对话历史和当前 UserMessage。
     * StepExecutionNode 在 rebuildWorkingContext 时从中取历史消息做压缩。
     */
    @SuppressWarnings("unchecked")
    public List<Message> messages() {
        return state.<List<Message>>value(MateClawStateKeys.MESSAGES).orElse(List.of());
    }

    // ===== 工作上下文 =====

    /**
     * 压缩后的对话+步骤上下文（≤6000 字符），Plan-Execute 避免 prompt 膨胀的核心机制。
     *
     * 写入者：
     *   - buildInitialState() → buildWorkingContext(history, List.of()) 初始压缩
     *   - StepExecutionNode → 首步 rebuildWorkingContext() or 后续 appendStepIncremental()
     * 读取者：
     *   - PlanGenerationNode（分流时感知对话约束）
     *   - StepExecutionNode（构建步骤 Prompt 的 Layer 4）
     *   - PlanSummaryNode（汇总时注入对话上下文）
     */
    public String workingContext() {
        return state.value(WORKING_CONTEXT, "");
    }

    // ===== 输出构建器 =====

    public static OutputBuilder output() {
        return new OutputBuilder();
    }

    /**
     ★ Fluent 输出构建器 — 节点通过链式调用 .goal(...).planSteps(...).build() 产出输出 Map
     *
     * 为什么不用 new HashMap 然后 put？
     * 1. 类型安全：planSteps(String...) 编译报错，new HashMap 的 put 只接受 Object
     * 2. key 不会拼错：写 output().planSteps(x) IDE 有自动补全，手写 map.put("plann_steps", x) 才发现
     * 3. mergeUsage 逻辑集中：累加 token usage 时从 state 读已有值再 + 新增，分散在各节点容易写漏
     *
     * 在调用链中的位置：
     *   节点 NodeAction.apply(OverAllState state) 执行完毕
     *     → PlanStateAccessor.output().xxx().build() 构建输出 Map
     *     → 引擎将该 Map 合并到全局 state
     *     → 下一个节点通过 new PlanStateAccessor(state) 读取
     *
     * 典型用法（PlanGenerationNode 为例）：
     * <pre>{@code
     *   return PlanStateAccessor.output()
     *       .needsPlanning(true)           // → PlanGenerationDispatcher 读取，路由到 StepExecutionNode
     *       .planId(plan.getId())          // → StepExecutionNode 读取，定位 DB 记录
     *       .planSteps(steps)              // → StepExecutionNode 读取，获取步骤列表
     *       .currentStepIndex(0)           // → StepExecutionNode 读取，从第 0 步开始
     *       .currentPhase("plan_generated")// → StreamDelta 事件，前端状态栏展示
     *       .mergeUsage(state, result)     // → 累加 token usage（读 state 旧值 + 新增）
     *       .events(events)                // → executeStream 读取，推送 SSE 结构化事件
     *       .build();
     * }</pre>
     *
     * 关键细节：
     * - completedResults(String) 写入单条结果，但键本身是 APPEND 策略，引擎自动追加到列表尾部
     * - mergeUsage(currentState, result) 从 currentState 读已有 token 数，再加本次增量（不覆盖）
     * - contentStreamed/thinkingStreamed 写入共享键 MateClawStateKeys，由 executeStream 做去重判断
     * - 共享键（MESSAGES, CURRENT_PHASE, PENDING_EVENTS 等）通过 OutputBuilder 统一写入
     */
    public static final class OutputBuilder {
        private final Map<String, Object> map = new HashMap<>();

        private OutputBuilder() {}

        public OutputBuilder put(String key, Object value) {
            map.put(key, value);
            return this;
        }

        // ---- 输入 ----
        public OutputBuilder goal(String goal) {
            return put(GOAL, goal);
        }

        // ---- 会话消息（写入共享键 MateClawStateKeys.MESSAGES）----
        public OutputBuilder messages(List<Message> msgs) {
            return put(MateClawStateKeys.MESSAGES, msgs);
        }

        // ---- 工作上下文 ----
        public OutputBuilder workingContext(String ctx) {
            return put(WORKING_CONTEXT, ctx);
        }

        // ---- 计划 ----
        public OutputBuilder planId(Long id) {
            return put(PLAN_ID, id);
        }

        public OutputBuilder planSteps(List<String> steps) {
            return put(PLAN_STEPS, steps);
        }

        public OutputBuilder planValid(boolean valid) {
            return put(PLAN_VALID, valid);
        }

        public OutputBuilder needsPlanning(boolean needs) {
            return put(NEEDS_PLANNING, needs);
        }

        // ---- 步骤控制 ----
        public OutputBuilder currentStepIndex(int index) {
            return put(CURRENT_STEP_INDEX, index);
        }

        public OutputBuilder currentStepTitle(String title) {
            return put(CURRENT_STEP_TITLE, title);
        }

        public OutputBuilder currentStepResult(String result) {
            return put(CURRENT_STEP_RESULT, result);
        }

        /**
         * 追加到 COMPLETED_RESULTS（APPEND 策略，传入单条结果包装为 List）
         */
        public OutputBuilder completedResults(String result) {
            return put(COMPLETED_RESULTS, List.of(result));
        }

        // ---- 终止 ----
        public OutputBuilder finalSummary(String summary) {
            return put(FINAL_SUMMARY, summary);
        }

        public OutputBuilder directAnswer(String answer) {
            return put(DIRECT_ANSWER, answer);
        }

        // ---- Thinking ----
        public OutputBuilder finalSummaryThinking(String thinking) {
            return put(FINAL_SUMMARY_THINKING, thinking);
        }

        public OutputBuilder currentStepThinking(String thinking) {
            return put(CURRENT_STEP_THINKING, thinking);
        }

        // ---- 流式防重（写入共享键）----
        public OutputBuilder contentStreamed(boolean streamed) {
            return put(MateClawStateKeys.CONTENT_STREAMED, streamed);
        }

        public OutputBuilder thinkingStreamed(boolean streamed) {
            return put(MateClawStateKeys.THINKING_STREAMED, streamed);
        }

        // ---- 事件流（写入共享键 MateClawStateKeys.PENDING_EVENTS）----
        public OutputBuilder events(List<GraphEventPublisher.GraphEvent> events) {
            return put(MateClawStateKeys.PENDING_EVENTS, events);
        }

        // ---- 阶段标记（写入共享键 MateClawStateKeys.CURRENT_PHASE）----
        public OutputBuilder currentPhase(String phase) {
            return put(MateClawStateKeys.CURRENT_PHASE, phase);
        }

        // ---- Token Usage（写入共享键）----

        /** 将本次 LLM 调用的 usage 累加到 state 已有值上 */
        public OutputBuilder mergeUsage(OverAllState currentState,
                                        NodeStreamingChatHelper.StreamResult result) {
            int existingPrompt = currentState.value(MateClawStateKeys.PROMPT_TOKENS, 0);
            int existingCompletion = currentState.value(MateClawStateKeys.COMPLETION_TOKENS, 0);
            int existingLlmCalls = currentState.value(MateClawStateKeys.LLM_CALL_COUNT, 0);
            map.put(MateClawStateKeys.PROMPT_TOKENS, existingPrompt + result.promptTokens());
            map.put(MateClawStateKeys.COMPLETION_TOKENS, existingCompletion + result.completionTokens());
            map.put(MateClawStateKeys.LLM_CALL_COUNT, existingLlmCalls + 1);
            return this;
        }

        public Map<String, Object> build() {
            return map;
        }
    }
}
