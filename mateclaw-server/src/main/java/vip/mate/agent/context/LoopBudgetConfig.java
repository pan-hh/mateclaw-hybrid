package vip.mate.agent.context;

/**
 * Configuration for per-reasoning-loop message budgeting.
 *
 * <p>Used by {@link LoopMessageBudgeter} to decide when and how to trim the
 * working message list that a ReAct iteration hands to the LLM. Distinct from
 * the multi-turn history compression configured by
 * {@link vip.mate.config.ConversationWindowProperties}: this one applies inside
 * a single user turn while the ReAct loop accumulates reasoning steps and
 * tool-call/tool-response pairs.
 *
 * <p>Field semantics:
 * <ul>
 *   <li>{@code triggerTokens} — token threshold above which budgeting kicks
 *       in. Compared against {@code historyTokens + reservedPrefixTokens}
 *       so the budgeter accounts for the full prompt the LLM will see,
 *       not just the message list.</li>
 *   <li>{@code keepTailTokens} — token budget reserved for the tail (recent
 *       observations + the current user message). Scales with the model
 *       window instead of relying on a fixed count.</li>
 *   <li>{@code minTailMessages} — floor on the kept-tail count. Prevents a
 *       single huge tool output from collapsing the tail to one message and
 *       losing recent reasoning context.</li>
 *   <li>{@code tailSoftCeilingRatio} — multiplier applied to
 *       {@code keepTailTokens} when honoring the floor or pulling back to
 *       keep a tool pair whole. Lets the tail overshoot the hard budget by
 *       up to this factor before more aggressive cuts kick in.</li>
 *   <li>{@code reservedPrefixTokens} — estimated tokens consumed by the
 *       non-history portion of the prompt (system prompt, skill catalog,
 *       runtime context, wiki injection, tool schemas, output reserve).
 *       Surfaces these from the caller so the budget covers the whole
 *       prompt, not just the message list.</li>
 *   <li>{@code targetMaxMessages} — soft ceiling on the count fed to the
 *       LLM. Best-effort: the budgeter may exceed it slightly to keep a
 *       tool pair whole rather than orphan a call/response — that case is
 *       reported via {@code BudgetTrace.capExceededForPairIntegrity}.</li>
 * </ul>
 */
public record LoopBudgetConfig(
        int triggerTokens,
        int keepTailTokens,
        int minTailMessages,
        double tailSoftCeilingRatio,
        int reservedPrefixTokens,
        int targetMaxMessages) {

    /** Smallest useful trigger threshold; below this budgeting is effectively disabled. */
    public static final int MIN_TRIGGER_TOKENS = 1_000;

    /** Smallest sensible tail budget; below this even one observation may not fit. */
    public static final int MIN_TAIL_TOKENS = 2_000;

    /** Floor on minTailMessages — fewer than 3 collapses recent context too aggressively. */
    public static final int MIN_TAIL_MESSAGES_FLOOR = 3;

    /** Floor on the soft ceiling ratio — anything below 1.0 is degenerate. */
    public static final double MIN_TAIL_SOFT_CEILING_RATIO = 1.0;

    /** Smallest sensible target cap; below this even a normal ReAct loop trips it. */
    public static final int MIN_TARGET_MAX = 20;

    public LoopBudgetConfig {
        if (triggerTokens < MIN_TRIGGER_TOKENS) {
            throw new IllegalArgumentException(
                    "triggerTokens must be >= " + MIN_TRIGGER_TOKENS + ", got " + triggerTokens);
        }
        if (keepTailTokens < MIN_TAIL_TOKENS) {
            throw new IllegalArgumentException(
                    "keepTailTokens must be >= " + MIN_TAIL_TOKENS + ", got " + keepTailTokens);
        }
        if (minTailMessages < MIN_TAIL_MESSAGES_FLOOR) {
            throw new IllegalArgumentException(
                    "minTailMessages must be >= " + MIN_TAIL_MESSAGES_FLOOR
                            + ", got " + minTailMessages);
        }
        if (tailSoftCeilingRatio < MIN_TAIL_SOFT_CEILING_RATIO) {
            throw new IllegalArgumentException(
                    "tailSoftCeilingRatio must be >= " + MIN_TAIL_SOFT_CEILING_RATIO
                            + ", got " + tailSoftCeilingRatio);
        }
        if (reservedPrefixTokens < 0) {
            throw new IllegalArgumentException(
                    "reservedPrefixTokens must be >= 0, got " + reservedPrefixTokens);
        }
        if (targetMaxMessages < MIN_TARGET_MAX) {
            throw new IllegalArgumentException(
                    "targetMaxMessages must be >= " + MIN_TARGET_MAX
                            + ", got " + targetMaxMessages);
        }
        if (keepTailTokens >= triggerTokens) {
            throw new IllegalArgumentException(
                    "keepTailTokens (" + keepTailTokens + ") must be < triggerTokens ("
                            + triggerTokens + ") — otherwise budgeting would never reduce anything");
        }
    }

    /** Tail budget after applying the soft ceiling. */
    public int tailSoftCeilingTokens() {
        return (int) (keepTailTokens * tailSoftCeilingRatio);
    }

    /**
     * 从模型上下文窗口大小推导 L2 预算配置。
     *
     * <h3>参数推导策略</h3>
     * <pre>
     *   模型最大输入: contextWindowTokens (如 128000)
     *   ┌──────────────────────────────────────────────────────────────┐
     *   │                                            │               │
     *   │   历史（可被裁剪的部分）                        │  预留前缀      │
     *   │                                            │ (system+工具)  │
     *   │  ┌────────────┬─────────────────────┐       │               │
     *   │  │  丢弃部分   │   tail = 30% ~38K   │       │   ~20%        │
     *   │  └────────────┴─────────────────────┘       │               │
     *   │                                            │               │
     *   │  trigger = 50% ~64K —— 与 L1 多轮压缩使用相同的阈值  │
     *   └──────────────────────────────────────────────────────────────┘
     * </pre>
     *
     * <ul>
     *   <li><b>trigger = 50%</b> — 与 L1 多轮压缩相同，保持两层校准一致</li>
     *   <li><b>tail = 30%</b> — 近期消息的 token 预算，
     *     at least one full reasoning/action cycle stays visible</li>
     *   <li><b>minTailMessages = 4</b> — 最少保留4条，防止一条巨大工具输出
     *     吃掉全部尾部 → 模型丢失最近的推理上下文</li>
     *   <li><b>tailSoftCeilingRatio = 1.5</b> — 尾部可超预算 50%（如为了
     *     保持 tool pair 完整性或满足最小消息数）</li>
     *   <li><b>reservedPrefixTokens = 0</b> — 配置层只给消息预算；
     *     调用方（ReasoningNode）会覆盖为真实的 system+工具估算值</li>
     *   <li><b>targetMaxMessages = 200</b> — 远超正常 ReAct 20-40 条消息，
     *     作为有意义的护栏而非主动触发项</li>
     * </ul>
     */
    public static LoopBudgetConfig forContext(int contextWindowTokens) {
        if (contextWindowTokens <= 0) {
            // 避免除以零等异常情况
            contextWindowTokens = 32_000;
        }
        int trigger = Math.max(MIN_TRIGGER_TOKENS, (int) (contextWindowTokens * 0.50));
        int tail = Math.max(MIN_TAIL_TOKENS, (int) (contextWindowTokens * 0.30));
        // 安全约束：tail 预算不能 ≥ trigger 阈值，否则预算裁剪零效果
        if (tail >= trigger) {
            tail = Math.max(MIN_TAIL_TOKENS, trigger - MIN_TRIGGER_TOKENS);
        }
        return new LoopBudgetConfig(trigger, tail, 4, 1.5, 0, 200);
    }

    /** Return a copy with {@code reservedPrefixTokens} replaced. */
    public LoopBudgetConfig withReservedPrefixTokens(int reservedPrefixTokens) {
        return new LoopBudgetConfig(triggerTokens, keepTailTokens, minTailMessages,
                tailSoftCeilingRatio, reservedPrefixTokens, targetMaxMessages);
    }
}
