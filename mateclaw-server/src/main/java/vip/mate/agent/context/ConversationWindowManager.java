package vip.mate.agent.context;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.ai.chat.messages.AssistantMessage;
import org.springframework.ai.chat.messages.Message;
import org.springframework.ai.chat.messages.SystemMessage;
import org.springframework.ai.chat.messages.ToolResponseMessage;
import org.springframework.ai.chat.messages.UserMessage;
import org.springframework.ai.chat.model.ChatModel;
import org.springframework.ai.chat.model.ChatResponse;
import org.springframework.ai.chat.prompt.ChatOptions;
import org.springframework.ai.chat.prompt.Prompt;
import org.springframework.ai.tool.ToolCallback;
import com.alibaba.cloud.ai.dashscope.chat.DashScopeChatOptions;
import org.springframework.stereotype.Component;
import vip.mate.agent.graph.executor.ToolResultStorage;
import vip.mate.agent.prompt.PromptLoader;
import vip.mate.config.ConversationWindowProperties;
import vip.mate.memory.spi.MemoryManager;
import vip.mate.workspace.conversation.ConversationService;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 会话历史上下文窗口管理器（四阶段压缩升级版）
 * <p>
 * 四阶段压缩策略：
 * <ol>
 *   <li>Soft Trim — 裁剪旧工具结果为 head+tail</li>
 *   <li>Hard Clear — 替换所有旧工具结果为占位符</li>
 *   <li>Pre-Prune — 喂给摘要 LLM 前清理工具输出（减少摘要输入 token）</li>
 *   <li>LLM 结构化摘要 — Goal/Progress/Decisions/Files/NextSteps 模板，支持迭代更新</li>
 * </ol>
 * <p>
 * 关键特性：
 * <ul>
 *   <li>迭代摘要更新：多轮压缩时将旧摘要 + 新轮次合并，信息不丢失</li>
 *   <li>动态 Token 预算：基于模型上下文长度计算尾部保护和摘要预算</li>
 *   <li>压缩冷却机制：摘要失败后 10 分钟内不重试，防止雪崩</li>
 *   <li>MemoryProvider 钩子：压缩前通知记忆 provider 提取关键信息</li>
 * </ul>
 * <p>
 * 安全设计：摘要内容作为 UserMessage 注入（非 SystemMessage），
 * 避免历史用户输入被提升为系统级指令。
 *
 * @author MateClaw Team
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class ConversationWindowManager {

    // ==================== Prompt 模板 ====================

    /** 首次压缩：结构化摘要系统提示 */
    private static final String STRUCTURED_SUMMARY_SYSTEM = PromptLoader.loadPrompt("context/structured-summary-system");
    /** 首次压缩：用户提示模板 */
    private static final String STRUCTURED_SUMMARY_USER = PromptLoader.loadPrompt("context/structured-summary-user");
    /** 迭代更新：合并旧摘要 + 新轮次 */
    private static final String STRUCTURED_SUMMARY_UPDATE = PromptLoader.loadPrompt("context/structured-summary-update");

    /** 摘要注入前缀 (package-private for test assertions) */
    static final String SUMMARY_PREFIX =
            "[上下文压缩] 更早的对话轮次已被压缩为摘要以节省上下文空间。" +
            "以下摘要描述了已完成的工作，当前会话状态可能已反映这些变更。" +
            "请基于摘要和当前状态继续，避免重复已完成的工作：\n\n";

    /**
     * Marker prefix used by the first-user anchor. Lets compaction skip
     * previously-injected anchors when looking for the "real" first user
     * message in a subsequent round.
     *
     * <p>Package-private so unit tests can assert on the marker.
     */
    static final String ANCHOR_PREFIX = "[Original goal]\n";

    // ==================== 序列化截断参数 ====================

    private static final int CONTENT_MAX = 6000;
    private static final int CONTENT_HEAD = 4000;
    private static final int CONTENT_TAIL = 1500;

    /**
     * Minimum body size at which the duplicate-output placeholder is preferred
     * over keeping the verbatim copy. Below this size the placeholder text
     * (~80 chars) is comparable to the body itself, so deduplication only
     * complicates the prompt without saving meaningful tokens. Above this
     * size the dedup placeholder is a real win.
     */
    private static final int DEDUP_MIN_CHARS = 500;

    /**
     * Tool names whose results must never be compacted into a one-line
     * summary. Sub-agent delegations are irreplaceable: the child runs an
     * independent LLM session that the parent cannot reproduce, so dropping
     * earlier batches forces the parent to re-dispatch the same children to
     * recover what was lost. Every other tool (read_file, shell, search,
     * memory) can be re-invoked cheaply if the parent decides it needs
     * the data again.
     */
    private static final java.util.Set<String> PRUNE_EXEMPT_TOOLS = java.util.Set.of(
            "delegateToAgent",
            "delegateParallel"
    );

    // ==================== 冷却机制 ====================

    /** 摘要失败后的冷却时间（毫秒）：10 分钟 */
    private static final long SUMMARY_COOLDOWN_MS = 600_000;

    // ==================== 依赖 ====================

    private final ConversationWindowProperties properties;
    private final MemoryManager memoryManager;
    private final ConversationService conversationService;

    /**
     * Optional spill store, injected via setter so unit tests and the two
     * existing 3-arg constructor callers in tests stay source-compatible.
     * When {@code null}, prune falls back to "keep originals verbatim" — no
     * lossy summary rewrite is ever applied. Spring autowires this when
     * {@link ToolResultStorage} is on the context.
     */
    private ToolResultStorage toolResultStorage;

    @org.springframework.beans.factory.annotation.Autowired(required = false)
    public void setToolResultStorage(ToolResultStorage toolResultStorage) {
        this.toolResultStorage = toolResultStorage;
    }

    /**
     * Optional stream tracker for broadcasting {@code compact_status}
     * SSE events. Wired via setter so unit tests can leave it {@code null}
     * without dragging in the channel layer. When present, every
     * compaction emits start/skipped/summarize/done events so the
     * frontend can render a boundary card and a status line in real
     * time.
     */
    private vip.mate.channel.web.ChatStreamTracker streamTracker;

    @org.springframework.beans.factory.annotation.Autowired(required = false)
    public void setStreamTracker(vip.mate.channel.web.ChatStreamTracker streamTracker) {
        this.streamTracker = streamTracker;
    }

    // ==================== 状态 ====================

    /** 摘要缓存：key = "conversationId:oldMessageCount" */
    private final ConcurrentHashMap<String, CachedSummary> summaryCache = new ConcurrentHashMap<>();
    private static final long CACHE_TTL_MS = 30 * 60 * 1000L;

    /** 迭代摘要：上一次压缩生成的摘要文本（per conversation） */
    private final ConcurrentHashMap<String, String> previousSummaries = new ConcurrentHashMap<>();

    /** 每个会话的压缩次数 */
    private final ConcurrentHashMap<String, Integer> compressionCounts = new ConcurrentHashMap<>();

    /** 每个会话的摘要冷却截止时间 */
    private final ConcurrentHashMap<String, Long> summaryCooldownUntil = new ConcurrentHashMap<>();

    /** Per-conversation last-PTL-forced-compaction timestamp. The structured
     *  PTL retry path is guarded by {@link #PTL_FORCE_LLM_COOLDOWN_MS} — a
     *  second PTL hit within the cooldown falls straight back to tail-only
     *  trimming. Without this, a model that keeps regenerating tool-call
     *  loops can drive a chain of summary-LLM calls and lock the
     *  conversation in a compaction storm. */
    private final ConcurrentHashMap<String, Long> ptlForceCompactAt = new ConcurrentHashMap<>();

    /**
     * Default max input tokens for the configured model window. Surfaced for
     * the per-loop budgeter so the L1 (multi-turn compaction) and L2
     * (per-iteration trim) layers stay calibrated to the same number.
     */
    public int getDefaultMaxInputTokens() {
        return properties != null ? properties.getDefaultMaxInputTokens() : 0;
    }

    /** Cooldown window after a structured PTL compaction during which a
     *  follow-up PTL is downgraded to tail-only. Picked so a single ReAct
     *  loop that retries within seconds can't burn another summary LLM
     *  call, while still letting the next real conversation turn (minutes
     *  later) get a fresh structured pass. */
    private static final long PTL_FORCE_LLM_COOLDOWN_MS = 60_000L;

    // ==================== 主入口 ====================

    /**
     * 将会话历史裁剪到上下文窗口内。
     *
     * @param messages          已转换的 Spring AI 消息列表（不含当前用户消息）
     * @param systemPrompt      系统提示词文本
     * @param currentUserMessage 当前用户输入（纳入窗口预算计算，但不拼入返回结果）
     * @param maxInputTokens    模型最大输入 token（0 或 null 使用全局默认）
     * @param chatModel         用于生成摘要的 ChatModel
     * @param conversationId    会话 ID（用于缓存和迭代摘要）
     * @param agentId           Agent ID（用于 MemoryProvider 钩子）
     * @return 裁剪后的消息列表
     */
    public List<Message> fitToWindow(List<Message> messages, String systemPrompt,
                                     String currentUserMessage,
                                     Integer maxInputTokens, ChatModel chatModel,
                                     String conversationId, Long agentId) {
        return fitToWindow(messages, systemPrompt, currentUserMessage,
                maxInputTokens, chatModel, conversationId, agentId, null, null);
    }

    /**
     * Same as the 7-arg overload but additionally accounts for the tool
     * definitions sent on every LLM call. Without {@code toolCallbacks},
     * the budget calculation underestimates the actual request size by the
     * full size of the tools schema (often several thousand tokens for
     * agents bound to multiple MCP servers), making compression fire too
     * late and producing HTTP 400 once the request hits the model.
     */
    public List<Message> fitToWindow(List<Message> messages, String systemPrompt,
                                     String currentUserMessage,
                                     Integer maxInputTokens, ChatModel chatModel,
                                     String conversationId, Long agentId,
                                     java.util.Collection<ToolCallback> toolCallbacks) {
        return fitToWindow(messages, systemPrompt, currentUserMessage,
                maxInputTokens, chatModel, conversationId, agentId, toolCallbacks, null);
    }

    /**
     * Most comprehensive overload — adds {@code workspaceBasePath} so the
     * pre-pass that prunes old tool results can route oversized bodies to
     * the agent's workspace spill directory via {@link ToolResultStorage}.
     *
     * <p>When {@code workspaceBasePath} is {@code null}, spill files land in
     * the configured base dir, or the JVM tmpdir as last resort (see
     * {@link ToolResultStorage#resolveBaseDir(String)}). Workspace-aware
     * callers should always pass the path so historical spill files stay
     * grouped with the workspace that produced them.
     */
    /**
     * 【上下文窗口管理主入口】将多轮会话历史裁剪到模型上下文窗口以内。
     *
     * <h3>预算计算逻辑</h3>
     * <pre>
     *   模型最大输入 token（effectiveMax，默认 128000）
     *   触发阈值 = effectiveMax × compactTriggerRatio（默认 75%）
     *
     *   总 token = system提示词 + 当前用户消息 + 历史消息 + 工具schema
     *   历史可用预算 = effectiveMax - 预留 token（system + 当前消息 + 工具schema + 5%安全余量）
     *
     *   如果总 token ≤ 触发阈值 → 不压缩，直接返回
     *   如果总 token > 触发阈值 → 进入四阶段压缩（compactMessages）
     * </pre>
     *
     * <h3>安全封顶</h3>
     * 对于小窗口模型（Ollama 16K、本地 8K），system 提示词 + 当前消息
     * 可能就接近甚至超过 effectiveMax 的 50%。此时如果不封顶，
     * historyBudget 会变成负数 → 压缩目标比压缩前还大 → 死循环。
     */
    public List<Message> fitToWindow(List<Message> messages, String systemPrompt,
                                     String currentUserMessage,
                                     Integer maxInputTokens, ChatModel chatModel,
                                     String conversationId, Long agentId,
                                     java.util.Collection<ToolCallback> toolCallbacks,
                                     String workspaceBasePath) {
        // ==== 第0步：前置检查 ====
        if (messages == null || messages.isEmpty()) {
            return messages;
        }
        // 记录溢出计数起点，用于后续统计本次压缩触发了多少次磁盘溢写
        long spillsAtEntry = (toolResultStorage != null) ? toolResultStorage.getSpillCount() : 0L;

        // ==== 第0.5步：预清理 — 去重旧工具结果、溢写超大结果到磁盘 ====
        messages = pruneOldToolResultsForModelInput(messages, conversationId, workspaceBasePath);

        // ==== 第1步：计算有效 token 上限和触发阈值 ====
        int effectiveMax = (maxInputTokens != null && maxInputTokens > 0)
                ? maxInputTokens : properties.getDefaultMaxInputTokens();
        int triggerThreshold = (int) (effectiveMax * properties.getCompactTriggerRatio());

        // ==== 第2步：分段估算当前 token 使用量 ====
        // system提示词、当前用户消息、完整历史、所有工具的函数定义schema
        int systemTokens = TokenEstimator.estimateTokens(systemPrompt);
        int currentMsgTokens = TokenEstimator.estimateTokens(currentUserMessage) + TokenEstimator.PER_MESSAGE_OVERHEAD;
        int historyTokens = TokenEstimator.estimateTokens(messages);
        int toolsTokens = TokenEstimator.estimateToolsTokens(toolCallbacks);
        int totalTokens = systemTokens + currentMsgTokens + historyTokens + toolsTokens;

        // ==== 第3步：未超阈值 → 无需压缩 ====
        if (totalTokens <= triggerThreshold) {
            return messages;
        }

        log.info("[ConversationWindow] 超阈值: {} tokens (system={}, current={}, history={}, tools={}) > {} 触发阈值 (max={}), conv={}",
                totalTokens, systemTokens, currentMsgTokens, historyTokens, toolsTokens,
                triggerThreshold, effectiveMax, conversationId);

        // ==== 第4步：清理过期缓存和冷却记录 ====
        evictExpiredEntries();

        // ==== 第5步：计算历史消息可用预算 ====
        // 预留 token = 必须保留的（system + 当前消息 + 工具schema）+ 5%安全余量
        int reservedTokens = systemTokens + currentMsgTokens + toolsTokens + (int) (effectiveMax * 0.05);
        // ⚠️ 硬封顶：小上下文模型下 system+工具schema 就占了大量 token，
        //    预留太多会导致 historyBudget 变负数 → 压缩死循环
        int reservedCap = Math.max(1024, effectiveMax / 2);
        if (reservedTokens > reservedCap) {
            log.warn("[ConversationWindow] 预留 token {} 超过上下文窗口 50% {}，封顶至 {}",
                    reservedTokens, effectiveMax, reservedCap);
            reservedTokens = reservedCap;
        }
        // 历史消息可用预算 = 总窗口 - 不可压缩的预留部分
        int historyBudget = effectiveMax - reservedTokens;

        // ==== 第6步：计算尾部保护预算 ====
        // 尾部保护：阈值的 20%，确保最近的对话轮次有足够的 token 空间
        int tailTokenBudget = (int) (triggerThreshold * 0.20);

        // ==== 第7步：进入四阶段压缩 ====
        return compactMessages(messages, historyBudget, tailTokenBudget, chatModel,
                conversationId, agentId, totalTokens, spillsAtEntry, "token_threshold");
    }

    /**
     * 向后兼容：不传 agentId 的旧签名（agentId = null，不触发 Memory 钩子）
     */
    public List<Message> fitToWindow(List<Message> messages, String systemPrompt,
                                     String currentUserMessage,
                                     Integer maxInputTokens, ChatModel chatModel,
                                     String conversationId) {
        return fitToWindow(messages, systemPrompt, currentUserMessage,
                maxInputTokens, chatModel, conversationId, null);
    }

    // ==================== 核心压缩逻辑 ====================

    /** Broadcast a single compact_status event; silent no-op when no tracker is wired. */
    private void broadcastCompactStatus(String conversationId, String status, Map<String, Object> extra) {
        if (streamTracker == null || conversationId == null || conversationId.isEmpty()) {
            return;
        }
        try {
            Map<String, Object> payload = new java.util.LinkedHashMap<>();
            payload.put("status", status);
            payload.put("timestamp", System.currentTimeMillis());
            if (extra != null) payload.putAll(extra);
            streamTracker.broadcastObject(conversationId, "compact_status", payload);
        } catch (Exception e) {
            log.debug("[ConversationWindow] broadcast compact_status failed: {}", e.getMessage());
        }
    }

    /**
     * 【四阶段压缩核心】将消息列表拆分为"旧消息"和"近期消息"两部分，
     * 对旧消息应用逐级增强的压缩策略，直到满足历史 token 预算。
     *
     * <h3>压缩流程</h3>
     * <pre>
     *   Phase 0: 拆分 — 基于 token 预算计算尾部边界（findTailBoundary）
     *           + Pair Safety 调整（enforcePairSafeBoundary）
     *             → oldMessages（待压缩） + recentMessages（保留尾部）
     *
     *   Phase 1: Soft Trim     → 工具结果做 head+tail 截断（保留首尾）
     *            若仍超预算 ↓
     *   Phase 2: Hard Clear    → 工具结果替换为 "[tool result removed]"
     *            若仍超预算 ↓
     *   Phase 2.5: Memory钩子  → 让 MemoryProvider 在压缩前提取关键信息
     *            若仍超预算 ↓
     *   Phase 3: LLM 摘要      → 调用专用 LLM 生成结构化摘要
     *                            （Goal/Progress/Decisions/Files/NextSteps）
     *                            支持迭代更新模式
     *
     *   组装结果: [摘要, Anchor(原始目标), ...recentMessages]
     *   若摘要失败 → 降级保留最近 4 条旧消息
     * </pre>
     *
     * @return 压缩后的消息列表（摘要 + anchor + 近期消息）
     */
    private List<Message> compactMessages(List<Message> messages, int historyBudget,
                                          int tailTokenBudget, ChatModel chatModel,
                                          String conversationId, Long agentId,
                                          int preTokens, long spillsAtEntry,
                                          String trigger) {
        // 向前端广播压缩开始事件
        broadcastCompactStatus(conversationId, "start", Map.of(
                "preTokens", preTokens,
                "messagesIn", messages.size(),
                "trigger", trigger
        ));

        // ==== Phase 0: 基于 token 预算动态计算尾部保护边界 ====
        // 替代旧的固定 preserveRecentPairs 配置，改为按 token 预算从后往前累加
        int headEnd = 0;
        int tailStart = findTailBoundary(messages, headEnd, tailTokenBudget);

        if (tailStart <= headEnd) {
            // 消息数不足以拆分 → 无可压缩
            log.debug("[ConversationWindow] 消息数不足以拆分，跳过压缩");
            broadcastCompactStatus(conversationId, "skipped",
                    Map.of("reason", "insufficient_messages"));
            return messages;
        }

        // ==== Phase 0: Pair Safety 边界调整 ====
        // 核心安全约束：永远不能把 AssistantMessage 的 tool_calls 和
        // 对应的 ToolResponseMessage 分到 oldMessages 和 recentMessages 两边。
        // 拆分 tool pair 会导致所有 OpenAI 兼容的 provider 返回 HTTP 400。
        int pairSafeCut = enforcePairSafeBoundary(messages, headEnd, tailStart);
        if (pairSafeCut <= headEnd) {
            // 找不到安全的切割点 → 跳过本轮压缩（宁可多占一点 token）
            broadcastCompactStatus(conversationId, "skipped",
                    Map.of("reason", "pair_boundary_collapsed"));
            return messages;
        }
        if (pairSafeCut != tailStart) {
            broadcastCompactStatus(conversationId, "pair_safe", Map.of(
                    "movedFrom", tailStart, "movedTo", pairSafeCut));
        }
        tailStart = pairSafeCut;

        // 拆分消息列表：oldMessages（待压缩） + recentMessages（保留的尾部）
        List<Message> oldMessages = new ArrayList<>(messages.subList(headEnd, tailStart));
        List<Message> recentMessages = messages.subList(tailStart, messages.size());

        // ═══════════════════════════════════════════════════════════════
        // Phase 1: Soft Trim — 对旧工具结果做 head+tail 截断，保留首尾各200字符
        // 目标：温和压缩，大部分信息仍然可见
        // ═══════════════════════════════════════════════════════════════
        int softTrimmed = softTrimToolResults(oldMessages);
        if (softTrimmed > 0) {
            int afterTrimTokens = TokenEstimator.estimateTokens(oldMessages) + TokenEstimator.estimateTokens(recentMessages);
            log.info("[ConversationWindow] Phase 1 Soft trim: {} tool results trimmed, tokens={}, budget={}",
                    softTrimmed, afterTrimTokens, historyBudget);
            if (afterTrimTokens <= historyBudget) {
                // 软裁剪后满足预算 → 完成
                List<Message> result = new ArrayList<>(oldMessages);
                result.addAll(recentMessages);
                return result;
            }
        }

        // ═══════════════════════════════════════════════════════════════
        // Phase 2: Hard Clear — 所有旧工具结果替换为占位符
        // 目标：激进压缩，只保留 "工具 X 被执行过" 的语义
        // ═══════════════════════════════════════════════════════════════
        int hardCleared = hardClearToolResults(oldMessages);
        if (hardCleared > 0) {
            int afterClearTokens = TokenEstimator.estimateTokens(oldMessages) + TokenEstimator.estimateTokens(recentMessages);
            log.info("[ConversationWindow] Phase 2 Hard clear: {} replaced, tokens={}, budget={}",
                    hardCleared, afterClearTokens, historyBudget);
            if (afterClearTokens <= historyBudget) {
                List<Message> result = new ArrayList<>(oldMessages);
                result.addAll(recentMessages);
                return result;
            }
        }

        // ═══════════════════════════════════════════════════════════════
        // Phase 2.5: MemoryProvider 钩子 — 压缩前让记忆系统提取关键信息
        // 例如：从对话历史中提取用户偏好、项目决策等持久化信息
        // ═══════════════════════════════════════════════════════════════
        String memoryExtraContext = "";
        if (agentId != null && memoryManager != null) {
            try {
                String preserved = memoryManager.onPreCompress(agentId, oldMessages);
                if (preserved != null && !preserved.isBlank()) {
                    memoryExtraContext = preserved;
                    log.debug("[ConversationWindow] MemoryProvider onPreCompress contributed {} chars", preserved.length());
                }
            } catch (Exception e) {
                log.debug("[ConversationWindow] onPreCompress hook failed: {}", e.getMessage());
            }
        }

        // ═══════════════════════════════════════════════════════════════
        // Phase 3: LLM 结构化摘要 — 终极压缩手段
        // 1. 先 Pre-prune：把给摘要 LLM 看的工具输出也清理掉（减少输入 token）
        // 2. 计算动态摘要预算（被压缩内容的 20%，500-3000 字）
        // 3. 检查缓存（相同 conversationId + 消息数的摘要可复用）
        // 4. 调用摘要 LLM（结构化模板：Goal/Progress/Decisions/Files/NextSteps）
        // ═══════════════════════════════════════════════════════════════

        // Pre-prune：清理摘要 LLM 输入中的工具输出，减少摘要调用本身的 token 消耗
        List<Message> forSummary = new ArrayList<>(oldMessages);
        int prePruned = prePruneForSummary(forSummary);
        if (prePruned > 0) {
            log.info("[ConversationWindow] Phase 3 Pre-prune: {} tool results cleared before summarization", prePruned);
        }

        // 动态摘要预算：被压缩内容 token × 20%，上下限由配置控制
        int summaryBudget = computeSummaryBudget(forSummary);

        broadcastCompactStatus(conversationId, "summarize", Map.of(
                "messagesToSummarize", oldMessages.size(),
                "summaryBudget", summaryBudget
        ));

        // 摘要缓存：key = conversationId + 被压缩消息数
        // 同一轮对话的同一批消息 → 已缓存摘要 → 跳过 LLM 调用
        String cacheKey = conversationId + ":" + oldMessages.size();
        CachedSummary cached = summaryCache.get(cacheKey);
        String summary;
        boolean fromCache = false;

        if (cached != null && !cached.isExpired(CACHE_TTL_MS)) {
            summary = cached.summary();
            fromCache = true;
            log.debug("[ConversationWindow] 命中摘要缓存, conv={}", conversationId);
        } else {
            // 未命中缓存 → 调用摘要 LLM（含冷却检查）
            summary = generateSummary(forSummary, chatModel, conversationId, summaryBudget, memoryExtraContext);
            if (summary != null) {
                summaryCache.put(cacheKey, new CachedSummary(summary, System.currentTimeMillis()));
                int count = compressionCounts.merge(conversationId, 1, Integer::sum);
                log.info("[ConversationWindow] 生成结构化摘要 ({} 字符, 第 {} 次压缩), 压缩 {} 条旧消息, conv={}",
                        summary.length(), count, oldMessages.size(), conversationId);
            }
        }

        // ═══════════════════════════════════════════════════════════════
        // 组装最终结果
        // ═══════════════════════════════════════════════════════════════
        List<Message> result = new ArrayList<>();
        boolean anchored = false;
        if (summary != null && !summary.isBlank()) {
            // 摘要作为 UserMessage 注入（非 SystemMessage）避免权限升级
            result.add(new UserMessage(SUMMARY_PREFIX + summary));

            // 嵌入原始用户目标（Anchor）：长任务可能在几十轮后忘记
            // 最初被问的是什么，Anchor 让模型始终能看到最初的问题
            Message anchor = buildFirstUserAnchor(oldMessages);
            if (anchor != null) {
                result.add(anchor);
                anchored = true;
            }
        } else if (!oldMessages.isEmpty()) {
            // 摘要失败降级：只保留最近 4 条旧消息
            log.warn("[ConversationWindow] 摘要生成失败，降级为保留最近 4 条旧消息, conv={}", conversationId);
            int fallbackKeep = Math.min(4, oldMessages.size());
            result.addAll(oldMessages.subList(oldMessages.size() - fallbackKeep, oldMessages.size()));
            broadcastCompactStatus(conversationId, "failed", Map.of(
                    "reason", "summary_generation_failed",
                    "fallbackKept", fallbackKeep
            ));
        }
        // 尾部近期消息原样保留
        result.addAll(recentMessages);

        // ═══════════════════════════════════════════════════════════════
        // 压缩后校验：如果结果仍然超过预算，执行二次裁剪（从前往后丢弃）
        // ═══════════════════════════════════════════════════════════════
        int resultTokens = TokenEstimator.estimateTokens(result);
        if (resultTokens > historyBudget && result.size() > 2) {
            log.warn("[ConversationWindow] 压缩后仍超预算: {} > {}, 执行二次裁剪", resultTokens, historyBudget);
            result = trimToFit(result, historyBudget);
            resultTokens = TokenEstimator.estimateTokens(result);
        }

        // 持久化压缩边界记录（用于前端展示"上下文已压缩"卡片）
        if (summary != null && !summary.isBlank() && conversationService != null && !fromCache) {
            long spillsThisTurn = (toolResultStorage != null)
                    ? Math.max(0L, toolResultStorage.getSpillCount() - spillsAtEntry)
                    : 0L;
            Map<String, Object> boundaryMetadata = new java.util.LinkedHashMap<>();
            boundaryMetadata.put("trigger", trigger);
            boundaryMetadata.put("preTokens", preTokens);
            boundaryMetadata.put("postTokens", resultTokens);
            boundaryMetadata.put("messagesSummarized", oldMessages.size());
            boundaryMetadata.put("tailKept", recentMessages.size());
            boundaryMetadata.put("toolResultsSpilled", spillsThisTurn);
            boundaryMetadata.put("anchored", anchored);
            Long summaryId = null;
            try {
                summaryId = conversationService.saveCompressionSummaryReturningId(
                        conversationId, SUMMARY_PREFIX + summary, oldMessages.size(),
                        boundaryMetadata);
            } catch (Exception e) {
                log.warn("[ConversationWindow] Failed to persist compression boundary: {}", e.getMessage());
            }
            if (summaryId != null) {
                boundaryMetadata.put("summaryId", summaryId);
            }
            broadcastCompactStatus(conversationId, "done", boundaryMetadata);
        } else if (summary != null && !summary.isBlank() && fromCache) {
            // 缓存命中路径：不发 DB 行，但仍通知前端状态更新
            broadcastCompactStatus(conversationId, "done", Map.of(
                    "preTokens", preTokens,
                    "postTokens", resultTokens,
                    "messagesSummarized", oldMessages.size(),
                    "tailKept", recentMessages.size(),
                    "fromCache", true
            ));
        }

        return result;
    }

    // ==================== 动态 Token 预算 ====================

    /**
     * 【动态尾部边界计算】替代固定的 preserveRecentPairs 配置。
     *
     * <h3>算法</h3>
     * 从消息列表末尾向前累加每条消息的 token 估算值，直到：
     * <ol>
     *   <li>累加的 token 超过软上限（tailTokenBudget × 1.5），且</li>
     *   <li>已保留的消息数 ≥ 最小尾部消息数（minTail）</li>
     * </ol>
     *
     * <h3>与旧配置的兼容</h3>
     * 如果 protectLastMinMessages 未配置但 preserveRecentPairs 有值，
     * 则使用 pairs×2 作为 minTail（每对 = user+assistant = 2条消息）。
     *
     * @return 切割索引：消息在这个索引及之后的保留在尾部
     */
    private int findTailBoundary(List<Message> messages, int headEnd, int tailTokenBudget) {
        int n = messages.size();
        if (n <= headEnd + 1) return headEnd;

        // 最小尾部消息数：优先用新配置，兼容旧配置
        int minTail = Math.min(properties.getProtectLastMinMessages(), n - headEnd - 1);
        int pairsBased = properties.getPreserveRecentPairs() * 2;
        if (pairsBased > minTail) {
            minTail = Math.min(pairsBased, n - headEnd - 1);
        }

        // 软上限：允许超出 token 预算 50%，避免因一条大消息而丢弃所有尾部
        int softCeiling = (int) (tailTokenBudget * 1.5);
        int accumulated = 0;
        int cutIdx = n;

        // 从后往前扫描：累加 token，直到超过软上限且已满足最小消息数
        for (int i = n - 1; i >= headEnd; i--) {
            int msgTokens = TokenEstimator.estimateTokens(messages.get(i));
            if (accumulated + msgTokens > softCeiling && (n - i) >= minTail) {
                break;
            }
            accumulated += msgTokens;
            cutIdx = i;
        }

        // 兜底：至少保留 minTail 条消息
        int fallbackCut = n - minTail;
        if (cutIdx > fallbackCut) {
            cutIdx = fallbackCut;
        }

        return Math.max(cutIdx, headEnd + 1);
    }

    /**
     * Adjust the candidate boundary so an {@link AssistantMessage}'s
     * {@code toolCalls} are never separated from their matching
     * {@link ToolResponseMessage}s.
     *
     * <p>Walks forward, collecting every {@code tool_call_id}'s assistant
     * index and the indices of its matching responses. Whenever an
     * assistant in the prefix has at least one response in the tail, the
     * cut moves backward to that assistant — pulling the whole cluster
     * into the tail. The walk repeats until convergence because moving
     * the cut can expose pairs that were previously fully in the tail.
     *
     * <p>The method preserves pair integrity above any other concern. If
     * the cut collapses all the way to {@code headEnd}, callers must
     * interpret the return as "skip compaction this turn" — splitting a
     * pair would produce HTTP 400 on every OpenAI-compatible provider,
     * which is a worse failure mode than letting context grow by one turn.
     *
     * <p>An orphan {@code ToolResponseMessage} (id matching no
     * assistant in scope) does not trigger movement; the upstream code
     * paths should never produce one, and logging at WARN gives us a
     * breadcrumb if they ever do.
     *
     * @return adjusted cut index, or {@code headEnd} when no pair-safe
     *         cut larger than {@code headEnd} can be produced.
     */
    // Package-private so unit tests in the same package can drive it directly
    // without standing up a ChatModel + the rest of the compactMessages pipeline.
    int enforcePairSafeBoundary(List<Message> messages, int headEnd, int tailStart) {
        if (tailStart <= headEnd || tailStart >= messages.size()) {
            return tailStart;
        }
        int cut = tailStart;
        int safety = messages.size() + 1; // hard guard against pathological loops
        while (safety-- > 0) {
            // Map: tool_call_id -> earliest assistant index that issued it.
            java.util.Map<String, Integer> assistantIdxById = new java.util.HashMap<>();
            // Map: tool_call_id -> max response index closing it.
            java.util.Map<String, Integer> latestResponseIdxById = new java.util.HashMap<>();

            for (int i = headEnd; i < messages.size(); i++) {
                Message m = messages.get(i);
                if (m instanceof AssistantMessage am && am.getToolCalls() != null) {
                    for (AssistantMessage.ToolCall tc : am.getToolCalls()) {
                        String tid = tc.id();
                        if (tid == null || tid.isEmpty()) continue;
                        // Keep the first occurrence so the cut "snaps" to the
                        // earliest assistant for any duplicated ids; the same
                        // id should never repeat anyway.
                        assistantIdxById.putIfAbsent(tid, i);
                    }
                } else if (m instanceof ToolResponseMessage trm) {
                    for (ToolResponseMessage.ToolResponse r : trm.getResponses()) {
                        String tid = r.id();
                        if (tid == null || tid.isEmpty()) continue;
                        latestResponseIdxById.merge(tid, i, Math::max);
                    }
                }
            }

            // Find the earliest in-prefix assistant whose pair is split.
            int earliestSplitAssistant = Integer.MAX_VALUE;
            for (var e : assistantIdxById.entrySet()) {
                String id = e.getKey();
                int aIdx = e.getValue();
                Integer rIdx = latestResponseIdxById.get(id);
                if (rIdx == null) {
                    // Assistant issued a call but no response — orphan call,
                    // would already break the provider. Not a pair-split, ignore.
                    continue;
                }
                if (aIdx < cut && rIdx >= cut && aIdx < earliestSplitAssistant) {
                    earliestSplitAssistant = aIdx;
                }
                if (aIdx >= cut && rIdx < cut) {
                    log.warn("[ConversationWindow] Orphan tool response in prefix without preceding assistant in tail (id={}); leaving boundary alone",
                            id);
                }
            }

            if (earliestSplitAssistant == Integer.MAX_VALUE) {
                break; // converged: no splits remain
            }
            cut = earliestSplitAssistant;
        }

        if (cut <= headEnd) {
            log.info("[ConversationWindow] Pair-safe boundary collapsed to {} for conv: skipping compaction this turn to avoid splitting a tool_call ↔ tool_response pair",
                    headEnd);
            return headEnd;
        }

        int prefixSize = cut - headEnd;
        int minPrefix = Math.max(0, properties.getPairSafeMinPrefixToCompact());
        if (prefixSize < minPrefix) {
            log.info("[ConversationWindow] Pair-safe boundary left {} prefix message(s) (< minPrefix={}); skipping compaction",
                    prefixSize, minPrefix);
            return headEnd;
        }

        if (cut != tailStart) {
            log.info("[ConversationWindow] Pair-safe boundary moved {} -> {} to keep tool_call ↔ tool_response pairs intact",
                    tailStart, cut);
        }
        return cut;
    }

    /**
     * Build an anchor message replaying the first <em>real</em> user input
     * found in the compressed prefix. "Real" here excludes prior
     * compaction artifacts ({@link #SUMMARY_PREFIX} / {@link #ANCHOR_PREFIX}
     * messages from earlier rounds), because anchoring the previous
     * summary defeats the purpose — the model would just see "[Original
     * goal] [上下文压缩] …" pointing at compressor output, not at the user's
     * actual request.
     *
     * <p>Sizing rules:
     * <ul>
     *   <li>≤ {@code firstUserAnchorMaxTokens}: keep the original text verbatim.</li>
     *   <li>≤ 3× the budget: head+tail truncate to the budget so most of
     *       the prompt-cache benefit survives.</li>
     *   <li>&gt; 3× the budget: degrade to a 200-char pointer line so we
     *       don't blow prompt cache or the summary budget on a single
     *       message that was probably a pasted spec the model can re-read
     *       from the workspace anyway.</li>
     * </ul>
     *
     * <p>Always returns a {@link UserMessage}. {@code null} when anchoring
     * is disabled, no real first user exists in the prefix, or the body is
     * blank.
     *
     * <p>Package-private for direct unit testing — the surrounding
     * {@link #compactMessages} path needs a ChatModel and the whole
     * structured-summary pipeline, which the anchor logic does not.
     */
    Message buildFirstUserAnchor(List<Message> oldMessages) {
        if (!properties.isFirstUserAnchorEnabled()) {
            return null;
        }
        UserMessage firstUser = null;
        for (Message m : oldMessages) {
            if (!(m instanceof UserMessage um)) continue;
            String text = um.getText();
            if (text == null) continue;
            // Skip synthetic prior-round artifacts.
            if (text.startsWith(SUMMARY_PREFIX) || text.startsWith(ANCHOR_PREFIX)) {
                continue;
            }
            firstUser = um;
            break;
        }
        if (firstUser == null) return null;

        String text = firstUser.getText();
        if (text == null || text.isBlank()) return null;

        int maxAnchorTokens = Math.max(40, properties.getFirstUserAnchorMaxTokens());
        int textTokens = TokenEstimator.estimateTokens(text);

        if (textTokens <= maxAnchorTokens) {
            return new UserMessage(ANCHOR_PREFIX + text);
        }

        // > 3× budget: cheap pointer line so we don't pay token tax for a
        // gigantic pasted spec. The model still knows the original goal
        // existed without seeing the full body.
        if (textTokens > maxAnchorTokens * 3L) {
            int pointerChars = Math.min(text.length(), 200);
            String pointer = text.substring(0, pointerChars).stripTrailing()
                    + (text.length() > pointerChars ? "..." : "");
            log.info("[ConversationWindow] First-user anchor downgraded to pointer ({} tokens > 3× budget {})",
                    textTokens, maxAnchorTokens);
            return new UserMessage(ANCHOR_PREFIX + pointer);
        }

        // Within 3× — head+tail truncate to the budget. The 2 chars/token
        // ratio is a deliberate over-estimate so the anchor never inflates
        // past the configured budget on ASCII-heavy input.
        int budgetChars = Math.max(160, maxAnchorTokens * 2);
        if (budgetChars >= text.length()) {
            return new UserMessage(ANCHOR_PREFIX + text);
        }
        int headLen = (int) (budgetChars * 0.6);
        int tailLen = Math.max(40, budgetChars - headLen - 40);
        if (headLen + tailLen >= text.length()) {
            return new UserMessage(ANCHOR_PREFIX + text);
        }
        String truncated = text.substring(0, headLen)
                + "\n...[" + (text.length() - headLen - tailLen) + " chars truncated]...\n"
                + text.substring(text.length() - tailLen);
        log.info("[ConversationWindow] First-user anchor head+tail truncated ({} -> ~{} chars)",
                text.length(), truncated.length());
        return new UserMessage(ANCHOR_PREFIX + truncated);
    }

    /**
     * 计算摘要字数预算：被压缩内容 token 的 20%，不低于 500、不超过 3000。
     */
    private int computeSummaryBudget(List<Message> turnsToSummarize) {
        int contentTokens = TokenEstimator.estimateTokens(turnsToSummarize);
        int budget = (int) (contentTokens * properties.getSummaryBudgetRatio());
        return Math.max(properties.getSummaryBudgetFloor(),
                Math.min(budget, properties.getSummaryBudgetCeiling()));
    }

    // ==================== 工具结果处理 ====================

    /**
     * Backwards-compatible overload — older tool results that are oversized
     * stay verbatim because no {@link ToolResultStorage} target is in
     * scope. New call sites should use the 3-arg overload with explicit
     * {@code conversationId} and {@code workspaceBasePath} so oversized
     * bodies can be spilled to disk and recovered via {@code read_file}.
     */
    public List<Message> pruneOldToolResultsForModelInput(List<Message> messages) {
        return pruneOldToolResultsForModelInput(messages, null, null);
    }

    /**
     * 【预清理：旧工具结果优化】在压缩/发送给模型之前，对工具结果做无损或低损优化。
     *
     * <h3>处理策略（从最新到最旧扫描）：</h3>
     * <ol>
     *   <li><b>Pass Through</b> — 以下情况原样保留：
     *     <ul>
     *       <li>已溢写到磁盘的（带有 SPILL_MARKER_PREFIX 前缀）</li>
     *       <li>最新的那条工具响应（模型正在推理的）</li>
     *       <li>豁免工具（delegateToAgent/delegateParallel，子Agent不可重放）</li>
     *       <li>空内容</li>
     *     </ul>
     *   </li>
     *   <li><b>去重</b> — 与更新的轮次中内容完全相同的 → 替换为 "duplicate tool output omitted"</li>
     *   <li><b>溢写到磁盘</b> — 超大结果（超出阈值）→ 写入磁盘文件，
     *     上下文内只保留预览 + 文件路径，模型可通过 read_file 按需恢复完整内容</li>
     *   <li><b>保留原样</b> — 不满足以上条件的，宁可多传几个 token 也不错删</li>
     * </ol>
     *
     * <h3>设计原则</h3>
     * 早期版本对所有旧工具结果做"一刀切"的损失性摘要，在实际使用中破坏了
     * 长任务的上下文连贯性。现在的策略是：优先无损优化（去重、溢写），
     * 只在确实需要时才进入 S1-S3 的损失性压缩。
     */
    public List<Message> pruneOldToolResultsForModelInput(List<Message> messages,
                                                          String conversationId,
                                                          String workspaceBasePath) {
        if (messages == null || messages.isEmpty()) {
            return messages;
        }
        int latestToolResponseIndex = -1;
        for (int i = messages.size() - 1; i >= 0; i--) {
            if (messages.get(i) instanceof ToolResponseMessage) {
                latestToolResponseIndex = i;
                break;
            }
        }
        if (latestToolResponseIndex <= 0) {
            return messages;
        }

        boolean canSpill = toolResultStorage != null
                && conversationId != null && !conversationId.isEmpty();

        List<Message> pruned = new ArrayList<>(messages);
        java.util.Set<String> seenLargeOutputs = new java.util.HashSet<>();
        int changed = 0;
        int spilled = 0;
        for (int i = pruned.size() - 1; i >= 0; i--) {
            if (!(pruned.get(i) instanceof ToolResponseMessage trm)) {
                continue;
            }
            boolean keepFull = i == latestToolResponseIndex;
            List<ToolResponseMessage.ToolResponse> newResponses = new ArrayList<>();
            boolean messageChanged = false;
            for (ToolResponseMessage.ToolResponse r : trm.getResponses()) {
                String data = r.responseData();
                String name = r.name();
                boolean exempt = name != null && PRUNE_EXEMPT_TOOLS.contains(name);
                boolean alreadySpilled = data != null
                        && data.startsWith(ToolResultStorage.SPILL_MARKER_PREFIX);

                // Pass through: the latest response, exempt tools, empty bodies,
                // already-spilled previews — none should be rewritten.
                if (keepFull || exempt || data == null || data.isEmpty() || alreadySpilled) {
                    newResponses.add(r);
                    if (data != null && data.length() > DEDUP_MIN_CHARS) {
                        seenLargeOutputs.add(data);
                    }
                    continue;
                }

                // Dedup: identical body seen in a later turn already.
                if (data.length() > DEDUP_MIN_CHARS && seenLargeOutputs.contains(data)) {
                    String replacement = "[" + name
                            + "] duplicate tool output omitted; same content appeared later.";
                    newResponses.add(new ToolResponseMessage.ToolResponse(r.id(), name, replacement));
                    messageChanged = true;
                    continue;
                }

                // Spill on demand: route oversized bodies to disk so the model
                // can read_file them rather than losing them to a lossy summary.
                if (canSpill) {
                    String candidate = toolResultStorage.persistIfOversized(
                            data, name, r.id(), conversationId, workspaceBasePath);
                    if (candidate != null
                            && candidate.startsWith(ToolResultStorage.SPILL_MARKER_PREFIX)) {
                        newResponses.add(new ToolResponseMessage.ToolResponse(r.id(), name, candidate));
                        seenLargeOutputs.add(data);
                        messageChanged = true;
                        spilled++;
                        continue;
                    }
                    // returned unchanged: under threshold, excluded tool, or write failed.
                    // Fall through to "keep verbatim".
                }

                // Default: keep the body verbatim. Better to send a few extra
                // tokens than to silently destroy data the model might need.
                newResponses.add(r);
                if (data.length() > DEDUP_MIN_CHARS) {
                    seenLargeOutputs.add(data);
                }
            }
            if (messageChanged) {
                pruned.set(i, ToolResponseMessage.builder().responses(newResponses).build());
                changed++;
            }
        }
        if (changed > 0) {
            log.info("[ConversationWindow] Pruned {} older tool response message(s) ({} spilled to disk) before model request",
                    changed, spilled);
        }
        return changed > 0 ? pruned : messages;
    }

    /**
     * Spill-marker responses already point at an on-disk full copy via
     * {@code path=...} in their body. Trimming, replacing, or pre-pruning
     * them would destroy the very pointer the model needs to recover the
     * original output with {@code read_file} — which is the whole reason
     * we spilled in the first place. All three compaction phases consult
     * this guard before touching a response.
     */
    static boolean isSpillMarker(ToolResponseMessage.ToolResponse r) {
        return r != null
                && r.responseData() != null
                && r.responseData().startsWith(ToolResultStorage.SPILL_MARKER_PREFIX);
    }

    /**
     * Age-based compaction. Replace bodies of all tool responses older than
     * the {@code keepRecentN} most recent with a one-line placeholder, while
     * preserving the toolCallId and tool name so the assistant/tool pairing
     * remains valid and the model still sees "I called X earlier" in history.
     *
     * <p>Complementary to {@link #pruneOldToolResultsForModelInput}: that pass
     * targets oversized or duplicate bodies regardless of age (and may spill
     * to disk); this one targets aged bodies regardless of size. Both can run
     * in any order — the intersection collapses to the same placeholder.
     *
     * <p>Spill-marker bodies retain their on-disk {@code path=} pointer
     * inside the placeholder so a later {@code read_file} can still recover
     * the original output. {@link #PRUNE_EXEMPT_TOOLS} (sub-agent delegations)
     * bypass the pass entirely — their transcripts are not replayable.
     *
     * @param messages     full conversation in chronological order
     * @param keepRecentN  number of newest {@link ToolResponseMessage}s kept
     *                     verbatim; older ones are compacted. Negative or zero
     *                     disables the pass.
     */
    public List<Message> compactAgedToolResponses(List<Message> messages, int keepRecentN) {
        if (messages == null || messages.isEmpty() || keepRecentN <= 0) {
            return messages;
        }
        List<Message> out = new ArrayList<>(messages);
        int seen = 0;
        int compacted = 0;
        boolean anyChange = false;
        for (int i = out.size() - 1; i >= 0; i--) {
            if (!(out.get(i) instanceof ToolResponseMessage trm)) {
                continue;
            }
            if (seen < keepRecentN) {
                seen++;
                continue;
            }
            seen++;

            List<ToolResponseMessage.ToolResponse> newResponses =
                    new ArrayList<>(trm.getResponses().size());
            boolean messageChanged = false;
            for (ToolResponseMessage.ToolResponse r : trm.getResponses()) {
                String body = r.responseData();
                String name = r.name();
                boolean exempt = name != null && PRUNE_EXEMPT_TOOLS.contains(name);
                if (exempt || body == null || body.isEmpty()) {
                    newResponses.add(r);
                    continue;
                }
                String placeholder = buildAgedPlaceholder(name, body);
                if (placeholder.length() < body.length()) {
                    newResponses.add(new ToolResponseMessage.ToolResponse(r.id(), name, placeholder));
                    messageChanged = true;
                    compacted++;
                } else {
                    // Body is already shorter than the placeholder would be —
                    // collapsing it would only add tokens. Keep verbatim.
                    newResponses.add(r);
                }
            }
            if (messageChanged) {
                out.set(i, ToolResponseMessage.builder().responses(newResponses).build());
                anyChange = true;
            }
        }
        if (compacted > 0) {
            log.info("[ConversationWindow] Aged-compacted {} tool response entries (keepRecent={}) before model request",
                    compacted, keepRecentN);
        }
        return anyChange ? out : messages;
    }

    /**
     * Build the one-line "old tool output cleared" body. When the original
     * was a spill marker, extract its {@code path=} hint so the model can
     * still recover the full output via {@code read_file} on demand.
     */
    static String buildAgedPlaceholder(String toolName, String body) {
        String safeName = (toolName == null || toolName.isBlank()) ? "tool" : toolName;
        if (body != null && body.startsWith(ToolResultStorage.SPILL_MARKER_PREFIX)) {
            int idx = body.indexOf(" path=");
            if (idx >= 0) {
                int end = body.indexOf('\n', idx);
                String path = (end > 0 ? body.substring(idx + 6, end) : body.substring(idx + 6)).trim();
                if (!path.isEmpty()) {
                    return "[Old tool output cleared — '" + safeName
                            + "' result was spilled to " + path
                            + "; use read_file on that path if you still need it.]";
                }
            }
        }
        return "[Old tool output cleared — '" + safeName
                + "' can be called again if its result is needed.]";
    }

    /**
     * Phase 1 - Soft trim：对工具结果做 head+tail 裁剪（保留首尾各 200 字符）。
     * <p>Spill-marker responses are left untouched so their on-disk pointer
     * survives intact across compaction.
     */
    int softTrimToolResults(List<Message> messages) {
        int trimmed = 0;
        for (int i = 0; i < messages.size(); i++) {
            if (messages.get(i) instanceof ToolResponseMessage trm) {
                List<ToolResponseMessage.ToolResponse> newResponses = new ArrayList<>();
                boolean changed = false;
                for (ToolResponseMessage.ToolResponse r : trm.getResponses()) {
                    if (isSpillMarker(r)) {
                        // Pointer + preview already; trimming would lose the path.
                        newResponses.add(r);
                        continue;
                    }
                    String data = r.responseData();
                    if (data != null && data.length() > 500) {
                        String marker = "\n...[trimmed " + data.length() + " chars; "
                                + StructuredTruncator.FIDELITY_NOTE + "]...\n";
                        newResponses.add(new ToolResponseMessage.ToolResponse(
                                r.id(), r.name(), StructuredTruncator.truncate(data, 200, 200, marker)));
                        changed = true;
                    } else {
                        newResponses.add(r);
                    }
                }
                if (changed) {
                    messages.set(i, ToolResponseMessage.builder().responses(newResponses).build());
                    trimmed++;
                }
            }
        }
        return trimmed;
    }

    /**
     * Phase 2 - Hard clear：将所有旧工具结果替换为占位符。
     * <p>Spill-marker responses are left untouched so the on-disk pointer
     * survives — a placeholder here would force the model to abandon a
     * tool output it could otherwise recover via {@code read_file}.
     */
    int hardClearToolResults(List<Message> messages) {
        int cleared = 0;
        for (int i = 0; i < messages.size(); i++) {
            if (messages.get(i) instanceof ToolResponseMessage trm) {
                boolean changed = false;
                List<ToolResponseMessage.ToolResponse> replaced = new ArrayList<>(trm.getResponses().size());
                for (ToolResponseMessage.ToolResponse r : trm.getResponses()) {
                    if (isSpillMarker(r)) {
                        replaced.add(r);
                        continue;
                    }
                    replaced.add(new ToolResponseMessage.ToolResponse(r.id(), r.name(), "[tool result removed]"));
                    changed = true;
                }
                if (changed) {
                    messages.set(i, ToolResponseMessage.builder().responses(replaced).build());
                    cleared++;
                }
            }
        }
        return cleared;
    }

    /**
     * Phase 3 Pre-prune：在 LLM 摘要前，将工具输出替换为占位符（减少摘要输入 token）。
     * <p>Spill-marker responses are left untouched so the summary input
     * still has the on-disk path the model might cite back in its summary.
     */
    int prePruneForSummary(List<Message> messages) {
        int pruned = 0;
        for (int i = 0; i < messages.size(); i++) {
            if (messages.get(i) instanceof ToolResponseMessage trm) {
                boolean hasSubstantial = trm.getResponses().stream()
                        .anyMatch(r -> !isSpillMarker(r)
                                && r.responseData() != null
                                && r.responseData().length() > 200);
                if (hasSubstantial) {
                    List<ToolResponseMessage.ToolResponse> placeholders = new ArrayList<>(trm.getResponses().size());
                    for (ToolResponseMessage.ToolResponse r : trm.getResponses()) {
                        if (isSpillMarker(r)) {
                            placeholders.add(r);
                            continue;
                        }
                        placeholders.add(new ToolResponseMessage.ToolResponse(r.id(), r.name(),
                                "[旧工具输出已清理以节省上下文空间]"));
                    }
                    messages.set(i, ToolResponseMessage.builder().responses(placeholders).build());
                    pruned++;
                }
            }
        }
        return pruned;
    }

    // ==================== LLM 摘要生成（结构化 + 迭代更新） ====================

    /**
     * 生成结构化摘要。支持首次压缩和迭代更新两种模式。
     * 包含冷却机制：LLM 调用失败后 10 分钟内不重试。
     */
    private String generateSummary(List<Message> oldMessages, ChatModel chatModel,
                                   String conversationId, int summaryBudget,
                                   String memoryExtraContext) {
        // 冷却检查
        if (isInSummaryCooldown(conversationId)) {
            log.info("[ConversationWindow] 摘要在冷却中，跳过 LLM 调用, conv={}", conversationId);
            return null;
        }

        try {
            String conversationText = serializeForSummary(oldMessages);

            // 如果 MemoryProvider 有额外上下文，追加到对话文本中
            if (memoryExtraContext != null && !memoryExtraContext.isBlank()) {
                conversationText += "\n\n[Memory Provider 补充上下文]\n" + memoryExtraContext;
            }

            String previousSummary = previousSummaries.get(conversationId);
            String systemPrompt;
            String userPrompt;

            // System prompt always carries the budget directive; both branches
            // must replace the placeholder. The previous code applied the
            // replace only on the first-compression branch, so iterative-mode
            // calls leaked the literal "{summary_budget}" string to the LLM.
            systemPrompt = STRUCTURED_SUMMARY_SYSTEM
                    .replace("{summary_budget}", String.valueOf(summaryBudget));
            if (previousSummary != null) {
                // Iterative update: previous summary + new turns.
                userPrompt = STRUCTURED_SUMMARY_UPDATE
                        .replace("{previous_summary}", previousSummary)
                        .replace("{conversation}", conversationText);
                log.debug("[ConversationWindow] 使用迭代更新模式（第 {} 次压缩）, conv={}",
                        compressionCounts.getOrDefault(conversationId, 0) + 1, conversationId);
            } else {
                userPrompt = STRUCTURED_SUMMARY_USER
                        .replace("{conversation}", conversationText);
                log.debug("[ConversationWindow] 使用首次压缩模式, conv={}", conversationId);
            }

            List<Message> promptMessages = new ArrayList<>();
            promptMessages.add(new SystemMessage(systemPrompt));
            promptMessages.add(new UserMessage(userPrompt));

            ChatOptions options = DashScopeChatOptions.builder()
                    .withMaxToken(properties.getSummaryMaxTokens())
                    .build();

            ChatResponse response = chatModel.call(new Prompt(promptMessages, options));
            if (response != null && response.getResult() != null
                    && response.getResult().getOutput() != null) {
                String summary = response.getResult().getOutput().getText();
                if (summary != null && !summary.isBlank()) {
                    // 成功：保存摘要供下次迭代更新，清除冷却
                    previousSummaries.put(conversationId, summary);
                    clearSummaryCooldown(conversationId);
                    return summary;
                }
            }
            log.warn("[ConversationWindow] LLM 摘要返回空结果, conv={}", conversationId);
            setSummaryCooldown(conversationId);
            return null;

        } catch (Exception e) {
            log.warn("[ConversationWindow] LLM 摘要生成失败（进入 {} 秒冷却）: {}, conv={}",
                    SUMMARY_COOLDOWN_MS / 1000, e.getMessage(), conversationId);
            setSummaryCooldown(conversationId);
            return null;
        }
    }

    // ==================== 消息序列化（智能截断） ====================

    /**
     * 将消息列表序列化为摘要 LLM 可消化的文本格式。
     * 长内容做 head+tail 截断，比简单截断保留更多信息。
     */
    private String serializeForSummary(List<Message> messages) {
        StringBuilder sb = new StringBuilder();
        for (Message msg : messages) {
            String role = switch (msg) {
                case UserMessage ignored -> "[USER]";
                case SystemMessage ignored -> "[SYSTEM]";
                case AssistantMessage ignored -> "[ASSISTANT]";
                case ToolResponseMessage ignored -> "[TOOL RESULT]";
                default -> "[OTHER]";
            };

            String text = msg.getText();
            if (text != null && text.length() > CONTENT_MAX) {
                String marker = "\n...[truncated " + text.length() + " chars; "
                        + StructuredTruncator.FIDELITY_NOTE + "]...\n";
                text = StructuredTruncator.truncate(text, CONTENT_HEAD, CONTENT_TAIL, marker);
            }

            sb.append(role).append(": ").append(text != null ? text : "").append("\n\n");
        }
        return sb.toString();
    }

    // ==================== 辅助方法 ====================

    /**
     * 二次裁剪：从前往后移除消息直到 token 预算满足。
     */
    private List<Message> trimToFit(List<Message> messages, int budget) {
        int startIndex = 0;
        int totalTokens = TokenEstimator.estimateTokens(messages);

        while (totalTokens > budget && startIndex < messages.size() - 2) {
            totalTokens -= TokenEstimator.estimateTokens(messages.get(startIndex));
            startIndex++;
        }

        if (startIndex > 0) {
            log.info("[ConversationWindow] 二次裁剪移除 {} 条消息, 最终 {} tokens", startIndex, totalTokens);
            return new ArrayList<>(messages.subList(startIndex, messages.size()));
        }
        return messages;
    }

    // ==================== PTL 紧急压缩 ====================

    /**
     * Structured PTL (Prompt Too Long) recovery — reuses the full
     * {@link #compactMessages} pipeline (pair-safe boundary, soft/hard
     * trim, MemoryProvider hook, LLM summary, anchor of the first user
     * goal) under a forced-tight history budget so the retry actually fits.
     * <p>
     * Differences vs the {@link #compactForRetry(List)} fallback:
     * <ul>
     *   <li>Preserves the original user goal via anchor instead of dropping
     *       it with the head — long tasks lose context every PTL otherwise.</li>
     *   <li>Pair-safe cuts, so the retry doesn't break a
     *       {@code AssistantMessage.tool_calls} / {@code ToolResponseMessage}
     *       cluster and 400 the provider a second time.</li>
     *   <li>Runs through summary generation so semantic continuity (user
     *       preferences, completed steps) survives the trim.</li>
     *   <li>Tags the persisted boundary row with
     *       {@code trigger=prompt_too_long} so the summary is retrievable
     *       via the same {@code mate_conversation_summary} schema as a
     *       normal token-threshold compaction.</li>
     * </ul>
     * <p>
     * A 60s cooldown ({@link #PTL_FORCE_LLM_COOLDOWN_MS}) downgrades the
     * second-and-subsequent PTL hit on one conversation to tail-only, so
     * a model stuck in a tool-call retry loop can't drag the summary LLM
     * along with it.
     *
     * @param messages       Current history that overflowed the model window.
     * @param chatModel      Used for the summary generation step.
     * @param conversationId Cooldown / cache key.
     * @param agentId        Drives the {@code MemoryProvider.onPreCompress}
     *                       hook. Nullable — the hook is a no-op when null.
     * @return Compacted history with summary + anchor + tail, or the
     *         {@link #compactForRetry(List)} tail-only fallback when the
     *         cooldown is active or the structured pass produces no
     *         reduction. {@code null} when the input is too small to
     *         compact (matches the legacy contract).
     */
    public List<Message> compactForRetry(List<Message> messages,
                                          ChatModel chatModel,
                                          String conversationId,
                                          Long agentId) {
        if (messages == null || messages.size() <= 2) {
            return null;
        }

        // Sweep the cooldown map on every PTL entry. The summaryCache sweep
        // already covers normal-compaction traffic via fitToWindow; without
        // this call here, a conversation that only ever hits PTL never
        // releases its ptlForceCompactAt entry.
        evictExpiredEntries();

        // Race-safe claim: compute is atomic per key, so two concurrent
        // PTL hits on the same conv can't both pass the cooldown check.
        // The {@code claimed} flag is set inside the atomic block so we can
        // distinguish "this call's stamp won" from "previous call's stamp
        // happened to equal our now" (Windows clock has 15 ms granularity —
        // identity-on-timestamp would misfire for back-to-back invocations).
        long now = System.currentTimeMillis();
        final boolean[] claimed = {false};
        ptlForceCompactAt.compute(conversationId, (k, prev) -> {
            if (prev != null && now - prev < PTL_FORCE_LLM_COOLDOWN_MS) {
                claimed[0] = false;
                return prev;
            }
            claimed[0] = true;
            return now;
        });
        if (!claimed[0]) {
            long prevStamp = ptlForceCompactAt.getOrDefault(conversationId, now);
            long remainingMs = Math.max(0L, PTL_FORCE_LLM_COOLDOWN_MS - (now - prevStamp));
            log.warn("[ConversationWindow] PTL cooldown active for conv={} (remaining {} ms), falling back to tail-only",
                    conversationId, remainingMs);
            broadcastCompactStatus(conversationId, "ptl_cooldown_skipped", Map.of(
                    "trigger", "prompt_too_long",
                    "cooldownRemainingMs", remainingMs));
            return compactForRetry(messages);
        }

        int currentTokens = TokenEstimator.estimateTokens(messages);
        // Force the history budget into the bottom quartile of current size
        // — but never under 2k so the post-trim window still has room for
        // summary + anchor + a couple of recent turns. Tail budget is one
        // quarter of that so the recent window doesn't dominate.
        int forcedBudget = Math.max(2000, currentTokens / 4);
        int forcedTailBudget = forcedBudget / 4;

        log.warn("[ConversationWindow] PTL forced compaction: messages={}, currentTokens={}, forcedBudget={}, forcedTail={}",
                messages.size(), currentTokens, forcedBudget, forcedTailBudget);

        // Note: no separate "ptl_start" broadcast — the inner compactMessages
        // call broadcasts "start" with trigger="prompt_too_long" in its
        // payload, which is sufficient differentiation for the frontend
        // (one event per compaction, with the trigger field carrying the
        // semantic distinction).

        // Spill count is the manager's private view of toolResultStorage —
        // computed inside the manager so callers don't need to touch the
        // storage SPI.
        long spillsAtEntry = (toolResultStorage != null) ? toolResultStorage.getSpillCount() : 0L;

        List<Message> compacted = compactMessages(messages, forcedBudget, forcedTailBudget,
                chatModel, conversationId, agentId, currentTokens, spillsAtEntry,
                "prompt_too_long");

        if (compacted == messages || TokenEstimator.estimateTokens(compacted) >= currentTokens) {
            log.warn("[ConversationWindow] PTL structured compaction had no effect for conv={}, falling back to tail-only",
                    conversationId);
            return compactForRetry(messages);
        }
        return compacted;
    }

    /**
     * PTL (Prompt Too Long) 恢复用的紧急压缩。
     * 不调用 LLM 摘要，直接丢弃较旧消息，只保留最近 4 条。
     */
    public List<Message> compactForRetry(List<Message> messages) {
        if (messages == null || messages.size() <= 2) {
            return null;
        }

        int preserveCount = Math.min(4, messages.size());
        int splitPoint = messages.size() - preserveCount;

        if (splitPoint <= 0) {
            return null;
        }

        List<Message> recentMessages = new ArrayList<>(messages.subList(splitPoint, messages.size()));
        log.info("[ConversationWindow] PTL 紧急压缩: {} -> {} 条消息 (丢弃 {} 条旧消息)",
                messages.size(), recentMessages.size(), splitPoint);
        return recentMessages;
    }

    // ==================== 冷却机制 ====================

    private boolean isInSummaryCooldown(String conversationId) {
        Long until = summaryCooldownUntil.get(conversationId);
        return until != null && System.currentTimeMillis() < until;
    }

    private void setSummaryCooldown(String conversationId) {
        summaryCooldownUntil.put(conversationId, System.currentTimeMillis() + SUMMARY_COOLDOWN_MS);
    }

    private void clearSummaryCooldown(String conversationId) {
        summaryCooldownUntil.remove(conversationId);
    }

    // ==================== 缓存管理 ====================

    private void evictExpiredEntries() {
        summaryCache.entrySet().removeIf(entry -> entry.getValue().isExpired(CACHE_TTL_MS));
        long ptlCutoff = System.currentTimeMillis() - PTL_FORCE_LLM_COOLDOWN_MS;
        ptlForceCompactAt.entrySet().removeIf(entry -> entry.getValue() < ptlCutoff);
    }

    record CachedSummary(String summary, long createdAt) {
        boolean isExpired(long ttlMs) {
            return System.currentTimeMillis() - createdAt > ttlMs;
        }
    }
}
