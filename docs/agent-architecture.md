# Agent 调用流程全景架构图

## 1. 总体三层架构

```
┌──────────────────────────────────────────────────────────────────────────────────┐
│                              前端 / IM 渠道                                       │
│          浏览器 SSE EventSource          │          QQ/微信/飞书 Webhook          │
└──────────────────┬───────────────────────┴───────────────────┬───────────────────┘
                   │ GET /api/v1/agents/{id}/chat/stream       │
                   │ POST /api/v1/agents/{id}/chat             │
                   │ POST /api/v1/agents/{id}/execute          │
                   ▼                                            ▼
┌──────────────────────────────────────────────────────────────────────────────────┐
│                            【第1层】HTTP 入口层                                    │
│                                                                                  │
│  ┌────────────────────────────────────────────────────────────────────────┐     │
│  │                AgentController  (HTTP Controller)                       │     │
│  │                                                                         │     │
│  │  chatStream(id, msg, convId)                                            │     │
│  │    │ 1. 校验 Agent enabled + workspace 权限                              │     │
│  │    │ 2. 创建 Utf8SseEmitter (超时5分钟)                                  │     │
│  │    │ 3. 异步执行: agentService.chatStream() → Flux → emitter.send()     │     │
│  │    │ 4. 返回 SseEmitter 给前端                                          │     │
│  │    ▼                                                                     │     │
│  │  chat(id, msg, convId)        → agentService.chat()                     │     │
│  │  execute(id, goal, convId)    → agentService.execute()                  │     │
│  └──────────────────────────────────┬─────────────────────────────────────┘     │
└─────────────────────────────────────┼───────────────────────────────────────────┘
                                      │
                                      ▼
┌──────────────────────────────────────────────────────────────────────────────────┐
│                         【第2层】业务服务层                                         │
│                                                                                  │
│  ┌────────────────────────────────────────────────────────────────────────┐     │
│  │                      AgentService  (门面 + 缓存)                         │     │
│  │                                                                         │     │
│  │  职责: 1. CRUD (agentMapper)  2. 缓存 agentInstances                    │     │
│  │        3. 生命周期中介器钩子 (memoryLifecycleMediator)                     │     │
│  │                                                                         │     │
│  │  chatStream(agentId, msg, convId)                                       │     │
│  │    │ ① memoryRecallTracker.trackRecalls()  — 记忆召回追踪                │     │
│  │    │ ② getOrBuildAgentForConversation(agentId, convId)                  │     │
│  │    │       │                                                            │     │
│  │    │       ├─ 查 conv pinned model → modelKey                           │     │
│  │    │       └─ agentInstances.computeIfAbsent(agentId)                   │     │
│  │    │             .computeIfAbsent(modelKey, ...)                         │     │
│  │    │                 └─→ agentGraphBuilder.build(entity, ...)  ───────┐ │     │
│  │    │ ③ Wraps with Lifecycle hooks (beforeLlmCall / afterLlmCall)     │ │     │
│  │    │ ④ flux → emitter.send(event) → SSE 推送到前端                    │ │     │
│  │    ▼                                                                   │ │     │
│  │  chatStructuredStream(...)                                             │ │     │
│  │    │ ⑤ 检查 agent instanceof StructuredStreamCapable                  │ │     │
│  │    │ ⑥ capable.chatStructuredStream(msg, convId, requesterId) ──────┐ │ │     │
│  │    │                                                                 │ │ │     │
│  │  缓存结构:                                                            │ │ │     │
│  │    Map<agentId, Map<modelKey, BaseAgent>>                             │ │ │     │
│  │    modelKey="" → Agent/全局默认模型                                     │ │ │     │
│  │    modelKey="provider::model" → 会话pinned模型                          │ │ │     │
│  └───────────────────────────────────────────────────────────────────────┘ │ │     │
└──────────────────────────────────────────────────────────────────────────┼──┼────┘
                                                                           │  │
                      ┌────────────────────────────────────────────────────┘  │
                      ▼                                                       │
┌─────────────────────────────────────────────────────────────────────────────┼──┐
│                       【第3层】图构建与执行层                                  │  │
│                                                                             │  │
│  ┌─────────────────────────────────────────────────────────────────────────┐│  │
│  │                    AgentGraphBuilder  (工厂)                             ││  │
│  │                                                                         ││  │
│  │  build(entity, modelProvider, modelName)                                ││  │
│  │    │ 1. 解析工具集 (toolRegistry + agentBinding + skillBinding)          ││  │
│  │    │ 2. 解析模型 (conversation pin > agent override > global default)   ││  │
│  │    │ 3. 构建 System Prompt (identity + memory + tool guidance + wiki)   ││  │
│  │    │ 4. ★ 模式分支 ★                                                   ││  │
│  │    │    ├─ agentType="plan_execute" → buildPlanExecuteAgent() ───────┐ ││  │
│  │    │    └─ 其他(默认)               → buildReActAgent() ──────────┐  │ ││  │
│  │    │                                                               │  │ ││  │
│  │    │ 5. 注入公共属性: agentId, systemPrompt, maxIterations, etc    │  │ ││  │
│  │    │ 6. 返回 BaseAgent 实例 → 缓存到 AgentService.agentInstances   │  │ ││  │
│  │    └───────────────────────────────────────────────────────────────┼──┼─┘│  │
│  └────────────────────────────────────────────────────────────────────┼──┼──┘  │
│                                                                       │  │     │
│                   ┌───────────────────────────────────────────────────┘  │     │
│                   ▼                                                      │     │
│     ┌─────────────────────────────────┐   ┌─────────────────────────────▼──┐  │
│     │  StateGraphReActAgent            │   │  StateGraphPlanExecuteAgent    │  │
│     │  (ReAct 模式)                    │   │  (Plan-Execute 模式)           │  │
│     │                                 │   │                                │  │
│     │  extends BaseAgent              │   │  extends BaseAgent              │  │
│     │  implements StructuredStream-   │   │  implements StructuredStream-  │  │
│     │    Capable                      │   │    Capable                     │  │
│     │                                 │   │                                │  │
│     │  compiledGraph.stream() ───────────→  │  compiledGraph.stream() ────────→│
│     │    ↓                            │   │    ↓                           │  │
│     │    flatMapIterable:             │   │    flatMapIterable:            │  │
│     │    - 提取 PENDING_EVENTS        │   │    - 提取 PENDING_EVENTS       │  │
│     │    - 提取 FINAL_ANSWER          │   │    - 提取 FINAL_ANSWER         │  │
│     │    - 防重 (compareAndSet)       │   │    - 防重 (compareAndSet)      │  │
│     │    - 追加 _usage_final event    │   │    - 追加 _usage_final event   │  │
│     │   → Flux<StreamDelta>           │   │   → Flux<StreamDelta>          │  │
│     └────────────┬────────────────────┘   └───────────────┬────────────────┘  │
│                  │                                        │                   │
└──────────────────┼────────────────────────────────────────┼───────────────────┘
                   │                                        │
                   ▼                                        ▼
┌──────────────────────────────────────────────────────────────────────────────────┐
│                         【第4层】StateGraph 引擎层                                  │
│                                                                                  │
│  框架: com.alibaba.cloud.ai.graph (= Spring AI Alibaba Graph)                     │
│                                                                                  │
│  ┌──────────────────────────────────┐   ┌──────────────────────────────────────┐ │
│  │     ReAct 图拓扑 (5节点)          │   │     Plan-Execute 图拓扑 (4+1节点)     │ │
│  │                                  │   │                                      │ │
│  │ START                            │   │ START                                │ │
│  │   ↓                              │   │   ↓                                  │ │
│  │ [ReasoningNode] ←────────────┐   │   │ [PlanGenerationNode] (分流+规划)     │ │
│  │   ├─── needsToolCall=true ──→│   │   │   ↓                                 │ │
│  │   │    [ActionNode]          │   │   │ PlanGenerationDispatcher             │ │
│  │   │      ↓                   │   │   │   ├─ needsPlanning=false             │ │
│  │   │    [ObservationNode]     │   │   │   │   └→ [DirectAnswerNode]          │ │
│  │   │      ↓                   │   │   │   └─ needsPlanning=true              │ │
│  │   │    ObservationDispatcher─┼───┘   │   │       └→ [StepExecutionNode]     │ │
│  │   │    ├→ ReasoningNode (循环)│   │   │           │ (while循环, LLM+工具)    │ │
│  │   │    ├→ SummarizingNode    │   │   │           ↓                          │ │
│  │   │    ├→ LimitExceededNode  │   │   │       StepProgressDispatcher         │ │
│  │   │    └→ FinalAnswerNode    │   │   │           ├→ StepExecutionNode (循环) │ │
│  │   │                          │   │   │           ├→ awaiting_approval → END  │ │
│  │   ├─── shouldSummarize=true  │   │   │           ├→ plan_aborted → END      │ │
│  │   │    [SummarizingNode] ────┘   │   │           └→ [PlanSummaryNode]       │ │
│  │   │      压缩上下文后回到 Reasoning│   │                   ↓                  │ │
│  │   │                              │   │           (active goal?)              │ │
│  │   ├─── 直接回答                  │   │           ├→ GoalEvaluationNode       │ │
│  │   │    [FinalAnswerNode]         │   │           └→ END                      │ │
│  │   │                              │   │                                      │ │
│  │   └─── 超限                      │   │   [GoalEvaluationNode] (共享节点)     │ │
│  │        [LimitExceededNode]       │   │     ↓                                │ │
│  │              ↓                   │   │   GoalEvaluationDispatcher            │ │
│  │        [FinalAnswerNode]         │   │     ├→ needFollowup → PlanGeneration  │ │
│  │              ↓                   │   │     └→ END                            │ │
│  │       (active goal?)             │   │                                      │ │
│  │        ├→ GoalEvaluationNode     │   │                                      │ │
│  │        └→ END                    │   │                                      │ │
│  │                                  │   │                                      │ │
│  │ [GoalEvaluationNode] (共享)      │   │                                      │ │
│  │   ↓                              │   │                                      │ │
│  │ GoalEvaluationDispatcher         │   │                                      │ │
│  │   ├→ needFollowup → ReasoningNode│   │                                      │ │
│  │   └→ END                         │   │                                      │ │
│  └──────────────────────────────────┘   └──────────────────────────────────────┘ │
│                                                                                  │
└──────────────────────────────────────────────────────────────────────────────────┘
```

## 2. ReAct 模式完整调用链

```
HTTP Request
  │
  ▼
AgentController.chatStream(id, message, conversationId)
  │  ① 校验 Agent enabled + workspace 权限
  │  ② new Utf8SseEmitter(5*60*1000L)
  │  ③ sseExecutor.execute(() -> {
  │       agentService.chatStream(id, message, conversationId)
  │         .doOnNext(chunk → emitter.send(event("message", chunk)))
  │         .doOnComplete(→ send [DONE] + emitter.complete())
  │         .doOnError(emitter::completeWithError)
  │         .subscribe()
  │     })
  │  ④ return emitter;
  │
  ▼
AgentService.chatStream(agentId, message, conversationId)
  │  ① memoryRecallTracker.trackRecalls()
  │  ② agent = getOrBuildAgentForConversation(agentId, conversationId)
  │     └─ agentInstances.computeIfAbsent(agentId, ...)
  │        .computeIfAbsent(modelKey, key → agentGraphBuilder.build(entity, ...))
  │
  │  ③ Flux.defer(() → {
  │       ChatOriginHolder.set(origin)
  │       return withLifecycleFlux(agentId, message, conversationId,
  │                  (msg, convId) → agent.chatStream(msg, convId),
  │                  chunk → chunk)
  │     }).doFinally(signal → ChatOriginHolder.clear())
  │
  ▼
StateGraphReActAgent.chatStructuredStream(userMessage, conversationId, requesterId)
  │  ① setState(RUNNING)
  │  ② inputs = buildInitialState(userMessage, conversationId)
  │     │ // 加载对话历史 → buildConversationHistory()
  │     │ // 上下文窗口裁剪 → fitToWindow()
  │     │ // 构建当前用户消息 → buildCurrentUserMessageWithRouting()
  │     │ // 注入: USER_MESSAGE, MESSAGES, MAX_ITERATIONS, SYSTEM_PROMPT, ACTIVE_GOAL...
  │     │ //      ChatOrigin, RoutingDecision, etc.
  │     └─ return Map<String,Object>
  │
  │  ③ return compiledGraph.stream(inputs, config)     ← ★ 图引擎启动
  │          .flatMapIterable(output → {
  │            // 提取 PENDING_EVENTS (工具开始/结束、阶段切换)
  │            // 检查 FINAL_ANSWER (防重: compareAndSet)
  │            // 提取 FINAL_THINKING
  │            // 提取 STREAMED_CONTENT (中间推理 → segmentOnly)
  │            // 累计 token usage
  │            return List<StreamDelta>
  │          })
  │          .concatWith(_usage_final event)
  │
  ▼
compiledGraph.stream(inputs, config)
  │
  ├─ [ReasoningNode.apply(state)]
  │    │ ① 从 state 读取 MESSAGES + SYSTEM_PROMPT
  │    │ ② 组装 Prompt: system + 技能目录 + wiki + progressLedger
  │    │ ③ chatModel.call(prompt)  ← LLM 推理
  │    │ ④ NodeStreamingChatHelper.stream() → SSE 实时推 content/thinking
  │    │ ⑤ 解析返回:
  │    │    - 有 toolCalls → TOOL_CALLS, NEEDS_TOOL_CALL=true
  │    │    - 无 toolCalls → FINAL_ANSWER + FINISH_REASON=NORMAL
  │    │ ⑥ return outputMap
  │    │
  │    ├──→ ReasoningDispatcher.apply(state)
  │    │      ├─ iteration ≥ max?  → LimitExceededNode
  │    │      ├─ !needsToolCall?   → FinalAnswerNode
  │    │      ├─ llmCallCount≥5x?  → LimitExceededNode
  │    │      ├─ needsToolCall?    → ActionNode
  │    │      ├─ shouldSummarize?  → SummarizingNode
  │    │      └─ fallback          → FinalAnswerNode
  │    │
  │    ├──→ [ActionNode.apply(state)]
  │    │    │ ① 读取 TOOL_CALLS (AssistantMessage.ToolCall 列表)
  │    │    │ ② ToolExecutionExecutor.execute()
  │    │    │    ├─ ToolGuard 安全检查
  │    │    │    ├─ 分段并发执行
  │    │    │    ├─ 审批 barrier (需要审批 → 暂停)
  │    │    │    └─ returnDirect (RFC-052) → 设置 DIRECT_TOOL_OUTPUTS
  │    │    │ ③ 构建 ToolResponseMessage → MESSAGES (APPEND)
  │    │    │ ④ return outputMap (含 TOOL_RESULTS, AWAITING_APPROVAL, etc.)
  │    │    │
  │    │    └──→ [ObservationNode.apply(state)]
  │    │         │ ① ObservationProcessor.process() (截断长结果)
  │    │         │ ② CURRENT_ITERATION + 1
  │    │         │ ③ 预算压力预警 (70%/90%)
  │    │         │ ④ 重复观测检测 (连续3次 → 终止)
  │    │         │ ⑤ 判断 shouldSummarize
  │    │         │ ⑥ return outputMap
  │    │         │
  │    │         └──→ ObservationDispatcher.apply(state)
  │    │               ├─ awaitingApproval? → FinalAnswerNode (审批暂停)
  │    │               ├─ returnDirect?     → FinalAnswerNode (跳过LLM)
  │    │               ├─ iteration≥max?    → LimitExceededNode
  │    │               ├─ hasError?         → LimitExceededNode
  │    │               ├─ shouldSummarize?  → SummarizingNode → Reasoning
  │    │               └─ else              → ReasoningNode (继续循环)
  │    │
  │    ├──→ [SummarizingNode.apply(state)]
  │    │     LLM 压缩观察历史 → SUMMARIZED_CONTEXT
  │    │        → ReasoningNode (固定边)
  │    │
  │    ├──→ [LimitExceededNode.apply(state)]
  │    │     LLM 基于 SUMMARIZED_CONTEXT 生成最后回答
  │    │        → FinalAnswerNode (固定边)
  │    │
  │    └──→ [FinalAnswerNode.apply(state)]
  │           ├─ returnDirect → 组装 DIRECT_TOOL_OUTPUTS
  │           ├─ awaitingApproval → 保留流式内容
  │           ├─ finalAnswerDraft → 来自 Summarizing/LimitExceeded
  │           ├─ finalAnswer      → 来自 Reasoning
  │           ├─ 证据校验 → SourceEvidenceLedger.validateAnswer()
  │           ├─ 虚假 URL 清洗 → scrubFakeUrls()
  │           └─ Markdown 规范化 → MarkdownNormalizer
  │
  └──→ GoalEvaluationNode (如有 active goal 且未评估)
         └─ GoalEvaluationDispatcher
              ├─ needFollowup → ReasoningNode (re-plan)
              └─ END
```

## 3. Plan-Execute 模式完整调用链

```
HTTP Request → AgentController → AgentService.execute() → agent.execute()
  │
  ▼
StateGraphPlanExecuteAgent.execute(goal, conversationId)
  │  与 ReAct 的区别: 不调用 buildConversationHistory()
  │  GOAL 状态键直接是用户消息，不拼接历史对话
  │
  ▼
compiledGraph.stream(inputs, config)
  │
  ├─ [PlanGenerationNode.apply(state)]
  │    │ // 三步处理
  │    │ ① Goal跟进注入 → goalService.findActiveByConversation()
  │    │                  有followup → 注入 GOAL_FOLLOWUP_PROMPT
  │    │ ② 审批重放注入 → FORCED_TOOL_CALL / PRE_APPROVED_TOOL_CALL
  │    │ ③ LLM调用 (分流) → 判断是简单问答 / 单步任务 / 多步任务
  │    │                   输出: NEEDS_PLANNING, PLAN_STEPS, PLAN_ID
  │    │                   needsPlanning=false → DIRECT_ANSWER (简单问答)
  │    │                   needsPlanning=true  → PLAN_STEPS (1或多步)
  │    │
  │    └──→ PlanGenerationDispatcher.apply(state)
  │          └─ needsPlanning ? StepExecutionNode : DirectAnswerNode
  │
  │    ┌───────────────────────────────────────────────────┐
  │    │            分流: 路径 A (简单问答)                   │
  │    │ [DirectAnswerNode.apply(state)]                    │
  │    │   设置 FINAL_SUMMARY = goal (一行, 不调LLM)         │
  │    │   → (active goal?) GoalEvaluationNode : END       │
  │    └───────────────────────────────────────────────────┘
  │
  │    ┌───────────────────────────────────────────────────┐
  │    │            分流: 路径 B/C (需要多步/单步执行)        │
  │    │                                                    │
  │    ├──→ [StepExecutionNode.apply(state)]                │
  │    │    │ ★ while 循环 (每次一个步骤)                   │
  │    │    │                                              │
  │    │    │ 每轮:                                         │
  │    │    │ ① 读取 PLAN_STEPS[current_step_index]         │
  │    │    │ ② 组装 Prompt: goal + steps + current step   │
  │    │    │    + working_context (前几步结果摘要)          │
  │    │    │ ③ chatModel.call(prompt) ← LLM 推理+工具调用  │
  │    │    │    NodeStreamingChatHelper.streamContent()    │
  │    │    │ ④ 如需工具: ToolExecutionExecutor.execute()   │
  │    │    │    工具结果注入 working_context                │
  │    │    │ ⑤ 迭代循环，直到:                              │
  │    │    │    - LLM 返回 final answer (无需工具)          │
  │    │    │    - returnDirect 短路                        │
  │    │    │    - 审批暂停 (currentPhase=awaiting_approval) │
  │    │    │    - 异常中止 (currentPhase=plan_aborted)      │
  │    │    │    - 超过100轮硬限                              │
  │    │    │                                              │
  │    │    │ working_context 压缩策略:                      │
  │    │    │   最多保留 6000 字符                          │
  │    │    │   每步结果追加后截断, 防止 prompt 膨胀          │
  │    │    │                                              │
  │    │    └──→ StepProgressDispatcher.apply(state)       │
  │    │          ├─ awaiting_approval? → END              │
  │    │          ├─ plan_aborted?     → END               │
  │    │          ├─ 所有步骤完成?      → PlanSummaryNode   │
  │    │          └─ 否则              → StepExecutionNode (循环)│
  │    │                                                    │
  │    └──→ [PlanSummaryNode.apply(state)]                  │
  │         │ ① 收集 COMPLETED_RESULTS + GOAL               │
  │         │ ② LLM 调用: 基于每步结果生成最终汇总            │
  │         │    NodeStreamingChatHelper.streamContent()    │
  │         │ ③ 降级兜底: buildFallbackSummary()            │
  │         │    (LLM失败时直接拼接各步骤结果)               │
  │         │ ④ 设置 FINAL_SUMMARY                          │
  │         │                                               │
  │         └──→ (active goal?) GoalEvaluationNode : END   │
  │    └───────────────────────────────────────────────────┘
  │
  └──→ [GoalEvaluationNode] (共享节点, same as ReAct)
         └─ GoalEvaluationDispatcher
              ├─ needFollowup → PlanGenerationNode (re-plan)
              └─ END
```

## 4. 核心类关系图 (UML-like)

```
                    ┌─────────────────┐
                    │ AgentController │  HTTP API 层
                    │ (RestController)│
                    └────────┬────────┘
                             │ chatStream(agentId, msg, convId)
                             ▼
              ┌──────────────────────────┐
              │      AgentService         │  业务门面层
              │  (CRUD + Cache + 中介器)  │
              │                          │
              │  agentInstances:          │
              │   Map<Long,              │  ← LRU 的缓存结构
              │     Map<modelKey,        │
              │       BaseAgent>>        │
              └────────┬─────────────────┘
                       │ getOrBuildAgent()
                       ▼
              ┌──────────────────────────┐
              │    AgentGraphBuilder      │  图工厂层
              │    (Component)            │
              │                          │
              │  build(entity, pvd, mdl) │
              │   ├─ buildReActAgent()   │────→ StateGraphReActAgent
              │   └─ buildPlanExecute-   │────→ StateGraphPlanExecuteAgent
              │       Agent()            │
              └────────┬─────────────────┘
                       │
         ┌─────────────┴─────────────┐
         ▼                           ▼
 ┌───────────────────┐     ┌────────────────────────┐
 │StateGraphReActAgent│     │StateGraphPlanExecute-  │  执行体层
 │ (extends BaseAgent)│     │   Agent (extends       │
 │                   │     │   BaseAgent)            │
 │ - compiledGraph   │     │ - compiledGraph         │
 │                   │     │ - planningService       │
 │ buildInitialState()│    │ buildInitialState()     │
 │ chatStructured-   │     │ chatStructured-         │
 │   Stream()        │     │   Stream()              │
 └────────┬──────────┘     └───────────┬────────────┘
          │                            │
          │ .stream()                  │ .stream()
          ▼                            ▼
┌──────────────────────────────────────────────────────────────┐
│                     CompiledGraph                             │  StateGraph 引擎
│  (com.alibaba.cloud.ai.graph)                                │
│                                                              │
│  引擎负责: 调度节点执行、节点间状态合并(KeyStrategy)、条件边路由  │
└──────────────────────────────────────────────────────────────┘
```

## 5. 两类 Node 实现规范

```
┌─────────────────────────────────────────────────────────────┐
│                   NodeAction (节点)                           │
│                                                             │
│  所有节点实现: NodeAction 接口                                │
│                                                             │
│  Map<String,Object> apply(OverAllState state)               │
│    输入: state (图全局状态 Map, 含所有 KeyStrategy 键)        │
│    输出: Map<String,Object> (仅返回要更新的键)                │
│                                                             │
│  输出构建: MateClawStateAccessor.output()                    │
│            .finalAnswer("...")                              │
│            .needsToolCall(true)                             │
│            .events(List.of(...))                            │
│            .build()                                         │
│                                                             │
│  状态合并策略: KeyStrategy                                    │
│    - REPLACE: 直接覆盖 (大部分键)                             │
│    - APPEND : 追加到列表 (MESSAGES, PENDING_EVENTS)          │
└─────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────┐
│                  EdgeAction (路由)                            │
│                                                             │
│  所有路由实现: EdgeAction 接口                                │
│                                                             │
│  String apply(OverAllState state) → 返回下一个节点名          │
│                                                             │
│  ReasoningDispatcher (6路分支):                              │
│    超限 → LimitExceededNode                                 │
│    直接回答 → FinalAnswerNode                                │
│    LLM调用超限 → LimitExceededNode                           │
│    工具调用 → ActionNode                                     │
│    需要总结 → SummarizingNode                                │
│    兜底 → FinalAnswerNode                                   │
│                                                             │
│  ObservationDispatcher (5路分支):                            │
│    审批暂停 → FinalAnswerNode                                │
│    returnDirect → FinalAnswerNode                           │
│    迭代超限 → LimitExceededNode                              │
│    错误 → LimitExceededNode                                  │
│    需要总结 → SummarizingNode                                │
│    否则 → ReasoningNode (继续循环)                            │
│                                                             │
│  PlanGenerationDispatcher (2路分支):                         │
│    needsPlanning=false → DirectAnswerNode                   │
│    needsPlanning=true  → StepExecutionNode                  │
│                                                             │
│  StepProgressDispatcher (4路分支):                           │
│    awaiting_approval → END                                  │
│    plan_aborted → END                                       │
│    步骤完成 → PlanSummaryNode                                │
│    否则 → StepExecutionNode (循环)                            │
└─────────────────────────────────────────────────────────────┘
```

## 6. 辅助组件说明

| 组件 | 作用 | 被谁使用 |
|------|------|---------|
| **NodeStreamingChatHelper** | LLM 流式调用的统一封装 (含 failover 链) | ReasoningNode, SummarizingNode, LimitExceededNode, StepExecutionNode, PlanSummaryNode |
| **ToolExecutionExecutor** | 工具安全执行 (ToolGuard + 分段并发 + 审批 barrier) | ActionNode, StepExecutionNode |
| **ObservationProcessor** | 工具结果标准化 (截断长文本、统一格式) | ObservationNode, LimitExceededNode |
| **GraphEventPublisher** | 图事件工具 (tool_call_started, plan_created 等) | 所有 Node |
| **ConversationWindowManager** | 上下文窗口裁剪 (防止超出模型输入限制) | buildInitialState() |
| **MateClawStateAccessor** | OverAllState 的类型安全访问封装 | 所有 Node, 所有 Dispatcher |
| **PlanStateKeys** | Plan-Execute 专属状态键常量 | Plan-Execute 各节点 |
| **MateClawStateKeys** | 共享状态键常量 | 所有 Node, Agent |
| **SourceEvidenceLedger** | LLM 回答的证据校验 | ActionNode (写入), FinalAnswerNode (校验) |

## 7. 关键状态键数据流 (ReAct 模式)

```
BUILD             MESSAGES  ← buildConversationHistory() + current user message
  │                 ↑             (每次迭代: ActionNode APPEND tool_response)
  │  USER_MESSAGE  ↓             
  │  MAX_ITERATIONS              
  │  SYSTEM_PROMPT               
  ▼                               
ReasoningNode ──→ TOOL_CALLS ──→ ActionNode ──→ TOOL_RESULTS ──→ ObservationNode
  │                 (工具调用列表)      (执行结果)        (处理后)
  │  FINAL_ANSWER ←─────────────────────←(returnDirect)←┘
  │  (直接回答)                              ↓
  │                                     ObservationHistory
  │  SHOULD_SUMMARIZE ←───────────────── need compress?
  │       ↓
  │  SummarizingNode ──→ SUMMARIZED_CONTEXT ──→ 下一轮 ReasoningNode
  │                          (压缩后文本)
  ▼
FinalAnswerNode ──→ FINAL_ANSWER + FINISH_REASON + FINAL_THINKING
  │                        ↓
  │                  StreamDelta → SSE → 前端
  ▼
GoalEvaluationNode (如有 active goal)
  └─→ GOAL_EVALUATION_RESULT → (need followup?)
         └─→ GOAL_FOLLOWUP_PROMPT → ReasoningNode (re-plan)
```

## 8. 关键状态键数据流 (Plan-Execute 模式)

```
BUILD
  │  GOAL (用户目标)
  │  CONVERSATION_ID
  │  SYSTEM_PROMPT
  ▼
PlanGenerationNode
  │  ┌──────────────────────┐
  │  │ NEEDS_PLANNING=false │──→ DirectAnswerNode
  │  │ DIRECT_ANSWER=goal   │      FINAL_SUMMARY = goal
  │  └──────────────────────┘
  │
  │  ┌──────────────────────┐
  │  │ NEEDS_PLANNING=true  │──→ StepExecutionNode (while循环)
  │  │ PLAN_ID              │      CURRENT_STEP_INDEX: 0→1→2→...
  │  │ PLAN_STEPS: [s1,s2]  │      CURRENT_STEP_TITLE: "下载数据"
  │  └──────────────────────┘      CURRENT_STEP_RESULT: "..."
  │                                    ↓
  │                                COMPLETED_RESULTS: [r1, r2, ...] (APPEND)
  │                                WORKING_CONTEXT: "前几步摘要" (≤6000字符)
  │
  └──→ PlanSummaryNode ← COMPLETED_RESULTS + GOAL
         │
         FINAL_SUMMARY → StreamDelta → SSE → 前端
         │
         GoalEvaluationNode (如有 active goal)
           └─→ (need followup?) → PlanGenerationNode (re-plan)
```

## 9. ReAct vs Plan-Execute 对比

| 维度 | ReAct | Plan-Execute |
|------|-------|-------------|
| **适用场景** | 通用对话、工具联动 | 复杂多步任务、批量处理 |
| **迭代方式** | 开放式循环 (max=150) | 固定步数 (由 plan 决定) |
| **LLM 调用频率** | 每轮1次 (ReasoningNode) | 每步多次 (LLM推理+工具) |
| **历史上下文** | 包含完整对话历史 | 不拼接历史，仅 goal+working_context |
| **终止条件** | 迭代上限/LLM给出的最终答案 | 所有步骤执行完毕 |
| **超限处理** | LimitExceededNode → LLM 生成包装回答 | 无超限概念，步骤数固定 |
| **审批重放** | chatWithReplay → FORCED_TOOL_CALL → ReasoningNode | chatWithReplay → FORCED_TOOL_CALL → PlanGenerationNode (re-plan) |
| **流式输出** | NodeStreamingChatHelper 实时推送 per-iteration narration → segmentOnly | NodeStreamingChatHelper 实时推送 per-step thinking → 前端 events |
