# MateClaw-Hybrid（基于 MateClaw 的二次开源增强版）

> **在 MateClaw 原生 Agent 框架之上，重构了 RAG 检索引擎与长期记忆系统，面向企业私有化部署场景，提供生产级知识检索能力。**

---

## 与上游 MateClaw 的核心差异

MateClaw 原生已具备完整的 Agent 执行框架（StateGraph 编排 + MemoryProvider SPI + Wiki 知识库），
其 RAG 检索基于 **MySQL 全量扫描计算余弦相似度**，原生记忆系统已有**五维评分算法**（频率/新鲜度/多样性/时效性/增速）。

MateClaw-Hybrid 在保留原生全部能力的前提下，做了以下针对性增强：

### 检索架构升级

| | 原生 MateClaw | MateClaw-Hybrid | 提升点 |
|:---|:---|:---|:---|
| **全文检索** | MySQL LIKE | **Elasticsearch BM25 + IK 中文分词** | 新增：关键词精确匹配 |
| **向量检索** | MySQL 全表扫描 + JVM 余弦计算 | **Milvus HNSW + 余弦相似度（1024 维）** | 从 O(N) 暴力扫描升级为 O(log N) ANN 索引 |
| **融合排序** | 无 | **RRF（Reciprocal Rank Fusion）** | 新增：双路结果共识投票 |
| **故障容错** | 无降级 | **三级降级（ES↔Milvus 互备 → MySQL 兜底）+ 轻量熔断器** | 新增：检索链路自愈 |
| **向量化写入** | 同步阻塞（HTTP 线程等 IO） | **@Async + CompletableFuture 异步流水线 + 持久化补偿队列** | 从秒级响应提升为毫秒级，支持断点续传 |

### 记忆系统优化

| | 原生 | Hybrid | 说明 |
|:---|:---|:---|:---|
| **记忆粒度** | 文件级（整个文件共享五维评分） | **Chunk 级**（512 字符语义片段独立评分） | 评分算法不变，评分对象缩小 |
| **评分算法** | 五维加权（频率 0.30 + 新鲜度 0.25 + 多样性 0.20 + 时效性 0.15 + 增速 0.10） | **同上** | 原生算法保留 |
| **记忆注入** | 高评分文件整体写入 MEMORY.md | 高评分 Chunk 按文件聚合后，**只注入相关片段** | 减少 60~80% 无效上下文 |

### 工具扩展

| | 原生 | Hybrid |
|:---|:---|:---|
| **知识检索工具** | wiki_read_page 等页面级工具 | 新增 `knowledge_retrieval` 工具（ES + Milvus 混合检索） |

### 私有化部署适配

| | 原生 | Hybrid |
|:---|:---|:---|
| **多租户隔离** | MySQL WHERE workspace_id 过滤 | 新增 ES 索引级 + Milvus Collection 级物理隔离 |
| **基础设施弹性** | 仅依赖 MySQL | ES/Milvus 一键开关，不支持时自动降级到 MySQL |
| **部署方式** | Docker Compose（MySQL + App） | Docker Compose（MySQL + ES + Milvus + App 四容器） |

---

## Hybrid 检索架构

```
Agent 用户查询
  │
  ▼
KnowledgeRetrievalTool（@Tool 注解，Agent 自主调用）
  │
  ▼
HybridRetriever.retrieve(workspaceId, query, topK, mode)
  │
  ├─ CompletableFuture → ES 路（独立 try-catch，挂了返回空）
  │   索引: wiki_{workspaceId}
  │   分词: ik_max_word
  │   算法: BM25 多字段加权（content³ + title² + summary）
  │
  ├─ CompletableFuture → 向量路（独立 try-catch，挂了走 MySQL 降级）
  │   正常: Milvus HNSW 索引 → 余弦相似度 TopK
  │   降级: MySQL wiki_chunk 表全量扫描 → JVM 余弦计算（回退原生方式）
  │   再降: MySQL LIKE 文本匹配
  │
  └─ CompletableFuture.allOf().get(30s)
       │
       ▼
     RRF 融合: score = Σ 1/(60 + rank_i)
       │
       ▼
     Top-K 结果 → Markdown 格式化 → 返回 Agent
```

### 降级链路

```
正常: ES(BM25) + Milvus(HNSW) → RRF 融合
  ├─ ES 挂 → Milvus 独立检索（无需融合）
  ├─ Milvus 挂 → ES 独立检索 + MySQL 向量兜底
  └─ 双挂  → MySQL 向量（原生方式）→ MySQL LIKE（最终兜底）
```

### 异步 Embedding 流水线

```
文档上传 → @Transactional 秒级返回
  │
  ▼
@Async("embeddingThreadPool") 后台执行
  ├─ Chunk 切分（句子级，512 字符，64 重叠）
  ├─ 逐 Chunk embedding（3 次重试，5s/10s/15s 线性退避）
  ├─ 失败 → recordFailedChunk() → embedding_task 表（MySQL 持久化）
  ├─ 批量写入 Milvus（3 次重试）
  └─ 写入失败 → embedding_task 表
       │
       ▼
  @Scheduled(60s) 定时补偿扫描
    → 指数退避重试（2ⁿ×5min，最多 5 次）
    → ABORTED → 运维手动 retryAllFailedTasks()
```

---

## 快速启动

### Docker Compose（完整 Hybrid）

```bash
git clone https://github.com/pan-hh/mateclaw-hybrid.git
cd mateclaw-hybrid
cp .env.example .env

# 编辑 .env，开启 Hybrid 组件
MILVUS_ENABLED=true
ES_ENABLED=true

docker compose --profile hybrid up -d
# Web: http://localhost:18080
# 默认: admin / admin123
```

### 最小部署（仅 MySQL，自动降级）

```bash
# 不装 ES/Milvus 也能跑，检索自动回退原生方式
MILVUS_ENABLED=false
ES_ENABLED=false

docker compose up -d
```

---

## Hybrid 配置参考

```yaml
mate:
  hybrid:
    milvus:
      enabled: true
      host: localhost
      port: 19530
      dimension: 1024              # DashScope text-embedding-v3
      hnsw-m: 16                   # 低配机器可设为 8
      ef-construction: 200
      ef: 100                      # 查询搜索宽度，精度换速度
      batch-size: 100
      connect-timeout: 5000
      query-timeout: 30000
      max-retries: 3
    elasticsearch:
      enabled: true
      host: localhost
      port: 9200
      scheme: http
      index-prefix: wiki           # 实际索引: wiki_{workspaceId}
      connect-timeout: 5000
      socket-timeout: 30000
      batch-size: 100
    async:
      core-pool-size: 4            # 4C8G 默认值
      max-pool-size: 8
      queue-capacity: 1000
```

---

## 五维评分算法说明（继承自原生）

```
score = 0.30 × frequency    +  0.25 × recency    +  0.20 × diversity
      + 0.15 × freshness     +  0.10 × velocity

frequency:  归一化召回次数（recallCount / maxRecallCount）
recency:    指数衰减 exp(-0.693 × daysSinceRecall / 7)，半衰期 7 天
diversity:  归一化不同查询数（不同问题的命中次数）
freshness:  文件日期线性衰减（30 天窗口）
velocity:   增长速度（dailyCount / recallCount）
```

Hybrid 的改动：将评分对象从**整个文件**替换为**单个 Chunk（512 字符语义片段）**，
每个 Chunk 独立计算五维评分，Dreaming 阶段按原始文件分组聚合（平均分 + 最高分 Chunk 代表）。

---

## 硬件建议

| 规模 | CPU | 内存 | 说明 |
|:---|:---|:---|:---|
| **最小（仅 MySQL）** | 2C | 4GB | 不启用 ES/Milvus，检索自动降级 |
| **推荐（完整 Hybrid）** | 4C | 8GB | MySQL + ES + Milvus + App，四容器 |
| **生产（万级文档）** | 8C | 16GB+ | Milvus 10 万向量 ~500MB，ES 索引 ~1GB |

---

## 技术栈

| 层次 | 技术 |
|:---|:---|
| 后端框架 | Spring Boot 3.5 · Spring AI Alibaba 1.1 |
| Agent 运行时 | StateGraph · ReAct + Plan-Execute |
| 全文检索 | Elasticsearch 8.x（BM25 + IK 分词） |
| 向量检索 | Milvus 2.x（HNSW 索引 + 余弦相似度） |
| 融合排序 | RRF（Reciprocal Rank Fusion，k=60） |
| 模型接入 | DashScope · OpenAI 兼容 · Ollama · vLLM · DeepSeek 等 8+ 协议 |
| 数据库 | H2（开发）· MySQL 8.0+（生产） |
| 部署 | Docker Compose · JAR 单包交付 |

---

## 致谢

本项目基于 [MateClaw](https://github.com/matevip/mateclaw)（Apache 2.0）进行二次开发。
完整保留了上游的 Agent 执行框架、MCP 协议支持、多模型故障转移、工作流引擎、
Skill/MCP/ACP 扩展、审批流、RBAC 等全部功能，仅对 RAG 检索与记忆模块做了增强。

## 许可证

[Apache License 2.0](LICENSE)
