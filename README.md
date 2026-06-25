<div align="center">

<p align="center">
  <img src="mateclaw-ui/public/logo/mateclaw_logo_s.png" alt="MateClaw Logo" width="120">
</p>

# MateClaw-Hybrid

<p align="center"><b>基于 MateClaw 的二次开源增强版</b></p>

<p align="center"><sub><b>企业私有化部署 · 混合检索引擎 · Chunk 级长期记忆 · Spring Boot 3.5</b></sub></p>

[![Java Version](https://img.shields.io/badge/Java-17+-blue.svg?logo=openjdk&label=Java)](https://adoptium.net/)
[![Spring Boot](https://img.shields.io/badge/Spring%20Boot-3.5-brightgreen.svg?logo=springboot)](https://spring.io/projects/spring-boot)
[![License](https://img.shields.io/badge/license-Apache--2.0-red.svg?logo=opensourceinitiative&label=License)](LICENSE)

[上游原版](https://github.com/matevip/mateclaw) · [上游原版 README](README_UPSTREAM.md) · [中文](README_zh.md)

</div>

---

> **在 MateClaw 原生 Agent 框架之上，重构了 RAG 检索引擎与长期记忆系统，面向企业私有化部署场景，提供生产级知识检索能力。**

---

## 与上游 MateClaw 的核心差异

MateClaw 原生已具备完整的 Agent 执行框架（StateGraph 编排 + MemoryProvider SPI + Wiki 知识库），
其 RAG 检索基于 **MySQL 全量扫描计算余弦相似度**，原生记忆系统已有**五维评分算法**。

MateClaw-Hybrid 在保留原生全部能力的前提下，做了以下针对性增强：

### 检索架构升级

| | 原生 MateClaw | MateClaw-Hybrid | 提升点 |
|:---|:---|:---|:---|
| **全文检索** | MySQL LIKE | **Elasticsearch BM25 + IK 分词** | 新增关键词精确匹配 |
| **向量检索** | MySQL 全表扫描 + JVM 余弦计算 | **Milvus HNSW + 余弦相似度（1024 维）** | O(N)→O(log N)，ANN 索引加速 |
| **融合排序** | 无 | **RRF（Reciprocal Rank Fusion）** | 双路结果共识投票 |
| **故障容错** | 无降级 | **三级降级 + 轻量熔断器** | 检索链路自愈 |
| **向量化写入** | 同步阻塞 | **@Async + CompletableFuture + 补偿队列** | 秒级→毫秒级，支持断点续传 |

### 记忆系统优化

| | 原生 | Hybrid | 说明 |
|:---|:---|:---|:---|
| **记忆粒度** | 文件级 | **Chunk 级**（512 字符独立评分） | 评分算法不变，评分对象缩小 |
| **评分算法** | 五维加权（频率/新鲜度/多样性/时效性/增速） | **同上** | 原生算法完整保留 |
| **记忆注入** | 高评分文件整体写入 MEMORY.md | 按文件聚合后**只注入相关片段** | 减少 60~80% 无效上下文 |

### 工具扩展与私有化部署

| | 原生 | Hybrid |
|:---|:---|:---|
| **知识检索工具** | wiki_read_page 等 | 新增 `knowledge_retrieval`（ES+Milvus 混合） |
| **多租户隔离** | MySQL WHERE 过滤 | +ES 索引级 + Milvus Collection 级 |
| **基础设施弹性** | 仅依赖 MySQL | ES/Milvus 一键开关，不支持自动降级 |

---

## Hybrid 检索架构

```
Agent 查询 → KnowledgeRetrievalTool → HybridRetriever
  ├─ ES (BM25, wiki_{workspaceId})  ──┐
  └─ Milvus (HNSW, 1024维)  ─────────┤
                                      ▼
                              RRF: score = Σ 1/(60 + rank_i)
                                      │
                                      ▼
                                 Top-K → Agent
```

### 降级链路

```
正常: ES + Milvus → RRF 融合
  ├─ ES 挂 → Milvus 独立检索
  ├─ Milvus 挂 → ES + MySQL 向量兜底
  └─ 双挂 → MySQL 向量（原生方式）→ MySQL LIKE（最终兜底）
```

### 异步 Embedding 流水线

```
文档上传 → @Transactional 秒级返回
  → @Async 后台 Chunk 切分 → embedding(3次重试) → 批量写 Milvus(3次重试)
  → 失败 → embedding_task 表 → @Scheduled(60s) 补偿(指数退避, 最多5次)
```

---

## 快速启动

```bash
git clone https://github.com/pan-hh/mateclaw-hybrid.git
cd mateclaw-hybrid && cp .env.example .env

# 完整 Hybrid
MILVUS_ENABLED=true ES_ENABLED=true
docker compose --profile hybrid up -d     # http://localhost:18080

# 最小部署（仅 MySQL，自动降级到原生检索方式）
MILVUS_ENABLED=false ES_ENABLED=false
docker compose up -d
```

---

## Hybrid 配置参考

```yaml
mate:
  hybrid:
    milvus:
      enabled: true
      dimension: 1024
      hnsw-m: 16
      ef: 100
    elasticsearch:
      enabled: true
      index-prefix: wiki
    async:
      core-pool-size: 4
      max-pool-size: 8
      queue-capacity: 1000
```

---

## 五维评分算法（继承自原生）

```
score = 0.30×频率 + 0.25×新鲜度 + 0.20×多样性 + 0.15×时效性 + 0.10×增速
```

Hybrid 改动：评分对象从文件缩小到 Chunk（512 字符），Dreaming 按文件聚合。

---

## 技术栈

| 层次 | 技术 |
|:---|:---|
| 后端 | Spring Boot 3.5 · Spring AI Alibaba 1.1 |
| Agent | StateGraph · ReAct + Plan-Execute |
| 全文检索 | Elasticsearch 8.x（BM25 + IK） |
| 向量检索 | Milvus 2.x（HNSW + 余弦相似度） |
| 融合排序 | RRF（k=60） |
| 部署 | Docker Compose · JAR 单包 |

---

## 致谢

基于 [MateClaw](https://github.com/matevip/mateclaw)（Apache 2.0）二次开发。保留上游全部能力，增强 RAG 检索与记忆模块。

[上游原版 README](README_UPSTREAM.md)

## 许可证

[Apache License 2.0](LICENSE)
