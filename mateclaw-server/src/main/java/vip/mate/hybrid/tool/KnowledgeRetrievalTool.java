package vip.mate.hybrid.tool;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.ai.tool.annotation.Tool;
import org.springframework.ai.tool.annotation.ToolParam;
import org.springframework.stereotype.Component;
import vip.mate.wiki.service.HybridRetriever;
import vip.mate.wiki.service.HybridRetriever.RetrievalMode;
import vip.mate.wiki.service.HybridRetriever.SearchHit;

/**
 * 知识检索 MCP 工具。
 * <p>
 * 将 HybridRetriever 封装为 Spring AI {@code @Tool}，供 Agent 自主调用知识检索。
 * 支持三种检索模式：全文检索（es_only）、向量检索（vector_only）、混合检索（hybrid，默认）。
 *
 * @author MateClaw Team
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class KnowledgeRetrievalTool {

    private final HybridRetriever hybridRetriever;

    /**
     * 从知识库中检索相关知识。
     * 支持 Elasticsearch 全文检索 + Milvus 向量语义检索的混合模式。
     *
     * @param workspaceId 工作空间 ID（用于多租户隔离，必填）
     * @param query       检索查询文本（必填）
     * @param mode        检索模式：hybrid / es_only / vector_only（可选，默认 hybrid）
     * @param topK        返回结果数量：1-20（可选，默认 5）
     * @return 格式化的 Markdown 检索结果
     */
    @Tool(name = "knowledge_retrieval",
            description = "从知识库中检索相关知识。支持全文检索（Elasticsearch BM25）和语义检索（Milvus 向量）的混合模式。" +
                    "适用于需要查询企业内部文档、Wiki 知识库、已存储的资料等场景。")
    public String retrieve(
            @ToolParam(description = "工作空间 ID，用于租户隔离（必填）") Long workspaceId,
            @ToolParam(description = "检索查询文本（必填）") String query,
            @ToolParam(description = "检索模式：hybrid（混合，默认）/ es_only（仅全文）/ vector_only（仅向量语义）", required = false) String mode,
            @ToolParam(description = "返回结果数量 1-20（默认 5）", required = false) Integer topK) {

        String actualMode = (mode != null && !mode.isBlank()) ? mode.trim().toLowerCase() : "hybrid";
        int actualTopK = (topK != null && topK > 0 && topK <= 20) ? topK : 5;

        RetrievalMode retrievalMode = parseMode(actualMode);

        log.info("[KnowledgeTool] Retrieval: workspaceId={}, query={}, mode={}, topK={}",
                workspaceId, query, actualMode, actualTopK);

        try {
            HybridRetriever.HybridResult result =
                    hybridRetriever.retrieve(workspaceId, query, actualTopK, retrievalMode);

            StringBuilder sb = new StringBuilder();
            sb.append("## 知识检索结果\n\n");

            if (result.hits().isEmpty()) {
                sb.append("未找到相关文档。");
            } else {
                for (int i = 0; i < result.hits().size(); i++) {
                    SearchHit hit = result.hits().get(i);
                    sb.append(String.format("### 结果 %d（综合评分: %.4f）\n", i + 1, hit.score()));
                    if (hit.title() != null && !hit.title().isEmpty()) {
                        sb.append("**标题**: ").append(hit.title()).append("\n");
                    }
                    sb.append("**来源**: ").append(hit.source()).append("\n");
                    sb.append("**内容**: ").append(truncateContent(hit.content(), 300)).append("\n\n");

                    if (hit.metadata() != null && !hit.metadata().isEmpty()) {
                        sb.append("**元数据**: ");
                        hit.metadata().forEach((k, v) -> {
                            if (!"content".equals(k) && !"title".equals(k)) {
                                sb.append(k).append("=").append(v).append(", ");
                            }
                        });
                        sb.append("\n\n");
                    }
                }
            }

            if (result.vectorFallbackUsed()) {
                sb.append("> ⚠️ 当前使用降级检索模式，部分功能可能受限。\n");
            }

            sb.append(String.format("\n**检索统计**: ES 命中 %d 条, 向量命中 %d 条",
                    result.esHitCount(), result.vectorHitCount()));

            return sb.toString();

        } catch (Exception e) {
            log.error("[KnowledgeTool] Retrieval failed: {}", e.getMessage(), e);
            return "检索失败: " + e.getMessage();
        }
    }

    private RetrievalMode parseMode(String mode) {
        return switch (mode) {
            case "es_only" -> RetrievalMode.ES_ONLY;
            case "vector_only" -> RetrievalMode.VECTOR_ONLY;
            default -> RetrievalMode.HYBRID;
        };
    }

    private String truncateContent(String content, int maxLength) {
        if (content == null) return "";
        return content.length() > maxLength
                ? content.substring(0, maxLength) + "..."
                : content;
    }
}
