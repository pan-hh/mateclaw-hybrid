package vip.mate.tool.mcp;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;
import vip.mate.hybrid.tool.KnowledgeRetrievalTool;

/**
 * 知识检索工具注册器。
 * <p>
 * 在应用启动完成后验证 {@link KnowledgeRetrievalTool} 是否已就绪。
 * 该工具通过 {@code @Component + @Tool} 注解自动被 Spring AI 工具扫描机制发现，
 * 无需手动注册到 MCP 客户端。
 * <p>
 * 此处仅做启动日志确认，确保运维人员可直观看到工具状态。
 *
 * @author MateClaw Team
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class McpKnowledgeToolRegistration {

    private final KnowledgeRetrievalTool knowledgeRetrievalTool;

    /**
     * 应用启动完成后确认知识检索工具已就绪。
     */
    @EventListener(ApplicationReadyEvent.class)
    public void onApplicationReady() {
        if (knowledgeRetrievalTool != null) {
            log.info("[MCP] Knowledge retrieval tool (knowledge_retrieval) is ready. "
                    + "It is auto-discovered via @Tool annotation and available to all agents.");
        } else {
            log.warn("[MCP] Knowledge retrieval tool is NOT available — agent knowledge search may not work.");
        }
    }
}
