package vip.mate.hybrid.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import vip.mate.wiki.model.WikiChunkEntity;
import vip.mate.wiki.repository.WikiChunkMapper;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

/**
 * 文档智能切片服务。
 * <p>
 * 支持三种切分策略：
 * <ul>
 *   <li>{@link ChunkStrategy#SENTENCE_BASED} — 基于句子切分（保证语义完整）</li>
 *   <li>{@link ChunkStrategy#PARAGRAPH_BASED} — 基于段落切分</li>
 *   <li>{@link ChunkStrategy#FIXED_LENGTH} — 固定长度切分（带重叠）</li>
 * </ul>
 * <p>
 * 默认参数：chunkSize=512，overlap=64。
 * <p>
 * 注意：此服务负责文本切分逻辑，chunk 持久化由 {@code WikiChunkService} 负责。
 *
 * @author MateClaw Team
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class DocumentChunkService {

    private final WikiChunkMapper wikiChunkMapper;

    private static final int DEFAULT_CHUNK_SIZE = 512;
    private static final int DEFAULT_CHUNK_OVERLAP = 64;
    private static final Pattern SENTENCE_SEPARATORS = Pattern.compile("[。！？；;\\n\\r]");
    private static final Pattern PARAGRAPH_SEPARATORS = Pattern.compile("\\n\\s*\\n");

    public enum ChunkStrategy {
        /** 基于句子切分（保证语义完整） */
        SENTENCE_BASED,
        /** 基于段落切分 */
        PARAGRAPH_BASED,
        /** 固定长度切分（带重叠） */
        FIXED_LENGTH
    }

    /**
     * 使用默认策略和参数切分文档。
     *
     * @param rawId   原始材料 ID
     * @param kbId    知识库 ID
     * @param content 文档内容
     * @return chunk 实体列表（未持久化）
     */
    public List<WikiChunkEntity> chunkDocument(Long rawId, Long kbId, String content) {
        return chunkDocument(rawId, kbId, content, ChunkStrategy.SENTENCE_BASED,
                DEFAULT_CHUNK_SIZE, DEFAULT_CHUNK_OVERLAP);
    }

    /**
     * 按指定策略和参数切分文档。
     *
     * @param rawId    原始材料 ID
     * @param kbId     知识库 ID
     * @param content  文档内容
     * @param strategy 切分策略
     * @param chunkSize chunk 最大字符数
     * @param overlap  重叠字符数
     * @return chunk 实体列表（未持久化）
     */
    public List<WikiChunkEntity> chunkDocument(Long rawId, Long kbId, String content,
                                                ChunkStrategy strategy, int chunkSize, int overlap) {

        List<WikiChunkEntity> chunks = new ArrayList<>();

        if (content == null || content.isEmpty()) {
            return chunks;
        }

        List<String> segments = switch (strategy) {
            case SENTENCE_BASED -> splitBySentence(content);
            case PARAGRAPH_BASED -> splitByParagraph(content);
            case FIXED_LENGTH -> splitByFixedLength(content, chunkSize, overlap);
        };

        int chunkIndex = 0;
        StringBuilder currentChunk = new StringBuilder();

        for (String segment : segments) {
            if (currentChunk.length() + segment.length() <= chunkSize) {
                if (!currentChunk.isEmpty()) {
                    currentChunk.append("。");
                }
                currentChunk.append(segment);
            } else {
                if (!currentChunk.isEmpty()) {
                    chunks.add(createChunk(rawId, kbId, currentChunk.toString(), chunkIndex++));
                }
                currentChunk = new StringBuilder(segment);
            }
        }

        if (!currentChunk.isEmpty()) {
            chunks.add(createChunk(rawId, kbId, currentChunk.toString(), chunkIndex));
        }

        log.info("[ChunkService] Created {} chunks for rawId={}", chunks.size(), rawId);
        return chunks;
    }

    /**
     * 批量持久化 chunk。
     */
    public void saveChunks(List<WikiChunkEntity> chunks) {
        if (chunks != null && !chunks.isEmpty()) {
            for (WikiChunkEntity chunk : chunks) {
                wikiChunkMapper.insert(chunk);
            }
            log.info("[ChunkService] Saved {} chunks", chunks.size());
        }
    }

    // ---- 切分策略 ----

    private List<String> splitBySentence(String content) {
        List<String> sentences = new ArrayList<>();
        String[] parts = SENTENCE_SEPARATORS.split(content);
        for (String part : parts) {
            String trimmed = part.trim();
            if (!trimmed.isEmpty()) {
                sentences.add(trimmed);
            }
        }
        return sentences;
    }

    private List<String> splitByParagraph(String content) {
        List<String> paragraphs = new ArrayList<>();
        String[] parts = PARAGRAPH_SEPARATORS.split(content);
        for (String part : parts) {
            String trimmed = part.trim();
            if (!trimmed.isEmpty()) {
                paragraphs.add(trimmed);
            }
        }
        return paragraphs;
    }

    private List<String> splitByFixedLength(String content, int chunkSize, int overlap) {
        List<String> result = new ArrayList<>();
        int start = 0;

        while (start < content.length()) {
            int end = Math.min(start + chunkSize, content.length());

            // 尽量在句子边界截断
            if (end < content.length()) {
                int lastPeriod = content.lastIndexOf('。', end);
                int lastQuestion = content.lastIndexOf('？', end);
                int lastExclaim = content.lastIndexOf('！', end);
                int splitPoint = Math.max(Math.max(lastPeriod, lastQuestion), lastExclaim);

                if (splitPoint > start + chunkSize / 2) {
                    end = splitPoint + 1;
                }
            }

            result.add(content.substring(start, end).trim());
            start = end - overlap;

            if (start <= 0 || start >= content.length()) {
                break;
            }
        }

        return result;
    }

    // ---- 内部方法 ----

    private WikiChunkEntity createChunk(Long rawId, Long kbId, String content, int index) {
        WikiChunkEntity chunk = new WikiChunkEntity();
        chunk.setRawId(rawId);
        chunk.setKbId(kbId);
        chunk.setContent(content);
        chunk.setOrdinal(index);
        chunk.setCharCount(content.length());
        chunk.setDeleted(0);
        return chunk;
    }
}
