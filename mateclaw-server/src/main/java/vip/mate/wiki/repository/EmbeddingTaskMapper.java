package vip.mate.wiki.repository;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import vip.mate.wiki.model.EmbeddingTaskEntity;

import java.util.List;

/**
 * 嵌入重试任务 Mapper。
 *
 * @author MateClaw Team
 */
@Mapper
public interface EmbeddingTaskMapper extends BaseMapper<EmbeddingTaskEntity> {

    /**
     * 查询到达重试时间的待处理任务（按重试时间升序，限制条数）。
     */
    @Select("SELECT * FROM mate_embedding_task WHERE status = 'PENDING' AND next_retry_time <= NOW() ORDER BY next_retry_time ASC LIMIT #{limit}")
    List<EmbeddingTaskEntity> selectPendingTasks(@Param("limit") int limit);

    /**
     * 统计待处理任务数。
     */
    @Select("SELECT COUNT(*) FROM mate_embedding_task WHERE status = 'PENDING' AND next_retry_time <= NOW()")
    int countPendingTasks();

    /**
     * 按状态查询任务列表。
     */
    @Select("SELECT * FROM mate_embedding_task WHERE status = #{status}")
    List<EmbeddingTaskEntity> selectByStatus(@Param("status") String status);
}
