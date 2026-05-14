package com.xy.etl.sync.writer.strategy;

import com.xy.etl.sync.model.BatchWriteResult;
import com.xy.etl.sync.model.ResolvedTableConfig;
import com.xy.etl.sync.reader.SourceRowMapper;
import com.xy.etl.sync.support.DbSyncConstants;
import com.xy.etl.sync.writer.TargetJdbcWriter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.sql.Connection;
import java.util.List;
import java.util.Map;

@Slf4j
@Component
public class UpsertBatchWriteStrategy extends AbstractBatchWriteStrategy {

    public UpsertBatchWriteStrategy(SourceRowMapper sourceRowMapper, TargetJdbcWriter targetJdbcWriter) {
        super(sourceRowMapper, targetJdbcWriter);
    }

    @Override
    public String writeMode() {
        return DbSyncConstants.WRITE_MODE_UPSERT;
    }

    @Override
    public BatchWriteResult write(Connection targetConn, ResolvedTableConfig config, List<Map<String, Object>> rows) {
        UpsertLikeWriteContext context = writeUpsertLike(targetConn, config, rows);
        BatchWriteResult result = context.result();

        log.info("upsert batch write finished, syncKey: {}, sourceRowCount: {}, upsertRowCount: {}, deleteMarkedRowCount: {}, affectedUpsertCount: {}, affectedDeleteCount: {}, mapMillis: {}, deleteMillis: {}, writeMillis: {}",
                config.syncKey(), rows.size(), context.upsertRowCount(), context.deleteMarkedRowCount(),
                result.upsertedCount(), result.deletedCount(), result.mapMillis(), result.deleteMillis(), result.writeMillis());
        return result;
    }
}
