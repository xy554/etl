package com.xy.etl.sync.writer.strategy;

import com.xy.etl.sync.model.BatchWriteResult;
import com.xy.etl.sync.model.ResolvedTableConfig;
import com.xy.etl.sync.model.SourceRow;
import com.xy.etl.sync.reader.SourceRowMapper;
import com.xy.etl.sync.support.DbSyncConstants;
import com.xy.etl.sync.support.SyncRuntimeSupport;
import com.xy.etl.sync.writer.TargetJdbcWriter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Slf4j
@Component
public class MultiValuesUpsertBatchWriteStrategy extends AbstractBatchWriteStrategy {

    public MultiValuesUpsertBatchWriteStrategy(SourceRowMapper sourceRowMapper, TargetJdbcWriter targetJdbcWriter) {
        super(sourceRowMapper, targetJdbcWriter);
    }

    @Override
    public String writeMode() {
        return DbSyncConstants.WRITE_MODE_MULTI_VALUES_UPSERT;
    }

    @Override
    public BatchWriteResult write(Connection targetConn, ResolvedTableConfig config, List<Map<String, Object>> rows) {
        long processedCount = 0L;
        long maxCursorId = 0L;
        String maxSyncTime = null;
        long mapStartMillis = System.currentTimeMillis();
        List<SourceRow> deleteRows = new ArrayList<>();
        List<SourceRow> upsertRows = new ArrayList<>();

        for (Map<String, Object> row : rows) {
            SourceRow sourceRow = sourceRowMapper.mapSourceRow(config, row);
            maxCursorId = Math.max(maxCursorId, sourceRow.cursorId());
            maxSyncTime = SyncRuntimeSupport.maxDatetime(maxSyncTime, sourceRow.syncTime());
            if (sourceRow.deleted()) {
                deleteRows.add(sourceRow);
            } else {
                upsertRows.add(sourceRow);
            }
            processedCount++;
        }
        long mapMillis = SyncRuntimeSupport.elapsedMillis(mapStartMillis);
        long deleteStartMillis = System.currentTimeMillis();
        long deletedCount = targetJdbcWriter.batchDeleteTarget(targetConn, config, deleteRows);
        long deleteMillis = SyncRuntimeSupport.elapsedMillis(deleteStartMillis);
        long writeStartMillis = System.currentTimeMillis();
        long upsertedCount = targetJdbcWriter.multiValuesUpsertTarget(targetConn, config, upsertRows);
        long writeMillis = SyncRuntimeSupport.elapsedMillis(writeStartMillis);

        log.info("multi_values_upsert batch write finished, syncKey: {}, sourceRowCount: {}, upsertRowCount: {}, deleteMarkedRowCount: {}, affectedUpsertCount: {}, affectedDeleteCount: {}, mapMillis: {}, deleteMillis: {}, writeMillis: {}",
                config.syncKey(), rows.size(), upsertRows.size(), deleteRows.size(), upsertedCount, deletedCount, mapMillis, deleteMillis, writeMillis);
        return new BatchWriteResult(processedCount, upsertedCount, deletedCount, maxCursorId, maxSyncTime, mapMillis, deleteMillis, writeMillis);
    }
}
