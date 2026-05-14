package com.xy.etl.sync.writer.strategy;

import com.xy.etl.sync.model.BatchWriteResult;
import com.xy.etl.sync.model.ResolvedTableConfig;
import com.xy.etl.sync.model.SourceRow;
import com.xy.etl.sync.reader.SourceRowMapper;
import com.xy.etl.sync.support.SyncRuntimeSupport;
import com.xy.etl.sync.writer.TargetJdbcWriter;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

abstract class AbstractBatchWriteStrategy implements BatchWriteStrategy {

    protected record UpsertLikeWriteContext(BatchWriteResult result,
                                            int upsertRowCount,
                                            int deleteMarkedRowCount) {
    }

    protected final SourceRowMapper sourceRowMapper;
    protected final TargetJdbcWriter targetJdbcWriter;

    protected AbstractBatchWriteStrategy(SourceRowMapper sourceRowMapper, TargetJdbcWriter targetJdbcWriter) {
        this.sourceRowMapper = sourceRowMapper;
        this.targetJdbcWriter = targetJdbcWriter;
    }

    @Override
    public abstract String writeMode();

    @Override
    public abstract BatchWriteResult write(Connection targetConn, ResolvedTableConfig config, List<Map<String, Object>> rows);

    protected UpsertLikeWriteContext writeUpsertLike(Connection targetConn,
                                                     ResolvedTableConfig config,
                                                     List<Map<String, Object>> rows) {
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

        return new UpsertLikeWriteContext(
                new BatchWriteResult(processedCount, upsertedCount, deletedCount, maxCursorId, maxSyncTime, mapMillis, deleteMillis, writeMillis),
                upsertRows.size(),
                deleteRows.size()
        );
    }
}
