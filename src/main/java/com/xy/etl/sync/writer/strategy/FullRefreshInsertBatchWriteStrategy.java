package com.xy.etl.sync.writer.strategy;

import com.xy.etl.sync.model.BatchWriteResult;
import com.xy.etl.sync.model.ResolvedTableConfig;
import com.xy.etl.sync.model.SourceRow;
import com.xy.etl.sync.reader.SourceRowMapper;
import com.xy.etl.sync.support.DbSyncConstants;
import com.xy.etl.sync.support.SyncRuntimeSupport;
import com.xy.etl.sync.writer.TargetJdbcWriter;
import org.springframework.stereotype.Component;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Component
public class FullRefreshInsertBatchWriteStrategy extends AbstractBatchWriteStrategy {

    public FullRefreshInsertBatchWriteStrategy(SourceRowMapper sourceRowMapper, TargetJdbcWriter targetJdbcWriter) {
        super(sourceRowMapper, targetJdbcWriter);
    }

    @Override
    public String writeMode() {
        return DbSyncConstants.WRITE_MODE_FULL_REFRESH_INSERT;
    }

    @Override
    public BatchWriteResult write(Connection targetConn, ResolvedTableConfig config, List<Map<String, Object>> rows) {
        long processedCount = 0L;
        long maxCursorId = 0L;
        String maxSyncTime = null;
        long mapStartMillis = System.currentTimeMillis();
        List<SourceRow> insertRows = new ArrayList<>();

        for (Map<String, Object> row : rows) {
            SourceRow sourceRow = sourceRowMapper.mapSourceRow(config, row);
            processedCount++;
            maxCursorId = Math.max(maxCursorId, sourceRow.cursorId());
            maxSyncTime = SyncRuntimeSupport.maxDatetime(maxSyncTime, sourceRow.syncTime());
            if (!sourceRow.deleted()) {
                insertRows.add(sourceRow);
            }
        }
        long mapMillis = SyncRuntimeSupport.elapsedMillis(mapStartMillis);
        long writeStartMillis = System.currentTimeMillis();
        long insertedCount = targetJdbcWriter.batchInsertTarget(targetConn, config, insertRows);
        long writeMillis = SyncRuntimeSupport.elapsedMillis(writeStartMillis);

        return new BatchWriteResult(processedCount, insertedCount, 0L, maxCursorId, maxSyncTime, mapMillis, 0L, writeMillis);
    }
}
