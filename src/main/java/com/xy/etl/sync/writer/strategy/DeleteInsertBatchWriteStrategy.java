package com.xy.etl.sync.writer.strategy;

import com.xy.etl.sync.model.BatchWriteResult;
import com.xy.etl.sync.model.ResolvedTableConfig;
import com.xy.etl.sync.model.SourceRow;
import com.xy.etl.sync.reader.SourceRowMapper;
import com.xy.etl.sync.support.SyncRuntimeSupport;
import com.xy.etl.sync.support.WriteMode;
import com.xy.etl.sync.writer.TargetJdbcWriter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
@Component
public class DeleteInsertBatchWriteStrategy extends AbstractBatchWriteStrategy {

    public DeleteInsertBatchWriteStrategy(SourceRowMapper sourceRowMapper, TargetJdbcWriter targetJdbcWriter) {
        super(sourceRowMapper, targetJdbcWriter);
    }

    @Override
    public WriteMode writeMode() {
        return WriteMode.DELETE_INSERT;
    }

    @Override
    public BatchWriteResult write(Connection targetConn, ResolvedTableConfig config, List<Map<String, Object>> rows) {
        LinkedHashMap<String, SourceRow> deduplicatedRows = new LinkedHashMap<>();
        long processedCount = 0L;
        long maxCursorId = 0L;
        String maxSyncTime = null;
        long mapStartMillis = System.currentTimeMillis();

        for (Map<String, Object> row : rows) {
            SourceRow sourceRow = sourceRowMapper.mapSourceRow(config, row);
            processedCount++;
            maxCursorId = Math.max(maxCursorId, sourceRow.cursorId());
            maxSyncTime = SyncRuntimeSupport.maxDatetime(maxSyncTime, sourceRow.syncTime());
            deduplicatedRows.put(sourceRowMapper.buildTargetKey(config, sourceRow.targetValues()), sourceRow);
        }
        long mapMillis = SyncRuntimeSupport.elapsedMillis(mapStartMillis);

        List<SourceRow> effectiveRows = new ArrayList<>(deduplicatedRows.values());
        long deleteStartMillis = System.currentTimeMillis();
        long deletedCount = targetJdbcWriter.batchDeleteTarget(targetConn, config, effectiveRows);
        long deleteMillis = SyncRuntimeSupport.elapsedMillis(deleteStartMillis);
        List<SourceRow> insertRows = effectiveRows.stream()
                .filter(row -> !row.deleted())
                .collect(Collectors.toList());
        long writeStartMillis = System.currentTimeMillis();
        long insertedCount = targetJdbcWriter.batchInsertTarget(targetConn, config, insertRows);
        long writeMillis = SyncRuntimeSupport.elapsedMillis(writeStartMillis);

        log.info("delete_insert batch write finished, syncKey: {}, sourceRowCount: {}, deduplicatedRowCount: {}, preDeleteAffectedCount: {}, insertCount: {}, mapMillis: {}, deleteMillis: {}, writeMillis: {}",
                config.syncKey(), rows.size(), effectiveRows.size(), deletedCount, insertedCount, mapMillis, deleteMillis, writeMillis);
        return new BatchWriteResult(processedCount, insertedCount, deletedCount, maxCursorId, maxSyncTime, mapMillis, deleteMillis, writeMillis);
    }
}
