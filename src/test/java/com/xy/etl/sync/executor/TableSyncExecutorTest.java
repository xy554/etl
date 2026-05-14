package com.xy.etl.sync.executor;

import com.xy.etl.dto.DbSyncRequest;
import com.xy.etl.dto.DbSyncTableConfigDTO;
import com.xy.etl.sync.checkpoint.CheckpointStore;
import com.xy.etl.sync.config.DbSyncConfigResolver;
import com.xy.etl.sync.datasource.DirectDataSourceResolver;
import com.xy.etl.sync.model.BatchWriteResult;
import com.xy.etl.sync.model.Checkpoint;
import com.xy.etl.sync.model.ResolvedDeleteRule;
import com.xy.etl.sync.model.ResolvedTableConfig;
import com.xy.etl.sync.model.TableSyncResult;
import com.xy.etl.sync.reader.SourceBatchReader;
import com.xy.etl.sync.support.DbSyncConstants;
import com.xy.etl.sync.writer.TargetJdbcWriter;
import com.xy.etl.sync.writer.strategy.BatchWriteStrategy;
import com.xy.etl.sync.writer.strategy.BatchWriteStrategyFactory;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.sql.Connection;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class TableSyncExecutorTest {

    @Test
    void shouldAdvanceCheckpointPerBatchInIncrementalMode() throws Exception {
        DbSyncConfigResolver configResolver = mock(DbSyncConfigResolver.class);
        DirectDataSourceResolver dataSourceResolver = mock(DirectDataSourceResolver.class);
        CheckpointStore checkpointStore = mock(CheckpointStore.class);
        SourceBatchReader sourceBatchReader = mock(SourceBatchReader.class);
        BatchWriteStrategyFactory strategyFactory = mock(BatchWriteStrategyFactory.class);
        TargetJdbcWriter targetJdbcWriter = mock(TargetJdbcWriter.class);
        TableSyncExecutor executor = new TableSyncExecutor(configResolver, dataSourceResolver, checkpointStore, sourceBatchReader, strategyFactory, targetJdbcWriter);

        ResolvedTableConfig config = incrementalConfig();
        DbSyncRequest request = DbSyncRequest.builder().build();
        DbSyncTableConfigDTO table = DbSyncTableConfigDTO.builder().build();
        DataSource sourceDs = mock(DataSource.class);
        DataSource targetDs = mock(DataSource.class);
        Connection conn = mock(Connection.class);
        BatchWriteStrategy strategy = mock(BatchWriteStrategy.class);
        List<Map<String, Object>> batchRows = List.of(Map.of("id", 1L));

        when(configResolver.resolveTableConfig(request, table)).thenReturn(config);
        when(dataSourceResolver.loadSourceDataSource(config)).thenReturn(sourceDs);
        when(dataSourceResolver.loadTargetDataSource(config)).thenReturn(targetDs);
        when(checkpointStore.loadCheckpoint(targetDs, config.checkpointTable(), config.syncKey())).thenReturn(new Checkpoint("2024-01-01 00:00:00", 1L));
        when(sourceBatchReader.fetchBatch(sourceDs, config, "2024-01-01 00:00:00", 1L)).thenReturn(batchRows);
        when(sourceBatchReader.fetchBatch(sourceDs, config, "2024-01-01 00:00:01", 2L)).thenReturn(List.of());
        when(strategyFactory.get(config.writeMode())).thenReturn(strategy);
        when(targetDs.getConnection()).thenReturn(conn);
        when(strategy.write(conn, config, batchRows)).thenReturn(new BatchWriteResult(1, 1, 0, 2L, "2024-01-01 00:00:01", 1, 1, 1));

        TableSyncResult result = executor.syncTable(request, table);

        assertEquals(1L, result.processedCount());
        assertEquals("2024-01-01 00:00:01", result.endSyncTime());
        verify(checkpointStore, times(1)).updateCheckpoint(conn, config.checkpointTable(), config.syncKey(), config.sourceTable(), config.targetTable(), "2024-01-01 00:00:01", 2L);
        verify(conn, times(1)).commit();
    }

    @Test
    void shouldOnlyUpdateCheckpointAfterSuccessfulFullRefresh() throws Exception {
        DbSyncConfigResolver configResolver = mock(DbSyncConfigResolver.class);
        DirectDataSourceResolver dataSourceResolver = mock(DirectDataSourceResolver.class);
        CheckpointStore checkpointStore = mock(CheckpointStore.class);
        SourceBatchReader sourceBatchReader = mock(SourceBatchReader.class);
        BatchWriteStrategyFactory strategyFactory = mock(BatchWriteStrategyFactory.class);
        TargetJdbcWriter targetJdbcWriter = mock(TargetJdbcWriter.class);
        TableSyncExecutor executor = new TableSyncExecutor(configResolver, dataSourceResolver, checkpointStore, sourceBatchReader, strategyFactory, targetJdbcWriter);

        ResolvedTableConfig config = fullRefreshConfig();
        DbSyncRequest request = DbSyncRequest.builder().build();
        DbSyncTableConfigDTO table = DbSyncTableConfigDTO.builder().build();
        DataSource sourceDs = mock(DataSource.class);
        DataSource targetDs = mock(DataSource.class);
        Connection conn = mock(Connection.class);
        BatchWriteStrategy strategy = mock(BatchWriteStrategy.class);
        List<Map<String, Object>> batchRows = List.of(Map.of("id", 1L));

        when(configResolver.resolveTableConfig(request, table)).thenReturn(config);
        when(dataSourceResolver.loadSourceDataSource(config)).thenReturn(sourceDs);
        when(dataSourceResolver.loadTargetDataSource(config)).thenReturn(targetDs);
        when(checkpointStore.loadCheckpoint(targetDs, config.checkpointTable(), config.syncKey())).thenReturn(new Checkpoint("2024-01-01 00:00:00", 1L));
        when(sourceBatchReader.fetchBatch(sourceDs, config, null, 0L)).thenReturn(batchRows);
        when(sourceBatchReader.fetchBatch(sourceDs, config, "2024-01-01 00:00:01", 2L)).thenReturn(List.of());
        when(strategyFactory.get(config.writeMode())).thenReturn(strategy);
        when(targetDs.getConnection()).thenReturn(conn);
        when(strategy.write(conn, config, batchRows)).thenReturn(new BatchWriteResult(1, 1, 0, 2L, "2024-01-01 00:00:01", 1, 0, 1));

        TableSyncResult result = executor.syncTable(request, table);

        assertEquals(1L, result.upsertedCount());
        verify(targetJdbcWriter, times(1)).clearTargetForFullRefresh(conn, config);
        verify(checkpointStore, times(1)).updateCheckpoint(conn, config.checkpointTable(), config.syncKey(), config.sourceTable(), config.targetTable(), "2024-01-01 00:00:01", 2L);
        verify(conn, times(1)).commit();
    }

    @Test
    void shouldRollbackAndKeepCheckpointWhenWriteFails() throws Exception {
        DbSyncConfigResolver configResolver = mock(DbSyncConfigResolver.class);
        DirectDataSourceResolver dataSourceResolver = mock(DirectDataSourceResolver.class);
        CheckpointStore checkpointStore = mock(CheckpointStore.class);
        SourceBatchReader sourceBatchReader = mock(SourceBatchReader.class);
        BatchWriteStrategyFactory strategyFactory = mock(BatchWriteStrategyFactory.class);
        TargetJdbcWriter targetJdbcWriter = mock(TargetJdbcWriter.class);
        TableSyncExecutor executor = new TableSyncExecutor(configResolver, dataSourceResolver, checkpointStore, sourceBatchReader, strategyFactory, targetJdbcWriter);

        ResolvedTableConfig config = incrementalConfig();
        DbSyncRequest request = DbSyncRequest.builder().build();
        DbSyncTableConfigDTO table = DbSyncTableConfigDTO.builder().build();
        DataSource sourceDs = mock(DataSource.class);
        DataSource targetDs = mock(DataSource.class);
        Connection conn = mock(Connection.class);
        BatchWriteStrategy strategy = mock(BatchWriteStrategy.class);
        List<Map<String, Object>> batchRows = List.of(Map.of("id", 1L));

        when(configResolver.resolveTableConfig(request, table)).thenReturn(config);
        when(dataSourceResolver.loadSourceDataSource(config)).thenReturn(sourceDs);
        when(dataSourceResolver.loadTargetDataSource(config)).thenReturn(targetDs);
        when(checkpointStore.loadCheckpoint(targetDs, config.checkpointTable(), config.syncKey())).thenReturn(new Checkpoint("2024-01-01 00:00:00", 1L));
        when(sourceBatchReader.fetchBatch(sourceDs, config, "2024-01-01 00:00:00", 1L)).thenReturn(batchRows);
        when(strategyFactory.get(config.writeMode())).thenReturn(strategy);
        when(targetDs.getConnection()).thenReturn(conn);
        doThrow(new RuntimeException("write failed")).when(strategy).write(conn, config, batchRows);

        RuntimeException ex = assertThrows(RuntimeException.class, () -> executor.syncTable(request, table));

        assertEquals("sync table failed, syncKey: syncKey, message: write failed", ex.getMessage());
        verify(conn, times(1)).rollback();
        verify(checkpointStore, never()).updateCheckpoint(any(), any(), any(), any(), any(), any(), eq(0L));
    }

    private static ResolvedTableConfig incrementalConfig() {
        return new ResolvedTableConfig(
                null, null, null, null, "syncKey", DbSyncConstants.SOURCE_MODE_TABLE, DbSyncConstants.WRITE_MODE_UPSERT,
                DbSyncConstants.FULL_REFRESH_DELETE_MODE_DELETE, "src_table", null, "tgt_table", "id", "update_time", null,
                null, "id", List.of(), "id", 100,
                List.of(), List.of(), new ResolvedDeleteRule(null, null, null, null),
                DbSyncConstants.DEFAULT_CHECKPOINT_TABLE, true
        );
    }

    private static ResolvedTableConfig fullRefreshConfig() {
        return new ResolvedTableConfig(
                null, null, null, null, "syncKey", DbSyncConstants.SOURCE_MODE_TABLE, DbSyncConstants.WRITE_MODE_FULL_REFRESH_INSERT,
                DbSyncConstants.FULL_REFRESH_DELETE_MODE_DELETE, "src_table", null, "tgt_table", "id", "update_time", null,
                null, "id", List.of(), "id", 100,
                List.of(), List.of(), new ResolvedDeleteRule(null, null, null, null),
                DbSyncConstants.DEFAULT_CHECKPOINT_TABLE, true
        );
    }
}
