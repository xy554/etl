package com.xy.etl.service.impl;

import com.xy.etl.dto.DbSyncRequest;
import com.xy.etl.dto.DbSyncResponse;
import com.xy.etl.dto.DbSyncTableConfigDTO;
import com.xy.etl.sync.checkpoint.CheckpointStore;
import com.xy.etl.sync.config.DbSyncConfigResolver;
import com.xy.etl.sync.datasource.DirectDataSourceResolver;
import com.xy.etl.sync.executor.TableSyncExecutor;
import com.xy.etl.sync.model.TableSyncResult;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class StandaloneDbSyncServiceImplTest {

    @Test
    void shouldContinueOnErrorAndAggregateSuccessRows() {
        DbSyncConfigResolver resolver = mock(DbSyncConfigResolver.class);
        DirectDataSourceResolver dataSourceResolver = mock(DirectDataSourceResolver.class);
        CheckpointStore checkpointStore = mock(CheckpointStore.class);
        TableSyncExecutor executor = mock(TableSyncExecutor.class);
        StandaloneDbSyncServiceImpl service = new StandaloneDbSyncServiceImpl(resolver, dataSourceResolver, checkpointStore, executor);

        DbSyncTableConfigDTO table1 = DbSyncTableConfigDTO.builder().syncKey("s1").sourceTable("a").targetTable("b").build();
        DbSyncTableConfigDTO table2 = DbSyncTableConfigDTO.builder().syncKey("s2").sourceTable("c").targetTable("d").build();
        DbSyncRequest request = DbSyncRequest.builder()
                .continueOnError(true)
                .tableConfigs(List.of(table1, table2))
                .build();

        when(executor.syncTable(request, table1)).thenReturn(new TableSyncResult("s1", "a", "b", 10, 8, 2, "t1", 1L, "t2", 2L));
        when(executor.syncTable(request, table2)).thenThrow(new RuntimeException("boom"));

        DbSyncResponse response = service.run(request);

        assertFalse(response.isSuccess());
        assertEquals(10L, response.getTotalProcessedCount());
        assertEquals(8L, response.getTotalUpsertedCount());
        assertEquals(2L, response.getTotalDeletedCount());
        assertEquals(2, response.getTableResults().size());
        assertTrue(response.getTableResults().get(0).isSuccess());
        assertFalse(response.getTableResults().get(1).isSuccess());
    }

    @Test
    void shouldStopOnFirstErrorWhenContinueOnErrorDisabled() {
        DbSyncConfigResolver resolver = mock(DbSyncConfigResolver.class);
        DirectDataSourceResolver dataSourceResolver = mock(DirectDataSourceResolver.class);
        CheckpointStore checkpointStore = mock(CheckpointStore.class);
        TableSyncExecutor executor = mock(TableSyncExecutor.class);
        StandaloneDbSyncServiceImpl service = new StandaloneDbSyncServiceImpl(resolver, dataSourceResolver, checkpointStore, executor);

        DbSyncTableConfigDTO table1 = DbSyncTableConfigDTO.builder().syncKey("s1").sourceTable("a").targetTable("b").build();
        DbSyncTableConfigDTO table2 = DbSyncTableConfigDTO.builder().syncKey("s2").sourceTable("c").targetTable("d").build();
        DbSyncRequest request = DbSyncRequest.builder()
                .continueOnError(false)
                .tableConfigs(List.of(table1, table2))
                .build();

        doThrow(new RuntimeException("boom")).when(executor).syncTable(request, table1);

        DbSyncResponse response = service.run(request);

        assertFalse(response.isSuccess());
        assertEquals(1, response.getTableResults().size());
        verify(executor, times(1)).syncTable(request, table1);
        verify(executor, times(0)).syncTable(request, table2);
    }
}
