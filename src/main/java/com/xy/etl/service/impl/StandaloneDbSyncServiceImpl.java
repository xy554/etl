package com.xy.etl.service.impl;

import com.xy.etl.dto.DbSyncCheckpointDTO;
import com.xy.etl.dto.DbSyncCheckpointResetRequest;
import com.xy.etl.dto.DbSyncRequest;
import com.xy.etl.dto.DbSyncResponse;
import com.xy.etl.dto.DbSyncTableConfigDTO;
import com.xy.etl.dto.DbSyncTableResultDTO;
import com.xy.etl.service.StandaloneDbSyncService;
import com.xy.etl.sync.checkpoint.CheckpointStore;
import com.xy.etl.sync.config.DbSyncConfigResolver;
import com.xy.etl.sync.datasource.DirectDataSourceResolver;
import com.xy.etl.sync.executor.TableSyncExecutor;
import com.xy.etl.sync.model.TableSyncResult;
import com.xy.etl.sync.support.DbSyncConstants;
import com.xy.etl.sync.support.SyncRuntimeSupport;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

@Slf4j
@Service
public class StandaloneDbSyncServiceImpl implements StandaloneDbSyncService {

    private final DbSyncConfigResolver configResolver;
    private final DirectDataSourceResolver dataSourceResolver;
    private final CheckpointStore checkpointStore;
    private final TableSyncExecutor tableSyncExecutor;

    public StandaloneDbSyncServiceImpl(DbSyncConfigResolver configResolver,
                                       DirectDataSourceResolver dataSourceResolver,
                                       CheckpointStore checkpointStore,
                                       TableSyncExecutor tableSyncExecutor) {
        this.configResolver = configResolver;
        this.dataSourceResolver = dataSourceResolver;
        this.checkpointStore = checkpointStore;
        this.tableSyncExecutor = tableSyncExecutor;
    }

    @Override
    public DbSyncResponse run(DbSyncRequest request) {
        configResolver.validateRequest(request);

        LocalDateTime startTime = LocalDateTime.now();
        boolean continueOnError = Boolean.TRUE.equals(request.getContinueOnError());
        List<DbSyncTableResultDTO> tableResults = new ArrayList<>();
        long totalProcessed = 0L;
        long totalUpserted = 0L;
        long totalDeleted = 0L;
        boolean success = true;

        log.info("start db sync request, tableCount: {}, requestBatchSize: {}, continueOnError: {}",
                request.getTableConfigs().size(), request.getBatchSize(), continueOnError);
        long requestStartMillis = System.currentTimeMillis();

        for (DbSyncTableConfigDTO tableConfig : request.getTableConfigs()) {
            DbSyncTableResultDTO tableResult;
            long tableStartMillis = System.currentTimeMillis();
            try {
                log.info("start single table sync, syncKey: {}, sourceMode: {}, sourceTable: {}, targetTable: {}",
                        tableConfig.getSyncKey(), tableConfig.getSourceMode(), tableConfig.getSourceTable(), tableConfig.getTargetTable());
                TableSyncResult syncResult = tableSyncExecutor.syncTable(request, tableConfig);
                tableResult = DbSyncTableResultDTO.builder()
                        .syncKey(syncResult.syncKey())
                        .sourceTable(syncResult.sourceTable())
                        .targetTable(syncResult.targetTable())
                        .success(true)
                        .processedCount(syncResult.processedCount())
                        .upsertedCount(syncResult.upsertedCount())
                        .deletedCount(syncResult.deletedCount())
                        .startSyncTime(syncResult.startSyncTime())
                        .startSyncId(syncResult.startSyncId())
                        .endSyncTime(syncResult.endSyncTime())
                        .endSyncId(syncResult.endSyncId())
                        .build();
                totalProcessed += syncResult.processedCount();
                totalUpserted += syncResult.upsertedCount();
                totalDeleted += syncResult.deletedCount();
                log.info("single table sync finished, syncKey: {}, processedCount: {}, upsertedCount: {}, deletedCount: {}, checkpoint: {}/{} -> {}/{}, elapsedMillis: {}",
                        syncResult.syncKey(), syncResult.processedCount(), syncResult.upsertedCount(), syncResult.deletedCount(),
                        syncResult.startSyncTime(), syncResult.startSyncId(), syncResult.endSyncTime(), syncResult.endSyncId(),
                        SyncRuntimeSupport.elapsedMillis(tableStartMillis));
            } catch (Exception e) {
                success = false;
                tableResult = DbSyncTableResultDTO.builder()
                        .syncKey(tableConfig.getSyncKey())
                        .sourceTable(tableConfig.getSourceTable())
                        .targetTable(tableConfig.getTargetTable())
                        .success(false)
                        .errorMessage(e.getMessage())
                        .processedCount(0L)
                        .upsertedCount(0L)
                        .deletedCount(0L)
                        .build();
                log.error("single table sync failed, sourceTable: {}, targetTable: {}, syncKey: {}, elapsedMillis: {}",
                        tableConfig.getSourceTable(), tableConfig.getTargetTable(), tableConfig.getSyncKey(),
                        SyncRuntimeSupport.elapsedMillis(tableStartMillis), e);
                tableResults.add(tableResult);
                if (!continueOnError) {
                    break;
                }
                continue;
            }
            tableResults.add(tableResult);
        }

        log.info("db sync request finished, success: {}, totalProcessed: {}, totalUpserted: {}, totalDeleted: {}, tableResultCount: {}, totalMillis: {}",
                success, totalProcessed, totalUpserted, totalDeleted, tableResults.size(), SyncRuntimeSupport.elapsedMillis(requestStartMillis));

        return DbSyncResponse.builder()
                .success(success)
                .startTime(startTime)
                .endTime(LocalDateTime.now())
                .totalProcessedCount(totalProcessed)
                .totalUpsertedCount(totalUpserted)
                .totalDeletedCount(totalDeleted)
                .tableResults(tableResults)
                .build();
    }

    @Override
    public List<DbSyncCheckpointDTO> getCheckpoints(Long targetDataSourceId,
                                                    com.xy.etl.dto.DirectDataSourceConfigDTO targetDataSourceConfig,
                                                    String syncKey) {
        DataSource targetDataSource = dataSourceResolver.resolveTargetDataSource(targetDataSourceId, targetDataSourceConfig);
        return checkpointStore.queryCheckpoints(targetDataSource, DbSyncConstants.DEFAULT_CHECKPOINT_TABLE, syncKey);
    }

    @Override
    public int resetCheckpoints(DbSyncCheckpointResetRequest request) {
        if (request == null) {
            throw new RuntimeException("request cannot be null");
        }
        if (request.getSyncKeys() == null || request.getSyncKeys().isEmpty()) {
            throw new RuntimeException("syncKeys cannot be empty");
        }

        DataSource targetDataSource = dataSourceResolver.resolveTargetDataSource(
                request.getTargetDataSourceId(),
                request.getTargetDataSourceConfig()
        );
        return checkpointStore.resetCheckpoints(targetDataSource, DbSyncConstants.DEFAULT_CHECKPOINT_TABLE, request.getSyncKeys());
    }
}
