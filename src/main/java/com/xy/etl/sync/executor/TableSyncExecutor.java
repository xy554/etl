package com.xy.etl.sync.executor;

import com.xy.etl.dto.DbSyncRequest;
import com.xy.etl.dto.DbSyncTableConfigDTO;
import com.xy.etl.sync.checkpoint.CheckpointStore;
import com.xy.etl.sync.config.DbSyncConfigResolver;
import com.xy.etl.sync.datasource.DirectDataSourceResolver;
import com.xy.etl.sync.model.BatchWriteResult;
import com.xy.etl.sync.model.Checkpoint;
import com.xy.etl.sync.model.ResolvedTableConfig;
import com.xy.etl.sync.model.TableSyncResult;
import com.xy.etl.sync.reader.SourceBatchReader;
import com.xy.etl.sync.support.DbSyncConstants;
import com.xy.etl.sync.support.SyncRuntimeSupport;
import com.xy.etl.sync.writer.TargetJdbcWriter;
import com.xy.etl.sync.writer.strategy.BatchWriteStrategyFactory;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.sql.Connection;
import java.util.List;
import java.util.Map;

@Slf4j
@Component
public class TableSyncExecutor {

    private final DbSyncConfigResolver configResolver;
    private final DirectDataSourceResolver dataSourceResolver;
    private final CheckpointStore checkpointStore;
    private final SourceBatchReader sourceBatchReader;
    private final BatchWriteStrategyFactory batchWriteStrategyFactory;
    private final TargetJdbcWriter targetJdbcWriter;

    public TableSyncExecutor(DbSyncConfigResolver configResolver,
                             DirectDataSourceResolver dataSourceResolver,
                             CheckpointStore checkpointStore,
                             SourceBatchReader sourceBatchReader,
                             BatchWriteStrategyFactory batchWriteStrategyFactory,
                             TargetJdbcWriter targetJdbcWriter) {
        this.configResolver = configResolver;
        this.dataSourceResolver = dataSourceResolver;
        this.checkpointStore = checkpointStore;
        this.sourceBatchReader = sourceBatchReader;
        this.batchWriteStrategyFactory = batchWriteStrategyFactory;
        this.targetJdbcWriter = targetJdbcWriter;
    }

    public TableSyncResult syncTable(DbSyncRequest request, DbSyncTableConfigDTO tableConfig) {
        ResolvedTableConfig config = configResolver.resolveTableConfig(request, tableConfig);
        DataSource sourceDataSource = dataSourceResolver.loadSourceDataSource(config);
        DataSource targetDataSource = dataSourceResolver.loadTargetDataSource(config);
        checkpointStore.ensureCheckpointTable(targetDataSource, config.checkpointTable(), config.autoCreateCheckpointTable());

        Checkpoint checkpoint = checkpointStore.loadCheckpoint(targetDataSource, config.checkpointTable(), config.syncKey());
        if (DbSyncConstants.WRITE_MODE_FULL_REFRESH_INSERT.equals(config.writeMode())) {
            return syncTableByFullRefreshInsert(config, sourceDataSource, targetDataSource, checkpoint);
        }
        return syncTableIncrementally(config, sourceDataSource, targetDataSource, checkpoint);
    }

    private TableSyncResult syncTableIncrementally(ResolvedTableConfig config,
                                                   DataSource sourceDataSource,
                                                   DataSource targetDataSource,
                                                   Checkpoint checkpoint) {
        String currentSyncTime = checkpoint.lastSyncTime();
        long currentSyncId = checkpoint.lastSyncId() == null ? 0L : checkpoint.lastSyncId();
        long processedCount = 0L;
        long upsertedCount = 0L;
        long deletedCount = 0L;
        int batchNo = 0;
        long tableSyncStartMillis = System.currentTimeMillis();

        log.info("start incremental/batch sync, syncKey: {}, sourceMode: {}, writeMode: {}, sourceTable: {}, targetTable: {}, batchSize: {}, targetKeyColumns: {}, initialCheckpoint: {}/{}",
                config.syncKey(), config.sourceMode(), config.writeMode(), config.sourceTable(), config.targetTable(), config.batchSize(),
                SyncRuntimeSupport.resolveTargetKeyColumns(config), currentSyncTime, currentSyncId);
        if (DbSyncConstants.SOURCE_MODE_SQL.equals(config.sourceMode())) {
            log.warn("current sourceMode=sql may re-execute sourceSql on each batch, syncKey: {}, batchSize: {}. If source fetch is slow, consider materializing SQL results or increasing batch size.",
                    config.syncKey(), config.batchSize());
        }

        while (true) {
            batchNo++;
            long batchStartMillis = System.currentTimeMillis();
            long fetchStartMillis = System.currentTimeMillis();
            List<Map<String, Object>> rows = sourceBatchReader.fetchBatch(sourceDataSource, config, currentSyncTime, currentSyncId);
            long fetchMillis = SyncRuntimeSupport.elapsedMillis(fetchStartMillis);
            if (rows.isEmpty()) {
                log.info("source has no more data, syncKey: {}, batchNo: {}, checkpoint remains: {}/{}, fetchMillis: {}",
                        config.syncKey(), batchNo, currentSyncTime, currentSyncId, fetchMillis);
                break;
            }

            log.info("source batch fetched, syncKey: {}, batchNo: {}, rowCount: {}, sourceMode: {}, currentCheckpoint: {}/{}, fetchMillis: {}",
                    config.syncKey(), batchNo, rows.size(), config.sourceMode(), currentSyncTime, currentSyncId, fetchMillis);

            long batchMaxId = currentSyncId;
            String batchMaxTime = currentSyncTime;
            long batchProcessed;
            long batchUpserted;
            long batchDeleted;
            BatchWriteResult batchWriteResult;

            try (Connection targetConn = targetDataSource.getConnection()) {
                targetConn.setAutoCommit(false);
                try {
                    batchWriteResult = batchWriteStrategyFactory.get(config.writeMode()).write(targetConn, config, rows);
                    batchMaxId = Math.max(batchMaxId, batchWriteResult.maxCursorId());
                    batchMaxTime = SyncRuntimeSupport.maxDatetime(batchMaxTime, batchWriteResult.maxSyncTime());
                    batchProcessed = batchWriteResult.processedCount();
                    batchUpserted = batchWriteResult.upsertedCount();
                    batchDeleted = batchWriteResult.deletedCount();
                    processedCount += batchProcessed;
                    upsertedCount += batchUpserted;
                    deletedCount += batchDeleted;

                    long checkpointStartMillis = System.currentTimeMillis();
                    checkpointStore.updateCheckpoint(targetConn, config.checkpointTable(), config.syncKey(), config.sourceTable(), config.targetTable(), batchMaxTime, batchMaxId);
                    long checkpointMillis = SyncRuntimeSupport.elapsedMillis(checkpointStartMillis);
                    long commitStartMillis = System.currentTimeMillis();
                    targetConn.commit();
                    long commitMillis = SyncRuntimeSupport.elapsedMillis(commitStartMillis);
                    log.info("batch commit finished, syncKey: {}, batchNo: {}, writeMode: {}, processedCount: {}, upsertedCount: {}, deletedCount: {}, checkpointAdvanced: {}/{} -> {}/{}, fetchMillis: {}, mapMillis: {}, deleteMillis: {}, writeMillis: {}, checkpointMillis: {}, commitMillis: {}, batchTotalMillis: {}",
                            config.syncKey(), batchNo, config.writeMode(), batchProcessed, batchUpserted, batchDeleted,
                            currentSyncTime, currentSyncId, batchMaxTime, batchMaxId,
                            fetchMillis, batchWriteResult.mapMillis(), batchWriteResult.deleteMillis(), batchWriteResult.writeMillis(),
                            checkpointMillis, commitMillis, SyncRuntimeSupport.elapsedMillis(batchStartMillis));
                } catch (Exception e) {
                    targetConn.rollback();
                    log.error("batch write failed and rolled back, syncKey: {}, batchNo: {}, checkpoint remains: {}/{}, error: {}",
                            config.syncKey(), batchNo, currentSyncTime, currentSyncId, e.getMessage(), e);
                    throw e;
                }
            } catch (Exception e) {
                throw new RuntimeException("sync table failed, syncKey: " + config.syncKey() + ", message: " + e.getMessage(), e);
            }

            currentSyncTime = batchMaxTime;
            currentSyncId = batchMaxId;
        }

        log.info("incremental/batch sync finished, syncKey: {}, processedCount: {}, upsertedCount: {}, deletedCount: {}, finalCheckpoint: {}/{}, totalMillis: {}",
                config.syncKey(), processedCount, upsertedCount, deletedCount, currentSyncTime, currentSyncId,
                SyncRuntimeSupport.elapsedMillis(tableSyncStartMillis));

        return new TableSyncResult(
                config.syncKey(),
                config.sourceTable(),
                config.targetTable(),
                processedCount,
                upsertedCount,
                deletedCount,
                checkpoint.lastSyncTime(),
                checkpoint.lastSyncId(),
                currentSyncTime,
                currentSyncId
        );
    }

    private TableSyncResult syncTableByFullRefreshInsert(ResolvedTableConfig config,
                                                         DataSource sourceDataSource,
                                                         DataSource targetDataSource,
                                                         Checkpoint checkpoint) {
        String currentSyncTime = null;
        long currentSyncId = 0L;
        long processedCount = 0L;
        long insertedCount = 0L;
        int batchNo = 0;
        long syncStartMillis = System.currentTimeMillis();

        log.warn("start full refresh sync, syncKey: {}, targetTable: {}, clearMode: {}, batchSize: {}. This mode ignores checkpoint during reads and only updates checkpoint after a successful full refresh.",
                config.syncKey(), config.targetTable(), config.fullRefreshDeleteMode(), config.batchSize());

        try (Connection targetConn = targetDataSource.getConnection()) {
            targetConn.setAutoCommit(false);
            long preClearStartMillis = System.currentTimeMillis();
            long preClearCount = targetJdbcWriter.clearTargetForFullRefresh(targetConn, config);
            long preClearMillis = SyncRuntimeSupport.elapsedMillis(preClearStartMillis);

            try {
                while (true) {
                    batchNo++;
                    long batchStartMillis = System.currentTimeMillis();
                    long fetchStartMillis = System.currentTimeMillis();
                    List<Map<String, Object>> rows = sourceBatchReader.fetchBatch(sourceDataSource, config, currentSyncTime, currentSyncId);
                    long fetchMillis = SyncRuntimeSupport.elapsedMillis(fetchStartMillis);
                    if (rows.isEmpty()) {
                        log.info("full refresh source has no more data, syncKey: {}, batchNo: {}, pendingCheckpoint: {}/{}, fetchMillis: {}",
                                config.syncKey(), batchNo, currentSyncTime, currentSyncId, fetchMillis);
                        break;
                    }

                    BatchWriteResult batchWriteResult = batchWriteStrategyFactory.get(config.writeMode()).write(targetConn, config, rows);
                    currentSyncId = Math.max(currentSyncId, batchWriteResult.maxCursorId());
                    currentSyncTime = SyncRuntimeSupport.maxDatetime(currentSyncTime, batchWriteResult.maxSyncTime());
                    processedCount += batchWriteResult.processedCount();
                    insertedCount += batchWriteResult.upsertedCount();

                    log.info("full refresh batch insert finished, syncKey: {}, batchNo: {}, sourceRowCount: {}, insertedCount: {}, checkpointProgress: {}/{}, fetchMillis: {}, mapMillis: {}, writeMillis: {}, batchTotalMillis: {}",
                            config.syncKey(), batchNo, rows.size(), batchWriteResult.upsertedCount(), currentSyncTime, currentSyncId,
                            fetchMillis, batchWriteResult.mapMillis(), batchWriteResult.writeMillis(), SyncRuntimeSupport.elapsedMillis(batchStartMillis));
                }

                long checkpointStartMillis = System.currentTimeMillis();
                checkpointStore.updateCheckpoint(targetConn, config.checkpointTable(), config.syncKey(), config.sourceTable(), config.targetTable(), currentSyncTime, currentSyncId);
                long checkpointMillis = SyncRuntimeSupport.elapsedMillis(checkpointStartMillis);
                long commitStartMillis = System.currentTimeMillis();
                targetConn.commit();
                long commitMillis = SyncRuntimeSupport.elapsedMillis(commitStartMillis);

                log.info("full refresh sync finished, syncKey: {}, targetTable: {}, preClearAffectedCount: {}, processedCount: {}, insertedCount: {}, finalCheckpoint: {}/{}, preClearMillis: {}, checkpointMillis: {}, commitMillis: {}, totalMillis: {}",
                        config.syncKey(), config.targetTable(), preClearCount, processedCount, insertedCount, currentSyncTime, currentSyncId,
                        preClearMillis, checkpointMillis, commitMillis, SyncRuntimeSupport.elapsedMillis(syncStartMillis));
            } catch (Exception e) {
                targetConn.rollback();
                log.error("full refresh sync failed and rolled back, syncKey: {}, targetTable: {}, clearMode: {}, error: {}",
                        config.syncKey(), config.targetTable(), config.fullRefreshDeleteMode(), e.getMessage(), e);
                throw e;
            }
        } catch (Exception e) {
            throw new RuntimeException("full refresh sync table failed, syncKey: " + config.syncKey() + ", message: " + e.getMessage(), e);
        }

        return new TableSyncResult(
                config.syncKey(),
                config.sourceTable(),
                config.targetTable(),
                processedCount,
                insertedCount,
                0L,
                checkpoint.lastSyncTime(),
                checkpoint.lastSyncId(),
                currentSyncTime,
                currentSyncId
        );
    }
}
