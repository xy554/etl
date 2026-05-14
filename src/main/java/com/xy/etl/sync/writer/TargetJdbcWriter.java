package com.xy.etl.sync.writer;

import com.xy.etl.sync.model.ResolvedTableConfig;
import com.xy.etl.sync.model.SourceRow;
import com.xy.etl.sync.support.DbSyncConstants;
import com.xy.etl.sync.support.SyncRuntimeSupport;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

@Slf4j
@Component
public class TargetJdbcWriter {

    private final TargetSqlBuilder targetSqlBuilder;

    public TargetJdbcWriter(TargetSqlBuilder targetSqlBuilder) {
        this.targetSqlBuilder = targetSqlBuilder;
    }

    public int clearTargetForFullRefresh(Connection targetConn, ResolvedTableConfig config) {
        if (DbSyncConstants.FULL_REFRESH_DELETE_MODE_TRUNCATE.equals(config.fullRefreshDeleteMode())) {
            log.warn("full refresh uses truncate to clear target table, syncKey: {}, targetTable: {}. Note: MySQL truncate may not rollback inside transactions.",
                    config.syncKey(), config.targetTable());
            String sql = "TRUNCATE TABLE " + config.targetTable();
            try (Statement statement = targetConn.createStatement()) {
                statement.execute(sql);
                return 0;
            } catch (Exception e) {
                throw new RuntimeException("truncate target failed, targetTable: " + config.targetTable() + ", message: " + e.getMessage(), e);
            }
        }

        String sql = "DELETE FROM " + config.targetTable();
        try (PreparedStatement ps = targetConn.prepareStatement(sql)) {
            return ps.executeUpdate();
        } catch (Exception e) {
            throw new RuntimeException("clear target failed, targetTable: " + config.targetTable() + ", message: " + e.getMessage(), e);
        }
    }

    public int batchDeleteTarget(Connection targetConn,
                                 ResolvedTableConfig config,
                                 List<SourceRow> rows) {
        if (rows.isEmpty()) {
            return 0;
        }

        int affectedRows = 0;
        List<String> keyColumns = SyncRuntimeSupport.resolveTargetKeyColumns(config);
        for (int start = 0; start < rows.size(); start += DbSyncConstants.DELETE_BATCH_SIZE) {
            List<SourceRow> batchRows = rows.subList(start, Math.min(start + DbSyncConstants.DELETE_BATCH_SIZE, rows.size()));
            String sql = targetSqlBuilder.buildBatchDeleteSql(config.targetTable(), keyColumns, batchRows.size());
            try (PreparedStatement ps = targetConn.prepareStatement(sql)) {
                int index = 1;
                for (SourceRow row : batchRows) {
                    for (String keyColumn : keyColumns) {
                        setPreparedStatementValue(ps, index++, row.targetValues().get(keyColumn));
                    }
                }
                affectedRows += ps.executeUpdate();
            } catch (Exception e) {
                throw new RuntimeException("batch delete target failed, targetTable: " + config.targetTable() + ", message: " + e.getMessage(), e);
            }
        }
        return affectedRows;
    }

    public int batchInsertTarget(Connection targetConn,
                                 ResolvedTableConfig config,
                                 List<SourceRow> rows) {
        if (rows.isEmpty()) {
            return 0;
        }

        int affectedRows = 0;
        List<String> insertColumns = new ArrayList<>(rows.get(0).targetValues().keySet());
        for (int start = 0; start < rows.size(); start += DbSyncConstants.MULTI_VALUES_INSERT_CHUNK_SIZE) {
            List<SourceRow> chunkRows = rows.subList(start, Math.min(start + DbSyncConstants.MULTI_VALUES_INSERT_CHUNK_SIZE, rows.size()));
            String sql = targetSqlBuilder.buildMultiValuesInsertSql(config, insertColumns, chunkRows.size());
            try (PreparedStatement ps = targetConn.prepareStatement(sql)) {
                int index = 1;
                for (SourceRow row : chunkRows) {
                    for (String column : insertColumns) {
                        setPreparedStatementValue(ps, index++, row.targetValues().get(column));
                    }
                }
                affectedRows += ps.executeUpdate();
            } catch (Exception e) {
                throw new RuntimeException("batch insert target failed, targetTable: " + config.targetTable() + ", message: " + e.getMessage(), e);
            }
        }
        return affectedRows;
    }

    public int batchUpsertTarget(Connection targetConn,
                                 ResolvedTableConfig config,
                                 List<SourceRow> rows) {
        if (rows.isEmpty()) {
            return 0;
        }

        int affectedRows = 0;
        List<String> insertColumns = new ArrayList<>(rows.get(0).targetValues().keySet());
        for (int start = 0; start < rows.size(); start += DbSyncConstants.MULTI_VALUES_UPSERT_CHUNK_SIZE) {
            List<SourceRow> chunkRows = rows.subList(start, Math.min(start + DbSyncConstants.MULTI_VALUES_UPSERT_CHUNK_SIZE, rows.size()));
            String sql = targetSqlBuilder.buildUpsertSql(config, insertColumns, chunkRows.size());
            try (PreparedStatement ps = targetConn.prepareStatement(sql)) {
                int index = 1;
                for (SourceRow row : chunkRows) {
                    for (String column : insertColumns) {
                        setPreparedStatementValue(ps, index++, row.targetValues().get(column));
                    }
                }
                affectedRows += ps.executeUpdate();
            } catch (Exception e) {
                throw new RuntimeException("batch upsert target failed, targetTable: " + config.targetTable() + ", message: " + e.getMessage(), e);
            }
        }
        return affectedRows;
    }

    public int multiValuesUpsertTarget(Connection targetConn,
                                       ResolvedTableConfig config,
                                       List<SourceRow> rows) {
        if (rows.isEmpty()) {
            return 0;
        }

        int affectedRows = 0;
        List<String> insertColumns = new ArrayList<>(rows.get(0).targetValues().keySet());
        for (int start = 0; start < rows.size(); start += DbSyncConstants.MULTI_VALUES_UPSERT_CHUNK_SIZE) {
            List<SourceRow> chunkRows = rows.subList(start, Math.min(start + DbSyncConstants.MULTI_VALUES_UPSERT_CHUNK_SIZE, rows.size()));
            String sql = targetSqlBuilder.buildUpsertSql(config, insertColumns, chunkRows.size());
            try (PreparedStatement ps = targetConn.prepareStatement(sql)) {
                int index = 1;
                for (SourceRow row : chunkRows) {
                    for (String column : insertColumns) {
                        setPreparedStatementValue(ps, index++, row.targetValues().get(column));
                    }
                }
                affectedRows += ps.executeUpdate();
            } catch (Exception e) {
                throw new RuntimeException("multi values upsert target failed, targetTable: " + config.targetTable() + ", message: " + e.getMessage(), e);
            }
        }
        return affectedRows;
    }

    private void setPreparedStatementValue(PreparedStatement ps, int index, Object value) throws Exception {
        if (value == null) {
            ps.setNull(index, java.sql.Types.NULL);
            return;
        }
        if (value instanceof LocalDateTime localDateTime) {
            ps.setTimestamp(index, Timestamp.valueOf(localDateTime));
            return;
        }
        if (value instanceof java.util.Date date) {
            ps.setTimestamp(index, new Timestamp(date.getTime()));
            return;
        }
        ps.setObject(index, value);
    }
}
