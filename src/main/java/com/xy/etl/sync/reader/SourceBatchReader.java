package com.xy.etl.sync.reader;

import com.xy.etl.sync.config.DbSyncConfigResolver;
import com.xy.etl.sync.model.ResolvedFilter;
import com.xy.etl.sync.model.ResolvedTableConfig;
import com.xy.etl.sync.support.DbSyncConstants;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
@Component
public class SourceBatchReader {

    private final DbSyncConfigResolver configResolver;

    public SourceBatchReader(DbSyncConfigResolver configResolver) {
        this.configResolver = configResolver;
    }

    public List<Map<String, Object>> fetchBatch(DataSource sourceDataSource,
                                                ResolvedTableConfig config,
                                                String lastSyncTime,
                                                long lastSyncId) {
        log.debug("prepare fetch source batch, syncKey: {}, sourceMode: {}, checkpoint: {}/{}",
                config.syncKey(), config.sourceMode(), lastSyncTime, lastSyncId);
        if (DbSyncConstants.SOURCE_MODE_SQL.equals(config.sourceMode())) {
            return fetchBatchBySql(sourceDataSource, config, lastSyncTime, lastSyncId);
        }
        return fetchBatchByTable(sourceDataSource, config, lastSyncTime, lastSyncId);
    }

    private List<Map<String, Object>> fetchBatchByTable(DataSource sourceDataSource,
                                                        ResolvedTableConfig config,
                                                        String lastSyncTime,
                                                        long lastSyncId) {
        List<String> selectColumns = configResolver.buildSelectColumns(config);
        String syncTimeExpression = configResolver.buildSyncTimeExpression(config);
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT ").append(String.join(", ", selectColumns))
                .append(" FROM ").append(config.sourceTable())
                .append(" WHERE ");

        if (!config.filters().isEmpty()) {
            String filtersSql = config.filters().stream()
                    .map(filter -> filter.column() + " = ?")
                    .collect(Collectors.joining(" AND "));
            sql.append(filtersSql).append(" AND ");
        }

        sql.append("(")
                .append(syncTimeExpression).append(" > ? OR (")
                .append(syncTimeExpression).append(" = ? AND ")
                .append(config.cursorIdColumn()).append(" > ?)) ")
                .append("ORDER BY ").append(syncTimeExpression).append(", ").append(config.cursorIdColumn())
                .append(" LIMIT ?");

        try (Connection connection = sourceDataSource.getConnection();
             PreparedStatement ps = connection.prepareStatement(sql.toString())) {
            int index = 1;
            for (ResolvedFilter filter : config.filters()) {
                ps.setObject(index++, filter.value());
            }

            Timestamp checkpointTime = Timestamp.valueOf(lastSyncTime == null ? DbSyncConstants.EPOCH_TIME : lastSyncTime);
            ps.setTimestamp(index++, checkpointTime);
            ps.setTimestamp(index++, checkpointTime);
            ps.setLong(index++, lastSyncId);
            ps.setInt(index, config.batchSize());
            log.debug("execute table mode source query, syncKey: {}, sourceTable: {}, batchSize: {}, filterCount: {}",
                    config.syncKey(), config.sourceTable(), config.batchSize(), config.filters().size());

            try (ResultSet rs = ps.executeQuery()) {
                return extractRows(rs);
            }
        } catch (Exception e) {
            throw new RuntimeException("fetch source data failed: " + e.getMessage(), e);
        }
    }

    private List<Map<String, Object>> fetchBatchBySql(DataSource sourceDataSource,
                                                      ResolvedTableConfig config,
                                                      String lastSyncTime,
                                                      long lastSyncId) {
        String syncTimeExpression = configResolver.buildSyncTimeExpression(config);
        String sql = "SELECT * FROM (" + config.sourceSql() + ") sync_src WHERE (" +
                syncTimeExpression + " > ? OR (" + syncTimeExpression + " = ? AND " +
                config.cursorIdColumn() + " > ?)) ORDER BY " + syncTimeExpression + ", " +
                config.cursorIdColumn() + " LIMIT ?";

        try (Connection connection = sourceDataSource.getConnection();
             PreparedStatement ps = connection.prepareStatement(sql)) {
            Timestamp checkpointTime = Timestamp.valueOf(lastSyncTime == null ? DbSyncConstants.EPOCH_TIME : lastSyncTime);
            ps.setTimestamp(1, checkpointTime);
            ps.setTimestamp(2, checkpointTime);
            ps.setLong(3, lastSyncId);
            ps.setInt(4, config.batchSize());
            log.debug("execute sql mode source query, syncKey: {}, sourceTable: {}, batchSize: {}",
                    config.syncKey(), config.sourceTable(), config.batchSize());

            try (ResultSet rs = ps.executeQuery()) {
                return extractRows(rs);
            }
        } catch (Exception e) {
            throw new RuntimeException("fetch sql source data failed: " + e.getMessage(), e);
        }
    }

    private List<Map<String, Object>> extractRows(ResultSet rs) throws Exception {
        ResultSetMetaData metaData = rs.getMetaData();
        int columnCount = metaData.getColumnCount();
        List<Map<String, Object>> rows = new ArrayList<>();
        while (rs.next()) {
            Map<String, Object> row = new LinkedHashMap<>();
            for (int i = 1; i <= columnCount; i++) {
                row.put(metaData.getColumnLabel(i), rs.getObject(i));
            }
            rows.add(row);
        }
        return rows;
    }
}
