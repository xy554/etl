package com.xy.etl.sync.checkpoint;

import com.xy.etl.dto.DbSyncCheckpointDTO;
import com.xy.etl.sync.model.Checkpoint;
import com.xy.etl.sync.support.SyncRuntimeSupport;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@Component
public class CheckpointStore {

    public void ensureCheckpointTable(DataSource targetDataSource, String checkpointTable, boolean autoCreateCheckpointTable) {
        if (checkpointTableExists(targetDataSource, checkpointTable)) {
            log.debug("checkpoint table already exists, checkpointTable: {}", checkpointTable);
            return;
        }
        if (!autoCreateCheckpointTable) {
            throw new RuntimeException("checkpoint table does not exist and auto creation is disabled: " + checkpointTable);
        }
        String ddl = "CREATE TABLE IF NOT EXISTS " + checkpointTable + " (" +
                "id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY, " +
                "sync_key VARCHAR(200) NOT NULL UNIQUE, " +
                "source_table VARCHAR(100) NOT NULL, " +
                "target_table VARCHAR(100) NOT NULL, " +
                "last_sync_time VARCHAR(19) NULL, " +
                "last_sync_id BIGINT NULL, " +
                "create_time DATETIME DEFAULT CURRENT_TIMESTAMP, " +
                "update_time DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP" +
                ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4";
        try (Connection connection = targetDataSource.getConnection();
             Statement statement = connection.createStatement()) {
            statement.execute(ddl);
        } catch (Exception e) {
            throw new RuntimeException("ensure checkpoint table failed: " + e.getMessage(), e);
        }
    }

    public boolean checkpointTableExists(DataSource targetDataSource, String checkpointTable) {
        try (Connection connection = targetDataSource.getConnection()) {
            String catalog = connection.getCatalog();
            try (ResultSet rs = connection.getMetaData().getTables(catalog, null, checkpointTable, new String[]{"TABLE"})) {
                if (rs.next()) {
                    return true;
                }
            }
            try (ResultSet rs = connection.getMetaData().getTables(catalog, null, checkpointTable.toUpperCase(), new String[]{"TABLE"})) {
                if (rs.next()) {
                    return true;
                }
            }
            try (ResultSet rs = connection.getMetaData().getTables(catalog, null, checkpointTable.toLowerCase(), new String[]{"TABLE"})) {
                return rs.next();
            }
        } catch (Exception e) {
            throw new RuntimeException("check checkpoint table exists failed: " + e.getMessage(), e);
        }
    }

    public Checkpoint loadCheckpoint(DataSource targetDataSource, String checkpointTable, String syncKey) {
        String sql = "SELECT last_sync_time, last_sync_id FROM " + checkpointTable + " WHERE sync_key = ?";
        try (Connection connection = targetDataSource.getConnection();
             PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setString(1, syncKey);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return new Checkpoint(rs.getString("last_sync_time"), SyncRuntimeSupport.readNullableLong(rs, "last_sync_id"));
                }
                return new Checkpoint(null, 0L);
            }
        } catch (Exception e) {
            throw new RuntimeException("load checkpoint failed: " + e.getMessage(), e);
        }
    }

    public void updateCheckpoint(Connection targetConn,
                                 String checkpointTable,
                                 String syncKey,
                                 String sourceTable,
                                 String targetTable,
                                 String lastSyncTime,
                                 long lastSyncId) {
        String sql = "INSERT INTO " + checkpointTable + " (sync_key, source_table, target_table, last_sync_time, last_sync_id) " +
                "VALUES (?, ?, ?, ?, ?) " +
                "ON DUPLICATE KEY UPDATE last_sync_time = VALUES(last_sync_time), last_sync_id = VALUES(last_sync_id)";
        try (PreparedStatement ps = targetConn.prepareStatement(sql)) {
            ps.setString(1, syncKey);
            ps.setString(2, sourceTable);
            ps.setString(3, targetTable);
            ps.setString(4, lastSyncTime);
            ps.setLong(5, lastSyncId);
            ps.executeUpdate();
        } catch (Exception e) {
            throw new RuntimeException("update checkpoint failed: " + e.getMessage(), e);
        }
    }

    public List<DbSyncCheckpointDTO> queryCheckpoints(DataSource targetDataSource, String checkpointTable, String syncKey) {
        ensureCheckpointTable(targetDataSource, checkpointTable, true);
        String sql = "SELECT id, sync_key, source_table, target_table, last_sync_time, last_sync_id, create_time, update_time " +
                "FROM " + checkpointTable +
                (SyncRuntimeSupport.isBlank(syncKey) ? "" : " WHERE sync_key = ?") +
                " ORDER BY id";

        try (Connection connection = targetDataSource.getConnection();
             PreparedStatement ps = connection.prepareStatement(sql)) {
            if (!SyncRuntimeSupport.isBlank(syncKey)) {
                ps.setString(1, syncKey);
            }
            try (ResultSet rs = ps.executeQuery()) {
                List<DbSyncCheckpointDTO> results = new ArrayList<>();
                while (rs.next()) {
                    results.add(DbSyncCheckpointDTO.builder()
                            .id(rs.getLong("id"))
                            .syncKey(rs.getString("sync_key"))
                            .sourceTable(rs.getString("source_table"))
                            .targetTable(rs.getString("target_table"))
                            .lastSyncTime(rs.getString("last_sync_time"))
                            .lastSyncId(SyncRuntimeSupport.readNullableLong(rs, "last_sync_id"))
                            .createTime(SyncRuntimeSupport.toLocalDateTime(rs.getTimestamp("create_time")))
                            .updateTime(SyncRuntimeSupport.toLocalDateTime(rs.getTimestamp("update_time")))
                            .build());
                }
                return results;
            }
        } catch (Exception e) {
            throw new RuntimeException("load checkpoints failed: " + e.getMessage(), e);
        }
    }

    public int resetCheckpoints(DataSource targetDataSource, String checkpointTable, List<String> syncKeys) {
        ensureCheckpointTable(targetDataSource, checkpointTable, true);
        String placeholders = syncKeys.stream().map(item -> "?").collect(Collectors.joining(", "));
        String sql = "DELETE FROM " + checkpointTable + " WHERE sync_key IN (" + placeholders + ")";

        try (Connection connection = targetDataSource.getConnection();
             PreparedStatement ps = connection.prepareStatement(sql)) {
            int index = 1;
            for (String syncKey : syncKeys) {
                ps.setString(index++, syncKey);
            }
            return ps.executeUpdate();
        } catch (Exception e) {
            throw new RuntimeException("reset checkpoints failed: " + e.getMessage(), e);
        }
    }
}
