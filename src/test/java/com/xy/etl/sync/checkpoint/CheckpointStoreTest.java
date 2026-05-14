package com.xy.etl.sync.checkpoint;

import com.xy.etl.dto.DbSyncCheckpointDTO;
import com.xy.etl.sync.model.Checkpoint;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class CheckpointStoreTest {

    private final CheckpointStore store = new CheckpointStore();

    @Test
    void shouldCreateCheckpointTableWhenMissingAndAutoCreateEnabled() throws Exception {
        DataSource dataSource = mock(DataSource.class);
        Connection connection = mock(Connection.class);
        DatabaseMetaData metaData = mock(DatabaseMetaData.class);
        ResultSet rs1 = resultSet(false);
        ResultSet rs2 = resultSet(false);
        ResultSet rs3 = resultSet(false);
        Statement statement = mock(Statement.class);

        when(dataSource.getConnection()).thenReturn(connection, connection);
        when(connection.getCatalog()).thenReturn("demo");
        when(connection.getMetaData()).thenReturn(metaData);
        when(metaData.getTables("demo", null, "cp_table", new String[]{"TABLE"})).thenReturn(rs1);
        when(metaData.getTables("demo", null, "CP_TABLE", new String[]{"TABLE"})).thenReturn(rs2);
        when(metaData.getTables("demo", null, "cp_table", new String[]{"TABLE"})).thenReturn(rs3);
        when(connection.createStatement()).thenReturn(statement);

        store.ensureCheckpointTable(dataSource, "cp_table", true);

        verify(statement, times(1)).execute(anyString());
    }

    @Test
    void shouldLoadCheckpoint() throws Exception {
        DataSource dataSource = mock(DataSource.class);
        Connection connection = mock(Connection.class);
        PreparedStatement ps = mock(PreparedStatement.class);
        ResultSet rs = mock(ResultSet.class);

        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(anyString())).thenReturn(ps);
        when(ps.executeQuery()).thenReturn(rs);
        when(rs.next()).thenReturn(true);
        when(rs.getString("last_sync_time")).thenReturn("2024-01-01 00:00:00");
        when(rs.getLong("last_sync_id")).thenReturn(11L);
        when(rs.wasNull()).thenReturn(false);

        Checkpoint checkpoint = store.loadCheckpoint(dataSource, "cp_table", "sync-1");

        assertEquals("2024-01-01 00:00:00", checkpoint.lastSyncTime());
        assertEquals(11L, checkpoint.lastSyncId());
        verify(ps, times(1)).setString(1, "sync-1");
    }

    @Test
    void shouldUpdateCheckpoint() throws Exception {
        Connection connection = mock(Connection.class);
        PreparedStatement ps = mock(PreparedStatement.class);
        when(connection.prepareStatement(anyString())).thenReturn(ps);

        store.updateCheckpoint(connection, "cp_table", "sync-1", "src", "tgt", "2024-01-01 00:00:00", 12L);

        verify(ps, times(1)).setString(1, "sync-1");
        verify(ps, times(1)).setString(2, "src");
        verify(ps, times(1)).setString(3, "tgt");
        verify(ps, times(1)).setString(4, "2024-01-01 00:00:00");
        verify(ps, times(1)).setLong(5, 12L);
        verify(ps, times(1)).executeUpdate();
    }

    @Test
    void shouldQueryCheckpoints() throws Exception {
        DataSource dataSource = mock(DataSource.class);
        Connection connection = mock(Connection.class);
        DatabaseMetaData metaData = mock(DatabaseMetaData.class);
        ResultSet existsRs = resultSet(true);
        PreparedStatement ps = mock(PreparedStatement.class);
        ResultSet rs = mock(ResultSet.class);

        when(dataSource.getConnection()).thenReturn(connection, connection);
        when(connection.getCatalog()).thenReturn("demo");
        when(connection.getMetaData()).thenReturn(metaData);
        when(metaData.getTables("demo", null, "cp_table", new String[]{"TABLE"})).thenReturn(existsRs);
        when(connection.prepareStatement(anyString())).thenReturn(ps);
        when(ps.executeQuery()).thenReturn(rs);
        when(rs.next()).thenReturn(true, false);
        when(rs.getLong("id")).thenReturn(1L);
        when(rs.getString("sync_key")).thenReturn("sync-1");
        when(rs.getString("source_table")).thenReturn("src");
        when(rs.getString("target_table")).thenReturn("tgt");
        when(rs.getString("last_sync_time")).thenReturn("2024-01-01 00:00:00");
        when(rs.getLong("last_sync_id")).thenReturn(3L);
        when(rs.wasNull()).thenReturn(false);
        when(rs.getTimestamp("create_time")).thenReturn(Timestamp.valueOf(LocalDateTime.of(2024, 1, 1, 0, 0)));
        when(rs.getTimestamp("update_time")).thenReturn(Timestamp.valueOf(LocalDateTime.of(2024, 1, 1, 1, 0)));

        List<DbSyncCheckpointDTO> results = store.queryCheckpoints(dataSource, "cp_table", "sync-1");

        assertEquals(1, results.size());
        assertEquals("sync-1", results.get(0).getSyncKey());
        assertNotNull(results.get(0).getCreateTime());
        verify(ps, times(1)).setString(1, "sync-1");
    }

    @Test
    void shouldResetCheckpoints() throws Exception {
        DataSource dataSource = mock(DataSource.class);
        Connection connection = mock(Connection.class);
        DatabaseMetaData metaData = mock(DatabaseMetaData.class);
        ResultSet existsRs = resultSet(true);
        PreparedStatement ps = mock(PreparedStatement.class);

        when(dataSource.getConnection()).thenReturn(connection, connection);
        when(connection.getCatalog()).thenReturn("demo");
        when(connection.getMetaData()).thenReturn(metaData);
        when(metaData.getTables("demo", null, "cp_table", new String[]{"TABLE"})).thenReturn(existsRs);
        when(connection.prepareStatement(anyString())).thenReturn(ps);
        when(ps.executeUpdate()).thenReturn(2);

        int deleted = store.resetCheckpoints(dataSource, "cp_table", List.of("s1", "s2"));

        assertEquals(2, deleted);
        verify(ps, times(1)).setString(1, "s1");
        verify(ps, times(1)).setString(2, "s2");
    }

    @Test
    void shouldNotCreateWhenCheckpointTableExists() throws Exception {
        DataSource dataSource = mock(DataSource.class);
        Connection connection = mock(Connection.class);
        DatabaseMetaData metaData = mock(DatabaseMetaData.class);
        ResultSet existsRs = resultSet(true);

        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.getCatalog()).thenReturn("demo");
        when(connection.getMetaData()).thenReturn(metaData);
        when(metaData.getTables("demo", null, "cp_table", new String[]{"TABLE"})).thenReturn(existsRs);

        store.ensureCheckpointTable(dataSource, "cp_table", true);

        verify(connection, never()).createStatement();
    }

    private static ResultSet resultSet(boolean exists) throws Exception {
        ResultSet rs = mock(ResultSet.class);
        when(rs.next()).thenReturn(exists);
        return rs;
    }
}
