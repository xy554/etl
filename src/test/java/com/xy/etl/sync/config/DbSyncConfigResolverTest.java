package com.xy.etl.sync.config;

import com.xy.etl.dto.DbSyncColumnMappingDTO;
import com.xy.etl.dto.DbSyncFilterDTO;
import com.xy.etl.dto.DbSyncRequest;
import com.xy.etl.dto.DbSyncTableConfigDTO;
import com.xy.etl.dto.DirectDataSourceConfigDTO;
import com.xy.etl.sync.model.ResolvedTableConfig;
import com.xy.etl.sync.support.DbSyncConstants;
import com.xy.etl.sync.support.SourceMode;
import com.xy.etl.sync.support.WriteMode;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DbSyncConfigResolverTest {

    private final DbSyncConfigResolver resolver = new DbSyncConfigResolver();

    @Test
    void shouldResolveDefaults() {
        DbSyncRequest request = DbSyncRequest.builder()
                .sourceDataSourceConfig(direct("jdbc:mysql://source/db"))
                .targetDataSourceConfig(direct("jdbc:mysql://target/db"))
                .tableConfigs(List.of(tableConfig()))
                .build();

        ResolvedTableConfig config = resolver.resolveTableConfig(request, tableConfig());

        assertEquals(DbSyncConstants.DEFAULT_BATCH_SIZE, config.batchSize());
        assertEquals(DbSyncConstants.DEFAULT_CHECKPOINT_TABLE, config.checkpointTable());
        assertEquals(SourceMode.TABLE, config.sourceMode());
        assertEquals(WriteMode.UPSERT, config.writeMode());
        assertTrue(config.syncKey().contains("source_table->"));
    }

    @Test
    void shouldRejectFiltersForSqlSourceMode() {
        DbSyncRequest request = DbSyncRequest.builder()
                .sourceDataSourceConfig(direct("jdbc:mysql://source/db"))
                .targetDataSourceConfig(direct("jdbc:mysql://target/db"))
                .tableConfigs(List.of(tableConfig()))
                .build();
        DbSyncTableConfigDTO base = tableConfig();
        DbSyncTableConfigDTO config = DbSyncTableConfigDTO.builder()
                .sourceTable(base.getSourceTable())
                .targetTable(base.getTargetTable())
                .cursorIdColumn(base.getCursorIdColumn())
                .syncTimeColumn(base.getSyncTimeColumn())
                .targetKeyColumn(base.getTargetKeyColumn())
                .targetKeySourceColumn(base.getTargetKeySourceColumn())
                .columnMappings(base.getColumnMappings())
                .sourceMode("sql")
                .sourceSql("select * from t_src")
                .filters(List.of(DbSyncFilterDTO.builder().column("status").value(1).build()))
                .build();

        RuntimeException ex = assertThrows(RuntimeException.class, () -> resolver.resolveTableConfig(request, config));

        assertEquals("filters are not supported when sourceMode is sql", ex.getMessage());
    }

    @Test
    void shouldRejectDeleteInsertWithoutCompositeKeyColumns() {
        DbSyncRequest request = DbSyncRequest.builder()
                .sourceDataSourceConfig(direct("jdbc:mysql://source/db"))
                .targetDataSourceConfig(direct("jdbc:mysql://target/db"))
                .tableConfigs(List.of(tableConfig()))
                .build();
        DbSyncTableConfigDTO base = tableConfig();
        DbSyncTableConfigDTO config = DbSyncTableConfigDTO.builder()
                .sourceTable(base.getSourceTable())
                .targetTable(base.getTargetTable())
                .cursorIdColumn(base.getCursorIdColumn())
                .syncTimeColumn(base.getSyncTimeColumn())
                .targetKeyColumn("id")
                .targetKeySourceColumn(base.getTargetKeySourceColumn())
                .columnMappings(base.getColumnMappings())
                .writeMode(WriteMode.DELETE_INSERT.value())
                .targetKeyColumns(List.of())
                .build();

        RuntimeException ex = assertThrows(RuntimeException.class, () -> resolver.resolveTableConfig(request, config));

        assertEquals("targetKeyColumns cannot be empty when writeMode is delete_insert", ex.getMessage());
    }

    @Test
    void shouldRequireTargetKeyIncludedInMappings() {
        DbSyncRequest request = DbSyncRequest.builder()
                .sourceDataSourceConfig(direct("jdbc:mysql://source/db"))
                .targetDataSourceConfig(direct("jdbc:mysql://target/db"))
                .tableConfigs(List.of(tableConfig()))
                .build();
        DbSyncTableConfigDTO base = tableConfig();
        DbSyncTableConfigDTO config = DbSyncTableConfigDTO.builder()
                .sourceTable(base.getSourceTable())
                .targetTable(base.getTargetTable())
                .cursorIdColumn(base.getCursorIdColumn())
                .syncTimeColumn(base.getSyncTimeColumn())
                .targetKeySourceColumn(base.getTargetKeySourceColumn())
                .targetKeyColumns(List.of("biz_id", "tenant_id"))
                .columnMappings(List.of(
                        mapping("id", "biz_id"),
                        mapping("name", "target_name")
                ))
                .build();

        RuntimeException ex = assertThrows(RuntimeException.class, () -> resolver.resolveTableConfig(request, config));

        assertEquals("columnMappings must include target key column: tenant_id", ex.getMessage());
    }

    private static DbSyncTableConfigDTO tableConfig() {
        return DbSyncTableConfigDTO.builder()
                .sourceTable("source_table")
                .targetTable("target_table")
                .cursorIdColumn("id")
                .syncTimeColumn("update_time")
                .targetKeyColumn("id")
                .targetKeySourceColumn("id")
                .columnMappings(List.of(
                        mapping("id", "id"),
                        mapping("name", "target_name")
                ))
                .build();
    }

    private static DbSyncColumnMappingDTO mapping(String sourceColumn, String targetColumn) {
        return DbSyncColumnMappingDTO.builder()
                .sourceColumn(sourceColumn)
                .targetColumn(targetColumn)
                .build();
    }

    private static DirectDataSourceConfigDTO direct(String jdbcUrl) {
        return DirectDataSourceConfigDTO.builder()
                .jdbcUrl(jdbcUrl)
                .jdbcUsername("root")
                .jdbcPassword("pwd")
                .build();
    }
}
