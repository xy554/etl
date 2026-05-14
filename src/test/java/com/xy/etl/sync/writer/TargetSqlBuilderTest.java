package com.xy.etl.sync.writer;

import com.xy.etl.sync.model.ResolvedDeleteRule;
import com.xy.etl.sync.model.ResolvedTableConfig;
import com.xy.etl.sync.support.DbSyncConstants;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TargetSqlBuilderTest {

    private final TargetSqlBuilder builder = new TargetSqlBuilder();

    @Test
    void shouldExcludeKeyColumnsFromUpsertUpdate() {
        String sql = builder.buildUpsertSql(config("id", List.of()), List.of("id", "name", "age"), 1);

        assertTrue(sql.contains("ON DUPLICATE KEY UPDATE"));
        assertTrue(sql.contains("name = VALUES(name)"));
        assertFalse(sql.contains("id = VALUES(id),"));
    }

    @Test
    void shouldBuildCompositeKeyDeleteSql() {
        String sql = builder.buildBatchDeleteSql("t_target", List.of("biz_id", "tenant_id"), 2);

        assertEquals("DELETE FROM t_target WHERE (biz_id, tenant_id) IN ((?, ?), (?, ?))", sql);
    }

    @Test
    void shouldBuildMultiValuesUpsertSql() {
        String sql = builder.buildUpsertSql(config(null, List.of("biz_id", "tenant_id")), List.of("biz_id", "tenant_id", "name"), 2);

        assertTrue(sql.contains("VALUES (?, ?, ?), (?, ?, ?)"));
        assertTrue(sql.contains("name = VALUES(name)"));
    }

    private static ResolvedTableConfig config(String targetKeyColumn, List<String> targetKeyColumns) {
        return new ResolvedTableConfig(
                null, null, null, null, "syncKey", DbSyncConstants.SOURCE_MODE_TABLE, DbSyncConstants.WRITE_MODE_UPSERT,
                DbSyncConstants.FULL_REFRESH_DELETE_MODE_DELETE, "src", null, "tgt", "id", "update_time", null,
                null, targetKeyColumn, targetKeyColumns, targetKeyColumn, 100,
                List.of(), List.of(), new ResolvedDeleteRule(null, null, null, null),
                DbSyncConstants.DEFAULT_CHECKPOINT_TABLE, true
        );
    }
}
