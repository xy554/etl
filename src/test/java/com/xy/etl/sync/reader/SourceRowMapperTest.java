package com.xy.etl.sync.reader;

import com.xy.etl.sync.model.ResolvedColumnMapping;
import com.xy.etl.sync.model.ResolvedDeleteRule;
import com.xy.etl.sync.model.ResolvedTableConfig;
import com.xy.etl.sync.model.SourceRow;
import com.xy.etl.sync.support.DbSyncConstants;
import org.junit.jupiter.api.Test;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SourceRowMapperTest {

    private final SourceRowMapper mapper = new SourceRowMapper();

    @Test
    void shouldApplyFallbackSyncTimeAndMapValues() {
        ResolvedTableConfig config = config(List.of(
                new ResolvedColumnMapping("id", "id", true, null, null, false),
                new ResolvedColumnMapping("name", "name", true, 10, null, false)
        ));
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("id", 12L);
        row.put("update_time", null);
        row.put("fallback_time", Timestamp.valueOf(LocalDateTime.of(2024, 1, 2, 3, 4, 5)));
        row.put("name", "alice");

        SourceRow sourceRow = mapper.mapSourceRow(config, row);

        assertEquals(12L, sourceRow.cursorId());
        assertEquals("2024-01-02 03:04:05", sourceRow.syncTime());
        assertFalse(sourceRow.deleted());
        assertEquals("alice", sourceRow.targetValues().get("name"));
    }

    @Test
    void shouldTreatDeleteRuleRowAsDeleted() {
        ResolvedTableConfig config = new ResolvedTableConfig(
                null, null, null, null, "syncKey", DbSyncConstants.SOURCE_MODE_TABLE, DbSyncConstants.WRITE_MODE_UPSERT,
                DbSyncConstants.FULL_REFRESH_DELETE_MODE_DELETE, "src", null, "tgt", "id", "update_time", null,
                null, "id", List.of(), "id", 100,
                List.of(new ResolvedColumnMapping("id", "id", true, null, null, false)),
                List.of(),
                new ResolvedDeleteRule("op_type", 3, null, null),
                DbSyncConstants.DEFAULT_CHECKPOINT_TABLE, true
        );
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("id", 8L);
        row.put("update_time", "2024-01-01 12:00:00");
        row.put("op_type", 3);

        SourceRow sourceRow = mapper.mapSourceRow(config, row);

        assertTrue(sourceRow.deleted());
    }

    @Test
    void shouldValidateRequiredAndMaxLength() {
        ResolvedTableConfig config = config(List.of(
                new ResolvedColumnMapping("id", "id", true, null, null, false),
                new ResolvedColumnMapping("name", "name", true, 3, null, false)
        ));
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("id", 9L);
        row.put("update_time", "2024-01-01 00:00:00");
        row.put("name", "alice");

        RuntimeException ex = assertThrows(RuntimeException.class, () -> mapper.mapSourceRow(config, row));

        assertTrue(ex.getMessage().contains("column value exceeds max length"));
    }

    @Test
    void shouldValidateTargetKeyValue() {
        ResolvedTableConfig config = config(List.of(
                new ResolvedColumnMapping("name", "name", false, null, null, false)
        ));
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("id", 10L);
        row.put("update_time", "2024-01-01 00:00:00");
        row.put("name", "ok");

        RuntimeException ex = assertThrows(RuntimeException.class, () -> mapper.mapSourceRow(config, row));

        assertTrue(ex.getMessage().contains("target key value is null"));
    }

    private static ResolvedTableConfig config(List<ResolvedColumnMapping> mappings) {
        return new ResolvedTableConfig(
                null, null, null, null, "syncKey", DbSyncConstants.SOURCE_MODE_TABLE, DbSyncConstants.WRITE_MODE_UPSERT,
                DbSyncConstants.FULL_REFRESH_DELETE_MODE_DELETE, "src", null, "tgt", "id", "update_time", null,
                "fallback_time", "id", List.of(), "id", 100,
                mappings, List.of(), new ResolvedDeleteRule(null, null, null, null),
                DbSyncConstants.DEFAULT_CHECKPOINT_TABLE, true
        );
    }
}
