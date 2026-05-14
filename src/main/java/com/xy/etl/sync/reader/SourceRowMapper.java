package com.xy.etl.sync.reader;

import com.xy.etl.sync.model.ResolvedColumnMapping;
import com.xy.etl.sync.model.ResolvedDeleteRule;
import com.xy.etl.sync.model.ResolvedTableConfig;
import com.xy.etl.sync.model.SourceRow;
import com.xy.etl.sync.support.DbSyncConstants;
import com.xy.etl.sync.support.SyncRuntimeSupport;
import org.springframework.stereotype.Component;

import java.sql.Timestamp;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

@Component
public class SourceRowMapper {

    public SourceRow mapSourceRow(ResolvedTableConfig config, Map<String, Object> row) {
        Long cursorId = SyncRuntimeSupport.toLong(row.get(config.cursorIdColumn()));
        if (cursorId == null) {
            throw new RuntimeException("cursor id is null, column: " + config.cursorIdColumn());
        }

        String syncTime = SyncRuntimeSupport.toDatetimeString(row.get(config.syncTimeColumn()));
        if (syncTime == null && config.fallbackSyncTimeColumn() != null) {
            syncTime = SyncRuntimeSupport.toDatetimeString(row.get(config.fallbackSyncTimeColumn()));
        }
        if (syncTime == null) {
            syncTime = DbSyncConstants.EPOCH_TIME;
        }

        boolean deleted = isDeleted(config.deleteRule(), row);
        Map<String, Object> targetValues = new LinkedHashMap<>();

        for (ResolvedColumnMapping mapping : config.columnMappings()) {
            Object value = mapping.constantValueSet() ? mapping.constantValue() : row.get(mapping.sourceColumn());
            if (!deleted) {
                validateMappedValue(config, mapping, value, cursorId);
            }
            targetValues.put(mapping.targetColumn(), normalizeJdbcValue(value));
        }

        validateTargetKeyValues(config, targetValues, cursorId);
        return new SourceRow(cursorId, syncTime, deleted, targetValues);
    }

    public String buildTargetKey(ResolvedTableConfig config, Map<String, Object> targetValues) {
        return SyncRuntimeSupport.resolveTargetKeyColumns(config).stream()
                .map(column -> column + "=" + String.valueOf(targetValues.get(column)))
                .reduce((left, right) -> left + "|" + right)
                .orElse("");
    }

    private boolean isDeleted(ResolvedDeleteRule deleteRule, Map<String, Object> row) {
        if (deleteRule == null) {
            return false;
        }
        if (deleteRule.operationTypeColumn() != null && deleteRule.deleteOperationValue() != null) {
            Integer operationType = SyncRuntimeSupport.toInteger(row.get(deleteRule.operationTypeColumn()));
            if (Objects.equals(operationType, deleteRule.deleteOperationValue())) {
                return true;
            }
        }
        if (deleteRule.deleteFlagColumn() != null && deleteRule.deleteFlagValue() != null) {
            Integer deleteFlag = SyncRuntimeSupport.toInteger(row.get(deleteRule.deleteFlagColumn()));
            return Objects.equals(deleteFlag, deleteRule.deleteFlagValue());
        }
        return false;
    }

    private void validateMappedValue(ResolvedTableConfig config,
                                     ResolvedColumnMapping mapping,
                                     Object value,
                                     Long cursorId) {
        if (mapping.required() && SyncRuntimeSupport.isBlank(value)) {
            throw new RuntimeException("required column is empty, syncKey: " + config.syncKey()
                    + ", targetColumn: " + mapping.targetColumn() + ", cursorId: " + cursorId);
        }
        if (value != null && mapping.maxLength() != null && String.valueOf(value).length() > mapping.maxLength()) {
            throw new RuntimeException("column value exceeds max length, syncKey: " + config.syncKey()
                    + ", targetColumn: " + mapping.targetColumn()
                    + ", maxLength: " + mapping.maxLength()
                    + ", cursorId: " + cursorId
                    + ", value: " + value);
        }
    }

    private void validateTargetKeyValues(ResolvedTableConfig config, Map<String, Object> targetValues, Long cursorId) {
        if (DbSyncConstants.WRITE_MODE_FULL_REFRESH_INSERT.equals(config.writeMode()) && config.targetKeyColumn() == null
                && (config.targetKeyColumns() == null || config.targetKeyColumns().isEmpty())) {
            return;
        }
        for (String keyColumn : SyncRuntimeSupport.resolveTargetKeyColumns(config)) {
            Object value = targetValues.get(keyColumn);
            if (SyncRuntimeSupport.isBlank(value)) {
                throw new RuntimeException("target key value is null, syncKey: " + config.syncKey()
                        + ", targetColumn: " + keyColumn + ", cursorId: " + cursorId);
            }
        }
    }

    private Object normalizeJdbcValue(Object value) {
        if (value instanceof java.time.LocalDateTime localDateTime) {
            return Timestamp.valueOf(localDateTime);
        }
        if (value instanceof java.util.Date date && !(value instanceof Timestamp)) {
            return new Timestamp(date.getTime());
        }
        return value;
    }
}
