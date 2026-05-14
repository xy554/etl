package com.xy.etl.sync.writer;

import com.xy.etl.sync.model.ResolvedTableConfig;
import com.xy.etl.sync.support.SyncRuntimeSupport;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

@Component
public class TargetSqlBuilder {

    public String buildUpsertSql(ResolvedTableConfig config, List<String> insertColumns, int rowCount) {
        List<String> keyColumns = SyncRuntimeSupport.resolveTargetKeyColumns(config);
        String rowPlaceholders = "(" + insertColumns.stream().map(item -> "?").collect(Collectors.joining(", ")) + ")";
        String placeholders = Collections.nCopies(rowCount, rowPlaceholders).stream().collect(Collectors.joining(", "));
        String updateSql = insertColumns.stream()
                .filter(column -> !keyColumns.contains(column))
                .map(column -> column + " = VALUES(" + column + ")")
                .collect(Collectors.joining(", "));
        if (updateSql.isBlank()) {
            updateSql = keyColumns.get(0) + " = VALUES(" + keyColumns.get(0) + ")";
        }

        return "INSERT INTO " + config.targetTable() + " (" + String.join(", ", insertColumns) + ") VALUES " + placeholders + " " +
                "ON DUPLICATE KEY UPDATE " + updateSql;
    }

    public String buildBatchDeleteSql(String targetTable, List<String> keyColumns, int rowCount) {
        if (keyColumns.size() == 1) {
            String placeholders = Collections.nCopies(rowCount, "?").stream().collect(Collectors.joining(", "));
            return "DELETE FROM " + targetTable + " WHERE " + keyColumns.get(0) + " IN (" + placeholders + ")";
        }

        String tupleColumns = "(" + String.join(", ", keyColumns) + ")";
        String tuplePlaceholder = "(" + keyColumns.stream().map(item -> "?").collect(Collectors.joining(", ")) + ")";
        String tuplePlaceholders = Collections.nCopies(rowCount, tuplePlaceholder).stream().collect(Collectors.joining(", "));
        return "DELETE FROM " + targetTable + " WHERE " + tupleColumns + " IN (" + tuplePlaceholders + ")";
    }

    public String buildInsertSql(ResolvedTableConfig config, List<String> insertColumns) {
        String placeholders = insertColumns.stream().map(item -> "?").collect(Collectors.joining(", "));
        return "INSERT INTO " + config.targetTable() + " (" + String.join(", ", insertColumns) + ") VALUES (" + placeholders + ")";
    }

    public String buildMultiValuesInsertSql(ResolvedTableConfig config, List<String> insertColumns, int rowCount) {
        String rowPlaceholders = "(" + insertColumns.stream().map(item -> "?").collect(Collectors.joining(", ")) + ")";
        String placeholders = Collections.nCopies(rowCount, rowPlaceholders).stream().collect(Collectors.joining(", "));
        return "INSERT INTO " + config.targetTable() + " (" + String.join(", ", insertColumns) + ") VALUES " + placeholders;
    }
}
