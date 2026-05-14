package com.xy.etl.sync.config;

import com.xy.etl.dto.DbSyncColumnMappingDTO;
import com.xy.etl.dto.DbSyncDeleteRuleDTO;
import com.xy.etl.dto.DbSyncFilterDTO;
import com.xy.etl.dto.DbSyncRequest;
import com.xy.etl.dto.DbSyncTableConfigDTO;
import com.xy.etl.dto.DirectDataSourceConfigDTO;
import com.xy.etl.sync.model.ResolvedColumnMapping;
import com.xy.etl.sync.model.ResolvedDeleteRule;
import com.xy.etl.sync.model.ResolvedFilter;
import com.xy.etl.sync.model.ResolvedTableConfig;
import com.xy.etl.sync.support.DbSyncConstants;
import com.xy.etl.sync.support.SyncRuntimeSupport;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.StringJoiner;
import java.util.stream.Collectors;

@Slf4j
@Component
public class DbSyncConfigResolver {

    public void validateRequest(DbSyncRequest request) {
        if (request == null) {
            throw new RuntimeException("request cannot be null");
        }
        if (request.getTableConfigs() == null || request.getTableConfigs().isEmpty()) {
            throw new RuntimeException("tableConfigs cannot be empty");
        }
        if (request.getBatchSize() != null && request.getBatchSize() <= 0) {
            throw new RuntimeException("batchSize must be greater than 0");
        }
    }

    public ResolvedTableConfig resolveTableConfig(DbSyncRequest request, DbSyncTableConfigDTO tableConfig) {
        if (tableConfig == null) {
            throw new RuntimeException("tableConfig cannot be null");
        }

        Long sourceDataSourceId = tableConfig.getSourceDataSourceId() != null
                ? tableConfig.getSourceDataSourceId() : request.getSourceDataSourceId();
        DirectDataSourceConfigDTO sourceDataSourceConfig = tableConfig.getSourceDataSourceConfig() != null
                ? tableConfig.getSourceDataSourceConfig() : request.getSourceDataSourceConfig();
        Long targetDataSourceId = tableConfig.getTargetDataSourceId() != null
                ? tableConfig.getTargetDataSourceId() : request.getTargetDataSourceId();
        DirectDataSourceConfigDTO targetDataSourceConfig = tableConfig.getTargetDataSourceConfig() != null
                ? tableConfig.getTargetDataSourceConfig() : request.getTargetDataSourceConfig();
        if (sourceDataSourceId == null && sourceDataSourceConfig == null) {
            throw new RuntimeException("sourceDataSourceId and sourceDataSourceConfig cannot both be null");
        }
        if (targetDataSourceId == null && targetDataSourceConfig == null) {
            throw new RuntimeException("targetDataSourceId and targetDataSourceConfig cannot both be null");
        }

        String sourceMode = resolveSourceMode(tableConfig.getSourceMode());
        String sourceTable = DbSyncConstants.SOURCE_MODE_SQL.equals(sourceMode)
                ? buildSqlSourceLabel(tableConfig.getSourceSql())
                : requireIdentifier(tableConfig.getSourceTable(), "sourceTable");
        String sourceSql = DbSyncConstants.SOURCE_MODE_SQL.equals(sourceMode)
                ? requireSql(tableConfig.getSourceSql(), "sourceSql")
                : null;
        String writeMode = resolveWriteMode(tableConfig.getWriteMode());
        String fullRefreshDeleteMode = resolveFullRefreshDeleteMode(tableConfig.getFullRefreshDeleteMode());
        String targetTable = requireIdentifier(tableConfig.getTargetTable(), "targetTable");
        String cursorIdColumn = requireIdentifier(tableConfig.getCursorIdColumn(), "cursorIdColumn");
        String syncTimeColumn = requireIdentifier(tableConfig.getSyncTimeColumn(), "syncTimeColumn");
        String syncTimeExpression = nullableSqlExpression(tableConfig.getSyncTimeExpression(), "syncTimeExpression");
        String fallbackSyncTimeColumn = nullableIdentifier(tableConfig.getFallbackSyncTimeColumn(), "fallbackSyncTimeColumn");
        List<String> targetKeyColumns = tableConfig.getTargetKeyColumns() == null
                ? Collections.emptyList()
                : tableConfig.getTargetKeyColumns().stream()
                .map(item -> requireIdentifier(item, "targetKeyColumns item"))
                .collect(Collectors.toList());
        boolean compositeKeyMode = !targetKeyColumns.isEmpty();
        boolean fullRefreshMode = DbSyncConstants.WRITE_MODE_FULL_REFRESH_INSERT.equals(writeMode);
        String targetKeyColumn = compositeKeyMode || (fullRefreshMode && SyncRuntimeSupport.isBlank(tableConfig.getTargetKeyColumn()))
                ? null : requireIdentifier(tableConfig.getTargetKeyColumn(), "targetKeyColumn");
        String targetKeySourceColumn = compositeKeyMode || (fullRefreshMode && SyncRuntimeSupport.isBlank(tableConfig.getTargetKeySourceColumn()))
                ? null : requireIdentifier(tableConfig.getTargetKeySourceColumn(), "targetKeySourceColumn");
        int batchSize = resolveBatchSize(request, tableConfig);

        List<ResolvedColumnMapping> columnMappings = resolveColumnMappings(tableConfig.getColumnMappings());
        if (DbSyncConstants.WRITE_MODE_DELETE_INSERT.equals(writeMode) && !compositeKeyMode) {
            throw new RuntimeException("targetKeyColumns cannot be empty when writeMode is delete_insert");
        }
        if (compositeKeyMode) {
            for (String keyColumn : targetKeyColumns) {
                boolean hasTargetKeyMapping = columnMappings.stream().anyMatch(mapping -> mapping.targetColumn().equals(keyColumn));
                if (!hasTargetKeyMapping) {
                    throw new RuntimeException("columnMappings must include target key column: " + keyColumn);
                }
            }
        } else if (targetKeyColumn != null) {
            boolean hasTargetKeyMapping = columnMappings.stream().anyMatch(mapping -> mapping.targetColumn().equals(targetKeyColumn));
            if (!hasTargetKeyMapping) {
                throw new RuntimeException("columnMappings must include target key column: " + targetKeyColumn);
            }
        }
        List<ResolvedFilter> filters = resolveFilters(tableConfig.getFilters());
        if (DbSyncConstants.SOURCE_MODE_SQL.equals(sourceMode) && !filters.isEmpty()) {
            throw new RuntimeException("filters are not supported when sourceMode is sql");
        }
        ResolvedDeleteRule resolvedDeleteRule = resolveDeleteRule(tableConfig.getDeleteRule());
        String checkpointTable = resolveCheckpointTable(request.getCheckpointTable());
        boolean autoCreateCheckpointTable = !Boolean.FALSE.equals(request.getAutoCreateCheckpointTable());

        String syncKey = SyncRuntimeSupport.isBlank(tableConfig.getSyncKey())
                ? buildDefaultSyncKey(sourceDataSourceId, sourceDataSourceConfig, targetDataSourceId, targetDataSourceConfig,
                sourceMode, sourceTable, targetTable, filters)
                : tableConfig.getSyncKey().trim();

        log.info("single table sync config resolved, syncKey: {}, sourceMode: {}, writeMode: {}, fullRefreshDeleteMode: {}, sourceTable: {}, targetTable: {}, cursorIdColumn: {}, syncTimeColumn: {}, syncTimeExpression: {}, fallbackSyncTimeColumn: {}, batchSize: {}, filterCount: {}, mappingCount: {}, targetKeyColumns: {}, checkpointTable: {}, autoCreateCheckpointTable: {}",
                syncKey, sourceMode, writeMode, fullRefreshDeleteMode, sourceTable, targetTable, cursorIdColumn, syncTimeColumn, syncTimeExpression, fallbackSyncTimeColumn,
                batchSize, filters.size(), columnMappings.size(), SyncRuntimeSupport.resolveTargetKeyColumnsForLog(targetKeyColumn, targetKeyColumns),
                checkpointTable, autoCreateCheckpointTable);

        return new ResolvedTableConfig(
                sourceDataSourceId,
                sourceDataSourceConfig,
                targetDataSourceId,
                targetDataSourceConfig,
                syncKey,
                sourceMode,
                writeMode,
                fullRefreshDeleteMode,
                sourceTable,
                sourceSql,
                targetTable,
                cursorIdColumn,
                syncTimeColumn,
                syncTimeExpression,
                fallbackSyncTimeColumn,
                targetKeyColumn,
                targetKeyColumns,
                targetKeySourceColumn,
                batchSize,
                columnMappings,
                filters,
                resolvedDeleteRule,
                checkpointTable,
                autoCreateCheckpointTable
        );
    }

    public List<String> buildSelectColumns(ResolvedTableConfig config) {
        if (DbSyncConstants.SOURCE_MODE_SQL.equals(config.sourceMode())) {
            return Collections.emptyList();
        }
        LinkedHashSet<String> columns = new LinkedHashSet<>();
        columns.add(config.cursorIdColumn());
        columns.add(config.syncTimeColumn());
        if (config.fallbackSyncTimeColumn() != null) {
            columns.add(config.fallbackSyncTimeColumn());
        }
        if (config.targetKeySourceColumn() != null) {
            columns.add(config.targetKeySourceColumn());
        }
        if (config.deleteRule().operationTypeColumn() != null) {
            columns.add(config.deleteRule().operationTypeColumn());
        }
        if (config.deleteRule().deleteFlagColumn() != null) {
            columns.add(config.deleteRule().deleteFlagColumn());
        }
        for (ResolvedColumnMapping mapping : config.columnMappings()) {
            if (!mapping.constantValueSet() && mapping.sourceColumn() != null) {
                columns.add(mapping.sourceColumn());
            }
        }
        return new ArrayList<>(columns);
    }

    public String buildSyncTimeExpression(ResolvedTableConfig config) {
        if (config.syncTimeExpression() != null) {
            return config.syncTimeExpression();
        }
        if (config.fallbackSyncTimeColumn() == null) {
            return "COALESCE(" + config.syncTimeColumn() + ", '" + DbSyncConstants.EPOCH_TIME + "')";
        }
        return "COALESCE(" + config.syncTimeColumn() + ", " + config.fallbackSyncTimeColumn() + ", '" + DbSyncConstants.EPOCH_TIME + "')";
    }

    private List<ResolvedColumnMapping> resolveColumnMappings(List<DbSyncColumnMappingDTO> columnMappings) {
        if (columnMappings == null || columnMappings.isEmpty()) {
            throw new RuntimeException("columnMappings cannot be empty");
        }

        List<ResolvedColumnMapping> resolvedMappings = new ArrayList<>();
        for (DbSyncColumnMappingDTO mapping : columnMappings) {
            if (mapping == null) {
                throw new RuntimeException("columnMappings contains null item");
            }
            String targetColumn = requireIdentifier(mapping.getTargetColumn(), "targetColumn");
            boolean hasConstantValue = mapping.getConstantValue() != null;
            String sourceColumn = null;
            if (!SyncRuntimeSupport.isBlank(mapping.getSourceColumn())) {
                sourceColumn = requireIdentifier(mapping.getSourceColumn(), "sourceColumn");
            }
            if (sourceColumn == null && !hasConstantValue) {
                throw new RuntimeException("mapping must provide sourceColumn or constantValue, targetColumn: " + targetColumn);
            }
            if (sourceColumn != null && hasConstantValue) {
                throw new RuntimeException("mapping cannot define both sourceColumn and constantValue, targetColumn: " + targetColumn);
            }
            resolvedMappings.add(new ResolvedColumnMapping(
                    sourceColumn,
                    targetColumn,
                    Boolean.TRUE.equals(mapping.getRequired()),
                    mapping.getMaxLength(),
                    mapping.getConstantValue(),
                    hasConstantValue
            ));
        }
        return resolvedMappings;
    }

    private List<ResolvedFilter> resolveFilters(List<DbSyncFilterDTO> filters) {
        if (filters == null || filters.isEmpty()) {
            return Collections.emptyList();
        }

        List<ResolvedFilter> resolvedFilters = new ArrayList<>();
        for (DbSyncFilterDTO filter : filters) {
            if (filter == null) {
                throw new RuntimeException("filters contains null item");
            }
            String column = requireIdentifier(filter.getColumn(), "filter.column");
            resolvedFilters.add(new ResolvedFilter(column, filter.getValue()));
        }
        return resolvedFilters;
    }

    private ResolvedDeleteRule resolveDeleteRule(DbSyncDeleteRuleDTO deleteRule) {
        if (deleteRule == null) {
            return new ResolvedDeleteRule(null, null, null, null);
        }
        String operationTypeColumn = nullableIdentifier(deleteRule.getOperationTypeColumn(), "operationTypeColumn");
        String deleteFlagColumn = nullableIdentifier(deleteRule.getDeleteFlagColumn(), "deleteFlagColumn");
        return new ResolvedDeleteRule(
                operationTypeColumn,
                deleteRule.getDeleteOperationValue(),
                deleteFlagColumn,
                deleteRule.getDeleteFlagValue()
        );
    }

    private int resolveBatchSize(DbSyncRequest request, DbSyncTableConfigDTO tableConfig) {
        Integer batchSize = tableConfig.getBatchSize() != null ? tableConfig.getBatchSize() : request.getBatchSize();
        if (batchSize == null) {
            batchSize = DbSyncConstants.DEFAULT_BATCH_SIZE;
        }
        if (batchSize <= 0) {
            throw new RuntimeException("batchSize must be greater than 0");
        }
        return batchSize;
    }

    private String buildDefaultSyncKey(Long sourceDataSourceId,
                                       DirectDataSourceConfigDTO sourceDataSourceConfig,
                                       Long targetDataSourceId,
                                       DirectDataSourceConfigDTO targetDataSourceConfig,
                                       String sourceMode,
                                       String sourceTable,
                                       String targetTable,
                                       Collection<ResolvedFilter> filters) {
        StringJoiner joiner = new StringJoiner("&");
        for (ResolvedFilter filter : filters) {
            joiner.add(filter.column() + "=" + String.valueOf(filter.value()));
        }
        String filterSignature = joiner.length() == 0 ? "all" : joiner.toString();
        String sourceIdentity = buildDataSourceIdentity(sourceDataSourceId, sourceDataSourceConfig, sourceTable);
        String targetIdentity = buildDataSourceIdentity(targetDataSourceId, targetDataSourceConfig, targetTable);
        return sourceMode + ":" + sourceIdentity + ":" + sourceTable + "->" + targetIdentity + ":" + targetTable + ":" + filterSignature;
    }

    private String resolveSourceMode(String sourceMode) {
        if (SyncRuntimeSupport.isBlank(sourceMode)) {
            return DbSyncConstants.SOURCE_MODE_TABLE;
        }
        String normalized = sourceMode.trim().toLowerCase();
        if (!DbSyncConstants.SOURCE_MODE_TABLE.equals(normalized) && !DbSyncConstants.SOURCE_MODE_SQL.equals(normalized)) {
            throw new RuntimeException("sourceMode must be table or sql");
        }
        return normalized;
    }

    private String resolveWriteMode(String writeMode) {
        if (SyncRuntimeSupport.isBlank(writeMode)) {
            return DbSyncConstants.WRITE_MODE_UPSERT;
        }
        String normalized = writeMode.trim().toLowerCase();
        if (!DbSyncConstants.WRITE_MODE_UPSERT.equals(normalized)
                && !DbSyncConstants.WRITE_MODE_DELETE_INSERT.equals(normalized)
                && !DbSyncConstants.WRITE_MODE_MULTI_VALUES_UPSERT.equals(normalized)
                && !DbSyncConstants.WRITE_MODE_FULL_REFRESH_INSERT.equals(normalized)) {
            throw new RuntimeException("writeMode must be upsert, delete_insert, multi_values_upsert or full_refresh_insert");
        }
        return normalized;
    }

    private String resolveFullRefreshDeleteMode(String fullRefreshDeleteMode) {
        if (SyncRuntimeSupport.isBlank(fullRefreshDeleteMode)) {
            return DbSyncConstants.FULL_REFRESH_DELETE_MODE_DELETE;
        }
        String normalized = fullRefreshDeleteMode.trim().toLowerCase();
        if (!DbSyncConstants.FULL_REFRESH_DELETE_MODE_DELETE.equals(normalized) && !DbSyncConstants.FULL_REFRESH_DELETE_MODE_TRUNCATE.equals(normalized)) {
            throw new RuntimeException("fullRefreshDeleteMode must be delete or truncate");
        }
        return normalized;
    }

    private String buildSqlSourceLabel(String sourceSql) {
        return "sql:" + Integer.toHexString(requireSql(sourceSql, "sourceSql").hashCode());
    }

    private String requireSql(String value, String fieldName) {
        if (SyncRuntimeSupport.isBlank(value)) {
            throw new RuntimeException(fieldName + " cannot be blank");
        }
        return value.trim().replaceAll(";+$", "");
    }

    private String buildDataSourceIdentity(Long dataSourceId,
                                           DirectDataSourceConfigDTO directDataSourceConfig,
                                           String fallback) {
        if (dataSourceId != null) {
            return String.valueOf(dataSourceId);
        }
        if (directDataSourceConfig != null && !SyncRuntimeSupport.isBlank(directDataSourceConfig.getJdbcUrl())) {
            return Integer.toHexString(directDataSourceConfig.getJdbcUrl().hashCode());
        }
        return fallback;
    }

    private String resolveCheckpointTable(String checkpointTable) {
        if (SyncRuntimeSupport.isBlank(checkpointTable)) {
            return DbSyncConstants.DEFAULT_CHECKPOINT_TABLE;
        }
        return requireIdentifier(checkpointTable, "checkpointTable");
    }

    private String requireIdentifier(String value, String fieldName) {
        if (SyncRuntimeSupport.isBlank(value)) {
            throw new RuntimeException(fieldName + " cannot be blank");
        }
        String trimmed = value.trim();
        if (!trimmed.matches("[A-Za-z0-9_]+")) {
            throw new RuntimeException(fieldName + " contains invalid characters: " + trimmed);
        }
        return trimmed;
    }

    private String nullableIdentifier(String value, String fieldName) {
        if (SyncRuntimeSupport.isBlank(value)) {
            return null;
        }
        return requireIdentifier(value, fieldName);
    }

    private String nullableSqlExpression(String value, String fieldName) {
        if (SyncRuntimeSupport.isBlank(value)) {
            return null;
        }
        String trimmed = value.trim();
        if (trimmed.contains(";") || trimmed.contains("--") || trimmed.contains("/*") || trimmed.contains("*/")) {
            throw new RuntimeException(fieldName + " contains invalid SQL comment or statement separator");
        }
        return trimmed;
    }
}
