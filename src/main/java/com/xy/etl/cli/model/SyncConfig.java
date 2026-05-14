package com.xy.etl.cli.model;

import lombok.Data;

import java.util.List;

@Data
public class SyncConfig {

    private DataSourceConfig source;
    private DataSourceConfig target;
    private SyncOptions options;
    private List<TableConfig> tables;

    @Data
    public static class DataSourceConfig {
        private String jdbcUrl;
        private String host;
        private Integer port;
        private String database;
        private String username;
        private String user;
        private String password;
        private String driver;
        private String charset;
        private Integer connectionPoolSize;
    }

    @Data
    public static class SyncOptions {
        private Integer batchSize;
        private Boolean continueOnError;
        private Boolean truncateBeforeLoad;
        private String checkpointTable;
        private Boolean autoCreateCheckpointTable;
    }

    @Data
    public static class TableConfig {
        private String name;
        private String syncKey;
        private String sourceMode;
        private String writeMode;
        private String fullRefreshDeleteMode;
        private String sourceTable;
        private String sql;
        private String sourceSql;
        private String targetTable;
        private String cursorIdColumn;
        private String syncTimeColumn;
        private String syncTimeExpression;
        private String fallbackSyncTimeColumn;
        private String targetKeyColumn;
        private List<String> targetKeyColumns;
        private String targetKeySourceColumn;
        private Integer batchSize;
        private Boolean truncateBeforeLoad;
        private List<String> columns;
        private List<ColumnMappingConfig> columnMappings;
        private List<FilterConfig> filters;
        private DeleteRuleConfig deleteRule;
    }

    @Data
    public static class ColumnMappingConfig {
        private String sourceColumn;
        private String targetColumn;
        private Boolean required;
        private Integer maxLength;
        private Object constantValue;
    }

    @Data
    public static class FilterConfig {
        private String column;
        private Object value;
    }

    @Data
    public static class DeleteRuleConfig {
        private String operationTypeColumn;
        private Integer deleteOperationValue;
        private String deleteFlagColumn;
        private Integer deleteFlagValue;
    }
}
