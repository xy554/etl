package com.xy.etl.cli.config;

import com.xy.etl.cli.model.SyncConfig;
import com.xy.etl.dto.DbSyncColumnMappingDTO;
import com.xy.etl.dto.DbSyncDeleteRuleDTO;
import com.xy.etl.dto.DbSyncFilterDTO;
import com.xy.etl.dto.DbSyncRequest;
import com.xy.etl.dto.DbSyncTableConfigDTO;
import com.xy.etl.dto.DirectDataSourceConfigDTO;
import com.xy.etl.sync.support.FullRefreshDeleteMode;
import com.xy.etl.sync.support.SourceMode;
import com.xy.etl.sync.support.WriteMode;
import org.springframework.beans.factory.config.YamlPropertiesFactoryBean;
import org.springframework.boot.context.properties.bind.Bindable;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.context.properties.source.ConfigurationPropertySource;
import org.springframework.boot.context.properties.source.MapConfigurationPropertySource;
import org.springframework.core.io.FileSystemResource;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class YamlConfigLoader {

    public static SyncConfig load(String configPath) {
        File file = new File(configPath);
        if (!file.exists()) {
            throw new RuntimeException("配置文件不存在: " + configPath);
        }

        YamlPropertiesFactoryBean factory = new YamlPropertiesFactoryBean();
        factory.setResources(new FileSystemResource(file));
        Properties properties = factory.getObject();

        if (properties == null || properties.isEmpty()) {
            throw new RuntimeException("配置文件为空: " + configPath);
        }

        ConfigurationPropertySource source = new MapConfigurationPropertySource(properties);
        Binder binder = new Binder(source);

        return binder.bind("", Bindable.of(SyncConfig.class))
                .orElseThrow(() -> new RuntimeException("配置文件解析失败: " + configPath));
    }

    public static DbSyncRequest toRequest(SyncConfig config) {
        if (config.getSource() == null) {
            throw new RuntimeException("配置缺少 source 段");
        }
        if (config.getTarget() == null) {
            throw new RuntimeException("配置缺少 target 段");
        }
        if (config.getTables() == null || config.getTables().isEmpty()) {
            throw new RuntimeException("配置缺少 tables 列表");
        }

        DirectDataSourceConfigDTO sourceConfig = buildDataSourceConfig(config.getSource(), "source");
        DirectDataSourceConfigDTO targetConfig = buildDataSourceConfig(config.getTarget(), "target");

        SyncConfig.SyncOptions opts = config.getOptions() != null ? config.getOptions() : new SyncConfig.SyncOptions();

        List<DbSyncTableConfigDTO> tableConfigs = new ArrayList<>();
        for (SyncConfig.TableConfig table : config.getTables()) {
            tableConfigs.add(buildTableConfig(table, opts));
        }

        return DbSyncRequest.builder()
                .sourceDataSourceConfig(sourceConfig)
                .targetDataSourceConfig(targetConfig)
                .batchSize(opts.getBatchSize())
                .continueOnError(opts.getContinueOnError())
                .checkpointTable(opts.getCheckpointTable())
                .autoCreateCheckpointTable(opts.getAutoCreateCheckpointTable())
                .tableConfigs(tableConfigs)
                .build();
    }

    private static DirectDataSourceConfigDTO buildDataSourceConfig(SyncConfig.DataSourceConfig ds, String role) {
        String jdbcUrl = ds.getJdbcUrl();
        if (jdbcUrl == null) {
            String host = ds.getHost();
            String database = ds.getDatabase();
            if (host == null || database == null) {
                throw new RuntimeException(role + " 数据源必须配置 jdbcUrl 或 host+database");
            }
            int port = ds.getPort() != null ? ds.getPort() : 3306;
            String charset = ds.getCharset() != null ? ds.getCharset() : "utf8mb4";
            jdbcUrl = buildMysqlJdbcUrl(host, port, database, charset);
        }

        String username = ds.getUsername() != null ? ds.getUsername() : ds.getUser();

        return DirectDataSourceConfigDTO.builder()
                .sourceName(role)
                .jdbcUrl(jdbcUrl)
                .jdbcDriver(ds.getDriver())
                .jdbcUsername(username)
                .jdbcPassword(ds.getPassword())
                .connectionPoolSize(ds.getConnectionPoolSize())
                .build();
    }

    private static String buildMysqlJdbcUrl(String host, int port, String database, String charset) {
        String normalizedCharset = normalizeJdbcCharacterEncoding(charset);
        StringBuilder jdbcUrl = new StringBuilder("jdbc:mysql://")
                .append(host)
                .append(":")
                .append(port)
                .append("/")
                .append(database)
                .append("?useSSL=false&useUnicode=true&characterEncoding=")
                .append(normalizedCharset);
        if (isUtf8mb4(charset)) {
            jdbcUrl.append("&connectionCollation=utf8mb4_general_ci");
        }
        return jdbcUrl.toString();
    }

    private static DbSyncTableConfigDTO buildTableConfig(SyncConfig.TableConfig table, SyncConfig.SyncOptions opts) {
        String sourceSql = firstNonBlank(table.getSql(), table.getSourceSql());

        String sourceMode = table.getSourceMode();
        if (sourceMode == null) {
            sourceMode = sourceSql != null ? SourceMode.SQL.value() : SourceMode.TABLE.value();
        }

        String writeMode = table.getWriteMode();
        boolean hasColumns = table.getColumns() != null && !table.getColumns().isEmpty();
        Boolean truncateBeforeLoad = table.getTruncateBeforeLoad();
        if (truncateBeforeLoad == null) {
            truncateBeforeLoad = opts.getTruncateBeforeLoad();
        }
        if (writeMode == null) {
            writeMode = hasColumns || Boolean.TRUE.equals(truncateBeforeLoad)
                    ? WriteMode.FULL_REFRESH_INSERT.value()
                    : WriteMode.UPSERT.value();
        }

        String fullRefreshDeleteMode = Boolean.TRUE.equals(truncateBeforeLoad)
                ? FullRefreshDeleteMode.TRUNCATE.value()
                : null;

        List<DbSyncColumnMappingDTO> columnMappings = new ArrayList<>();
        if (table.getColumns() != null) {
            for (String col : table.getColumns()) {
                columnMappings.add(DbSyncColumnMappingDTO.builder()
                        .sourceColumn(col)
                        .targetColumn(col)
                        .build());
            }
        }
        if (table.getColumnMappings() != null) {
            for (SyncConfig.ColumnMappingConfig cm : table.getColumnMappings()) {
                columnMappings.add(DbSyncColumnMappingDTO.builder()
                        .sourceColumn(cm.getSourceColumn())
                        .targetColumn(cm.getTargetColumn())
                        .required(cm.getRequired())
                        .maxLength(cm.getMaxLength())
                        .constantValue(cm.getConstantValue())
                        .build());
            }
        }

        List<DbSyncFilterDTO> filters = new ArrayList<>();
        if (table.getFilters() != null) {
            for (SyncConfig.FilterConfig f : table.getFilters()) {
                filters.add(DbSyncFilterDTO.builder()
                        .column(f.getColumn())
                        .value(f.getValue())
                        .build());
            }
        }

        DbSyncDeleteRuleDTO deleteRule = null;
        if (table.getDeleteRule() != null) {
            SyncConfig.DeleteRuleConfig dr = table.getDeleteRule();
            deleteRule = DbSyncDeleteRuleDTO.builder()
                    .operationTypeColumn(dr.getOperationTypeColumn())
                    .deleteOperationValue(dr.getDeleteOperationValue())
                    .deleteFlagColumn(dr.getDeleteFlagColumn())
                    .deleteFlagValue(dr.getDeleteFlagValue())
                    .build();
        }

        String syncKey = table.getSyncKey();
        if (syncKey == null || syncKey.isBlank()) {
            syncKey = table.getName();
        }

        return DbSyncTableConfigDTO.builder()
                .syncKey(syncKey)
                .sourceMode(sourceMode)
                .writeMode(writeMode)
                .fullRefreshDeleteMode(fullRefreshDeleteMode)
                .sourceTable(table.getSourceTable())
                .sourceSql(sourceSql)
                .targetTable(table.getTargetTable())
                .cursorIdColumn(table.getCursorIdColumn())
                .syncTimeColumn(table.getSyncTimeColumn())
                .syncTimeExpression(table.getSyncTimeExpression())
                .fallbackSyncTimeColumn(table.getFallbackSyncTimeColumn())
                .targetKeyColumn(table.getTargetKeyColumn())
                .targetKeyColumns(table.getTargetKeyColumns())
                .targetKeySourceColumn(table.getTargetKeySourceColumn())
                .batchSize(table.getBatchSize())
                .columnMappings(columnMappings.isEmpty() ? null : columnMappings)
                .filters(filters.isEmpty() ? null : filters)
                .deleteRule(deleteRule)
                .build();
    }

    private static String firstNonBlank(String first, String second) {
        if (first != null && !first.isBlank()) {
            return first;
        }
        if (second != null && !second.isBlank()) {
            return second;
        }
        return null;
    }

    private static String normalizeJdbcCharacterEncoding(String charset) {
        if (charset == null || charset.isBlank()) {
            return "UTF-8";
        }
        if (isUtf8mb4(charset) || "utf8".equalsIgnoreCase(charset)) {
            return "UTF-8";
        }
        return charset;
    }

    private static boolean isUtf8mb4(String charset) {
        return "utf8mb4".equalsIgnoreCase(charset);
    }
}
