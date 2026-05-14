package com.xy.etl.cli.config;

import com.xy.etl.cli.model.SyncConfig;
import com.xy.etl.dto.*;
import com.xy.etl.sync.support.DbSyncConstants;
import org.springframework.beans.factory.config.YamlPropertiesFactoryBean;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.context.properties.bind.Bindable;
import org.springframework.boot.context.properties.source.ConfigurationPropertySource;
import org.springframework.boot.context.properties.source.MapConfigurationPropertySource;
import org.springframework.core.io.FileSystemResource;

import java.io.File;
import java.util.*;

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

    /**
     * 加载并合并多个配置文件
     * 后面的配置会覆盖/追加到前面的配置
     */
    public static SyncConfig loadMultiple(List<String> configPaths) {
        if (configPaths == null || configPaths.isEmpty()) {
            throw new RuntimeException("配置文件列表为空");
        }
        if (configPaths.size() == 1) {
            return load(configPaths.get(0));
        }

        SyncConfig merged = null;
        for (String path : configPaths) {
            SyncConfig config = load(path);
            if (merged == null) {
                merged = config;
            } else {
                merged = merge(merged, config);
            }
        }
        return merged;
    }

    /**
     * 合并两个配置，b 的配置会覆盖/追加到 a
     */
    private static SyncConfig merge(SyncConfig a, SyncConfig b) {
        SyncConfig result = new SyncConfig();

        // source: b 覆盖 a
        result.setSource(b.getSource() != null ? b.getSource() : a.getSource());
        // target: b 覆盖 a
        result.setTarget(b.getTarget() != null ? b.getTarget() : a.getTarget());

        // options: 字段级别合并，b 的非空值优先
        SyncConfig.SyncOptions mergedOpts = new SyncConfig.SyncOptions();
        SyncConfig.SyncOptions optsA = a.getOptions();
        SyncConfig.SyncOptions optsB = b.getOptions();
        mergedOpts.setBatchSize(optsB != null && optsB.getBatchSize() != null ? optsB.getBatchSize()
                : optsA != null ? optsA.getBatchSize() : null);
        mergedOpts.setContinueOnError(optsB != null && optsB.getContinueOnError() != null ? optsB.getContinueOnError()
                : optsA != null ? optsA.getContinueOnError() : null);
        mergedOpts.setTruncateBeforeLoad(optsB != null && optsB.getTruncateBeforeLoad() != null ? optsB.getTruncateBeforeLoad()
                : optsA != null ? optsA.getTruncateBeforeLoad() : null);
        mergedOpts.setCheckpointTable(optsB != null && optsB.getCheckpointTable() != null ? optsB.getCheckpointTable()
                : optsA != null ? optsA.getCheckpointTable() : null);
        mergedOpts.setAutoCreateCheckpointTable(optsB != null && optsB.getAutoCreateCheckpointTable() != null ? optsB.getAutoCreateCheckpointTable()
                : optsA != null ? optsA.getAutoCreateCheckpointTable() : null);
        result.setOptions(mergedOpts);

        // tables: 按出现顺序合并，同名 table 用后面的配置覆盖
        result.setTables(mergeTables(a.getTables(), b.getTables()));

        return result;
    }

    private static List<SyncConfig.TableConfig> mergeTables(List<SyncConfig.TableConfig> aTables,
                                                            List<SyncConfig.TableConfig> bTables) {
        List<SyncConfig.TableConfig> mergedTables = new ArrayList<>();
        Map<String, Integer> keyedIndexes = new LinkedHashMap<>();
        mergeTableList(mergedTables, keyedIndexes, aTables);
        mergeTableList(mergedTables, keyedIndexes, bTables);
        return mergedTables;
    }

    private static void mergeTableList(List<SyncConfig.TableConfig> mergedTables,
                                       Map<String, Integer> keyedIndexes,
                                       List<SyncConfig.TableConfig> tables) {
        if (tables == null) {
            return;
        }
        for (SyncConfig.TableConfig table : tables) {
            String key = resolveTableMergeKey(table);
            if (key == null) {
                mergedTables.add(table);
                continue;
            }
            Integer existingIndex = keyedIndexes.get(key);
            if (existingIndex == null) {
                keyedIndexes.put(key, mergedTables.size());
                mergedTables.add(table);
            } else {
                mergedTables.set(existingIndex, table);
            }
        }
    }

    private static String resolveTableMergeKey(SyncConfig.TableConfig table) {
        if (table == null) {
            return null;
        }
        String key = table.getSyncKey() != null ? table.getSyncKey() : table.getName();
        if (key == null || key.isBlank()) {
            return null;
        }
        return key.trim();
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

        // 推断 sourceMode
        String sourceMode = table.getSourceMode();
        if (sourceMode == null) {
            sourceMode = sourceSql != null ? DbSyncConstants.SOURCE_MODE_SQL : DbSyncConstants.SOURCE_MODE_TABLE;
        }

        // 推断 writeMode
        String writeMode = table.getWriteMode();
        boolean hasColumns = table.getColumns() != null && !table.getColumns().isEmpty();
        Boolean truncateBeforeLoad = table.getTruncateBeforeLoad();
        if (truncateBeforeLoad == null) {
            truncateBeforeLoad = opts.getTruncateBeforeLoad();
        }
        if (writeMode == null) {
            writeMode = hasColumns || Boolean.TRUE.equals(truncateBeforeLoad)
                    ? DbSyncConstants.WRITE_MODE_FULL_REFRESH_INSERT
                    : DbSyncConstants.WRITE_MODE_UPSERT;
        }

        String fullRefreshDeleteMode = Boolean.TRUE.equals(truncateBeforeLoad) ? DbSyncConstants.FULL_REFRESH_DELETE_MODE_TRUNCATE : null;

        // 构建 columnMappings
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

        // 构建 filters
        List<DbSyncFilterDTO> filters = new ArrayList<>();
        if (table.getFilters() != null) {
            for (SyncConfig.FilterConfig f : table.getFilters()) {
                filters.add(DbSyncFilterDTO.builder()
                        .column(f.getColumn())
                        .value(f.getValue())
                        .build());
            }
        }

        // 构建 deleteRule
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

        // syncKey: 优先用 syncKey，其次用 name
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
