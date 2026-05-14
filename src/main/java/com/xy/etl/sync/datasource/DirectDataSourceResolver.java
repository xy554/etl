package com.xy.etl.sync.datasource;

import com.xy.etl.dto.DirectDataSourceConfigDTO;
import com.xy.etl.manager.DataSourceManager;
import com.xy.etl.sync.model.ResolvedTableConfig;
import com.xy.etl.sync.support.SyncRuntimeSupport;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;

@Slf4j
@Component
public class DirectDataSourceResolver {

    private final DataSourceManager dataSourceManager;

    public DirectDataSourceResolver(DataSourceManager dataSourceManager) {
        this.dataSourceManager = dataSourceManager;
    }

    public DataSource loadSourceDataSource(ResolvedTableConfig config) {
        if (config.sourceDataSourceConfig() != null) {
            log.info("resolve source datasource by direct config, syncKey: {}, sourceName: {}, jdbcUrl: {}",
                    config.syncKey(), config.sourceDataSourceConfig().getSourceName(), config.sourceDataSourceConfig().getJdbcUrl());
            return resolveDirectDataSource("source", config.sourceDataSourceId(), config.sourceDataSourceConfig());
        }
        throw new RuntimeException("source datasource is not configured");
    }

    public DataSource loadTargetDataSource(ResolvedTableConfig config) {
        if (config.targetDataSourceConfig() != null) {
            log.info("resolve target datasource by direct config, syncKey: {}, sourceName: {}, jdbcUrl: {}",
                    config.syncKey(), config.targetDataSourceConfig().getSourceName(), config.targetDataSourceConfig().getJdbcUrl());
            return resolveDirectDataSource("target", config.targetDataSourceId(), config.targetDataSourceConfig());
        }
        throw new RuntimeException("target datasource is not configured");
    }

    public DataSource resolveTargetDataSource(Long targetDataSourceId, DirectDataSourceConfigDTO targetDataSourceConfig) {
        if (targetDataSourceConfig != null) {
            return resolveDirectDataSource("target", targetDataSourceId, targetDataSourceConfig);
        }
        throw new RuntimeException("target datasource is not configured");
    }

    public DataSource resolveDirectDataSource(String role,
                                              Long dataSourceId,
                                              DirectDataSourceConfigDTO directDataSourceConfig) {
        validateDirectDataSourceConfig(role, directDataSourceConfig);
        return dataSourceManager.getDirectDataSource(
                directDataSourceConfig.getJdbcUrl(),
                directDataSourceConfig.getJdbcDriver(),
                directDataSourceConfig.getJdbcUsername(),
                directDataSourceConfig.getJdbcPassword(),
                directDataSourceConfig.getConnectionPoolSize(),
                directDataSourceConfig.getSourceName() != null ? directDataSourceConfig.getSourceName() : role + "-" + dataSourceId
        );
    }

    private void validateDirectDataSourceConfig(String role, DirectDataSourceConfigDTO directDataSourceConfig) {
        if (directDataSourceConfig == null) {
            return;
        }
        if (SyncRuntimeSupport.isBlank(directDataSourceConfig.getJdbcUrl())) {
            throw new RuntimeException(role + "DataSourceConfig.jdbcUrl cannot be blank");
        }
        if (SyncRuntimeSupport.isBlank(directDataSourceConfig.getJdbcUsername())) {
            throw new RuntimeException(role + "DataSourceConfig.jdbcUsername cannot be blank");
        }
        if (directDataSourceConfig.getConnectionPoolSize() != null && directDataSourceConfig.getConnectionPoolSize() <= 0) {
            throw new RuntimeException(role + "DataSourceConfig.connectionPoolSize must be greater than 0");
        }
    }
}
