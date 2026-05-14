package com.xy.etl.manager;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 数据源管理器
 * 管理 HikariCP 连接池
 */
@Slf4j
@Component
public class DataSourceManager {

    private final Map<String, DataSource> directDataSourceCache = new ConcurrentHashMap<>();


    public DataSource getDirectDataSource(String jdbcUrl,
                                          String jdbcDriver,
                                          String jdbcUsername,
                                          String jdbcPassword,
                                          Integer connectionPoolSize,
                                          String sourceName) {
        if (jdbcUrl == null || jdbcUrl.isBlank()) {
            throw new IllegalArgumentException("jdbcUrl cannot be blank");
        }

        String cacheKey = buildDirectCacheKey(jdbcUrl, jdbcDriver, jdbcUsername, jdbcPassword, connectionPoolSize);
        DataSource cached = directDataSourceCache.get(cacheKey);
        if (cached != null) {
            return cached;
        }

        synchronized (this) {
            cached = directDataSourceCache.get(cacheKey);
            if (cached != null) {
                return cached;
            }

            log.info("create direct datasource pool, sourceName: {}, jdbcUrl: {}", sourceName, jdbcUrl);
            HikariDataSource dataSource = createDataSource(
                    jdbcUrl,
                    jdbcDriver,
                    jdbcUsername,
                    jdbcPassword,
                    connectionPoolSize,
                    "DirectHikariPool-" + Integer.toHexString(cacheKey.hashCode())
            );
            directDataSourceCache.put(cacheKey, dataSource);
            return dataSource;
        }
    }


    private HikariDataSource createDataSource(String jdbcUrl,
                                              String jdbcDriver,
                                              String jdbcUsername,
                                              String jdbcPassword,
                                              Integer connectionPoolSize,
                                              String poolName) {
        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setJdbcUrl(ensureRewriteBatchedStatements(jdbcUrl));
        hikariConfig.setUsername(jdbcUsername);
        hikariConfig.setPassword(jdbcPassword);
        hikariConfig.setDriverClassName(jdbcDriver != null && !jdbcDriver.isBlank() ? jdbcDriver : "com.mysql.cj.jdbc.Driver");

        int poolSize = connectionPoolSize != null && connectionPoolSize > 0 ? connectionPoolSize : 10;
        hikariConfig.setMinimumIdle(1);
        hikariConfig.setMaximumPoolSize(poolSize);
        hikariConfig.setConnectionTimeout(30000);
        hikariConfig.setIdleTimeout(600000);
        hikariConfig.setMaxLifetime(1800000);
        hikariConfig.setConnectionTestQuery("SELECT 1");
        hikariConfig.setPoolName(poolName);
        return new HikariDataSource(hikariConfig);
    }

    private String buildDirectCacheKey(String jdbcUrl,
                                       String jdbcDriver,
                                       String jdbcUsername,
                                       String jdbcPassword,
                                       Integer connectionPoolSize) {
        return String.join("|",
                safe(jdbcUrl),
                safe(jdbcDriver),
                safe(jdbcUsername),
                safe(jdbcPassword),
                String.valueOf(connectionPoolSize));
    }

    private String safe(String value) {
        return value == null ? "" : value;
    }

    /**
     * 确保 MySQL JDBC URL 包含 rewriteBatchedStatements=true，
     * 这是批量写入性能的关键参数，可将 N 条 INSERT 重写为一条多值 INSERT，
     * 性能提升通常在 10 倍以上。
     */
    private String ensureRewriteBatchedStatements(String jdbcUrl) {
        if (jdbcUrl == null || !jdbcUrl.contains("mysql")) {
            return jdbcUrl;
        }
        if (jdbcUrl.contains("rewriteBatchedStatements")) {
            return jdbcUrl;
        }
        String separator = jdbcUrl.contains("?") ? "&" : "?";
        return jdbcUrl + separator + "rewriteBatchedStatements=true";
    }
}
