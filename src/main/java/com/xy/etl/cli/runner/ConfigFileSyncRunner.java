package com.xy.etl.cli.runner;

import com.xy.etl.cli.config.ConfigFileRequestFactory;
import com.xy.etl.cli.exception.ConfigFileSyncException;
import com.xy.etl.cli.logging.ConfigFileSyncResultLogger;
import com.xy.etl.cli.support.ConfigFileCliSupport;
import com.xy.etl.dto.DbSyncRequest;
import com.xy.etl.dto.DbSyncResponse;
import com.xy.etl.service.StandaloneDbSyncService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import java.util.List;

@Slf4j
@Component
@Order(0)
public class ConfigFileSyncRunner implements ApplicationRunner {

    private final StandaloneDbSyncService syncService;
    private final ConfigFileRequestFactory requestFactory;
    private final ConfigFileSyncResultLogger resultLogger;

    public ConfigFileSyncRunner(StandaloneDbSyncService syncService,
                                ConfigFileRequestFactory requestFactory,
                                ConfigFileSyncResultLogger resultLogger) {
        this.syncService = syncService;
        this.requestFactory = requestFactory;
        this.resultLogger = resultLogger;
    }

    @Override
    public void run(ApplicationArguments ignored) {
        List<String> configPaths = ConfigFileCliSupport.getConfigPaths();
        if (configPaths.isEmpty()) {
            log.info("未使用配置文件方式启动（未找到 -Dconfig 系统属性），等待 Web 请求");
            return;
        }

        log.info("检测到配置文件方式启动，配置文件数: {}, paths: {}", configPaths.size(), configPaths);
        try {
            log.info("加载配置文件: {}", configPaths);
            DbSyncRequest request = requestFactory.create(configPaths);
            resultLogger.logLoadedConfig(configPaths.size(), request.getTableConfigs().size());
            log.info("开始执行同步，表数: {}", request.getTableConfigs().size());

            long startTime = System.currentTimeMillis();
            DbSyncResponse response = syncService.run(request);
            long elapsedMillis = System.currentTimeMillis() - startTime;
            resultLogger.logExecutionResult(response, elapsedMillis);

        } catch (Exception e) {
            throw new ConfigFileSyncException("配置文件同步执行失败: " + e.getMessage(), e);
        }

        log.info("同步任务执行完成");
    }
}
