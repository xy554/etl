package com.xy.etl.cli.logging;

import com.xy.etl.dto.DbSyncResponse;
import com.xy.etl.dto.DbSyncTableResultDTO;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class ConfigFileSyncResultLogger {

    public void logLoadedConfig(int configCount, int tableCount) {
        log.info("配置文件加载成功，配置文件数: {}, 表数量: {}", configCount, tableCount);
    }

    public void logExecutionResult(DbSyncResponse response, long elapsedMillis) {
        long successCount = response.getTableResults() != null
                ? response.getTableResults().stream().filter(DbSyncTableResultDTO::isSuccess).count()
                : 0;
        long failureCount = response.getTableResults() != null
                ? response.getTableResults().stream().filter(result -> !result.isSuccess()).count()
                : 0;

        log.info("");
        log.info("========== 同步执行结果 ==========");
        if (response.getTableResults() != null) {
            for (DbSyncTableResultDTO tableResult : response.getTableResults()) {
                log.info("表 {} 同步结果: success={}, processedCount={}, upsertedCount={}, deletedCount={}, endSyncTime={}",
                        tableResult.getSyncKey(), tableResult.isSuccess(), tableResult.getProcessedCount(),
                        tableResult.getUpsertedCount(), tableResult.getDeletedCount(), tableResult.getEndSyncTime());
            }
        }
        log.info("总表数: {}, 成功: {}, 失败: {}",
                response.getTableResults() != null ? response.getTableResults().size() : 0, successCount, failureCount);
        log.info("总处理行数: {}, 总upsert行数: {}, 总delete行数: {}",
                response.getTotalProcessedCount(), response.getTotalUpsertedCount(), response.getTotalDeletedCount());
        log.info("总耗时: {} ms ({} 秒)", elapsedMillis, elapsedMillis / 1000.0);
        log.info("==================================");
        log.info("");
    }
}
