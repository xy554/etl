package com.xy.etl.controller;

import com.xy.etl.dto.*;
import com.xy.etl.service.StandaloneDbSyncService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@Slf4j
@RestController
@RequestMapping("/api/db-sync")
public class DbSyncController {

    private final StandaloneDbSyncService standaloneDbSyncService;

    public DbSyncController(StandaloneDbSyncService standaloneDbSyncService) {
        this.standaloneDbSyncService = standaloneDbSyncService;
    }

    @PostMapping("/run")
    public DbSyncResponse run(@RequestBody DbSyncRequest request) {
        if (request == null) {
            throw new IllegalArgumentException("request cannot be null");
        }
        log.info("receive db sync run request, tableCount: {}, batchSize: {}, continueOnError: {}, hasSourceConfig: {}, hasTargetConfig: {}",
                request.getTableConfigs() == null ? 0 : request.getTableConfigs().size(),
                request.getBatchSize(),
                request.getContinueOnError(),
                request.getSourceDataSourceConfig() != null || request.getSourceDataSourceId() != null,
                request.getTargetDataSourceConfig() != null || request.getTargetDataSourceId() != null);
        return standaloneDbSyncService.run(request);
    }

    @PostMapping("/checkpoints/query")
    public List<DbSyncCheckpointDTO> getCheckpoints(@RequestBody DbSyncCheckpointQueryRequest request) {
        if (request == null) {
            throw new IllegalArgumentException("request cannot be null");
        }
        log.info("receive db sync checkpoint query request, syncKey: {}, hasTargetConfig: {}",
                request.getSyncKey(),
                request.getTargetDataSourceConfig() != null || request.getTargetDataSourceId() != null);
        return standaloneDbSyncService.getCheckpoints(
                request.getTargetDataSourceId(),
                request.getTargetDataSourceConfig(),
                request.getSyncKey()
        );
    }

    @PostMapping("/checkpoints/reset")
    public Integer resetCheckpoints(@RequestBody DbSyncCheckpointResetRequest request) {
        if (request == null) {
            throw new IllegalArgumentException("request cannot be null");
        }
        log.info("receive db sync checkpoint reset request, syncKeyCount: {}, hasTargetConfig: {}",
                request.getSyncKeys() == null ? 0 : request.getSyncKeys().size(),
                request.getTargetDataSourceConfig() != null || request.getTargetDataSourceId() != null);
        return standaloneDbSyncService.resetCheckpoints(request);
    }
}
