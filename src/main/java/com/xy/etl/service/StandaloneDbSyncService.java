package com.xy.etl.service;

import com.xy.etl.dto.*;

import java.util.List;

public interface StandaloneDbSyncService {

    DbSyncResponse run(DbSyncRequest request);

    List<DbSyncCheckpointDTO> getCheckpoints(Long targetDataSourceId,
                                             DirectDataSourceConfigDTO targetDataSourceConfig,
                                             String syncKey);

    int resetCheckpoints(DbSyncCheckpointResetRequest request);
}
