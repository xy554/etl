package com.xy.etl.cli.config;

import com.xy.etl.dto.DbSyncRequest;
import com.xy.etl.cli.model.SyncConfig;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class ConfigFileRequestFactory {

    public DbSyncRequest create(List<String> configPaths) {
        SyncConfig syncConfig = YamlConfigLoader.loadMultiple(configPaths);
        return YamlConfigLoader.toRequest(syncConfig);
    }
}
