package com.xy.etl.cli.config;

import com.xy.etl.dto.DbSyncRequest;
import com.xy.etl.cli.model.SyncConfig;
import org.springframework.stereotype.Component;

@Component
public class ConfigFileRequestFactory {

    public DbSyncRequest create(String configPath) {
        SyncConfig syncConfig = YamlConfigLoader.load(configPath);
        return YamlConfigLoader.toRequest(syncConfig);
    }
}
