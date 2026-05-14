package com.xy.etl.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class DbSyncCheckpointResetRequest {
    private Long targetDataSourceId;
    private DirectDataSourceConfigDTO targetDataSourceConfig;
    private List<String> syncKeys;
}
