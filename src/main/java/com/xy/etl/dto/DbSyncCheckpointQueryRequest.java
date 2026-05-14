package com.xy.etl.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class DbSyncCheckpointQueryRequest {
    private Long targetDataSourceId;
    private DirectDataSourceConfigDTO targetDataSourceConfig;
    private String syncKey;
}
