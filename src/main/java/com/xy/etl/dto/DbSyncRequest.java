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
public class DbSyncRequest {
    private Long sourceDataSourceId;
    private DirectDataSourceConfigDTO sourceDataSourceConfig;
    private Long targetDataSourceId;
    private DirectDataSourceConfigDTO targetDataSourceConfig;
    private String checkpointTable;
    private Boolean autoCreateCheckpointTable;
    private Integer batchSize;
    private Boolean continueOnError;
    private List<DbSyncTableConfigDTO> tableConfigs;
}
