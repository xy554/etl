package com.xy.etl.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class DbSyncTableResultDTO {
    private String syncKey;
    private String sourceTable;
    private String targetTable;
    private boolean success;
    private String errorMessage;
    private Long processedCount;
    private Long upsertedCount;
    private Long deletedCount;
    private String startSyncTime;
    private Long startSyncId;
    private String endSyncTime;
    private Long endSyncId;
}
