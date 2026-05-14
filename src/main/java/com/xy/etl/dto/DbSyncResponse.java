package com.xy.etl.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;
import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class DbSyncResponse {
    private boolean success;
    private LocalDateTime startTime;
    private LocalDateTime endTime;
    private Long totalProcessedCount;
    private Long totalUpsertedCount;
    private Long totalDeletedCount;
    private List<DbSyncTableResultDTO> tableResults;
}
