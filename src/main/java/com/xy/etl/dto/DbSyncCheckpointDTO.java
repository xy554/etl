package com.xy.etl.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class DbSyncCheckpointDTO {
    private Long id;
    private String syncKey;
    private String sourceTable;
    private String targetTable;
    private String lastSyncTime;
    private Long lastSyncId;
    private LocalDateTime createTime;
    private LocalDateTime updateTime;
}
