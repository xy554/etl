package com.xy.etl.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class DbSyncColumnMappingDTO {
    private String sourceColumn;
    private String targetColumn;
    private Boolean required;
    private Integer maxLength;
    private Object constantValue;
}
