package com.xy.etl.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class DbSyncDeleteRuleDTO {
    private String operationTypeColumn;
    private Integer deleteOperationValue;
    private String deleteFlagColumn;
    private Integer deleteFlagValue;
}
