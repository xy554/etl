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
public class DbSyncTableConfigDTO {
    private Long sourceDataSourceId;
    private DirectDataSourceConfigDTO sourceDataSourceConfig;
    private Long targetDataSourceId;
    private DirectDataSourceConfigDTO targetDataSourceConfig;
    private String syncKey;
    private String sourceMode;
    private String writeMode;
    private String fullRefreshDeleteMode;
    private String sourceTable;
    private String sourceSql;
    private String targetTable;
    private String cursorIdColumn;
    private String syncTimeColumn;
    private String syncTimeExpression;
    private String fallbackSyncTimeColumn;
    private String targetKeyColumn;
    private List<String> targetKeyColumns;
    private String targetKeySourceColumn;
    private Integer batchSize;
    private List<DbSyncColumnMappingDTO> columnMappings;
    private List<DbSyncFilterDTO> filters;
    private DbSyncDeleteRuleDTO deleteRule;
}
