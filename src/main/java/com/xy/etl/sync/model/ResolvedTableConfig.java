package com.xy.etl.sync.model;

import com.xy.etl.dto.DirectDataSourceConfigDTO;

import java.util.List;

public record ResolvedTableConfig(Long sourceDataSourceId,
                                  DirectDataSourceConfigDTO sourceDataSourceConfig,
                                  Long targetDataSourceId,
                                  DirectDataSourceConfigDTO targetDataSourceConfig,
                                  String syncKey,
                                  String sourceMode,
                                  String writeMode,
                                  String fullRefreshDeleteMode,
                                  String sourceTable,
                                  String sourceSql,
                                  String targetTable,
                                  String cursorIdColumn,
                                  String syncTimeColumn,
                                  String syncTimeExpression,
                                  String fallbackSyncTimeColumn,
                                  String targetKeyColumn,
                                  List<String> targetKeyColumns,
                                  String targetKeySourceColumn,
                                  int batchSize,
                                  List<ResolvedColumnMapping> columnMappings,
                                  List<ResolvedFilter> filters,
                                  ResolvedDeleteRule deleteRule,
                                  String checkpointTable,
                                  boolean autoCreateCheckpointTable) {
}
