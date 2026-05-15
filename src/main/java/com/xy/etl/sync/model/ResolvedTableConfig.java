package com.xy.etl.sync.model;

import com.xy.etl.dto.DirectDataSourceConfigDTO;
import com.xy.etl.sync.support.FullRefreshDeleteMode;
import com.xy.etl.sync.support.SourceMode;
import com.xy.etl.sync.support.WriteMode;

import java.util.List;

public record ResolvedTableConfig(Long sourceDataSourceId,
                                  DirectDataSourceConfigDTO sourceDataSourceConfig,
                                  Long targetDataSourceId,
                                  DirectDataSourceConfigDTO targetDataSourceConfig,
                                  String syncKey,
                                  SourceMode sourceMode,
                                  WriteMode writeMode,
                                  FullRefreshDeleteMode fullRefreshDeleteMode,
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

    public boolean sqlSourceMode() {
        return sourceMode.isSql();
    }

    public boolean fullRefreshWriteMode() {
        return writeMode.isFullRefreshInsert();
    }

    public boolean truncateBeforeFullRefresh() {
        return fullRefreshDeleteMode.isTruncate();
    }

    public String sourceModeValue() {
        return sourceMode.value();
    }

    public String writeModeValue() {
        return writeMode.value();
    }

    public String fullRefreshDeleteModeValue() {
        return fullRefreshDeleteMode.value();
    }
}
