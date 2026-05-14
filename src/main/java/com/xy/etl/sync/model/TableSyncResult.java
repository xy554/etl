package com.xy.etl.sync.model;

public record TableSyncResult(String syncKey,
                              String sourceTable,
                              String targetTable,
                              long processedCount,
                              long upsertedCount,
                              long deletedCount,
                              String startSyncTime,
                              Long startSyncId,
                              String endSyncTime,
                              Long endSyncId) {
}
