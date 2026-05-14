package com.xy.etl.sync.model;

public record BatchWriteResult(long processedCount,
                               long upsertedCount,
                               long deletedCount,
                               long maxCursorId,
                               String maxSyncTime,
                               long mapMillis,
                               long deleteMillis,
                               long writeMillis) {
}
