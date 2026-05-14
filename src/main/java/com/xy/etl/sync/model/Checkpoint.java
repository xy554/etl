package com.xy.etl.sync.model;

public record Checkpoint(String lastSyncTime, Long lastSyncId) {
}
