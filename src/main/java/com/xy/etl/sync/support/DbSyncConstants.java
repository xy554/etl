package com.xy.etl.sync.support;

import java.time.format.DateTimeFormatter;

public final class DbSyncConstants {

    public static final String DEFAULT_CHECKPOINT_TABLE = "edu_db_sync_checkpoint";
    public static final int DEFAULT_BATCH_SIZE = 2000;
    public static final String EPOCH_TIME = "1970-01-01 00:00:00";
    public static final DateTimeFormatter DATETIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    public static final String SOURCE_MODE_TABLE = "table";
    public static final String SOURCE_MODE_SQL = "sql";
    public static final String WRITE_MODE_UPSERT = "upsert";
    public static final String WRITE_MODE_DELETE_INSERT = "delete_insert";
    public static final String WRITE_MODE_MULTI_VALUES_UPSERT = "multi_values_upsert";
    public static final String WRITE_MODE_FULL_REFRESH_INSERT = "full_refresh_insert";
    public static final String FULL_REFRESH_DELETE_MODE_DELETE = "delete";
    public static final String FULL_REFRESH_DELETE_MODE_TRUNCATE = "truncate";
    public static final int DELETE_BATCH_SIZE = 500;
    public static final int MULTI_VALUES_UPSERT_CHUNK_SIZE = 500;
    public static final int MULTI_VALUES_INSERT_CHUNK_SIZE = 500;

    private DbSyncConstants() {
    }
}
