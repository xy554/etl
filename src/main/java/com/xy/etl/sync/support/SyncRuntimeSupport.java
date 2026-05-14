package com.xy.etl.sync.support;

import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;

import com.xy.etl.sync.model.ResolvedTableConfig;

public final class SyncRuntimeSupport {

    private SyncRuntimeSupport() {
    }

    public static Long toLong(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Long longValue) {
            return longValue;
        }
        if (value instanceof Number number) {
            return number.longValue();
        }
        return Long.parseLong(String.valueOf(value));
    }

    public static Integer toInteger(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Integer intValue) {
            return intValue;
        }
        if (value instanceof Number number) {
            return number.intValue();
        }
        return Integer.parseInt(String.valueOf(value));
    }

    public static String toDatetimeString(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Timestamp timestamp) {
            return timestamp.toLocalDateTime().format(DbSyncConstants.DATETIME_FORMATTER);
        }
        if (value instanceof java.util.Date date) {
            return new Timestamp(date.getTime()).toLocalDateTime().format(DbSyncConstants.DATETIME_FORMATTER);
        }
        if (value instanceof LocalDateTime dateTime) {
            return dateTime.format(DbSyncConstants.DATETIME_FORMATTER);
        }
        String text = String.valueOf(value).trim();
        if (text.isEmpty()) {
            return null;
        }
        if (text.length() > 19) {
            text = text.substring(0, 19);
        }
        return text;
    }

    public static String maxDatetime(String left, String right) {
        if (left == null) {
            return right;
        }
        if (right == null) {
            return left;
        }
        return left.compareTo(right) >= 0 ? left : right;
    }

    public static Long readNullableLong(ResultSet rs, String columnLabel) throws Exception {
        long value = rs.getLong(columnLabel);
        return rs.wasNull() ? null : value;
    }

    public static LocalDateTime toLocalDateTime(Timestamp timestamp) {
        return timestamp == null ? null : timestamp.toLocalDateTime();
    }

    public static boolean isBlank(Object value) {
        if (value == null) {
            return true;
        }
        return String.valueOf(value).trim().isEmpty();
    }

    public static long elapsedMillis(long startMillis) {
        return System.currentTimeMillis() - startMillis;
    }

    public static List<String> resolveTargetKeyColumns(ResolvedTableConfig config) {
        if (config.targetKeyColumns() != null && !config.targetKeyColumns().isEmpty()) {
            return config.targetKeyColumns();
        }
        return Collections.singletonList(config.targetKeyColumn());
    }

    public static List<String> resolveTargetKeyColumnsForLog(String targetKeyColumn, List<String> targetKeyColumns) {
        if (targetKeyColumns != null && !targetKeyColumns.isEmpty()) {
            return targetKeyColumns;
        }
        if (targetKeyColumn != null) {
            return Collections.singletonList(targetKeyColumn);
        }
        return Collections.emptyList();
    }

    public static int sumBatchCounts(int[] batchCounts) {
        int total = 0;
        for (int count : batchCounts) {
            if (count > 0) {
                total += count;
            } else if (count == Statement.SUCCESS_NO_INFO) {
                total++;
            }
        }
        return total;
    }
}
