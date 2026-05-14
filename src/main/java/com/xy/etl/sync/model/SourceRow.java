package com.xy.etl.sync.model;

import java.util.Map;

public record SourceRow(Long cursorId,
                        String syncTime,
                        boolean deleted,
                        Map<String, Object> targetValues) {
}
