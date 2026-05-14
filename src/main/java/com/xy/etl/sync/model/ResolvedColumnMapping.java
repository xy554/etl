package com.xy.etl.sync.model;

public record ResolvedColumnMapping(String sourceColumn,
                                    String targetColumn,
                                    boolean required,
                                    Integer maxLength,
                                    Object constantValue,
                                    boolean constantValueSet) {
}
