package com.xy.etl.sync.model;

public record ResolvedDeleteRule(String operationTypeColumn,
                                 Integer deleteOperationValue,
                                 String deleteFlagColumn,
                                 Integer deleteFlagValue) {
}
