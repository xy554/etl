package com.xy.etl.sync.writer.strategy;

import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

@Component
public class BatchWriteStrategyFactory {

    private final Map<String, BatchWriteStrategy> strategies;

    public BatchWriteStrategyFactory(List<BatchWriteStrategy> strategies) {
        this.strategies = strategies.stream().collect(Collectors.toMap(BatchWriteStrategy::writeMode, Function.identity()));
    }

    public BatchWriteStrategy get(String writeMode) {
        BatchWriteStrategy strategy = strategies.get(writeMode);
        if (strategy == null) {
            throw new RuntimeException("unsupported write mode: " + writeMode);
        }
        return strategy;
    }
}
