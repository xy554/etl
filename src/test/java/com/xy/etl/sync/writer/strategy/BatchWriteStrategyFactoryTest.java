package com.xy.etl.sync.writer.strategy;

import com.xy.etl.sync.reader.SourceRowMapper;
import com.xy.etl.sync.support.WriteMode;
import com.xy.etl.sync.writer.TargetJdbcWriter;
import com.xy.etl.sync.writer.TargetSqlBuilder;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class BatchWriteStrategyFactoryTest {

    @Test
    void shouldRouteAllWriteModes() {
        SourceRowMapper mapper = new SourceRowMapper();
        TargetJdbcWriter writer = new TargetJdbcWriter(new TargetSqlBuilder());
        BatchWriteStrategyFactory factory = new BatchWriteStrategyFactory(List.of(
                new UpsertBatchWriteStrategy(mapper, writer),
                new DeleteInsertBatchWriteStrategy(mapper, writer),
                new MultiValuesUpsertBatchWriteStrategy(mapper, writer),
                new FullRefreshInsertBatchWriteStrategy(mapper, writer)
        ));

        assertEquals(UpsertBatchWriteStrategy.class, factory.get(WriteMode.UPSERT).getClass());
        assertEquals(DeleteInsertBatchWriteStrategy.class, factory.get(WriteMode.DELETE_INSERT).getClass());
        assertEquals(MultiValuesUpsertBatchWriteStrategy.class, factory.get(WriteMode.MULTI_VALUES_UPSERT).getClass());
        assertEquals(FullRefreshInsertBatchWriteStrategy.class, factory.get(WriteMode.FULL_REFRESH_INSERT).getClass());
    }
}
