package com.xy.etl.sync.writer.strategy;

import com.xy.etl.sync.model.BatchWriteResult;
import com.xy.etl.sync.model.ResolvedTableConfig;
import com.xy.etl.sync.reader.SourceRowMapper;
import com.xy.etl.sync.writer.TargetJdbcWriter;
import java.sql.Connection;
import java.util.List;
import java.util.Map;

abstract class AbstractBatchWriteStrategy implements BatchWriteStrategy {

    protected final SourceRowMapper sourceRowMapper;
    protected final TargetJdbcWriter targetJdbcWriter;

    protected AbstractBatchWriteStrategy(SourceRowMapper sourceRowMapper, TargetJdbcWriter targetJdbcWriter) {
        this.sourceRowMapper = sourceRowMapper;
        this.targetJdbcWriter = targetJdbcWriter;
    }

    @Override
    public abstract String writeMode();

    @Override
    public abstract BatchWriteResult write(Connection targetConn, ResolvedTableConfig config, List<Map<String, Object>> rows);
}
