package com.xy.etl.sync.writer.strategy;

import com.xy.etl.sync.model.BatchWriteResult;
import com.xy.etl.sync.model.ResolvedTableConfig;
import java.sql.Connection;
import java.util.List;
import java.util.Map;

public interface BatchWriteStrategy {

    String writeMode();

    BatchWriteResult write(Connection targetConn, ResolvedTableConfig config, List<Map<String, Object>> rows);
}
