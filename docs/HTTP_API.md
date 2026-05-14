# HTTP API 调用说明

本文档只说明 Web 接口调用方式，不涉及学校私有 YAML 配置。

## 基础信息

- 基础路径：`/api/db-sync`
- 默认本地地址：`http://127.0.0.1:8080`
- Content-Type：`application/json`

当前提供 3 个接口：

- `POST /api/db-sync/run`
- `POST /api/db-sync/checkpoints/query`
- `POST /api/db-sync/checkpoints/reset`

## 1. 执行同步

### 接口

```http
POST /api/db-sync/run
Content-Type: application/json
```

### 请求体字段

顶层字段：

| 字段 | 必填 | 说明 |
|---|---|---|
| `sourceDataSourceId` | 否 | 源数据源 ID |
| `sourceDataSourceConfig` | 否 | 源数据源直连配置 |
| `targetDataSourceId` | 否 | 目标数据源 ID |
| `targetDataSourceConfig` | 否 | 目标数据源直连配置 |
| `checkpointTable` | 否 | 断点表名 |
| `autoCreateCheckpointTable` | 否 | 是否自动建断点表 |
| `batchSize` | 否 | 默认批大小 |
| `continueOnError` | 否 | 单表失败后是否继续 |
| `tableConfigs` | 是 | 表同步配置列表 |

说明：

- 源端必须提供 `sourceDataSourceId` 或 `sourceDataSourceConfig` 其中之一
- 目标端必须提供 `targetDataSourceId` 或 `targetDataSourceConfig` 其中之一
- 当前项目实际最常用的是 `sourceDataSourceConfig + targetDataSourceConfig`

### 数据源直连配置

字段：

| 字段 | 必填 | 说明 |
|---|---|---|
| `sourceName` | 否 | 仅用于日志标识 |
| `jdbcUrl` | 是 | JDBC 地址 |
| `jdbcDriver` | 否 | 驱动类名，通常可不填 |
| `jdbcUsername` | 是 | 用户名 |
| `jdbcPassword` | 是 | 密码 |
| `connectionPoolSize` | 否 | 连接池大小 |

### 表配置字段

| 字段 | 必填 | 说明 |
|---|---|---|
| `syncKey` | 否 | 同步标识，不填会自动生成 |
| `sourceMode` | 否 | `table` 或 `sql`，不填时自动推断 |
| `writeMode` | 否 | `upsert` / `delete_insert` / `multi_values_upsert` / `full_refresh_insert` |
| `fullRefreshDeleteMode` | 否 | `delete` 或 `truncate` |
| `sourceTable` | 表模式必填 | 源表名 |
| `sourceSql` | SQL 模式必填 | 源 SQL |
| `targetTable` | 是 | 目标表名 |
| `cursorIdColumn` | 是 | 游标列 |
| `syncTimeColumn` | 是 | 同步时间列 |
| `syncTimeExpression` | 否 | 自定义同步时间表达式 |
| `fallbackSyncTimeColumn` | 否 | 备选同步时间列 |
| `targetKeyColumn` | 部分模式必填 | 单列目标键 |
| `targetKeyColumns` | `delete_insert` 必填 | 复合目标键 |
| `targetKeySourceColumn` | 否 | 目标键对应的源列 |
| `batchSize` | 否 | 表级批大小 |
| `columnMappings` | 是 | 列映射 |
| `filters` | 否 | 仅 table 模式支持 |
| `deleteRule` | 否 | 删除标记规则 |

### 常见模式约束

#### `sourceMode=table`

- 使用 `sourceTable`
- 支持 `filters`

#### `sourceMode=sql`

- 使用 `sourceSql`
- 不支持 `filters`
- `sourceSql` 结果中必须真实包含：
  - `cursorIdColumn`
  - `syncTimeColumn`
  - 如果配置了 `fallbackSyncTimeColumn`，也要能查到

#### `writeMode=upsert`

- 需要 `targetKeyColumn`
- `targetKeyColumn` 必须出现在 `columnMappings.targetColumn` 中

#### `writeMode=multi_values_upsert`

- 约束和 `upsert` 一样

#### `writeMode=delete_insert`

- 必须配置 `targetKeyColumns`
- 不能只写 `targetKeyColumn`
- `targetKeyColumns` 每一项都必须出现在 `columnMappings.targetColumn`

#### `writeMode=full_refresh_insert`

- 仍然要求 `cursorIdColumn` 和 `syncTimeColumn`
- 如果需要先清空目标表，可配：
  - `fullRefreshDeleteMode=truncate`
  - 或 `fullRefreshDeleteMode=delete`

### 请求示例 1：table + upsert

```json
{
  "sourceDataSourceConfig": {
    "sourceName": "source-db",
    "jdbcUrl": "jdbc:mysql://127.0.0.1:3306/source_db?useSSL=false&useUnicode=true&characterEncoding=UTF-8",
    "jdbcUsername": "root",
    "jdbcPassword": "your_password"
  },
  "targetDataSourceConfig": {
    "sourceName": "target-db",
    "jdbcUrl": "jdbc:mysql://127.0.0.1:3306/target_db?useSSL=false&useUnicode=true&characterEncoding=UTF-8",
    "jdbcUsername": "root",
    "jdbcPassword": "your_password"
  },
  "checkpointTable": "edu_db_sync_checkpoint",
  "autoCreateCheckpointTable": true,
  "batchSize": 1000,
  "continueOnError": false,
  "tableConfigs": [
    {
      "syncKey": "order-upsert-demo",
      "sourceMode": "table",
      "writeMode": "upsert",
      "sourceTable": "source_order",
      "targetTable": "target_order",
      "cursorIdColumn": "id",
      "syncTimeColumn": "update_time",
      "fallbackSyncTimeColumn": "create_time",
      "targetKeyColumn": "order_id",
      "columnMappings": [
        {
          "sourceColumn": "id",
          "targetColumn": "order_id",
          "required": true
        },
        {
          "sourceColumn": "user_id",
          "targetColumn": "user_id",
          "required": true
        },
        {
          "sourceColumn": "status",
          "targetColumn": "status"
        },
        {
          "sourceColumn": "amount",
          "targetColumn": "amount"
        }
      ],
      "filters": [
        {
          "column": "status",
          "value": "PAID"
        }
      ]
    }
  ]
}
```

### 请求示例 2：sql + full_refresh_insert

```json
{
  "sourceDataSourceConfig": {
    "sourceName": "source-db",
    "jdbcUrl": "jdbc:mysql://127.0.0.1:3306/source_db?useSSL=false&useUnicode=true&characterEncoding=UTF-8",
    "jdbcUsername": "root",
    "jdbcPassword": "your_password"
  },
  "targetDataSourceConfig": {
    "sourceName": "target-db",
    "jdbcUrl": "jdbc:mysql://127.0.0.1:3306/target_db?useSSL=false&useUnicode=true&characterEncoding=UTF-8",
    "jdbcUsername": "root",
    "jdbcPassword": "your_password"
  },
  "checkpointTable": "edu_db_sync_checkpoint",
  "autoCreateCheckpointTable": true,
  "batchSize": 1000,
  "continueOnError": false,
  "tableConfigs": [
    {
      "syncKey": "organization-full-refresh-demo",
      "sourceMode": "sql",
      "writeMode": "full_refresh_insert",
      "fullRefreshDeleteMode": "truncate",
      "sourceSql": "SELECT MIN(id) AS id, MAX(update_time) AS update_time, MAX(create_time) AS create_time, org_code, MAX(org_name) AS org_name FROM source_organization GROUP BY org_code",
      "targetTable": "target_organization",
      "cursorIdColumn": "id",
      "syncTimeColumn": "update_time",
      "fallbackSyncTimeColumn": "create_time",
      "columnMappings": [
        {
          "sourceColumn": "org_code",
          "targetColumn": "org_code",
          "required": true
        },
        {
          "sourceColumn": "org_name",
          "targetColumn": "org_name",
          "required": true
        }
      ]
    }
  ]
}
```

### 返回示例

```json
{
  "success": true,
  "startTime": "2026-05-14T10:00:00",
  "endTime": "2026-05-14T10:00:08",
  "totalProcessedCount": 1200,
  "totalUpsertedCount": 1195,
  "totalDeletedCount": 5,
  "tableResults": [
    {
      "syncKey": "order-upsert-demo",
      "sourceTable": "source_order",
      "targetTable": "target_order",
      "success": true,
      "errorMessage": null,
      "processedCount": 1200,
      "upsertedCount": 1195,
      "deletedCount": 5,
      "startSyncTime": "2026-05-13 00:00:00",
      "startSyncId": 0,
      "endSyncTime": "2026-05-14 09:59:59",
      "endSyncId": 1200
    }
  ]
}
```

## 2. 查询断点

### 接口

```http
POST /api/db-sync/checkpoints/query
Content-Type: application/json
```

### 请求体

| 字段 | 必填 | 说明 |
|---|---|---|
| `targetDataSourceId` | 否 | 目标数据源 ID |
| `targetDataSourceConfig` | 否 | 目标数据源直连配置 |
| `syncKey` | 否 | 指定同步标识；不传通常表示查全部 |

说明：

- 目标端必须提供 `targetDataSourceId` 或 `targetDataSourceConfig`

### 请求示例

```json
{
  "targetDataSourceConfig": {
    "sourceName": "target-db",
    "jdbcUrl": "jdbc:mysql://127.0.0.1:3306/target_db?useSSL=false&useUnicode=true&characterEncoding=UTF-8",
    "jdbcUsername": "root",
    "jdbcPassword": "your_password"
  },
  "syncKey": "order-upsert-demo"
}
```

### 返回示例

```json
[
  {
    "id": 1,
    "syncKey": "order-upsert-demo",
    "sourceTable": "source_order",
    "targetTable": "target_order",
    "lastSyncTime": "2026-05-14 09:59:59",
    "lastSyncId": 1200,
    "createTime": "2026-05-14T10:00:00",
    "updateTime": "2026-05-14T10:00:08"
  }
]
```

## 3. 重置断点

### 接口

```http
POST /api/db-sync/checkpoints/reset
Content-Type: application/json
```

### 请求体

| 字段 | 必填 | 说明 |
|---|---|---|
| `targetDataSourceId` | 否 | 目标数据源 ID |
| `targetDataSourceConfig` | 否 | 目标数据源直连配置 |
| `syncKeys` | 否 | 需要重置的同步标识列表 |

说明：

- 目标端必须提供 `targetDataSourceId` 或 `targetDataSourceConfig`
- `syncKeys` 为空时，具体行为以服务实现为准，建议显式传值

### 请求示例

```json
{
  "targetDataSourceConfig": {
    "sourceName": "target-db",
    "jdbcUrl": "jdbc:mysql://127.0.0.1:3306/target_db?useSSL=false&useUnicode=true&characterEncoding=UTF-8",
    "jdbcUsername": "root",
    "jdbcPassword": "your_password"
  },
  "syncKeys": [
    "order-upsert-demo",
    "organization-full-refresh-demo"
  ]
}
```

### 返回示例

```json
2
```

表示成功重置了 2 条断点记录。

## cURL 示例

### 执行同步

```bash
curl -X POST "http://127.0.0.1:8080/api/db-sync/run" ^
  -H "Content-Type: application/json" ^
  -d "{...}"
```

### 查询断点

```bash
curl -X POST "http://127.0.0.1:8080/api/db-sync/checkpoints/query" ^
  -H "Content-Type: application/json" ^
  -d "{...}"
```

### 重置断点

```bash
curl -X POST "http://127.0.0.1:8080/api/db-sync/checkpoints/reset" ^
  -H "Content-Type: application/json" ^
  -d "{...}"
```

## 常见问题

### 1. SQL 模式为什么还是要求 `cursorIdColumn` 和 `syncTimeColumn`

因为当前实现里，SQL 模式也是按“时间 + 游标”做分页读取。  
这两个字段不只是校验需要，运行时的分页 SQL 也会直接引用。

### 2. 为什么 SQL 模式不能传 `filters`

因为 SQL 模式下查询语句完全由 `sourceSql` 决定，当前实现不会再额外拼接 `filters`。

### 3. 什么时候用 HTTP，什么时候用 CLI

- HTTP：适合外部系统调用、调试接口、由平台触发同步
- CLI：适合固定配置文件批量执行、脚本调度、学校私有配置本地运行
