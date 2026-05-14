# ETL - MySQL 数据库同步服务

基于 Spring Boot 的 MySQL → MySQL 数据同步工具，通过 REST API 提交同步任务，支持增量同步和全量覆盖。

## 技术栈

- Java 21 + Spring Boot 3.2.5
- MySQL Connector/J 8.0.33
- HikariCP 连接池
- Lombok

## 功能特性

- **多表同步**：一次请求可同步多张表
- **增量同步**：基于时间戳 + 自增 ID 的游标分页，支持断点续传
- **全量覆盖**：先清空目标表再全量写入，支持 TRUNCATE 或 DELETE 两种清空模式
- **多种写入模式**：
  - `upsert` — INSERT ON DUPLICATE KEY UPDATE
  - `delete_insert` — 先删后插
  - `multi_values_upsert` — 多值 UPSERT（高性能）
  - `full_refresh_insert` — 全量覆盖插入
- **列映射**：支持源列与目标列的自定义映射、常量值注入
- **数据过滤**：支持按列值过滤源数据
- **删除标记**：支持通过操作类型列或删除标记列识别已删除数据
- **断点管理**：自动维护同步检查点，支持查询和重置
- **高性能批量写入**：多值 INSERT SQL，不依赖 JDBC 参数即可实现批量写入优化

## 快速开始

### 构建

```bash
./mvnw clean package -DskipTests
```

### 运行

```bash
java -jar target/etl-0.0.1-SNAPSHOT.war
```

默认端口 `8080`，可在 `application.yaml` 中修改。

## API 接口

### 1. 执行同步

```
POST /api/db-sync/run
```

请求体示例：

```json
{
  "sourceDataSourceConfig": {
    "sourceName": "source-db",
    "jdbcUrl": "jdbc:mysql://source-host:3306/source_db?useSSL=false&characterEncoding=utf8mb4",
    "jdbcUsername": "root",
    "jdbcPassword": "password"
  },
  "targetDataSourceConfig": {
    "sourceName": "target-db",
    "jdbcUrl": "jdbc:mysql://target-host:3306/target_db?useSSL=false&characterEncoding=utf8mb4",
    "jdbcUsername": "root",
    "jdbcPassword": "password"
  },
  "batchSize": 2000,
  "continueOnError": false,
  "tableConfigs": [
    {
      "syncKey": "student-sync",
      "sourceMode": "table",
      "writeMode": "upsert",
      "sourceTable": "tbl_student",
      "targetTable": "IN_STUDENT",
      "cursorIdColumn": "id",
      "syncTimeColumn": "update_time",
      "targetKeyColumn": "student_code",
      "columnMappings": [
        { "sourceColumn": "student_code", "targetColumn": "student_code" },
        { "sourceColumn": "student_name", "targetColumn": "student_name" }
      ]
    },
    {
      "syncKey": "teacher-full-refresh",
      "sourceMode": "sql",
      "writeMode": "full_refresh_insert",
      "fullRefreshDeleteMode": "truncate",
      "sourceSql": "SELECT teacher_code, teacher_name FROM tbl_teacher WHERE status = 1",
      "targetTable": "IN_TEACHER",
      "cursorIdColumn": "id",
      "syncTimeColumn": "create_time",
      "columnMappings": [
        { "sourceColumn": "teacher_code", "targetColumn": "teacher_code" },
        { "sourceColumn": "teacher_name", "targetColumn": "teacher_name" }
      ]
    }
  ]
}
```

### 2. 查询断点

```
POST /api/db-sync/checkpoints/query
```

请求体示例：

```json
{
  "targetDataSourceConfig": {
    "jdbcUrl": "jdbc:mysql://target-host:3306/target_db?useSSL=false&characterEncoding=utf8mb4",
    "jdbcUsername": "root",
    "jdbcPassword": "password"
  },
  "syncKey": "student-sync"
}
```

### 3. 重置断点

```
POST /api/db-sync/checkpoints/reset
```

请求体示例：

```json
{
  "targetDataSourceConfig": {
    "jdbcUrl": "jdbc:mysql://target-host:3306/target_db?useSSL=false&characterEncoding=utf8mb4",
    "jdbcUsername": "root",
    "jdbcPassword": "password"
  },
  "syncKeys": ["student-sync", "teacher-full-refresh"]
}
```

## 核心配置说明

### 数据源配置（DirectDataSourceConfigDTO）

| 字段 | 说明 |
|---|---|
| `sourceName` | 数据源名称（用于日志标识） |
| `jdbcUrl` | JDBC 连接地址 |
| `jdbcDriver` | 驱动类名（默认 `com.mysql.cj.jdbc.Driver`） |
| `jdbcUsername` | 用户名 |
| `jdbcPassword` | 密码 |
| `connectionPoolSize` | 连接池大小（默认 10） |

### 表同步配置（DbSyncTableConfigDTO）

| 字段 | 说明 | 可选值 |
|---|---|---|
| `syncKey` | 同步任务唯一标识 | — |
| `sourceMode` | 数据源模式 | `table`（按表名）/ `sql`（自定义 SQL） |
| `writeMode` | 写入模式 | `upsert` / `delete_insert` / `multi_values_upsert` / `full_refresh_insert` |
| `fullRefreshDeleteMode` | 全量清空方式 | `truncate` / `delete` |
| `sourceTable` | 源表名（sourceMode=table 时） | — |
| `sourceSql` | 源查询 SQL（sourceMode=sql 时） | — |
| `targetTable` | 目标表名 | — |
| `cursorIdColumn` | 游标 ID 列（用于增量分页） | — |
| `syncTimeColumn` | 同步时间列 | — |
| `syncTimeExpression` | 同步时间表达式（覆盖 syncTimeColumn） | 如 `COALESCE(update_time, create_time)` |
| `targetKeyColumn` | 目标表主键列（单列） | — |
| `targetKeyColumns` | 目标表主键列（多列复合主键） | — |
| `batchSize` | 批次大小（默认 2000） | — |
| `columnMappings` | 列映射列表 | — |
| `filters` | 数据过滤条件 | — |
| `deleteRule` | 删除标记规则 | — |

### 列映射（DbSyncColumnMappingDTO）

| 字段 | 说明 |
|---|---|
| `sourceColumn` | 源列名 |
| `targetColumn` | 目标列名 |
| `required` | 是否必填（为空时跳过该行） |
| `maxLength` | 最大长度（超长截断） |
| `constantValue` | 常量值（不读源列，直接写入固定值） |

### 删除标记规则（DbSyncDeleteRuleDTO）

| 字段 | 说明 |
|---|---|
| `operationTypeColumn` | 操作类型列名 |
| `deleteOperationValue` | 删除操作对应的值 |
| `deleteFlagColumn` | 删除标记列名 |
| `deleteFlagValue` | 删除标记值 |

## 项目结构

```
src/main/java/com/xy/etl/
├── controller/          # REST 接口
├── dto/                 # 请求/响应数据传输对象
├── manager/             # 数据源连接池管理
├── service/             # 同步服务入口
│   └── impl/
├── sync/                # 同步核心逻辑
│   ├── checkpoint/      # 断点存储与恢复
│   ├── config/          # 配置解析
│   ├── datasource/      # 数据源解析
│   ├── executor/        # 表同步执行器
│   ├── model/           # 内部模型
│   ├── reader/          # 源数据批量读取
│   ├── support/         # 常量与工具
│   └── writer/          # 目标数据写入
│       └── strategy/    # 写入策略（upsert/delete_insert/...）
```
