# SIMPLE-ETL

基于 Spring Boot 的 `MySQL -> MySQL` 数据同步服务，支持通过 Web API 触发，也支持通过 YAML 配置文件直接以 CLI 模式运行。

HTTP API 调用说明见：[HTTP_API.md](D:/xyproject/etl/docs/HTTP_API.md:1)

## 功能概览

- 支持 `sourceMode=table` 和 `sourceMode=sql`
- 支持 4 种 `write_mode`
  - `upsert`
  - `delete_insert`
  - `multi_values_upsert`
  - `full_refresh_insert`
- 支持断点续传
- 支持多表同步
- 支持列映射、常量值、过滤条件、删除标记
- 支持通过配置文件直接运行同步任务

## 运行方式

### 1. Web 模式

直接启动应用：

```bash
java -jar target/etl-0.0.1-SNAPSHOT.war
```

或者在 IDEA 中直接运行 `com.xy.etl.EtlApplication`。

### 2. CLI 模式

当前 CLI 模式只支持 `-Dconfig=...`，不支持 `-c`。

单个配置文件：

```bash
java -Dconfig=D:\xyproject\etl\src\main\resources\selfconfig\your-config.yaml -jar target/etl-0.0.1-SNAPSHOT.war
```

多个配置文件：

```bash
java -Dconfig=config1.yaml,config2.yaml -jar target/etl-0.0.1-SNAPSHOT.war
```

多个配置文件不会合并，而是按书写顺序依次执行：

1. 先执行 `config1.yaml`
2. 再执行 `config2.yaml`
3. 如果前一个配置执行失败，后一个不会继续执行

CLI 模式下：

- 应用不会启动 Web 容器
- 同步执行完成后进程会自动退出
- 失败时会返回非 `0` 退出码

### 3. IDEA 启动 CLI 模式

在 IDEA 的运行配置里：

- `Main class` 选择 `com.xy.etl.EtlApplication`
- `VM options` 填：

```text
-Dconfig=$PROJECT_DIR$\src\main\resources\selfconfig\your-config.yaml
```

如果要顺序执行多个配置文件，也写在 `VM options` 里：

```text
-Dconfig=$PROJECT_DIR$\src\main\resources\selfconfig\config1.yaml,$PROJECT_DIR$\src\main\resources\selfconfig\config2.yaml
```

- `Program arguments` 留空

注意：`-Dconfig=...` 必须放在 `VM options`，不能放在 `Program arguments`。

## 配置文件位置

仓库中公开保留的是示例配置，位于 `src/main/resources/examples/`：

- [source-table-upsert.example.yaml](D:/xyproject/etl/src/main/resources/examples/source-table-upsert.example.yaml:1)
- [source-table-delete-insert.example.yaml](D:/xyproject/etl/src/main/resources/examples/source-table-delete-insert.example.yaml:1)
- [source-table-multi-values-upsert.example.yaml](D:/xyproject/etl/src/main/resources/examples/source-table-multi-values-upsert.example.yaml:1)
- [source-table-full-refresh-insert.example.yaml](D:/xyproject/etl/src/main/resources/examples/source-table-full-refresh-insert.example.yaml:1)
- [source-sql-upsert.example.yaml](D:/xyproject/etl/src/main/resources/examples/source-sql-upsert.example.yaml:1)
- [source-sql-delete-insert.example.yaml](D:/xyproject/etl/src/main/resources/examples/source-sql-delete-insert.example.yaml:1)
- [source-sql-multi-values-upsert.example.yaml](D:/xyproject/etl/src/main/resources/examples/source-sql-multi-values-upsert.example.yaml:1)
- [source-sql-full-refresh-insert.example.yaml](D:/xyproject/etl/src/main/resources/examples/source-sql-full-refresh-insert.example.yaml:1)

学校私有配置请放在 `src/main/resources/selfconfig/` 或你自己的私有目录中，不纳入仓库。

## 配置文件结构

配置文件主要包含 4 段：

```yaml
source:
target:
options:
tables:
```

### source / target

可以写 `host + port + username + password + database`，也可以直接写 `jdbcUrl`。

示例：

```yaml
source:
  host: "127.0.0.1"
  port: 3306
  username: "root"
  password: "your_password"
  database: "source_db"
  charset: "utf8mb4"
```

说明：

- `charset: utf8mb4` 现在是支持的
- 代码会自动把 JDBC `characterEncoding` 规范化为驱动可识别的值

### options

常见字段：

```yaml
options:
  batch_size: 1000
  truncate_before_load: true
  continue_on_error: false
  checkpoint_table: "edu_db_sync_checkpoint"
  auto_create_checkpoint_table: true
```

### tables

每个表配置支持这些关键字段：

| 字段 | 说明 |
|---|---|
| `name` | 表配置名称 |
| `sync_key` | 同步任务标识，可选 |
| `source_mode` | `table` 或 `sql` |
| `write_mode` | `upsert` / `delete_insert` / `multi_values_upsert` / `full_refresh_insert` |
| `source_table` | 表模式下的源表 |
| `source_sql` | SQL 模式下的查询 SQL |
| `target_table` | 目标表 |
| `cursor_id_column` | 游标列 |
| `sync_time_column` | 同步时间列 |
| `fallback_sync_time_column` | 备选时间列，可选 |
| `sync_time_expression` | 时间表达式，可选 |
| `target_key_column` | 单主键模式使用 |
| `target_key_columns` | 复合主键模式使用 |
| `column_mappings` | 列映射 |
| `filters` | 过滤条件 |
| `delete_rule` | 删除标记规则 |

## 模式约束

### sourceMode=table

- 使用 `source_table`
- 支持 `filters`
- 不需要自己写 SQL

### sourceMode=sql

- 使用 `source_sql`
- 不支持 `filters`
- `source_sql` 的结果里必须真实包含：
  - `cursor_id_column`
  - `sync_time_column`
  - 如果配置了 `fallback_sync_time_column`，也要能取到

也就是说，下面这种配置只写 YAML 还不够：

```yaml
cursor_id_column: "id"
sync_time_column: "update_time"
```

你还必须确保 `source_sql` 真实查询出了 `id` 和 `update_time`。

### write_mode=upsert

- 要求 `target_key_column`
- `target_key_column` 必须出现在 `column_mappings.target_column` 里

### write_mode=multi_values_upsert

- 约束和 `upsert` 一样
- 区别主要在批量写入 SQL 的拼接方式

### write_mode=delete_insert

- 必须使用 `target_key_columns`
- 不能只写 `target_key_column`
- `target_key_columns` 中每个字段都必须出现在 `column_mappings.target_column` 里

### write_mode=full_refresh_insert

- 会走全量刷新路径
- 依然要求：
  - `cursor_id_column`
  - `sync_time_column`
- 如果想在写入前清空目标表，可以配置：

```yaml
truncate_before_load: true
```

## 列映射说明

示例：

```yaml
column_mappings:
  - source_column: "student_code"
    target_column: "student_code"
    required: true
    max_length: 100
  - target_column: "is_active"
    constant_value: 1
```

规则：

- `source_column` 和 `constant_value` 二选一
- 不能同时写
- `required: true` 会做非空校验
- `max_length` 会做长度校验

## 常见问题

### 1. `Unsupported character encoding 'utf8mb4'`

这个问题已经在代码里做了兼容处理。

如果你配置的是：

```yaml
charset: utf8mb4
```

不需要再手动改成别的值。

### 2. `cursor id is null`

通常原因是：

- 配置了 `cursor_id_column`
- 但 `source_sql` 没有实际查询出这个字段

需要检查 `source_sql` 的 `SELECT` 列表。

### 3. `Unknown column ...`

通常是以下几种情况：

- `source_sql` 没有输出 `cursor_id_column`
- `source_sql` 没有输出 `sync_time_column`
- `source_sql` 没有输出 `fallback_sync_time_column`
- `column_mappings.source_column` 写的列在结果里不存在

### 4. `columnMappings must include target key column`

说明：

- 你配置了 `target_key_column` 或 `target_key_columns`
- 但这些目标键字段没有出现在 `column_mappings.target_column` 里

## 代码结构

当前主要结构：

```text
src/main/java/com/xy/etl/
├── controller
├── dto
├── service
│   └── impl
├── sync
│   ├── checkpoint
│   ├── config
│   ├── datasource
│   ├── executor
│   ├── model
│   ├── reader
│   ├── support
│   └── writer
│       └── strategy
└── cli
    ├── config
    ├── exception
    ├── logging
    ├── model
    ├── runner
    └── support
```

## 构建

Maven：

```bash
mvn clean package -DskipTests
```

打包后默认产物：

```text
target/etl-0.0.1-SNAPSHOT.war
```
