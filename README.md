# Flink CDC 3.5 PostgreSQL Pipeline Sink

这是一个供 Flink CDC 3.5 Pipeline YAML 使用的自定义 PostgreSQL Sink。工程以 D 盘的 MySQL Sink 为接口参考，重新实现了 PostgreSQL 的 schema 映射、DDL、类型映射、`ON CONFLICT` UPSERT 和 JDBC 值转换。

示例场景是 OceanBase/MySQL Binlog Source 同步到 PostgreSQL，也可以接收其他能输出 Flink CDC `Event` 的 Pipeline Source。

## 能力与语义

- Factory identifier：`postgres`，YAML 中写 `sink.type: postgres`。
- PostgreSQL database 必须预先创建；一个作业连接一个目标 database。
- 默认把源 database/schema 映射为同名 PostgreSQL schema，可用 `schema-prefix` 加前缀，或用 `schema-name` 强制汇总到固定 schema。
- 自动 `CREATE SCHEMA IF NOT EXISTS`、`CREATE TABLE IF NOT EXISTS`。
- 原生承接 Flink CDC `route` 的多分表合并结果，并支持在 transform 中重建合并表主键。
- 通过 transform 的 `partition-keys` 为指定合并表创建 PostgreSQL HASH、LIST 或 RANGE 分区表及子分区。
- 支持 ADD COLUMN、RENAME COLUMN、ALTER COLUMN TYPE。
- DROP COLUMN、DROP TABLE、TRUNCATE 默认拒绝；仅在 `dangerous-ddl.enabled=true` 时执行。
- INSERT、UPDATE、REPLACE 使用主键 `INSERT ... ON CONFLICT ... DO UPDATE`。
- UPDATE 修改主键时会先按 before image 删除旧主键行，再写入新主键行。
- DELETE 按主键执行。
- 无主键表默认失败。长期 CDC 的 UPDATE/DELETE 和故障重放需要主键保证幂等。
- 收到 Flink CDC `FlushEvent` 或 checkpoint 时执行 batch + commit，确保 schema 变更前清空旧 statement。
- JDBC 暂时失败时保留内存事件并重新建连重试；同表 UPSERT/DELETE 的原始顺序会保留。

这不是 XA 2PC 的严格端到端 exactly-once。checkpoint 前会提交 JDBC 事务，故障恢复时可能重放；主键 UPSERT/DELETE 使重放结果幂等。

## 构建

推荐环境：JDK 11、Maven 3.8+、Flink 1.20.0、Flink CDC 3.5.0、PostgreSQL 12+。

```bash
./bin/build.sh
```

产物：

```text
target/flink-cdc-pipeline-connector-postgres-sink-3.5.0.jar
```

产物只 shade PostgreSQL JDBC Driver；Flink 和 Flink CDC API 使用 `provided`，避免运行时 classloader 冲突。

## 安装与提交

```bash
export FLINK_CDC_HOME=/home/hadoop/flink-cdc-3.5.0
./bin/prepare-runtime.sh   # 示例 OceanBase/MySQL source 的依赖
./bin/install.sh

mkdir -p "$FLINK_CDC_HOME/jobs"
cp config/oceanbase-to-postgres.yaml "$FLINK_CDC_HOME/jobs/"
./bin/submit-yarn.sh "$FLINK_CDC_HOME/jobs/oceanbase-to-postgres.yaml"
```

提交前应修改 YAML 中的源端地址、表正则、PostgreSQL 地址、database、凭据和 checkpoint 路径。

## Sink 配置

必要配置：

| 配置 | 说明 |
|---|---|
| `hostname` | PostgreSQL 主机；提供 `jdbc-url` 时可省略 |
| `port` | 默认 `5432` |
| `database-name` | 已存在的目标 database |
| `username` | JDBC 用户 |
| `password` | JDBC 密码，默认空 |

映射与运行配置：

| 配置 | 默认值 | 说明 |
|---|---:|---|
| `jdbc-url` | 自动生成 | 可用于 SSL、PgBouncer 或额外 JDBC 参数 |
| `create-schema.enabled` | `true` | 自动创建目标 schema |
| `schema-prefix` | 空 | 加在源 schema 前面 |
| `schema-name` | 空 | 非空时全部写入这个固定 schema，优先级高于 prefix |
| `default-schema` | `public` | 事件没有 schema/namespace 时使用 |
| `sink.buffer-flush.max-rows` | `1000` | 达到行数时提交 |
| `sink.buffer-flush.interval` | `2s` | 有后续事件到达时检查时间阈值；checkpoint 也会强制提交 |
| `sink.max-retries` | `3` | JDBC 事务失败后的重试次数 |
| `statement-cache.max-size` | `256` | 每个并行实例缓存的活跃表 statement 数 |
| `server-time-zone` | `Asia/Shanghai` | Flink local-zoned timestamp 转换时区 |
| `partition.default-strategy` | `hash` | 设置了 `partition-keys`、但未设置表级策略时使用 |
| `partition.default-count` | `16` | HASH 分区的默认子分区数，范围 1～1024 |
| `dangerous-ddl.enabled` | `false` | 是否允许 DROP/TRUNCATE |
| `no-primary-key.action` | `fail` | 当前只支持 `fail` |

若使用 `jdbc-url`，仍需填写 `database-name`，它用于显式声明目标 database；URL 中的 database 应与它一致。

## 分表合并

分表合并由 Flink CDC 3.5 的 `route` 和 `transform` 完成，Sink 接收到的是已经路由、合并并补齐字段后的目标表事件。因此无需打开额外的 Sink 开关。需要注意的是，不同分表中相同的原始主键会发生冲突，必须像现网 YAML 一样构造全局唯一键：

```yaml
route:
  - source-table: app_db.order_[0-9]+,app_db.order_history
    sink-table: target_db.order_m

transform:
  - source-table: app_db.order_[0-9]+,app_db.order_history
    projection: "*, __table_name__ || '_' || id AS _pk_id"
    primary-keys: _pk_id
```

Flink CDC 的 Schema Operator 会把多个源表的初始 Schema 合并为一个目标 `CreateTableEvent`，后续字段变化会转换为目标表的 Schema 变更事件；PG Sink 按路由后的 `target_db.order_m` 建表并 UPSERT。

## PostgreSQL 分区表

只有 transform 声明了 `partition-keys` 的表才会创建为分区表；未声明的合并表仍是普通 PostgreSQL 表。表级配置放在 `table-options` 中，并覆盖 Sink 的默认策略。

### HASH：适合大量分表合并后的均匀写入

```yaml
transform:
  - source-table: app_db.order_[0-9]+,app_db.order_history
    projection: "*, __table_name__ || '_' || id AS _pk_id"
    primary-keys: _pk_id
    partition-keys: _pk_id
    table-options: "postgres.partition.strategy=hash,postgres.partition.count=32"
```

Sink 会创建父表以及 32 个 HASH 子分区。分区键 `_pk_id` 已包含在主键中，满足 PostgreSQL 分区表唯一约束要求。

### LIST：适合按业务类型或来源分区

```yaml
transform:
  - source-table: app_db.outbound_[0-9]+
    projection: "*, 1 AS wh_type, __table_name__ || '_' || outbound_id AS _pk_id"
    primary-keys: wh_type,_pk_id
    partition-keys: wh_type
    table-options: "postgres.partition.strategy=list,postgres.partition.list.internal=1,postgres.partition.list.supplier=2,postgres.partition.list.third_party=3,postgres.partition.default=true"
```

- `postgres.partition.list.<分区名>=值1|值2` 定义一个 LIST 子分区。
- `postgres.partition.default=true` 创建 DEFAULT 子分区，接住未枚举值。
- 如果同一目标表由多条 transform 规则合并，所有规则必须给出一致的 `partition-keys` 和 `table-options`。
- 若按原始分表名分区，需要在 projection 中加入 `__table_name__ AS _source_table`，并把 `_source_table` 同时加入 `primary-keys` 与 `partition-keys`。

### RANGE：适合按日期区间分区

```yaml
transform:
  - source-table: app_db.order_[0-9]+
    projection: "*, __table_name__ || '_' || id AS _pk_id"
    primary-keys: created_at,_pk_id
    partition-keys: created_at
    table-options: "postgres.partition.strategy=range,postgres.partition.range.p2026_01=2026-01-01|2026-02-01,postgres.partition.range.p2026_02=2026-02-01|2026-03-01,postgres.partition.default=true"
```

- `postgres.partition.range.<分区名>=FROM|TO` 定义一个左闭右开的 RANGE 子分区。
- 边界支持 `MINVALUE` 和 `MAXVALUE`。
- LIST/RANGE 分区在建表阶段按 YAML 静态创建；运行时不会在数据写入线程中执行逐行建分区 DDL。

PostgreSQL 要求分区表的 PRIMARY KEY/UNIQUE 约束包含全部分区键。插件会在建表前校验，不满足时直接报出目标表和缺失分区键，避免作业运行到 JDBC DDL 才得到模糊错误。完整示例见 `config/mysql-shards-to-postgres-partitioned.yaml`。

## 目标端权限

database 需要由管理员先创建，例如：

```sql
CREATE ROLE flink_cdc LOGIN PASSWORD 'StrongPassword';
CREATE DATABASE cdc_target OWNER flink_cdc;
```

如果 database 已有独立 owner，可按实际 schema 授予 `USAGE, CREATE`，并授予表的 `SELECT, INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER`。保持 `dangerous-ddl.enabled=false` 时可以进一步收紧 DROP 相关治理流程。

## 类型映射

- TINYINT/SMALLINT -> SMALLINT
- INT/BIGINT -> INTEGER/BIGINT
- FLOAT/DOUBLE -> REAL/DOUBLE PRECISION
- DECIMAL -> NUMERIC
- CHAR/VARCHAR -> CHARACTER/VARCHAR；超长字符串 -> TEXT
- BINARY/VARBINARY -> BYTEA
- DATE/TIME -> DATE/TIME WITHOUT TIME ZONE
- TIMESTAMP -> TIMESTAMP WITHOUT TIME ZONE
- TIMESTAMP_LTZ/TIMESTAMP_TZ -> TIMESTAMP WITH TIME ZONE
- ARRAY/MAP/ROW 会显式失败，不做静默字符串化

## 生产注意事项

1. 源表应有主键；目标 UPSERT 的冲突键就是复制出的主键。
2. PostgreSQL 不能通过当前 database 连接透明创建并写入另一个 database，所以本插件把多源库映射成一个 database 下的多个 schema。
3. 不复制二级索引、唯一索引、外键、触发器、identity/sequence 和源端 DEFAULT 表达式。主键、表注释和字段注释会复制。
4. 已有数据的表新增 `NOT NULL` 字段时，如果源 DDL 依赖 DEFAULT 回填，目标 DDL 可能失败。应先在目标端实施兼容变更，或把源变更拆成“可空字段 -> 回填 -> 非空”。
5. ALTER TYPE 使用 PostgreSQL 显式 cast；不兼容或有脏数据时会失败，不会静默截断。
6. 首次同步大量表时，可从并行度 8 开始，结合 source、TaskManager 与 PostgreSQL 写入压力调整。
7. `reWriteBatchedInserts=true` 已写入默认 JDBC URL。自定义 `jdbc-url` 时建议保留该参数。
8. 已存在的普通表不能通过 `CREATE TABLE IF NOT EXISTS` 原地变成分区表。首次启用分区前，应迁移或重建目标表，并从受控的 savepoint/checkpoint 启动作业。
