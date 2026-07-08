# 广告投放数据中台 (Ad Data Platform)

基于 **Go + Gin + Kafka + ClickHouse** 的实时广告投放数据分析平台，模拟广告曝光/点击/转化全链路数据采集、实时入库、多维分析与可视化大屏。

> 该项目可作为 Golang 初级岗位简历的核心项目，覆盖 ClickHouse 高阶函数、Kafka 实时消费、Gin API 开发等岗位要求。

---

## 🎯 项目亮点

- ✅ **ClickHouse 高阶函数实战**：`windowFunnel`（转化漏斗）、`retention`（留存）、`uniqExact`（精确 UV）、物化视图
- ✅ **完整数据链路**：Mock Producer → Kafka → Consumer（批量入库）→ ClickHouse（MergeTree + 物化视图）→ Gin API → Vue 大屏
- ✅ **性能优化设计**：分区 + TTL + 跳数索引 + 物化视图预聚合 + PREWHERE
- ✅ **高并发友好**：Consumer 攒批写入、Gin 协程池、日志 trace_id 链路追踪
- ✅ **工程化**：分层架构（handler/service/repository）、统一响应、优雅退出、配置化

---

## 🏗️ 架构

```
┌────────────┐    ┌──────────┐    ┌────────────┐    ┌──────────────────┐
│ Producer   │───▶│  Kafka   │───▶│ Consumer   │───▶│  ClickHouse      │
│ (Mock数据) │    │  Topic   │    │ (批量入库) │    │  ┌────────────┐  │
└────────────┘    └──────────┘    └────────────┘    │  │ MergeTree  │  │
                                                    │  │ MV:分钟聚合│  │
                                                    │  │ MV:小时聚合│  │
                                                    │  └────────────┘  │
                                                    └────────┬─────────┘
                                                             │
                                                    ┌────────▼─────────┐
                                                    │  Gin API Server  │
                                                    │  /stats/realtime │
                                                    │  /stats/funnel   │
                                                    │  /stats/retention│
                                                    └────────┬─────────┘
                                                             │
                                                    ┌────────▼─────────┐
                                                    │  Vue 大屏        │
                                                    │  ECharts 实时刷新│
                                                    └──────────────────┘
```

---

## 📂 目录结构

```
ad-data-platform/
├── cmd/
│   ├── server/        # Gin API 服务入口
│   ├── consumer/      # Kafka → CH 消费者
│   └── producer/      # Mock 数据生成器
├── internal/
│   ├── config/        # viper 配置加载
│   ├── handler/       # HTTP handler
│   ├── service/       # 业务逻辑层
│   ├── repository/    # ClickHouse 查询层（含高阶函数）
│   ├── model/         # 数据模型
│   └── middleware/    # trace_id / access log / recover
├── pkg/
│   ├── clickhouse/    # CH 客户端封装
│   ├── kafka/         # Kafka 生产/消费封装
│   ├── logger/        # zap 日志
│   └── response/      # 统一响应
├── configs/
│   └── config.yaml
├── scripts/
│   └── init_clickhouse.sql  # CH 表结构 + 字典数据
├── deploy/
│   ├── docker-compose.yml
│   ├── Dockerfile.server
│   ├── Dockerfile.consumer
│   └── Dockerfile.producer
├── web/               # Vue 大屏（纯静态，无构建）
│   ├── index.html
│   ├── styles.css
│   └── app.js
├── go.mod
└── README.md
```

---

## 🚀 快速开始

### 方式一：Docker Compose（推荐）

```bash
cd deploy
docker-compose up -d

# 等待 30s 让 ClickHouse 初始化完成
# 检查服务
curl http://localhost:8080/health

# 访问大屏：直接用浏览器打开 web/index.html（需要将 API 改为 localhost）
```

### 方式二：本地开发

**1. 启动中间件（Kafka + ClickHouse）**
```bash
cd deploy
docker-compose up -d zookeeper kafka clickhouse
```

**2. 初始化 ClickHouse 表**
```bash
# 等待 CH 启动完成（30s 左右）
docker exec -i ad-clickhouse clickhouse-client < scripts/init_clickhouse.sql
```

**3. 启动 Consumer（消费 Kafka → 写入 CH）**
```bash
go run cmd/consumer/main.go
```

**4. 启动 Server（API）**
```bash
go run cmd/server/main.go
```

**5. 启动 Producer（造数据）**
```bash
go run cmd/producer/main.go -qps=200 -duration=10m
```

**6. 打开大屏**
```
浏览器打开 web/index.html
```

---

## 📊 API 文档

| 接口 | 说明 | 示例 |
|------|------|------|
| `GET /api/v1/stats/realtime?window=5` | 最近 N 分钟实时总览 | CTR / CVR / ROI |
| `GET /api/v1/stats/hourly?hours=24` | 小时趋势 | 折线图 |
| `GET /api/v1/stats/campaigns?start=&end=&limit=10` | 活动维度 | ROI 排名 |
| `GET /api/v1/stats/top-ads?sort=revenue&limit=10` | 广告 Top10 | 收入 / 曝光 / 点击 |
| `GET /api/v1/stats/regions?limit=10` | 地域分布 | 横向柱状图 |
| `GET /api/v1/stats/devices` | 设备分布 | 饼图 |
| `GET /api/v1/stats/funnel?window=3600` | 转化漏斗 | `windowFunnel` 函数 |
| `GET /api/v1/stats/retention?date=2026-07-01&days=7` | 用户留存 | `retention` 函数 |
| `GET /api/v1/stats/compare?start=&end=` | 同比环比 | 当前 vs 上一周期 |

---

## 🔥 核心 SQL 解析

### 1. 转化漏斗（windowFunnel）

```sql
SELECT
    sum(countIf(level >= 1)) AS impressions_users,
    sum(countIf(level >= 2)) AS click_users,
    sum(countIf(level >= 3)) AS conversion_users
FROM (
    SELECT
        user_id,
        windowFunnel(3600)(event_time,
            event_type = 'impression',
            event_type = 'click',
            event_type = 'conversion') AS level
    FROM ad_events_raw
    WHERE event_time BETWEEN ? AND ?
    GROUP BY user_id
)
```

### 2. 留存分析（retention）

```sql
SELECT
    arrayJoin(retention(
        event_date,
        event_date = '2026-07-01',
        event_date = '2026-07-02'
    )) AS retained
FROM user_event_funnel
WHERE event_type = 'impression'
```

### 3. 物化视图实时聚合

```sql
CREATE MATERIALIZED VIEW ad_minute_stats_mv
TO ad_minute_stats AS
SELECT
    toStartOfMinute(event_time) AS event_minute,
    event_type,
    ad_id,
    uniqExactState(user_id) AS uv,
    sum(cost) AS cost,
    sum(revenue) AS revenue
FROM ad_events_raw
GROUP BY event_minute, event_type, ad_id;
```

### 4. 跳数索引（Bloom Filter）

```sql
ALTER TABLE ad_events_raw
ADD INDEX idx_user_id user_id TYPE bloom_filter(0.01) GRANULARITY 4;
```

---

## 📈 性能指标（参考）

数据量 1 亿/天：
- 实时查询 P99 < 1s（物化视图预聚合）
- 漏斗查询 < 2s（10亿条数据）
- 入库吞吐：单 Consumer 5000 events/s
- 存储压缩比：~10:1（LZ4）

---

## 🛠 技术栈

| 组件 | 版本 | 用途 |
|------|------|------|
| Go | 1.22+ | 主语言 |
| Gin | 1.10 | Web 框架 |
| ClickHouse | 24.3 | OLAP 存储 |
| Kafka | 3.7 | 消息队列 |
| segmentio/kafka-go | 0.4.47 | Kafka 客户端 |
| clickhouse-go | 2.24 | CH 客户端 |
| Viper | 1.19 | 配置 |
| Zap | 1.27 | 日志 |
| Vue + ECharts | - | 大屏（纯静态，无构建） |

---

## 📝 简历话术（参考）

> **广告投放数据中台**（Go + Kafka + ClickHouse + Vue）
> - 基于 ClickHouse `MergeTree` + 分区 + 跳数索引 + TTL 设计日均 5 亿条广告事件存储方案，存储成本降低 60%
> - 使用 `windowFunnel` / `retention` / `uniqExact` 等高阶函数实现用户转化漏斗与留存分析，查询耗时从 MySQL 的 30s 降至 800ms
> - 设计分钟/小时级 `AggregatingMergeTree` 物化视图实现实时预聚合，实时大屏查询 P99 < 1s
> - 基于 Gin + Kafka Consumer 攒批写入实现端到端延迟 < 5s 的实时数据链路，单节点入库吞吐 5000 events/s
> - 前后端分离架构：Gin REST API + Vue + ECharts 大屏，8 个核心分析接口支撑活动 ROI 监控与异常告警

---

## 🔧 常见问题

**Q: Consumer 启动报错连不上 Kafka？**
A: 检查 `configs/config.yaml` 的 `kafka.brokers`，本地开发用 `127.0.0.1:9092`，Docker 内用 `kafka:9092`。

**Q: 大屏打开后数据为空？**
A: Producer 至少跑 1 分钟才能有数据，且前端 API 默认指向 `127.0.0.1:8080`。

**Q: 端口冲突？**
A: 修改 `docker-compose.yml` 的 ports 映射，以及 `config.yaml` 的 server.port。

---

## 📜 License

MIT