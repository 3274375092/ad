# 广告投放数据中台

基于 **Go + Gin + Kafka + ClickHouse** 的实时广告数据分析平台，覆盖广告曝光/点击/转化全链路采集、实时入库、OLAP 多维分析及 ECharts 可视化大屏。

---

## 架构

```
                   ad.exe  一根命令启动所有
                        │
       ┌────────────────┼────────────────┐
       ▼                ▼                ▼
  Mock Producer     Kafka Consumer    Gin Server
  加权活动/地域     攒批 500 条        handler/service
  差异化 CTR/CVR    写入 ClickHouse    /repository 分层
       │                │                │
       └───────┬────────┘                │
               ▼                         │
          Apache Kafka                   │
          Topic: ad_events               │
          KRaft 模式                     │
               │                         │
               ▼                         ▼
          ┌──────────────────────────────────┐
          │           ClickHouse             │
          │  ad_events_raw (MergeTree)       │◀── 查询
          │  ad_minute_stats_mv (预聚合)      │
          │  ad_hourly_stats_mv              │
          │  user_event_funnel_mv            │
          └──────────────┬───────────────────┘
                         │
              ┌──────────▼──────────┐
              │  ECharts 可视化大屏  │
              │  8 视图 / 10s 刷新   │
              └─────────────────────┘
```

**设计要点**：

- **Kafka 做缓冲**：Producer 不直连 CH，Kafka 吸峰填谷；Consumer 攒 500 条一批写入，不逐条 insert
- **物化视图预聚合**：写入时就算好分钟/小时聚合，大屏 8 个接口不扫全表，P99 < 100ms
- **分层架构**：handler → service（依赖 `AnalyticsQuerier` 接口）→ repository，Service 接口抽象让单测可 Mock 掉 ClickHouse
- **本地三合一**：`ad.exe` 把 Producer、Consumer、Server 三个 goroutine 包在一起，开发一根命令启动；生产可拆为独立微服务

---

## 目录结构

```
ad-data-platform/
├── cmd/all/main.go                  # 一键启动（api + consumer + producer 三合一）
├── internal/
│   ├── handler/                     # 9 个 REST 路由
│   ├── service/                     # 参数校验、接口抽象（方便单测 Mock）
│   ├── repository/                  # ClickHouse SQL（windowFunnel / retention）
│   ├── model/                       # 数据模型
│   └── middleware/                  # trace_id / access_log / recover
├── pkg/
│   ├── clickhouse/                  # 连接池封装
│   ├── kafka/                       # reader / writer 封装
│   ├── logger/                      # zap
│   └── response/                    # 统一 JSON 响应
├── web/                             # ECharts 大屏（纯静态）
├── scripts/init_clickhouse.sql      # 建表 DDL
├── deploy/docker-compose.yml        # Kafka KRaft + ClickHouse
├── configs/config.yaml              # viper 配置
└── go.mod
```

---

## 快速开始

```bash
# 1. 启动中间件
cd deploy
docker-compose up -d

# 2. 初始化 ClickHouse 表
docker exec -i ad-clickhouse clickhouse-client --password 123456 < ../scripts/init_clickhouse.sql

# 3. 一键启动（回填 24h 历史数据 + 实时产数 + API 服务）
cd ..
go build -o bin/ad.exe ./cmd/all
.\bin\ad.exe

# 4. 浏览器打开
http://localhost:8080/
```

---

## API

| 接口 | 说明 |
|------|------|
| `GET /api/v1/stats/realtime?window=5` | 最近 N 分钟实时总览 |
| `GET /api/v1/stats/hourly?hours=24` | 24 小时趋势 |
| `GET /api/v1/stats/campaigns?start=&end=` | 活动维度排行 |
| `GET /api/v1/stats/top-ads?sort=revenue` | 广告 Top10 |
| `GET /api/v1/stats/regions` | 地域分布 |
| `GET /api/v1/stats/devices` | 设备分布 |
| `GET /api/v1/stats/funnel?window=3600` | 转化漏斗（windowFunnel） |
| `GET /api/v1/stats/retention?date=&days=7` | 用户留存（retention） |
| `GET /api/v1/stats/compare?start=&end=` | 同比环比 |

---

## 核心 SQL

```sql
-- 转化漏斗（windowFunnel 高阶函数）
SELECT countIf(level >= 1), countIf(level >= 2), countIf(level >= 3)
FROM (
    SELECT user_id,
        windowFunnel(3600)(toDateTime(event_time),
            event_type='impression',
            event_type='click',
            event_type='conversion') AS level
    FROM ad_events_raw
    WHERE event_time BETWEEN ? AND ?
    GROUP BY user_id
);

-- 物化视图（分钟预聚合）
CREATE MATERIALIZED VIEW ad_minute_stats_mv TO ad_minute_stats AS
SELECT toStartOfMinute(event_time) AS event_minute, event_type, ad_id,
       count() AS pv, uniqExactState(user_id) AS uv,
       sum(cost) AS cost, sum(revenue) AS revenue
FROM ad_events_raw
GROUP BY event_minute, event_type, ad_id;

-- 跳数索引
ALTER TABLE ad_events_raw ADD INDEX idx_user_id user_id TYPE bloom_filter(0.01) GRANULARITY 4;
```

---

## 运行测试

```bash
# 单元测试
go test -cover ./internal/...

# 集成测试（需要 ClickHouse）
INTEGRATION=1 go test -tags=integration ./internal/repository/...
```

---

## 技术栈

Go / Gin / ClickHouse `clickhouse-go/v2` / Kafka `segmentio/kafka-go` / Viper / Zap / testify / Docker Compose / ECharts + Axios

---

## License

MIT