CREATE DATABASE IF NOT EXISTS ad_platform;

USE ad_platform;

DROP TABLE IF EXISTS ad_events_raw;
CREATE TABLE ad_events_raw (
    event_id      String,
    event_time    DateTime64(3, 'Asia/Shanghai'),
    event_date    Date MATERIALIZED toDate(event_time),
    event_type    LowCardinality(String),
    ad_id         String,
    campaign_id   String,
    advertiser_id String,
    user_id       String,
    device        LowCardinality(String),
    os            LowCardinality(String),
    region        LowCardinality(String),
    city          LowCardinality(String),
    cost          Float64 DEFAULT 0,
    revenue       Float64 DEFAULT 0,
    extra         Map(String, String)
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(event_time)
ORDER BY (event_type, event_time, ad_id, user_id)
TTL event_time + INTERVAL 90 DAY
SETTINGS index_granularity = 8192;

ALTER TABLE ad_events_raw ADD INDEX idx_user_id user_id TYPE bloom_filter(0.01) GRANULARITY 4;
ALTER TABLE ad_events_raw ADD INDEX idx_region region TYPE set(100) GRANULARITY 4;
ALTER TABLE ad_events_raw ADD INDEX idx_campaign campaign_id TYPE bloom_filter(0.01) GRANULARITY 4;

DROP TABLE IF EXISTS ad_minute_stats;
CREATE TABLE ad_minute_stats (
    event_minute  DateTime,
    event_type    LowCardinality(String),
    ad_id         String,
    campaign_id   String,
    region        LowCardinality(String),
    device        LowCardinality(String),
    pv            UInt64,
    uv            AggregateFunction(uniqExact, String),
    cost          Float64,
    revenue       Float64
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(event_minute)
ORDER BY (event_minute, event_type, ad_id, campaign_id)
TTL event_minute + INTERVAL 30 DAY;

DROP TABLE IF EXISTS ad_minute_stats_mv;
CREATE MATERIALIZED VIEW ad_minute_stats_mv
TO ad_minute_stats AS
SELECT
    toStartOfMinute(event_time) AS event_minute,
    event_type,
    ad_id,
    campaign_id,
    region,
    device,
    count() AS pv,
    uniqExactState(user_id) AS uv,
    sum(cost) AS cost,
    sum(revenue) AS revenue
FROM ad_events_raw
GROUP BY event_minute, event_type, ad_id, campaign_id, region, device;

DROP TABLE IF EXISTS ad_hourly_stats;
CREATE TABLE ad_hourly_stats (
    event_hour    DateTime,
    event_type    LowCardinality(String),
    campaign_id   String,
    advertiser_id String,
    pv            UInt64,
    uv            AggregateFunction(uniqExact, String),
    cost          Float64,
    revenue       Float64
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(event_hour)
ORDER BY (event_hour, event_type, campaign_id, advertiser_id)
TTL event_hour + INTERVAL 180 DAY;

DROP TABLE IF EXISTS ad_hourly_stats_mv;
CREATE MATERIALIZED VIEW ad_hourly_stats_mv
TO ad_hourly_stats AS
SELECT
    toStartOfHour(event_time) AS event_hour,
    event_type,
    campaign_id,
    advertiser_id,
    count() AS pv,
    uniqExactState(user_id) AS uv,
    sum(cost) AS cost,
    sum(revenue) AS revenue
FROM ad_events_raw
GROUP BY event_hour, event_type, campaign_id, advertiser_id;

DROP TABLE IF EXISTS dim_ad;
CREATE TABLE dim_ad (
    ad_id         String,
    ad_name       String,
    campaign_id   String,
    advertiser_id String,
    ad_type       LowCardinality(String),
    created_at    DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY ad_id;

INSERT INTO dim_ad VALUES
    ('ad_001', 'splash-ad-A', 'camp_1001', 'adv_001', 'splash', now()),
    ('ad_002', 'banner-ad-B', 'camp_1001', 'adv_001', 'banner', now()),
    ('ad_003', 'feed-ad-C', 'camp_1002', 'adv_002', 'feed', now()),
    ('ad_004', 'video-ad-D', 'camp_1003', 'adv_003', 'video', now()),
    ('ad_005', 'popup-ad-E', 'camp_1002', 'adv_002', 'popup', now()),
    ('ad_006', 'splash-ad-F', 'camp_1004', 'adv_004', 'splash', now()),
    ('ad_007', 'banner-ad-G', 'camp_1004', 'adv_004', 'banner', now()),
    ('ad_008', 'feed-ad-H', 'camp_1005', 'adv_005', 'feed', now());

DROP TABLE IF EXISTS dim_campaign;
CREATE TABLE dim_campaign (
    campaign_id   String,
    campaign_name String,
    advertiser_id String,
    budget        Float64,
    start_date    Date,
    end_date      Date,
    status        LowCardinality(String)
) ENGINE = MergeTree()
ORDER BY campaign_id;

INSERT INTO dim_campaign VALUES
    ('camp_1001', '618-promotion', 'adv_001', 1000000.00, '2026-06-01', '2026-06-30', 'active'),
    ('camp_1002', 'game-launch', 'adv_002', 800000.00, '2026-06-15', '2026-07-15', 'active'),
    ('camp_1003', 'auto-summer', 'adv_003', 2000000.00, '2026-05-01', '2026-08-31', 'active'),
    ('camp_1004', 'local-life', 'adv_004', 500000.00, '2026-07-01', '2026-07-31', 'active'),
    ('camp_1005', 'edu-summer', 'adv_005', 600000.00, '2026-06-20', '2026-08-31', 'active');

DROP TABLE IF EXISTS user_event_funnel;
CREATE TABLE user_event_funnel (
    user_id       String,
    event_date    Date,
    event_type    LowCardinality(String),
    campaign_id   String
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(event_date)
ORDER BY (event_date, user_id, event_type);

DROP TABLE IF EXISTS user_event_funnel_mv;
CREATE MATERIALIZED VIEW user_event_funnel_mv
TO user_event_funnel AS
SELECT
    user_id,
    toDate(event_time) AS event_date,
    event_type,
    campaign_id
FROM ad_events_raw
WHERE event_type IN ('impression', 'click', 'conversion');
