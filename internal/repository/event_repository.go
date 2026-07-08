package repository

import (
	"context"
	"fmt"
	"time"

	"ad-platform/internal/model"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// EventRepository 提供广告事件分析所需的所有 SQL 查询能力
// 其方法签名同时满足 service 层的 AnalyticsQuerier 接口
type EventRepository struct {
	conn driver.Conn
}

func NewEventRepository(conn driver.Conn) *EventRepository {
	return &EventRepository{conn: conn}
}

// 编译期断言：EventRepository 必须满足 service.AnalyticsQuerier
// 实际断言在 service 包内通过 var _ AnalyticsQuerier = (*EventRepository)(nil) 完成

// 导出 driver.Conn 给 Service 层使用（用于自定义查询）
func (r *EventRepository) Conn() driver.Conn { return r.conn }

// =====================================================
// 写入（消费者批量插入）
// =====================================================

func (r *EventRepository) BatchInsert(ctx context.Context, events []model.AdEvent) error {
	if len(events) == 0 {
		return nil
	}

	batch, err := r.conn.PrepareBatch(ctx, `
		INSERT INTO ad_events_raw (
			event_id, event_time, event_type, ad_id, campaign_id,
			advertiser_id, user_id, device, os, region, city,
			cost, revenue
		)`)
	if err != nil {
		return err
	}

	for _, e := range events {
		if err := batch.Append(
			e.EventID, e.EventTime, e.EventType, e.AdID, e.CampaignID,
			e.AdvertiserID, e.UserID, e.Device, e.OS, e.Region, e.City,
			e.Cost, e.Revenue,
		); err != nil {
			return err
		}
	}

	return batch.Send()
}

// =====================================================
// 实时总览（最近 N 分钟）
// =====================================================

func (r *EventRepository) RealtimeOverview(ctx context.Context, windowMinutes int) (*model.RealtimeOverview, error) {
	query := `
SELECT
    countIf(event_type = 'impression') AS impressions,
    countIf(event_type = 'click')      AS clicks,
    countIf(event_type = 'conversion') AS conversions,
    uniqExact(user_id)                 AS uv,
    sum(cost)                          AS cost,
    sum(revenue)                       AS revenue
FROM ad_events_raw
WHERE event_time >= now() - INTERVAL ? MINUTE`

	row := r.conn.QueryRow(ctx, query, windowMinutes)

	var o model.RealtimeOverview
	if err := row.ScanStruct(&o); err != nil {
		return nil, err
	}

	o.Window = fmt.Sprintf("%dmin", windowMinutes)
	if o.Impressions > 0 {
		o.CTR = float64(o.Clicks) / float64(o.Impressions)
	}
	if o.Clicks > 0 {
		o.CVR = float64(o.Conversions) / float64(o.Clicks)
	}
	if o.Cost > 0 {
		o.ROI = o.Revenue / o.Cost
		o.RPM = o.Revenue / float64(o.Impressions) * 1000
	}
	return &o, nil
}

// =====================================================
// 小时趋势（最近 N 小时）
// =====================================================

func (r *EventRepository) HourlyTrend(ctx context.Context, hours int) ([]model.HourlyTrend, error) {
	query := `
SELECT
    toStartOfHour(event_time) AS hour,
    countIf(event_type = 'impression') AS impressions,
    countIf(event_type = 'click')      AS clicks,
    countIf(event_type = 'conversion') AS conversions,
    uniqExact(user_id)                 AS uv,
    sum(cost)                          AS cost,
    sum(revenue)                       AS revenue
FROM ad_events_raw
WHERE event_time >= now() - INTERVAL ? HOUR
GROUP BY hour
ORDER BY hour ASC`

	rows, err := r.conn.Query(ctx, query, hours)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var res []model.HourlyTrend
	for rows.Next() {
		var t model.HourlyTrend
		if err := rows.ScanStruct(&t); err != nil {
			return nil, err
		}
		res = append(res, t)
	}
	return res, nil
}

// =====================================================
// 活动维度统计
// =====================================================

func (r *EventRepository) CampaignStats(ctx context.Context, start, end time.Time, limit int) ([]model.CampaignStat, error) {
	query := `
SELECT
    r.campaign_id,
    c.campaign_name,
    countIf(r.event_type = 'impression') AS impressions,
    countIf(r.event_type = 'click')      AS clicks,
    countIf(r.event_type = 'conversion') AS conversions,
    uniqExact(r.user_id)                 AS uv,
    sum(r.cost)                          AS cost,
    sum(r.revenue)                       AS revenue
FROM ad_events_raw r
LEFT JOIN dim_campaign c ON r.campaign_id = c.campaign_id
WHERE r.event_time BETWEEN ? AND ?
GROUP BY r.campaign_id, c.campaign_name
ORDER BY cost DESC
LIMIT ?`

	rows, err := r.conn.Query(ctx, query, start, end, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var res []model.CampaignStat
	for rows.Next() {
		var s model.CampaignStat
		if err := rows.ScanStruct(&s); err != nil {
			return nil, err
		}
		if s.Impressions > 0 {
			s.CTR = float64(s.Clicks) / float64(s.Impressions)
		}
		if s.Clicks > 0 {
			s.CVR = float64(s.Conversions) / float64(s.Clicks)
		}
		if s.Cost > 0 {
			s.ROI = s.Revenue / s.Cost
		}
		res = append(res, s)
	}
	return res, nil
}

// =====================================================
// 广告维度 TopN
// =====================================================

func (r *EventRepository) TopAds(ctx context.Context, start, end time.Time, sortBy string, limit int) ([]model.AdStat, error) {
	query := `
SELECT
    r.ad_id,
    d.ad_name,
    countIf(r.event_type = 'impression') AS impressions,
    countIf(r.event_type = 'click')      AS clicks,
    countIf(r.event_type = 'conversion') AS conversions,
    sum(r.cost)                          AS cost,
    sum(r.revenue)                       AS revenue
FROM ad_events_raw r
LEFT JOIN dim_ad d ON r.ad_id = d.ad_id
WHERE r.event_time BETWEEN ? AND ?
GROUP BY r.ad_id, d.ad_name
ORDER BY ` + sortBy + ` DESC
LIMIT ?`

	rows, err := r.conn.Query(ctx, query, start, end, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var res []model.AdStat
	for rows.Next() {
		var a model.AdStat
		if err := rows.ScanStruct(&a); err != nil {
			return nil, err
		}
		if a.Impressions > 0 {
			a.CTR = float64(a.Clicks) / float64(a.Impressions)
		}
		if a.Clicks > 0 {
			a.CVR = float64(a.Conversions) / float64(a.Clicks)
		}
		res = append(res, a)
	}
	return res, nil
}

// =====================================================
// 地域分布
// =====================================================

func (r *EventRepository) RegionDistribution(ctx context.Context, start, end time.Time, limit int) ([]model.RegionStat, error) {
	query := `
SELECT
    region,
    countIf(event_type = 'impression') AS impressions,
    countIf(event_type = 'click')      AS clicks,
    uniqExact(user_id)                 AS uv,
    sum(cost)                          AS cost
FROM ad_events_raw
WHERE event_time BETWEEN ? AND ?
GROUP BY region
ORDER BY impressions DESC
LIMIT ?`

	rows, err := r.conn.Query(ctx, query, start, end, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var res []model.RegionStat
	for rows.Next() {
		var s model.RegionStat
		if err := rows.ScanStruct(&s); err != nil {
			return nil, err
		}
		res = append(res, s)
	}
	return res, nil
}

// =====================================================
// 设备分布
// =====================================================

func (r *EventRepository) DeviceDistribution(ctx context.Context, start, end time.Time) ([]model.DeviceStat, error) {
	query := `
SELECT
    device,
    countIf(event_type = 'impression') AS impressions,
    countIf(event_type = 'click')      AS clicks,
    countIf(event_type = 'conversion') AS conversions
FROM ad_events_raw
WHERE event_time BETWEEN ? AND ?
GROUP BY device
ORDER BY impressions DESC`

	rows, err := r.conn.Query(ctx, query, start, end)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var res []model.DeviceStat
	for rows.Next() {
		var s model.DeviceStat
		if err := rows.ScanStruct(&s); err != nil {
			return nil, err
		}
		res = append(res, s)
	}
	return res, nil
}

// =====================================================
// 转化漏斗（windowFunnel 高阶函数）
// =====================================================

func (r *EventRepository) Funnel(ctx context.Context, start, end time.Time, windowSeconds int) ([]model.FunnelStep, error) {
	query := fmt.Sprintf(`
SELECT
    countIf(level >= 1) AS s1,
    countIf(level >= 2) AS s2,
    countIf(level >= 3) AS s3
FROM (
    SELECT
        user_id,
        windowFunnel(%d)(toDateTime(event_time),
            event_type = 'impression',
            event_type = 'click',
            event_type = 'conversion') AS level
    FROM ad_events_raw
    WHERE event_time BETWEEN ? AND ?
    GROUP BY user_id
)`, windowSeconds)

	row := r.conn.QueryRow(ctx, query, start, end)

	var s1, s2, s3 uint64
	if err := row.Scan(&s1, &s2, &s3); err != nil {
		return nil, err
	}

	steps := []model.FunnelStep{
		{Step: "impression", Count: s1},
		{Step: "click", Count: s2},
		{Step: "conversion", Count: s3},
	}
	if s1 > 0 {
		steps[1].Rate = float64(s2) / float64(s1)
		steps[2].Rate = float64(s3) / float64(s1)
		steps[0].Rate = 1.0
	}
	return steps, nil
}

// =====================================================
// 用户留存
// =====================================================

func (r *EventRepository) Retention(ctx context.Context, start time.Time, eventType string, days int) ([]model.RetentionStat, error) {
	query := `
SELECT
    dateDiff('day', ?, event_date) AS day,
    countDistinct(user_id)         AS users
FROM user_event_funnel
WHERE event_type = ? AND event_date > ? AND event_date <= ? + INTERVAL ? DAY
GROUP BY day
ORDER BY day ASC`

	rows, err := r.conn.Query(ctx, query, start, eventType, start, start, days)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var res []model.RetentionStat
	for rows.Next() {
		var s model.RetentionStat
		if err := rows.ScanStruct(&s); err != nil {
			return nil, err
		}
		res = append(res, s)
	}
	return res, nil
}

// =====================================================
// 同比环比
// =====================================================

func (r *EventRepository) CompareWithLastPeriod(ctx context.Context, currentStart, currentEnd, lastStart, lastEnd time.Time) (*model.RealtimeOverview, *model.RealtimeOverview, error) {
	runOverview := func(start, end time.Time, label string) (*model.RealtimeOverview, error) {
		query := `
SELECT
    countIf(event_type = 'impression') AS impressions,
    countIf(event_type = 'click')      AS clicks,
    countIf(event_type = 'conversion') AS conversions,
    uniqExact(user_id)                 AS uv,
    sum(cost)                          AS cost,
    sum(revenue)                       AS revenue
FROM ad_events_raw
WHERE event_time BETWEEN ? AND ?`
		row := r.conn.QueryRow(ctx, query, start, end)
		var o model.RealtimeOverview
		if err := row.ScanStruct(&o); err != nil {
			return nil, err
		}
		o.Window = label
		if o.Impressions > 0 {
			o.CTR = float64(o.Clicks) / float64(o.Impressions)
		}
		if o.Clicks > 0 {
			o.CVR = float64(o.Conversions) / float64(o.Clicks)
		}
		if o.Cost > 0 {
			o.ROI = o.Revenue / o.Cost
			o.RPM = o.Revenue / float64(o.Impressions) * 1000
		}
		return &o, nil
	}

	cur, err := runOverview(currentStart, currentEnd, "current")
	if err != nil {
		return nil, nil, err
	}
	last, err := runOverview(lastStart, lastEnd, "last")
	if err != nil {
		return nil, nil, err
	}
	return cur, last, nil
}