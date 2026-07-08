package service

import (
	"context"
	"time"

	"ad-platform/internal/model"
)

// AnalyticsQuerier Service 层依赖的 Repository 接口（Go 惯例：消费者定义接口）
// 这样 Service 不需要知道 ClickHouse 的存在，便于单元测试使用 Mock
type AnalyticsQuerier interface {
	BatchInsert(ctx context.Context, events []model.AdEvent) error
	RealtimeOverview(ctx context.Context, windowMinutes int) (*model.RealtimeOverview, error)
	HourlyTrend(ctx context.Context, hours int) ([]model.HourlyTrend, error)
	CampaignStats(ctx context.Context, start, end time.Time, limit int) ([]model.CampaignStat, error)
	TopAds(ctx context.Context, start, end time.Time, sortBy string, limit int) ([]model.AdStat, error)
	RegionDistribution(ctx context.Context, start, end time.Time, limit int) ([]model.RegionStat, error)
	DeviceDistribution(ctx context.Context, start, end time.Time) ([]model.DeviceStat, error)
	Funnel(ctx context.Context, start, end time.Time, windowSeconds int) ([]model.FunnelStep, error)
	Retention(ctx context.Context, start time.Time, eventType string, days int) ([]model.RetentionStat, error)
	CompareWithLastPeriod(ctx context.Context, currentStart, currentEnd, lastStart, lastEnd time.Time) (*model.RealtimeOverview, *model.RealtimeOverview, error)
}

type AnalyticsService struct {
	repo AnalyticsQuerier
}

// NewAnalyticsService 构造 Service（接受接口，便于 Mock 测试）
func NewAnalyticsService(repo AnalyticsQuerier) *AnalyticsService {
	return &AnalyticsService{repo: repo}
}

// =====================================================
// 业务方法（含参数默认值、参数校验）
// =====================================================

func (s *AnalyticsService) RealtimeOverview(ctx context.Context, window int) (*model.RealtimeOverview, error) {
	if window <= 0 {
		window = 5
	}
	return s.repo.RealtimeOverview(ctx, window)
}

func (s *AnalyticsService) HourlyTrend(ctx context.Context, hours int) ([]model.HourlyTrend, error) {
	if hours <= 0 {
		hours = 24
	}
	if hours > 168 {
		hours = 168 // 最多查 7 天
	}
	return s.repo.HourlyTrend(ctx, hours)
}

func (s *AnalyticsService) CampaignStats(ctx context.Context, start, end time.Time, limit int) ([]model.CampaignStat, error) {
	if limit <= 0 {
		limit = 20
	}
	if limit > 100 {
		limit = 100
	}
	return s.repo.CampaignStats(ctx, start, end, limit)
}

func (s *AnalyticsService) TopAds(ctx context.Context, start, end time.Time, sortBy string, limit int) ([]model.AdStat, error) {
	if limit <= 0 {
		limit = 10
	}
	if limit > 50 {
		limit = 50
	}
	switch sortBy {
	case "impressions", "clicks", "conversions", "cost", "revenue":
	default:
		sortBy = "impressions"
	}
	return s.repo.TopAds(ctx, start, end, sortBy, limit)
}

func (s *AnalyticsService) RegionDistribution(ctx context.Context, start, end time.Time, limit int) ([]model.RegionStat, error) {
	if limit <= 0 {
		limit = 20
	}
	if limit > 100 {
		limit = 100
	}
	return s.repo.RegionDistribution(ctx, start, end, limit)
}

func (s *AnalyticsService) DeviceDistribution(ctx context.Context, start, end time.Time) ([]model.DeviceStat, error) {
	return s.repo.DeviceDistribution(ctx, start, end)
}

func (s *AnalyticsService) Funnel(ctx context.Context, start, end time.Time, windowSeconds int) ([]model.FunnelStep, error) {
	if windowSeconds <= 0 {
		windowSeconds = 3600
	}
	return s.repo.Funnel(ctx, start, end, windowSeconds)
}

func (s *AnalyticsService) Retention(ctx context.Context, start time.Time, eventType string, days int) ([]model.RetentionStat, error) {
	if days <= 0 {
		days = 7
	}
	if days > 30 {
		days = 30
	}
	if eventType == "" {
		eventType = "impression"
	}
	switch eventType {
	case "impression", "click", "conversion":
	default:
		eventType = "impression"
	}
	return s.repo.Retention(ctx, start, eventType, days)
}

func (s *AnalyticsService) CompareWithLastPeriod(ctx context.Context, start, end time.Time) (*model.RealtimeOverview, *model.RealtimeOverview, error) {
	if !start.Before(end) {
		return nil, nil, ErrInvalidRange
	}
	duration := end.Sub(start)
	lastEnd := start.Add(-time.Second)
	lastStart := lastEnd.Add(-duration)
	return s.repo.CompareWithLastPeriod(ctx, start, end, lastStart, lastEnd)
}