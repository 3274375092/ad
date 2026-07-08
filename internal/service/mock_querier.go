package service

import (
	"context"
	"sync"
	"time"

	"ad-platform/internal/model"
)

// MockAnalyticsQuerier Service 层测试用 Mock
// 实现了 AnalyticsQuerier 接口，可配置返回值与错误
type MockAnalyticsQuerier struct {
	mu sync.Mutex

	RealtimeOverviewFn      func(ctx context.Context, window int) (*model.RealtimeOverview, error)
	HourlyTrendFn           func(ctx context.Context, hours int) ([]model.HourlyTrend, error)
	CampaignStatsFn         func(ctx context.Context, start, end time.Time, limit int) ([]model.CampaignStat, error)
	TopAdsFn                func(ctx context.Context, start, end time.Time, sortBy string, limit int) ([]model.AdStat, error)
	RegionDistributionFn    func(ctx context.Context, start, end time.Time, limit int) ([]model.RegionStat, error)
	DeviceDistributionFn    func(ctx context.Context, start, end time.Time) ([]model.DeviceStat, error)
	FunnelFn                func(ctx context.Context, start, end time.Time, window int) ([]model.FunnelStep, error)
	RetentionFn             func(ctx context.Context, start time.Time, eventType string, days int) ([]model.RetentionStat, error)
	CompareWithLastPeriodFn func(ctx context.Context, cStart, cEnd, lStart, lEnd time.Time) (*model.RealtimeOverview, *model.RealtimeOverview, error)
	BatchInsertFn           func(ctx context.Context, events []model.AdEvent) error

	CallCount map[string]int
}

func NewMockQuerier() *MockAnalyticsQuerier {
	return &MockAnalyticsQuerier{CallCount: make(map[string]int)}
}

func (m *MockAnalyticsQuerier) record(name string) {
	m.mu.Lock()
	m.CallCount[name]++
	m.mu.Unlock()
}

func (m *MockAnalyticsQuerier) BatchInsert(ctx context.Context, events []model.AdEvent) error {
	m.record("BatchInsert")
	if m.BatchInsertFn != nil {
		return m.BatchInsertFn(ctx, events)
	}
	return nil
}

func (m *MockAnalyticsQuerier) RealtimeOverview(ctx context.Context, window int) (*model.RealtimeOverview, error) {
	m.record("RealtimeOverview")
	if m.RealtimeOverviewFn != nil {
		return m.RealtimeOverviewFn(ctx, window)
	}
	return &model.RealtimeOverview{}, nil
}

func (m *MockAnalyticsQuerier) HourlyTrend(ctx context.Context, hours int) ([]model.HourlyTrend, error) {
	m.record("HourlyTrend")
	if m.HourlyTrendFn != nil {
		return m.HourlyTrendFn(ctx, hours)
	}
	return nil, nil
}

func (m *MockAnalyticsQuerier) CampaignStats(ctx context.Context, start, end time.Time, limit int) ([]model.CampaignStat, error) {
	m.record("CampaignStats")
	if m.CampaignStatsFn != nil {
		return m.CampaignStatsFn(ctx, start, end, limit)
	}
	return nil, nil
}

func (m *MockAnalyticsQuerier) TopAds(ctx context.Context, start, end time.Time, sortBy string, limit int) ([]model.AdStat, error) {
	m.record("TopAds")
	if m.TopAdsFn != nil {
		return m.TopAdsFn(ctx, start, end, sortBy, limit)
	}
	return nil, nil
}

func (m *MockAnalyticsQuerier) RegionDistribution(ctx context.Context, start, end time.Time, limit int) ([]model.RegionStat, error) {
	m.record("RegionDistribution")
	if m.RegionDistributionFn != nil {
		return m.RegionDistributionFn(ctx, start, end, limit)
	}
	return nil, nil
}

func (m *MockAnalyticsQuerier) DeviceDistribution(ctx context.Context, start, end time.Time) ([]model.DeviceStat, error) {
	m.record("DeviceDistribution")
	if m.DeviceDistributionFn != nil {
		return m.DeviceDistributionFn(ctx, start, end)
	}
	return nil, nil
}

func (m *MockAnalyticsQuerier) Funnel(ctx context.Context, start, end time.Time, window int) ([]model.FunnelStep, error) {
	m.record("Funnel")
	if m.FunnelFn != nil {
		return m.FunnelFn(ctx, start, end, window)
	}
	return nil, nil
}

func (m *MockAnalyticsQuerier) Retention(ctx context.Context, start time.Time, eventType string, days int) ([]model.RetentionStat, error) {
	m.record("Retention")
	if m.RetentionFn != nil {
		return m.RetentionFn(ctx, start, eventType, days)
	}
	return nil, nil
}

func (m *MockAnalyticsQuerier) CompareWithLastPeriod(ctx context.Context, cStart, cEnd, lStart, lEnd time.Time) (*model.RealtimeOverview, *model.RealtimeOverview, error) {
	m.record("CompareWithLastPeriod")
	if m.CompareWithLastPeriodFn != nil {
		return m.CompareWithLastPeriodFn(ctx, cStart, cEnd, lStart, lEnd)
	}
	return nil, nil, nil
}

var _ AnalyticsQuerier = (*MockAnalyticsQuerier)(nil)