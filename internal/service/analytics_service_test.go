package service

import (
	"context"
	"errors"
	"testing"
	"time"

	"ad-platform/internal/model"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// =====================================================
// RealtimeOverview
// =====================================================

func TestAnalyticsService_RealtimeOverview_DefaultWindow(t *testing.T) {
	mock := NewMockQuerier()
	mock.RealtimeOverviewFn = func(ctx context.Context, window int) (*model.RealtimeOverview, error) {
		assert.Equal(t, 5, window, "window 应被归一化为 5")
		return &model.RealtimeOverview{Window: "5min"}, nil
	}

	svc := NewAnalyticsService(mock)
	got, err := svc.RealtimeOverview(context.Background(), 0)
	require.NoError(t, err)
	assert.Equal(t, "5min", got.Window)
	assert.Equal(t, 1, mock.CallCount["RealtimeOverview"])
}

func TestAnalyticsService_RealtimeOverview_NegativeWindow(t *testing.T) {
	mock := NewMockQuerier()
	mock.RealtimeOverviewFn = func(ctx context.Context, window int) (*model.RealtimeOverview, error) {
		assert.Equal(t, 5, window)
		return &model.RealtimeOverview{}, nil
	}
	svc := NewAnalyticsService(mock)
	_, err := svc.RealtimeOverview(context.Background(), -10)
	require.NoError(t, err)
}

func TestAnalyticsService_RealtimeOverview_PropagatesError(t *testing.T) {
	mock := NewMockQuerier()
	wantErr := errors.New("clickhouse timeout")
	mock.RealtimeOverviewFn = func(ctx context.Context, window int) (*model.RealtimeOverview, error) {
		return nil, wantErr
	}
	svc := NewAnalyticsService(mock)
	got, err := svc.RealtimeOverview(context.Background(), 5)
	assert.Nil(t, got)
	assert.ErrorIs(t, err, wantErr)
}

// =====================================================
// HourlyTrend
// =====================================================

func TestAnalyticsService_HourlyTrend_DefaultHours(t *testing.T) {
	mock := NewMockQuerier()
	mock.HourlyTrendFn = func(ctx context.Context, hours int) ([]model.HourlyTrend, error) {
		assert.Equal(t, 24, hours, "默认 24 小时")
		return []model.HourlyTrend{{Hour: time.Now()}}, nil
	}
	svc := NewAnalyticsService(mock)
	data, err := svc.HourlyTrend(context.Background(), 0)
	require.NoError(t, err)
	assert.Len(t, data, 1)
}

func TestAnalyticsService_HourlyTrend_MaxLimit(t *testing.T) {
	mock := NewMockQuerier()
	mock.HourlyTrendFn = func(ctx context.Context, hours int) ([]model.HourlyTrend, error) {
		assert.Equal(t, 168, hours, "上限为 168 小时（7天）")
		return nil, nil
	}
	svc := NewAnalyticsService(mock)
	_, err := svc.HourlyTrend(context.Background(), 10000)
	require.NoError(t, err)
}

// =====================================================
// CampaignStats
// =====================================================

func TestAnalyticsService_CampaignStats_LimitBounds(t *testing.T) {
	tests := []struct {
		name      string
		input     int
		expectLim int
	}{
		{"零值用默认", 0, 20},
		{"正常", 50, 50},
		{"超过上限截断", 200, 100},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mock := NewMockQuerier()
			mock.CampaignStatsFn = func(ctx context.Context, start, end time.Time, limit int) ([]model.CampaignStat, error) {
				assert.Equal(t, tt.expectLim, limit)
				return nil, nil
			}
			svc := NewAnalyticsService(mock)
			_, _ = svc.CampaignStats(context.Background(), time.Now().Add(-time.Hour), time.Now(), tt.input)
		})
	}
}

// =====================================================
// TopAds（参数校验：sortBy 白名单）
// =====================================================

func TestAnalyticsService_TopAds_SortByWhitelist(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"impressions", "impressions"},
		{"clicks", "clicks"},
		{"revenue", "revenue"},
		{"invalid_sql_inject", "impressions"},
		{"", "impressions"},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			mock := NewMockQuerier()
			mock.TopAdsFn = func(ctx context.Context, start, end time.Time, sortBy string, limit int) ([]model.AdStat, error) {
				assert.Equal(t, tt.expected, sortBy, "非法 sortBy 应被替换为 impressions")
				return nil, nil
			}
			svc := NewAnalyticsService(mock)
			_, _ = svc.TopAds(context.Background(), time.Now(), time.Now(), tt.input, 10)
		})
	}
}

func TestAnalyticsService_TopAds_LimitBounds(t *testing.T) {
	mock := NewMockQuerier()
	mock.TopAdsFn = func(ctx context.Context, start, end time.Time, sortBy string, limit int) ([]model.AdStat, error) {
		assert.Equal(t, 50, limit, "上限 50")
		return nil, nil
	}
	svc := NewAnalyticsService(mock)
	_, _ = svc.TopAds(context.Background(), time.Now(), time.Now(), "revenue", 1000)
}

// =====================================================
// Retention（event_type 白名单）
// =====================================================

func TestAnalyticsService_Retention_EventTypeWhitelist(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"impression", "impression"},
		{"click", "click"},
		{"conversion", "conversion"},
		{"hacker", "impression"}, // 非法值替换
		{"", "impression"},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			mock := NewMockQuerier()
			mock.RetentionFn = func(ctx context.Context, start time.Time, eventType string, days int) ([]model.RetentionStat, error) {
				assert.Equal(t, tt.expected, eventType)
				return nil, nil
			}
			svc := NewAnalyticsService(mock)
			_, _ = svc.Retention(context.Background(), time.Now(), tt.input, 7)
		})
	}
}

func TestAnalyticsService_Retention_DaysBounds(t *testing.T) {
	mock := NewMockQuerier()
	mock.RetentionFn = func(ctx context.Context, start time.Time, eventType string, days int) ([]model.RetentionStat, error) {
		assert.Equal(t, 30, days, "上限 30 天")
		return nil, nil
	}
	svc := NewAnalyticsService(mock)
	_, _ = svc.Retention(context.Background(), time.Now(), "impression", 365)
}

// =====================================================
// Funnel（默认 window）
// =====================================================

func TestAnalyticsService_Funnel_DefaultWindow(t *testing.T) {
	mock := NewMockQuerier()
	mock.FunnelFn = func(ctx context.Context, start, end time.Time, window int) ([]model.FunnelStep, error) {
		assert.Equal(t, 3600, window, "默认窗口 3600s")
		return []model.FunnelStep{{Step: "impression"}}, nil
	}
	svc := NewAnalyticsService(mock)
	data, err := svc.Funnel(context.Background(), time.Now(), time.Now(), 0)
	require.NoError(t, err)
	assert.Len(t, data, 1)
}

// =====================================================
// CompareWithLastPeriod（时间范围校验）
// =====================================================

func TestAnalyticsService_CompareWithLastPeriod_InvalidRange(t *testing.T) {
	mock := NewMockQuerier()
	svc := NewAnalyticsService(mock)

	// start 在 end 之后
	_, _, err := svc.CompareWithLastPeriod(context.Background(), time.Now(), time.Now().Add(-time.Hour))
	assert.ErrorIs(t, err, ErrInvalidRange)
	assert.Equal(t, 0, mock.CallCount["CompareWithLastPeriod"], "非法范围不应调用 Repository")
}

func TestAnalyticsService_CompareWithLastPeriod_ValidRange(t *testing.T) {
	mock := NewMockQuerier()
	mock.CompareWithLastPeriodFn = func(ctx context.Context, cStart, cEnd, lStart, lEnd time.Time) (*model.RealtimeOverview, *model.RealtimeOverview, error) {
		assert.True(t, cStart.Before(cEnd))
		assert.True(t, lStart.Before(lEnd))
		assert.True(t, lEnd.Before(cStart), "last 周期应在 current 之前")
		return &model.RealtimeOverview{Window: "current"}, &model.RealtimeOverview{Window: "last"}, nil
	}

	svc := NewAnalyticsService(mock)
	end := time.Now()
	start := end.Add(-24 * time.Hour)
	cur, last, err := svc.CompareWithLastPeriod(context.Background(), start, end)
	require.NoError(t, err)
	assert.Equal(t, "current", cur.Window)
	assert.Equal(t, "last", last.Window)
}

// =====================================================
// RegionDistribution
// =====================================================

func TestAnalyticsService_RegionDistribution_LimitBounds(t *testing.T) {
	mock := NewMockQuerier()
	mock.RegionDistributionFn = func(ctx context.Context, start, end time.Time, limit int) ([]model.RegionStat, error) {
		assert.Equal(t, 100, limit)
		return nil, nil
	}
	svc := NewAnalyticsService(mock)
	_, _ = svc.RegionDistribution(context.Background(), time.Now(), time.Now(), 500)
}

// =====================================================
// 并发测试
// =====================================================

func TestAnalyticsService_ConcurrentSafety(t *testing.T) {
	mock := NewMockQuerier()
	mock.RealtimeOverviewFn = func(ctx context.Context, window int) (*model.RealtimeOverview, error) {
		return &model.RealtimeOverview{}, nil
	}
	svc := NewAnalyticsService(mock)

	done := make(chan bool, 100)
	for i := 0; i < 100; i++ {
		go func() {
			_, _ = svc.RealtimeOverview(context.Background(), 5)
			done <- true
		}()
	}
	for i := 0; i < 100; i++ {
		<-done
	}
	assert.Equal(t, 100, mock.CallCount["RealtimeOverview"])
}