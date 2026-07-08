package handler

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"ad-platform/internal/middleware"
	"ad-platform/internal/model"
	"ad-platform/internal/service"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupRouter(t *testing.T, svc *service.AnalyticsService) *gin.Engine {
	t.Helper()
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(middleware.TraceID(), middleware.Recover())
	h := NewAnalyticsHandler(svc)
	api := r.Group("/api/v1")
	h.Register(api)
	return r
}

func TestAnalyticsHandler_Realtime_Success(t *testing.T) {
	mock := service.NewMockQuerier()
	mock.RealtimeOverviewFn = func(ctx context.Context, window int) (*model.RealtimeOverview, error) {
		return &model.RealtimeOverview{
			Window: "5min", Impressions: 1000, Clicks: 50, Conversions: 2,
			UV: 800, Cost: 100.0, Revenue: 200.0, CTR: 0.05, CVR: 0.04, ROI: 2.0,
		}, nil
	}
	svc := service.NewAnalyticsService(mock)
	r := setupRouter(t, svc)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/api/v1/stats/realtime?window=5", nil)
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var resp struct {
		Code    int                    `json:"code"`
		Message string                 `json:"message"`
		Data    *model.RealtimeOverview `json:"data"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	assert.Equal(t, 0, resp.Code)
	assert.Equal(t, "success", resp.Message)
	require.NotNil(t, resp.Data)
	assert.Equal(t, uint64(1000), resp.Data.Impressions)
	assert.Equal(t, uint64(50), resp.Data.Clicks)
}

func TestAnalyticsHandler_Realtime_DefaultWindow(t *testing.T) {
	mock := service.NewMockQuerier()
	var capturedWindow int
	mock.RealtimeOverviewFn = func(ctx context.Context, window int) (*model.RealtimeOverview, error) {
		capturedWindow = window
		return &model.RealtimeOverview{}, nil
	}
	svc := service.NewAnalyticsService(mock)
	r := setupRouter(t, svc)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/api/v1/stats/realtime", nil)
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, 5, capturedWindow, "未传 window 应使用 Service 层默认值 5")
}

func TestAnalyticsHandler_Realtime_ServiceError(t *testing.T) {
	mock := service.NewMockQuerier()
	mock.RealtimeOverviewFn = func(ctx context.Context, window int) (*model.RealtimeOverview, error) {
		return nil, errors.New("database down")
	}
	svc := service.NewAnalyticsService(mock)
	r := setupRouter(t, svc)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/api/v1/stats/realtime", nil)
	r.ServeHTTP(w, req)

	var resp struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	assert.NotEqual(t, 0, resp.Code)
	assert.Contains(t, resp.Message, "database down")
}

func TestAnalyticsHandler_Realtime_TraceIDPropagated(t *testing.T) {
	mock := service.NewMockQuerier()
	mock.RealtimeOverviewFn = func(ctx context.Context, window int) (*model.RealtimeOverview, error) {
		return &model.RealtimeOverview{}, nil
	}
	svc := service.NewAnalyticsService(mock)
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(middleware.TraceID(), middleware.Recover())
	h := NewAnalyticsHandler(svc)
	api := r.Group("/api/v1")
	h.Register(api)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/api/v1/stats/realtime", nil)
	req.Header.Set("X-Trace-ID", "custom-trace-123")
	r.ServeHTTP(w, req)

	assert.Equal(t, "custom-trace-123", w.Header().Get("X-Trace-ID"))
}

func TestAnalyticsHandler_Funnel(t *testing.T) {
	mock := service.NewMockQuerier()
	mock.FunnelFn = func(ctx context.Context, start, end time.Time, window int) ([]model.FunnelStep, error) {
		return []model.FunnelStep{
			{Step: "impression", Count: 1000, Rate: 1.0},
			{Step: "click", Count: 50, Rate: 0.05},
		}, nil
	}
	svc := service.NewAnalyticsService(mock)
	r := setupRouter(t, svc)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/api/v1/stats/funnel?window=1800", nil)
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
}

func TestAnalyticsHandler_Campaigns_InvalidRange(t *testing.T) {
	mock := service.NewMockQuerier()
	svc := service.NewAnalyticsService(mock)
	r := setupRouter(t, svc)

	// start >= end
	w := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/api/v1/stats/campaigns?start=2026-07-08%2012:00:00&end=2026-07-08%2010:00:00", nil)
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code) // Gin 的 status 总是 200，业务 code != 0
	var resp struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	assert.NotEqual(t, 0, resp.Code)
	assert.Contains(t, resp.Message, "start must be before end")
}

func TestAnalyticsHandler_TopAds_SortParam(t *testing.T) {
	tests := []struct {
		query string
		want  string // 期望 Service 层接收到的 sortBy
	}{
		{"?sort=revenue", "revenue"},
		{"?sort=clicks", "clicks"},
		{"?sort=DROP_TABLE", "impressions"}, // 非法值被替换
		{"", "impressions"},
	}

	for _, tt := range tests {
		t.Run(tt.query, func(t *testing.T) {
			mock := service.NewMockQuerier()
			var captured string
			mock.TopAdsFn = func(ctx context.Context, start, end time.Time, sortBy string, limit int) ([]model.AdStat, error) {
				captured = sortBy
				return nil, nil
			}
			svc := service.NewAnalyticsService(mock)
			r := setupRouter(t, svc)

			w := httptest.NewRecorder()
			url := "/api/v1/stats/top-ads" + tt.query
			req, _ := http.NewRequest("GET", url, nil)
			r.ServeHTTP(w, req)

			assert.Equal(t, http.StatusOK, w.Code)
			assert.Equal(t, tt.want, captured)
		})
	}
}

func TestAnalyticsHandler_Retention_BadDateFormat(t *testing.T) {
	mock := service.NewMockQuerier()
	svc := service.NewAnalyticsService(mock)
	r := setupRouter(t, svc)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/api/v1/stats/retention?date=2026/07/01", nil)
	r.ServeHTTP(w, req)

	var resp struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	assert.NotEqual(t, 0, resp.Code)
	assert.Contains(t, resp.Message, "YYYY-MM-DD")
}

func TestAnalyticsHandler_Health(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.GET("/health", func(c *gin.Context) {
		c.JSON(200, gin.H{"status": "ok"})
	})

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/health", nil)
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Contains(t, w.Body.String(), "ok")
}

func TestAnalyticsHandler_Compare(t *testing.T) {
	mock := service.NewMockQuerier()
	mock.CompareWithLastPeriodFn = func(ctx context.Context, cStart, cEnd, lStart, lEnd time.Time) (*model.RealtimeOverview, *model.RealtimeOverview, error) {
		assert.True(t, lEnd.Before(cStart))
		return &model.RealtimeOverview{Window: "current"}, &model.RealtimeOverview{Window: "last"}, nil
	}
	svc := service.NewAnalyticsService(mock)
	r := setupRouter(t, svc)

	w := httptest.NewRecorder()
	end := time.Now().Format("2006-01-02 15:04:05")
	start := time.Now().Add(-24 * time.Hour).Format("2006-01-02 15:04:05")
	url := "/api/v1/stats/compare?start=" + start + "&end=" + end
	req, _ := http.NewRequest("GET", url, nil)
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var resp struct {
		Data map[string]interface{} `json:"data"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	require.Contains(t, resp.Data, "current")
	require.Contains(t, resp.Data, "last")
}

func TestAnalyticsHandler_DefaultParams(t *testing.T) {
	mock := service.NewMockQuerier()
	var capturedLimit int
	mock.CampaignStatsFn = func(ctx context.Context, start, end time.Time, limit int) ([]model.CampaignStat, error) {
		capturedLimit = limit
		return nil, nil
	}
	svc := service.NewAnalyticsService(mock)
	r := setupRouter(t, svc)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/api/v1/stats/campaigns", nil)
	r.ServeHTTP(w, req)

	assert.Equal(t, 20, capturedLimit, "未传 limit 应使用默认值 20")
}