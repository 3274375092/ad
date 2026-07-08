package handler

import (
	"errors"
	"strconv"
	"time"

	"ad-platform/internal/service"
	"ad-platform/pkg/logger"
	"ad-platform/pkg/response"

	"github.com/gin-gonic/gin"
)

type AnalyticsHandler struct {
	svc *service.AnalyticsService
}

func NewAnalyticsHandler(svc *service.AnalyticsService) *AnalyticsHandler {
	return &AnalyticsHandler{svc: svc}
}

func (h *AnalyticsHandler) Register(rg *gin.RouterGroup) {
	g := rg.Group("/stats")
	{
		g.GET("/realtime", h.Realtime)
		g.GET("/hourly", h.HourlyTrend)
		g.GET("/campaigns", h.Campaigns)
		g.GET("/top-ads", h.TopAds)
		g.GET("/regions", h.Regions)
		g.GET("/devices", h.Devices)
		g.GET("/funnel", h.Funnel)
		g.GET("/retention", h.Retention)
		g.GET("/compare", h.Compare)
	}
}

func (h *AnalyticsHandler) Realtime(c *gin.Context) {
	window := atoiDefault(c.Query("window"), 5)
	data, err := h.svc.RealtimeOverview(c.Request.Context(), window)
	if err != nil {
		logError("realtime failed", err)
		response.Fail(c, 500, err.Error())
		return
	}
	response.OK(c, data)
}

func (h *AnalyticsHandler) HourlyTrend(c *gin.Context) {
	hours := atoiDefault(c.Query("hours"), 24)
	data, err := h.svc.HourlyTrend(c.Request.Context(), hours)
	if err != nil {
		logError("hourly failed", err)
		response.Fail(c, 500, err.Error())
		return
	}
	response.OK(c, data)
}

func (h *AnalyticsHandler) Campaigns(c *gin.Context) {
	start, end, err := parseRange(c)
	if err != nil {
		response.Fail(c, 400, err.Error())
		return
	}
	limit := atoiDefault(c.Query("limit"), 0)
	data, err := h.svc.CampaignStats(c.Request.Context(), start, end, limit)
	if err != nil {
		logError("campaigns failed", err)
		response.Fail(c, 500, err.Error())
		return
	}
	response.OK(c, data)
}

func (h *AnalyticsHandler) TopAds(c *gin.Context) {
	start, end, err := parseRange(c)
	if err != nil {
		response.Fail(c, 400, err.Error())
		return
	}
	sortBy := c.DefaultQuery("sort", "impressions")
	limit := atoiDefault(c.Query("limit"), 10)
	data, err := h.svc.TopAds(c.Request.Context(), start, end, sortBy, limit)
	if err != nil {
		logError("top ads failed", err)
		response.Fail(c, 500, err.Error())
		return
	}
	response.OK(c, data)
}

func (h *AnalyticsHandler) Regions(c *gin.Context) {
	start, end, err := parseRange(c)
	if err != nil {
		response.Fail(c, 400, err.Error())
		return
	}
	limit := atoiDefault(c.Query("limit"), 20)
	data, err := h.svc.RegionDistribution(c.Request.Context(), start, end, limit)
	if err != nil {
		logError("regions failed", err)
		response.Fail(c, 500, err.Error())
		return
	}
	response.OK(c, data)
}

func (h *AnalyticsHandler) Devices(c *gin.Context) {
	start, end, err := parseRange(c)
	if err != nil {
		response.Fail(c, 400, err.Error())
		return
	}
	data, err := h.svc.DeviceDistribution(c.Request.Context(), start, end)
	if err != nil {
		logError("devices failed", err)
		response.Fail(c, 500, err.Error())
		return
	}
	response.OK(c, data)
}

func (h *AnalyticsHandler) Funnel(c *gin.Context) {
	start, end, err := parseRange(c)
	if err != nil {
		response.Fail(c, 400, err.Error())
		return
	}
	window := atoiDefault(c.Query("window"), 3600)
	data, err := h.svc.Funnel(c.Request.Context(), start, end, window)
	if err != nil {
		logError("funnel failed", err)
		response.Fail(c, 500, err.Error())
		return
	}
	response.OK(c, data)
}

func (h *AnalyticsHandler) Retention(c *gin.Context) {
	dayStr := c.Query("date")
	var start time.Time
	if dayStr == "" {
		start = time.Now().AddDate(0, 0, -7)
	} else {
		t, err := time.Parse("2006-01-02", dayStr)
		if err != nil {
			response.Fail(c, 400, "date must be YYYY-MM-DD")
			return
		}
		start = t
	}
	eventType := c.DefaultQuery("event_type", "impression")
	days := atoiDefault(c.Query("days"), 7)
	data, err := h.svc.Retention(c.Request.Context(), start, eventType, days)
	if err != nil {
		logError("retention failed", err)
		response.Fail(c, 500, err.Error())
		return
	}
	response.OK(c, data)
}

func (h *AnalyticsHandler) Compare(c *gin.Context) {
	start, end, err := parseRange(c)
	if err != nil {
		response.Fail(c, 400, err.Error())
		return
	}
	cur, last, err := h.svc.CompareWithLastPeriod(c.Request.Context(), start, end)
	if err != nil {
		logError("compare failed", err)
		response.Fail(c, 500, err.Error())
		return
	}
	response.OK(c, gin.H{"current": cur, "last": last})
}

func parseRange(c *gin.Context) (time.Time, time.Time, error) {
	end := time.Now()
	if v := c.Query("end"); v != "" {
		t, err := time.Parse("2006-01-02 15:04:05", v)
		if err == nil {
			end = t
		}
	}
	start := end.AddDate(0, 0, -1)
	if v := c.Query("start"); v != "" {
		t, err := time.Parse("2006-01-02 15:04:05", v)
		if err == nil {
			start = t
		}
	}
	if !start.Before(end) {
		return time.Time{}, time.Time{}, errors.New("start must be before end")
	}
	return start, end, nil
}

func atoiDefault(s string, def int) int {
	if s == "" {
		return def
	}
	n, err := strconv.Atoi(s)
	if err != nil {
		return def
	}
	return n
}

func logError(msg string, err error) {
	if logger.L != nil {
		logger.L.Error(msg, logger.ZapError(err))
	}
}