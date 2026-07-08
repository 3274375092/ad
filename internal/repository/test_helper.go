package repository

import (
	"context"
	"fmt"
	"time"

	"ad-platform/internal/model"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// TestHelper Repository 测试辅助方法
type TestHelper struct {
	conn driver.Conn
}

func NewTestHelper(conn driver.Conn) *TestHelper {
	return &TestHelper{conn: conn}
}

// Truncate 清空所有测试表（注意：生产环境严禁使用）
func (h *TestHelper) Truncate(ctx context.Context) error {
	tables := []string{
		"ad_events_raw",
		"ad_minute_stats",
		"ad_hourly_stats",
		"user_event_funnel",
	}
	for _, t := range tables {
		if err := h.exec(ctx, fmt.Sprintf("TRUNCATE TABLE %s", t)); err != nil {
			return fmt.Errorf("truncate %s: %w", t, err)
		}
	}
	return nil
}

func (h *TestHelper) exec(ctx context.Context, query string, args ...interface{}) error {
	rows, err := h.conn.Query(ctx, query, args...)
	if err != nil {
		return err
	}
	return rows.Close()
}

// SeedEvents 写入测试事件
func (h *TestHelper) SeedEvents(ctx context.Context, events []model.AdEvent) error {
	repo := NewEventRepository(h.conn)
	return repo.BatchInsert(ctx, events)
}

// MakeEvent 构造一个测试事件（辅助函数）
func MakeEvent(eventType, campaignID, userID, region string, ts time.Time) model.AdEvent {
	cost := 0.0
	revenue := 0.0
	switch eventType {
	case "impression":
		cost = 0.01
	case "click":
		cost = 0.10
	case "conversion":
		revenue = 10.0
	}
	return model.AdEvent{
		EventID:      fmt.Sprintf("test-%s-%d", eventType, ts.UnixNano()),
		EventTime:    ts,
		EventType:    eventType,
		AdID:         "ad_001",
		CampaignID:   campaignID,
		AdvertiserID: "adv_001",
		UserID:       userID,
		Device:       "mobile",
		OS:           "iOS",
		Region:       region,
		City:         region,
		Cost:         cost,
		Revenue:      revenue,
	}
}

// WaitForMaterialized 等待物化视图刷新（CH MV 异步，最多等 N 秒）
func (h *TestHelper) WaitForMaterialized(ctx context.Context, timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		// 触发一次强制合并
		_ = h.exec(ctx, "OPTIMIZE TABLE ad_minute_stats FINAL")
		_ = h.exec(ctx, "OPTIMIZE TABLE ad_hourly_stats FINAL")
		time.Sleep(200 * time.Millisecond)
		if time.Now().Add(200 * time.Millisecond).After(deadline) {
			break
		}
	}
}