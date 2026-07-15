//go:build integration
// +build integration

// 集成测试：需要真实 ClickHouse 环境
// 运行方式：
//   1. cd deploy && docker-compose up -d zookeeper kafka clickhouse
//   2. docker exec -i ad-clickhouse clickhouse-client < scripts/init_clickhouse.sql
//   3. cd .. && INTEGRATION=1 go test -tags=integration ./internal/repository/...
package repository

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"ad-platform/internal/config"
	"ad-platform/internal/model"
	"ad-platform/pkg/clickhouse"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupCH(t testing.TB) (*EventRepository, *TestHelper) {
	t.Helper()
	if os.Getenv("INTEGRATION") == "" {
		t.Skip("set INTEGRATION=1 to enable integration tests")
	}

	addr := getenv("CLICKHOUSE_ADDR", "127.0.0.1:9000")
	cfg := &config.ClickHouseConfig{
		Addr:         addr,
		Database:     getenv("CLICKHOUSE_DB", "ad_platform"),
		Username:     getenv("CLICKHOUSE_USER", "default"),
		Password:     getenv("CLICKHOUSE_PASSWORD", "123456"),
		MaxOpenConns: 10,
		MaxIdleConns: 2,
		DialTimeout:  10,
		ReadTimeout:  30,
		WriteTimeout: 30,
	}

	if err := clickhouse.Init(cfg); err != nil {
		t.Skipf("ClickHouse unavailable: %v", err)
	}
	t.Cleanup(func() { _ = clickhouse.Close() })

	repo := NewEventRepository(clickhouse.DB())
	helper := NewTestHelper(clickhouse.DB())
	require.NoError(t, helper.Truncate(context.Background()))
	return repo, helper
}

func TestIntegration_BatchInsert_RealtimeOverview(t *testing.T) {
	repo, helper := setupCH(t)
	ctx := context.Background()

	now := time.Now().UTC()
	events := []model.AdEvent{
		MakeEvent("impression", "camp_1001", "u_1", "北京", now.Add(-1*time.Minute)),
		MakeEvent("impression", "camp_1001", "u_2", "上海", now.Add(-1*time.Minute)),
		MakeEvent("click", "camp_1001", "u_1", "北京", now.Add(-30*time.Second)),
		MakeEvent("conversion", "camp_1001", "u_1", "北京", now.Add(-10*time.Second)),
	}
	require.NoError(t, helper.SeedEvents(ctx, events))
	time.Sleep(500 * time.Millisecond)

	overview, err := repo.RealtimeOverview(ctx, 5)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, overview.Impressions, uint64(2))
	assert.GreaterOrEqual(t, overview.Clicks, uint64(1))
	assert.GreaterOrEqual(t, overview.Conversions, uint64(1))
	t.Logf("Overview: %+v", overview)
}

func TestIntegration_Funnel(t *testing.T) {
	repo, helper := setupCH(t)
	ctx := context.Background()

	now := time.Now().UTC()
	events := []model.AdEvent{
		MakeEvent("impression", "camp_1001", "u_1", "北京", now.Add(-3*time.Second)),
		MakeEvent("click", "camp_1001", "u_1", "北京", now.Add(-2*time.Second)),
		MakeEvent("conversion", "camp_1001", "u_1", "北京", now.Add(-1*time.Second)),
		MakeEvent("impression", "camp_1001", "u_2", "上海", now.Add(-3*time.Second)),
	}
	require.NoError(t, helper.SeedEvents(ctx, events))
	time.Sleep(500 * time.Millisecond)

	steps, err := repo.Funnel(ctx, now.Add(-time.Minute), now, 3600)
	require.NoError(t, err)
	require.Len(t, steps, 3)
	assert.GreaterOrEqual(t, steps[0].Count, uint64(2))
	assert.GreaterOrEqual(t, steps[1].Count, uint64(1))
	assert.GreaterOrEqual(t, steps[2].Count, uint64(1))
	t.Logf("Funnel: %+v", steps)
}

func TestIntegration_CampaignStats(t *testing.T) {
	repo, helper := setupCH(t)
	ctx := context.Background()

	now := time.Now().UTC()
	for i := 0; i < 100; i++ {
		events := []model.AdEvent{
			MakeEvent("impression", "camp_1001", fmt.Sprintf("u_%d", i), "北京", now),
			MakeEvent("impression", "camp_1002", fmt.Sprintf("u_%d", i+100), "上海", now),
		}
		require.NoError(t, helper.SeedEvents(ctx, events))
	}

	stats, err := repo.CampaignStats(ctx, now.Add(-time.Hour), now.Add(time.Hour), 10)
	require.NoError(t, err)
	assert.NotEmpty(t, stats)
	for _, s := range stats {
		assert.NotEmpty(t, s.CampaignID)
		t.Logf("Campaign %s: PV=%d UV=%d CTR=%.2f%% Cost=%.2f Revenue=%.2f ROI=%.2f",
			s.CampaignID, s.Impressions, s.UV, s.CTR*100, s.Cost, s.Revenue, s.ROI)
	}
}

func TestIntegration_HourlyTrend(t *testing.T) {
	repo, helper := setupCH(t)
	ctx := context.Background()

	now := time.Now().UTC()
	for i := 0; i < 50; i++ {
		ts := now.Add(-time.Duration(i) * time.Minute)
		require.NoError(t, helper.SeedEvents(ctx, []model.AdEvent{
			MakeEvent("impression", "camp_1001", fmt.Sprintf("u_%d", i), "北京", ts),
		}))
	}

	trend, err := repo.HourlyTrend(ctx, 2)
	require.NoError(t, err)
	assert.NotEmpty(t, trend)
}

func TestIntegration_RegionDistribution(t *testing.T) {
	repo, helper := setupCH(t)
	ctx := context.Background()

	now := time.Now().UTC()
	regions := []string{"北京", "上海", "广州", "深圳", "杭州"}
	for i, r := range regions {
		require.NoError(t, helper.SeedEvents(ctx, []model.AdEvent{
			MakeEvent("impression", "camp_1001", fmt.Sprintf("u_%d", i), r, now),
			MakeEvent("impression", "camp_1001", fmt.Sprintf("u_%d", i+10), r, now),
		}))
	}

	dist, err := repo.RegionDistribution(ctx, now.Add(-time.Hour), now.Add(time.Hour), 10)
	require.NoError(t, err)
	assert.NotEmpty(t, dist)
	for _, d := range dist {
		t.Logf("Region %s: PV=%d UV=%d", d.Region, d.Impressions, d.UV)
	}
}

// =====================================================
// 性能基准测试（需要在有数据的 CH 上跑）
// =====================================================

func printCompression(ctx context.Context, conn driver.Conn, t testing.TB) {
	t.Helper()
	type row struct {
		Name                string  `ch:"name"`
		TotalRows           uint64  `ch:"total_rows"`
		TotalBytes          uint64  `ch:"total_bytes"`
		CompressedBytes     uint64  `ch:"compressed_bytes"`
		UncompressedBytes   uint64  `ch:"uncompressed_bytes"`
		CompressionRatio    float64 `ch:"compression_ratio"`
	}
	var rows []row
	if err := conn.Select(ctx, &rows, `
		SELECT
			'raw' AS name,
			sum(rows) AS total_rows,
			sum(data_uncompressed_bytes+data_compressed_bytes) AS total_bytes,
			sum(data_compressed_bytes) AS compressed_bytes,
			sum(data_uncompressed_bytes) AS uncompressed_bytes,
			round(sum(data_uncompressed_bytes) / sum(data_compressed_bytes), 2) AS compression_ratio
		FROM system.parts
		WHERE active AND database='ad_platform' AND table='ad_events_raw'
	`); err != nil {
		t.Logf("compression stats: %v", err)
		return
	}
	for _, r := range rows {
		t.Logf("[compress] %s rows=%d total=%s compression_ratio=%.1f:1 compressed=%s uncompressed=%s",
			r.Name, r.TotalRows,
			formatBytes(r.TotalBytes),
			r.CompressionRatio,
			formatBytes(r.CompressedBytes),
			formatBytes(r.UncompressedBytes))
	}
}

func formatBytes(n uint64) string {
	const unit = 1024
	if n < unit { return fmt.Sprintf("%d B", n) }
	div, exp := uint64(unit), 0
	for n2 := n / unit; n2 >= unit; n2 /= unit { div *= unit; exp++ }
	return fmt.Sprintf("%.1f %ciB", float64(n)/float64(div), "KMGTPE"[exp])
}

func seedMillionEvents(ctx context.Context, helper *TestHelper, count int, t testing.TB) {
	batchSize := 5000
	batch := make([]model.AdEvent, 0, batchSize)
	now := time.Now().UTC()
	camps := []string{"camp_1001","camp_1002","camp_1003","camp_1004","camp_1005"}
	regions := []string{"北京","上海","广州","深圳","杭州"}
	types := []string{"impression","click","conversion"}
	for i := 0; i < count; i++ {
		ts := now.Add(-time.Duration(count-i) * time.Millisecond)
		ev := MakeEvent(types[i%3], camps[i%5], fmt.Sprintf("u_%d", i%50000), regions[i%5], ts)
		batch = append(batch, ev)
		if len(batch) >= batchSize {
			if err := helper.SeedEvents(ctx, batch); err != nil {
				t.Fatalf("seed failed: %v", err)
			}
			batch = batch[:0]
		}
	}
	if len(batch) > 0 {
		if err := helper.SeedEvents(ctx, batch); err != nil {
			t.Fatalf("seed failed: %v", err)
		}
	}
	t.Logf("seeded %d events", count)
}

func TestIntegration_StorageCompression(t *testing.T) {
	repo, helper := setupCH(t)
	ctx := context.Background()
	seedMillionEvents(ctx, helper, 200_000, t)
	printCompression(ctx, repo.conn, t)
}

func BenchmarkIntegration_BatchInsert(b *testing.B) {
	if os.Getenv("INTEGRATION") == "" {
		b.Skip("set INTEGRATION=1")
	}
	_, helper := setupCH(b)
	ctx := context.Background()
	repo := NewEventRepository(clickhouse.DB())
	now := time.Now().UTC()

	for _, batchSize := range []int{100, 500, 1000, 5000} {
		b.Run(fmt.Sprintf("batch_%d", batchSize), func(b *testing.B) {
			batch := make([]model.AdEvent, 0, batchSize)
			for i := 0; i < batchSize; i++ {
				batch = append(batch, MakeEvent("impression", "camp_1001",
					fmt.Sprintf("u_%d", i), "北京", now))
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := repo.BatchInsert(ctx, batch); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			_ = helper.Truncate(ctx)
		})
	}
}

func BenchmarkIntegration_RealtimeOverview(b *testing.B) {
	if os.Getenv("INTEGRATION") == "" {
		b.Skip("set INTEGRATION=1 to enable benchmarks")
	}
	repo, helper := setupCH(b)
	ctx := context.Background()

	// 灌入 100 万条数据
	now := time.Now().UTC()
	batchSize := 5000
	batch := make([]model.AdEvent, 0, batchSize)
	for i := 0; i < 1_000_000; i++ {
		batch = append(batch, MakeEvent("impression", "camp_1001", fmt.Sprintf("u_%d", i%10000), "北京", now))
		if len(batch) >= batchSize {
			_ = helper.SeedEvents(ctx, batch)
			batch = batch[:0]
		}
	}
	if len(batch) > 0 {
		_ = helper.SeedEvents(ctx, batch)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := repo.RealtimeOverview(ctx, 60)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}