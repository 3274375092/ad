package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"ad-platform/internal/config"
	"ad-platform/internal/model"
	"ad-platform/pkg/kafka"

	"github.com/google/uuid"
	kgo "github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

var (
	campaigns = []string{"camp_1001", "camp_1002", "camp_1003", "camp_1004", "camp_1005"}
	adIDs     = []string{"ad_001", "ad_002", "ad_003", "ad_004", "ad_005", "ad_006", "ad_007", "ad_008"}
	devices   = []string{"mobile", "pc", "tablet"}
	osList    = []string{"iOS", "Android", "Windows", "macOS", "HarmonyOS"}
	regions   = []string{"北京", "上海", "广州", "深圳", "杭州", "成都", "武汉", "南京", "西安", "重庆", "苏州", "天津"}
	citiesMap = map[string][]string{
		"北京": {"北京"}, "上海": {"上海"}, "广州": {"广州"}, "深圳": {"深圳"},
		"杭州": {"杭州"}, "成都": {"成都"}, "武汉": {"武汉"}, "南京": {"南京"},
		"西安": {"西安"}, "重庆": {"重庆"}, "苏州": {"苏州"}, "天津": {"天津"},
	}
	advertisers = map[string]string{
		"camp_1001": "adv_001",
		"camp_1002": "adv_002",
		"camp_1003": "adv_003",
		"camp_1004": "adv_004",
		"camp_1005": "adv_005",
	}
)

func main() {
	qps := flag.Int("qps", 100, "每秒事件数")
	duration := flag.Duration("duration", 0, "运行时长，0=无限")
	batchSize := flag.Int("batch", 100, "批量大小（攒批后发送）")
	configPath := flag.String("config", "./configs/config.yaml", "配置文件")
	flag.Parse()

	cfg, err := config.Load(*configPath)
	if err != nil {
		fmt.Printf("load config failed: %v\n", err)
		os.Exit(1)
	}

	zapLogger, _ := zap.NewProduction()
	defer zapLogger.Sync()

	writer := kafka.NewWriter(&cfg.Kafka)
	defer writer.Close()

	rand.Seed(time.Now().UnixNano())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	var endTime time.Time
	if *duration > 0 {
		endTime = time.Now().Add(*duration)
	}

	zapLogger.Info("producer started",
		zap.Int("qps", *qps),
		zap.Int("batch", *batchSize),
		zap.Duration("duration", *duration),
		zap.Strings("brokers", cfg.Kafka.Brokers),
		zap.String("topic", cfg.Kafka.Topic),
	)

	var totalSent int64
	startTime := time.Now()

	// 用定时器触发攒批
	ticker := time.NewTicker(time.Second / time.Duration(*qps))
	defer ticker.Stop()

	batch := make([]model.AdEvent, 0, *batchSize)
	flushTicker := time.NewTicker(500 * time.Millisecond)
	defer flushTicker.Stop()

	flush := func() {
		if len(batch) == 0 {
			return
		}
		msgs := make([]kgo.Message, 0, len(batch))
		for _, e := range batch {
			body, _ := json.Marshal(e)
			msgs = append(msgs, kgo.Message{
				Key:   []byte(e.UserID),
				Value: body,
				Time:  e.EventTime,
			})
		}
		if err := writer.WriteMessages(ctx, msgs...); err != nil {
			zapLogger.Error("flush batch failed", zap.Error(err))
			return
		}
		atomic.AddInt64(&totalSent, int64(len(batch)))
		batch = batch[:0]
	}

loop:
	for {
		select {
		case <-quit:
			flush()
			zapLogger.Info("producer shutting down",
				zap.Int64("total_sent", atomic.LoadInt64(&totalSent)),
				zap.Duration("cost", time.Since(startTime)),
			)
			break loop

		case <-flushTicker.C:
			flush()

		case <-ticker.C:
			if !endTime.IsZero() && time.Now().After(endTime) {
				flush()
				zapLogger.Info("producer finished by duration",
					zap.Int64("total_sent", atomic.LoadInt64(&totalSent)),
					zap.Duration("cost", time.Since(startTime)),
				)
				break loop
			}
			batch = append(batch, generateEvent())
			if len(batch) >= *batchSize {
				flush()
			}
		}
	}
}

// generateEvent 生成模拟广告事件
// 漏斗：impression → click (CTR 5%) → conversion (CVR 3%)
func generateEvent() model.AdEvent {
	now := time.Now().In(time.FixedZone("CST", 8*3600))

	region := regions[rand.Intn(len(regions))]
	cities := citiesMap[region]
	city := cities[rand.Intn(len(cities))]

	campaign := campaigns[rand.Intn(len(campaigns))]
	adID := adIDs[rand.Intn(len(adIDs))]
	device := devices[rand.Intn(len(devices))]
	osName := osList[rand.Intn(len(osList))]
	userID := fmt.Sprintf("u_%d", rand.Intn(50000)+1)

	// 漏斗概率
	roll := rand.Float64()
	var eventType string
	var cost, revenue float64
	switch {
	case roll < 0.92: // 92% 曝光
		eventType = "impression"
		cost = float64(rand.Intn(50) + 10) / 1000.0 // 0.01 ~ 0.06
	case roll < 0.97: // 5% 点击
		eventType = "click"
		cost = float64(rand.Intn(200) + 50) / 1000.0 // 0.05 ~ 0.25
	default: // 3% 转化
		eventType = "conversion"
		revenue = float64(rand.Intn(50000) + 5000) / 1000.0 // 5 ~ 55
	}

	return model.AdEvent{
		EventID:      uuid.NewString(),
		EventTime:    now,
		EventType:    eventType,
		AdID:         adID,
		CampaignID:   campaign,
		AdvertiserID: advertisers[campaign],
		UserID:       userID,
		Device:       device,
		OS:           osName,
		Region:       region,
		City:         city,
		Cost:         cost,
		Revenue:      revenue,
	}
}