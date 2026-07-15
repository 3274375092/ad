package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"net/http"
	"net/http/pprof"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"ad-platform/internal/config"
	"ad-platform/internal/handler"
	"ad-platform/internal/middleware"
	"ad-platform/internal/model"
	"ad-platform/internal/repository"
	"ad-platform/internal/service"
	"ad-platform/pkg/clickhouse"
	"ad-platform/pkg/kafka"
	"ad-platform/pkg/logger"

	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	kgo "github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

func main() {
	configPath := os.Getenv("CONFIG_PATH")
	if configPath == "" {
		configPath = "./configs/config.yaml"
	}
	cfg, err := config.Load(configPath)
	if err != nil {
		fmt.Printf("load config failed: %v\n", err)
		os.Exit(1)
	}

	if err := logger.Init(&cfg.Log); err != nil {
		fmt.Printf("init logger failed: %v\n", err)
		os.Exit(1)
	}
	defer logger.Sync()

	qps := flag.Int("qps", 100, "mock data qps")
	flag.Parse()

	logger.L.Info("all-in-one starting", zap.String("app", cfg.App.Name))

	if err := clickhouse.Init(&cfg.ClickHouse); err != nil {
		logger.L.Fatal("clickhouse init failed", zap.Error(err))
	}
	defer clickhouse.Close()

	repo := repository.NewEventRepository(clickhouse.DB())
	svc := service.NewAnalyticsService(repo)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 1. 启动 Kafka 消费者
	go runConsumer(ctx, cfg, repo)

	// 2. 启动 Mock 生产者
	go runProducer(ctx, cfg, *qps)

	// 3. 启动 HTTP API
	go runServer(cfg, svc)

	// 等待退出
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	logger.L.Info("shutting down...")
	cancel()
	time.Sleep(time.Second)
}

func runConsumer(ctx context.Context, cfg *config.Config, repo *repository.EventRepository) {
	reader := kafka.NewReader(&cfg.Kafka)
	defer reader.Close()

	batchSize := cfg.Kafka.BatchSize
	batchTimeout := time.Duration(cfg.Kafka.BatchTimeoutMs) * time.Millisecond
	batch := make([]model.AdEvent, 0, batchSize)

	ticker := time.NewTicker(batchTimeout)
	defer ticker.Stop()

	var total int64
	flush := func() {
		if len(batch) == 0 {
			return
		}
		toFlush := batch
		batch = batch[:0]
		if err := repo.BatchInsert(context.Background(), toFlush); err != nil {
			logger.L.Error("batch insert failed", zap.Error(err))
			return
		}
		atomic.AddInt64(&total, int64(len(toFlush)))
	}

	logger.L.Info("consumer started (embedded)")

	for {
		select {
		case <-ctx.Done():
			flush()
			logger.L.Info("consumer stopped", zap.Int64("total", total))
			return
		case <-ticker.C:
			flush()
		default:
		}

		fCtx, cancel := context.WithTimeout(ctx, time.Second)
		msg, err := reader.FetchMessage(fCtx)
		cancel()
		if err != nil {
			continue
		}

		var ev model.AdEvent
		if err := json.Unmarshal(msg.Value, &ev); err != nil {
			_ = reader.CommitMessages(context.Background(), msg)
			continue
		}

		batch = append(batch, ev)
		if len(batch) >= batchSize {
			flush()
		}
		_ = reader.CommitMessages(context.Background(), msg)
	}
}

var (
	campaigns   = []string{"camp_1001", "camp_1002", "camp_1003", "camp_1004", "camp_1005"}
	campWeights = []int{35, 25, 20, 12, 8}
	campCTR     = map[string]float64{
		"camp_1001": 0.035, "camp_1002": 0.05, "camp_1003": 0.04, "camp_1004": 0.06, "camp_1005": 0.025,
	}
	campCVR = map[string]float64{
		"camp_1001": 0.025, "camp_1002": 0.04, "camp_1003": 0.03, "camp_1004": 0.045, "camp_1005": 0.02,
	}
	adIDs       = []string{"ad_001", "ad_002", "ad_003", "ad_004", "ad_005", "ad_006", "ad_007", "ad_008"}
	devices     = []string{"mobile", "mobile", "mobile", "mobile", "mobile", "mobile", "pc", "pc", "tablet"}
	osList      = []string{"iOS", "iOS", "iOS", "iOS", "Android", "Android", "Android", "Android", "Android", "HarmonyOS"}
	regions     = []string{"Beijing", "Shanghai", "Guangzhou", "Shenzhen", "Hangzhou", "Chengdu", "Wuhan", "Nanjing", "Xian", "Chongqing", "Suzhou", "Tianjin"}
	regWeights  = []int{18, 16, 14, 12, 10, 8, 6, 5, 4, 3, 2, 2}
	advertisers = map[string]string{
		"camp_1001": "adv_001", "camp_1002": "adv_002", "camp_1003": "adv_003",
		"camp_1004": "adv_004", "camp_1005": "adv_005",
	}
)

func runProducer(ctx context.Context, cfg *config.Config, qps int) {
	writer := kafka.NewWriter(&cfg.Kafka)
	defer writer.Close()

	rand.Seed(time.Now().UnixNano())
	interval := time.Second / time.Duration(qps)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	batch := make([]kgo.Message, 0, 100)
	total := int64(0)

	// 启动时回填最近 24 小时历史数据
	logger.L.Info("backfilling history...")
	backfillBatch := make([]kgo.Message, 0, 500)
	bc := 0
	base := time.Now()
	for h := 23; h >= 0; h-- {
		for i := 0; i < qps*30; i++ {
			sec := rand.Intn(3600)
			for _, ev := range genEventBatchAt(base.Add(-time.Duration(h)*time.Hour).Add(time.Duration(sec)*time.Second)) {
				body, _ := json.Marshal(ev)
				backfillBatch = append(backfillBatch, kgo.Message{Key: []byte(ev.UserID), Value: body})
				bc++
				if len(backfillBatch) >= 500 {
					_ = writer.WriteMessages(context.Background(), backfillBatch...)
					backfillBatch = backfillBatch[:0]
				}
			}
		}
	}
	if len(backfillBatch) > 0 {
		_ = writer.WriteMessages(context.Background(), backfillBatch...)
	}
	logger.L.Info("backfill done", zap.Int("events", bc))

	logger.L.Info("producer started (embedded)", zap.Int("qps", qps))

	for {
		select {
		case <-ctx.Done():
			if len(batch) > 0 {
				_ = writer.WriteMessages(context.Background(), batch...)
			}
			logger.L.Info("producer stopped", zap.Int64("total", total))
			return
		case <-ticker.C:
			for _, ev := range genEventBatch(1) {
				body, _ := json.Marshal(ev)
				batch = append(batch, kgo.Message{Key: []byte(ev.UserID), Value: body})
				if len(batch) >= 100 {
					break
				}
			}
			if len(batch) >= 100 {
				if err := writer.WriteMessages(context.Background(), batch...); err != nil {
					logger.L.Error("producer write failed", zap.Error(err))
				}
				atomic.AddInt64(&total, int64(len(batch)))
				batch = batch[:0]
			}
		}
	}
}

func weightPick(items []string, weights []int) string {
	total := 0
	for _, w := range weights { total += w }
	r := rand.Intn(total)
	for i, w := range weights {
		r -= w
		if r < 0 { return items[i] }
	}
	return items[len(items)-1]
}

func genEventBatch(n int) []model.AdEvent {
	var secondsAgo int
	if rand.Float64() < 0.9 {
		secondsAgo = rand.Intn(21600)
	} else {
		secondsAgo = rand.Intn(64800) + 21600
	}
	return genEventBatchAt(time.Now().Add(-time.Duration(secondsAgo)*time.Second))
}

func genEventBatchAt(at time.Time) []model.AdEvent {
	region := weightPick(regions, regWeights)
	campaign := weightPick(campaigns, campWeights)
	adID := adIDs[rand.Intn(len(adIDs))]
	device := devices[rand.Intn(len(devices))]
	osName := osList[rand.Intn(len(osList))]

	events := make([]model.AdEvent, 0, 3)

	userID := fmt.Sprintf("u_%d", rand.Intn(50000)+1)
	ts := at

	imp := model.AdEvent{
			EventID:      fmt.Sprintf("evt_imp_%d_%d", ts.UnixNano(), rand.Intn(999999)),
			EventTime:    ts,
			EventType:    "impression",
			AdID:         adID,
			CampaignID:   campaign,
			AdvertiserID: advertisers[campaign],
			UserID:       userID,
			Device:       device,
			OS:           osName,
			Region:       region,
			City:         region,
			Cost:         float64(rand.Intn(50)+10) / 10000.0,
		}
		events = append(events, imp)

		if rand.Float64() < campCTR[campaign] {
			ts = ts.Add(time.Duration(rand.Intn(300)) * time.Second)
			click := imp
			click.EventID = fmt.Sprintf("evt_clk_%d_%d", ts.UnixNano(), rand.Intn(999999))
			click.EventTime = ts
			click.EventType = "click"
			click.Cost = float64(rand.Intn(200)+50) / 10000.0
			events = append(events, click)

			if rand.Float64() < campCVR[campaign] {
				ts = ts.Add(time.Duration(rand.Intn(600)) * time.Second)
				conv := click
				conv.EventID = fmt.Sprintf("evt_cnv_%d_%d", ts.UnixNano(), rand.Intn(999999))
				conv.EventTime = ts
				conv.EventType = "conversion"
				conv.Cost = 0
				conv.Revenue = float64(rand.Intn(50000)+5000) / 100.0
				events = append(events, conv)
			}
		}
	return events
}

func runServer(cfg *config.Config, svc *service.AnalyticsService) {
	gin.SetMode(cfg.Server.Mode)
	r := gin.New()
	r.Use(middleware.TraceID(), middleware.AccessLog(), middleware.Recover())

	r.Use(cors.New(cors.Config{
		AllowAllOrigins: true,
		AllowHeaders:    []string{"Origin", "Content-Type", "Authorization", "X-Trace-ID"},
		AllowMethods:    []string{"GET", "POST", "PUT", "DELETE", "OPTIONS"},
	}))

	r.GET("/health", func(c *gin.Context) {
		c.JSON(200, gin.H{"status": "ok", "ts": time.Now().Unix()})
	})

	r.StaticFile("/", "./web/index.html")
	r.Static("/static", "./web")

	r.GET("/debug/pprof/", gin.WrapF(pprof.Index))
	r.GET("/debug/pprof/cmdline", gin.WrapF(pprof.Cmdline))
	r.GET("/debug/pprof/profile", gin.WrapF(pprof.Profile))
	r.POST("/debug/pprof/symbol", gin.WrapF(pprof.Symbol))
	r.GET("/debug/pprof/symbol", gin.WrapF(pprof.Symbol))
	r.GET("/debug/pprof/trace", gin.WrapF(pprof.Trace))
	r.GET("/debug/pprof/heap", gin.WrapH(pprof.Handler("heap")))
	r.GET("/debug/pprof/goroutine", gin.WrapH(pprof.Handler("goroutine")))
	r.GET("/debug/pprof/block", gin.WrapH(pprof.Handler("block")))
	r.GET("/debug/pprof/mutex", gin.WrapH(pprof.Handler("mutex")))
	r.GET("/debug/pprof/allocs", gin.WrapH(pprof.Handler("allocs")))
	r.GET("/debug/pprof/threadcreate", gin.WrapH(pprof.Handler("threadcreate")))

	h := handler.NewAnalyticsHandler(svc)
	h.Register(r.Group("/api/v1"))

	addr := fmt.Sprintf("%s:%d", cfg.Server.Host, cfg.Server.Port)
	logger.L.Info("api server listening", zap.String("addr", addr))

	srv := &http.Server{
		Addr:         addr,
		Handler:      r,
		ReadTimeout:  time.Duration(cfg.Server.ReadTimeout) * time.Second,
		WriteTimeout: time.Duration(cfg.Server.WriteTimeout) * time.Second,
	}

	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		logger.L.Fatal("server failed", zap.Error(err))
	}
}