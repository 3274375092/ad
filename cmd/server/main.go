package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"ad-platform/internal/config"
	"ad-platform/internal/handler"
	"ad-platform/internal/middleware"
	"ad-platform/internal/repository"
	"ad-platform/internal/service"
	"ad-platform/pkg/clickhouse"
	"ad-platform/pkg/logger"

	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

func main() {
	// 1. 加载配置
	configPath := os.Getenv("CONFIG_PATH")
	if configPath == "" {
		configPath = "./configs/config.yaml"
	}
	cfg, err := config.Load(configPath)
	if err != nil {
		fmt.Printf("load config failed: %v\n", err)
		os.Exit(1)
	}

	// 2. 初始化日志
	if err := logger.Init(&cfg.Log); err != nil {
		fmt.Printf("init logger failed: %v\n", err)
		os.Exit(1)
	}
	defer logger.Sync()

	logger.L.Info("server starting",
		zap.String("app", cfg.App.Name),
		zap.String("version", cfg.App.Version),
		zap.String("env", cfg.App.Env),
	)

	// 3. 初始化 ClickHouse
	if err := clickhouse.Init(&cfg.ClickHouse); err != nil {
		logger.L.Fatal("init clickhouse failed", zap.Error(err))
	}
	defer clickhouse.Close()

	// 4. 装配各层
	eventRepo := repository.NewEventRepository(clickhouse.DB())
	var _ service.AnalyticsQuerier = eventRepo // 编译期断言：repo 必须满足接口
	analyticsSvc := service.NewAnalyticsService(eventRepo)
	analyticsHandler := handler.NewAnalyticsHandler(analyticsSvc)

	// 5. Gin 引擎
	gin.SetMode(cfg.Server.Mode)
	r := gin.New()
	r.Use(middleware.TraceID(), middleware.AccessLog(), middleware.Recover())

	corsConfig := cors.DefaultConfig()
	corsConfig.AllowAllOrigins = true
	corsConfig.AllowHeaders = []string{"Origin", "Content-Type", "Authorization", "X-Trace-ID"}
	corsConfig.AllowMethods = []string{"GET", "POST", "PUT", "DELETE", "OPTIONS"}
	r.Use(cors.New(corsConfig))

	// 健康检查
	r.GET("/health", func(c *gin.Context) {
		c.JSON(200, gin.H{"status": "ok", "ts": time.Now().Unix()})
	})

	r.StaticFile("/", "./web/index.html")
	r.Static("/static", "./web")

	// API 路由
	api := r.Group("/api/v1")
	analyticsHandler.Register(api)

	// 6. 启动服务
	addr := fmt.Sprintf("%s:%d", cfg.Server.Host, cfg.Server.Port)
	srv := &http.Server{
		Addr:         addr,
		Handler:      r,
		ReadTimeout:  time.Duration(cfg.Server.ReadTimeout) * time.Second,
		WriteTimeout: time.Duration(cfg.Server.WriteTimeout) * time.Second,
	}

	go func() {
		logger.L.Info("http server listening", zap.String("addr", addr))
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.L.Fatal("listen failed", zap.Error(err))
		}
	}()

	// 7. 优雅退出
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	logger.L.Info("shutting down server...")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		logger.L.Fatal("server forced shutdown", zap.Error(err))
	}
	logger.L.Info("server exited")
}