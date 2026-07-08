package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"ad-platform/internal/config"
	"ad-platform/internal/model"
	"ad-platform/internal/repository"
	"ad-platform/pkg/clickhouse"
	"ad-platform/pkg/kafka"

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

	logger, _ := zap.NewProduction()
	defer logger.Sync()

	// 初始化 ClickHouse
	if err := clickhouse.Init(&cfg.ClickHouse); err != nil {
		logger.Fatal("init clickhouse failed", zap.Error(err))
	}
	defer clickhouse.Close()

	repo := repository.NewEventRepository(clickhouse.DB())

	reader := kafka.NewReader(&cfg.Kafka)
	defer reader.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	logger.Info("consumer started",
		zap.Strings("brokers", cfg.Kafka.Brokers),
		zap.String("topic", cfg.Kafka.Topic),
		zap.String("group", cfg.Kafka.GroupID),
	)

	// 攒批配置
	batchSize := cfg.Kafka.BatchSize
	batchTimeout := time.Duration(cfg.Kafka.BatchTimeoutMs) * time.Millisecond

	var (
		batch   []model.AdEvent
		batchMu sync.Mutex
	)

	// flush 工作协程
	go func() {
		ticker := time.NewTicker(batchTimeout)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				batchMu.Lock()
				if len(batch) == 0 {
					batchMu.Unlock()
					continue
				}
				toFlush := batch
				batch = nil
				batchMu.Unlock()

				if err := repo.BatchInsert(context.Background(), toFlush); err != nil {
					logger.Error("batch insert failed", zap.Error(err), zap.Int("size", len(toFlush)))
				} else {
					logger.Info("batch inserted", zap.Int("size", len(toFlush)))
				}
			}
		}
	}()

	var totalConsumed int64
	for {
		select {
		case <-quit:
			cancel()
			// 最后再 flush 一次
			batchMu.Lock()
			toFlush := batch
			batch = nil
			batchMu.Unlock()
			if len(toFlush) > 0 {
				if err := repo.BatchInsert(context.Background(), toFlush); err != nil {
					logger.Error("final flush failed", zap.Error(err))
				}
			}
			logger.Info("consumer stopped", zap.Int64("total", totalConsumed))
			return
		default:
		}

		// 单条读取（攒批后批量写入 CH）
		fetchCtx, fCancel := context.WithTimeout(ctx, 5*time.Second)
		msg, err := reader.FetchMessage(fetchCtx)
		fCancel()
		if err != nil {
			if ctx.Err() != nil {
				continue
			}
			logger.Warn("fetch message failed", zap.Error(err))
			time.Sleep(time.Second)
			continue
		}

		var ev model.AdEvent
		if err := json.Unmarshal(msg.Value, &ev); err != nil {
			logger.Warn("unmarshal event failed", zap.Error(err), zap.ByteString("raw", msg.Value))
			_ = reader.CommitMessages(context.Background(), msg)
			continue
		}

		batchMu.Lock()
		batch = append(batch, ev)
		shouldFlush := len(batch) >= batchSize
		if shouldFlush {
			toFlush := batch
			batch = nil
			batchMu.Unlock()

			if err := repo.BatchInsert(context.Background(), toFlush); err != nil {
				logger.Error("batch insert failed", zap.Error(err), zap.Int("size", len(toFlush)))
				// 失败不提交 offset，等下次重试
				continue
			}
			if err := reader.CommitMessages(context.Background(), msg); err != nil {
				logger.Error("commit failed", zap.Error(err))
			}
			totalConsumed += int64(len(toFlush))
			logger.Debug("flushed", zap.Int64("total", totalConsumed))
		} else {
			batchMu.Unlock()
			// 攒批期间每条都先 commit offset 防止重复消费（幂等设计）
			_ = reader.CommitMessages(context.Background(), msg)
			totalConsumed++
		}
	}
}