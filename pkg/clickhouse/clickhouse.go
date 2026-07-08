package clickhouse

import (
	"context"
	"fmt"
	"time"

	"ad-platform/internal/config"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

var Conn driver.Conn

func Init(cfg *config.ClickHouseConfig) error {
	opts := &clickhouse.Options{
		Addr:         []string{cfg.Addr},
		Auth:         clickhouse.Auth{Database: cfg.Database, Username: cfg.Username, Password: cfg.Password},
		DialTimeout:  time.Duration(cfg.DialTimeout) * time.Second,
		ReadTimeout:  time.Duration(cfg.ReadTimeout) * time.Second,
		MaxOpenConns: cfg.MaxOpenConns,
		MaxIdleConns: cfg.MaxIdleConns,
		Compression: &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		},
	}

	conn, err := clickhouse.Open(opts)
	if err != nil {
		return fmt.Errorf("clickhouse open failed: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := conn.Ping(ctx); err != nil {
		return fmt.Errorf("clickhouse ping failed: %w", err)
	}

	Conn = conn
	return nil
}

func Close() error {
	if Conn != nil {
		return Conn.Close()
	}
	return nil
}

func DB() driver.Conn {
	return Conn
}