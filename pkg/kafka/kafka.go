package kafka

import (
	"context"
	"fmt"
	"time"

	"ad-platform/internal/config"

	"github.com/segmentio/kafka-go"
)

func NewWriter(cfg *config.KafkaConfig) *kafka.Writer {
	return &kafka.Writer{
		Addr:         kafka.TCP(cfg.Brokers...),
		Topic:        cfg.Topic,
		Balancer:     &kafka.Hash{},
		BatchSize:    cfg.BatchSize,
		BatchTimeout: time.Duration(cfg.BatchTimeoutMs) * time.Millisecond,
		RequiredAcks: kafka.RequireOne,
		Async:        false,
		Compression:  kafka.Snappy,
	}
}

func NewReader(cfg *config.KafkaConfig) *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:        cfg.Brokers,
		Topic:          cfg.Topic,
		GroupID:        cfg.GroupID,
		MinBytes:       1,
		MaxBytes:       10e6,
		MaxWait:        500 * time.Millisecond,
		CommitInterval: time.Second,
		StartOffset:    kafka.LastOffset,
	})
}

// CreateTopic 创建 topic（开发期使用，生产环境由运维管理）
func CreateTopic(cfg *config.KafkaConfig) error {
	conn, err := kafka.Dial("tcp", cfg.Brokers[0])
	if err != nil {
		return fmt.Errorf("dial broker failed: %w", err)
	}
	defer conn.Close()

	controller, err := conn.Controller()
	if err != nil {
		return fmt.Errorf("get controller failed: %w", err)
	}

	cConn, err := kafka.Dial("tcp", fmt.Sprintf("%s:%d", controller.Host, controller.Port))
	if err != nil {
		return fmt.Errorf("dial controller failed: %w", err)
	}
	defer cConn.Close()

	return cConn.CreateTopics(kafka.TopicConfig{
		Topic:             cfg.Topic,
		NumPartitions:     cfg.NumPartitions,
		ReplicationFactor: cfg.ReplicationFactor,
	})
}

// Ping 验证 kafka 可达
func Ping(ctx context.Context, brokers []string) error {
	if len(brokers) == 0 {
		return fmt.Errorf("no brokers configured")
	}
	conn, err := kafka.DialContext(ctx, "tcp", brokers[0])
	if err != nil {
		return err
	}
	defer conn.Close()
	_, err = conn.Brokers()
	return err
}