package config

import (
	"fmt"
	"strings"

	"github.com/spf13/viper"
)

type AppConfig struct {
	Name    string `mapstructure:"name"`
	Env     string `mapstructure:"env"`
	Version string `mapstructure:"version"`
}

type ServerConfig struct {
	Host         string `mapstructure:"host"`
	Port         int    `mapstructure:"port"`
	Mode         string `mapstructure:"mode"`
	ReadTimeout  int    `mapstructure:"read_timeout"`
	WriteTimeout int    `mapstructure:"write_timeout"`
}

type ClickHouseConfig struct {
	Addr          string `mapstructure:"addr"`
	HTTPAddr      string `mapstructure:"http_addr"`
	Database      string `mapstructure:"database"`
	Username      string `mapstructure:"username"`
	Password      string `mapstructure:"password"`
	MaxOpenConns  int    `mapstructure:"max_open_conns"`
	MaxIdleConns  int    `mapstructure:"max_idle_conns"`
	DialTimeout   int    `mapstructure:"dial_timeout"`
	ReadTimeout   int    `mapstructure:"read_timeout"`
	WriteTimeout  int    `mapstructure:"write_timeout"`
}

type KafkaConfig struct {
	Brokers           []string `mapstructure:"brokers"`
	Topic             string   `mapstructure:"topic"`
	GroupID           string   `mapstructure:"group_id"`
	NumPartitions     int      `mapstructure:"num_partitions"`
	ReplicationFactor int      `mapstructure:"replication_factor"`
	BatchSize         int      `mapstructure:"batch_size"`
	BatchTimeoutMs    int      `mapstructure:"batch_timeout_ms"`
}

type LogConfig struct {
	Level    string `mapstructure:"level"`
	Format   string `mapstructure:"format"`
	Output   string `mapstructure:"output"`
	FilePath string `mapstructure:"file_path"`
}

type Config struct {
	App        AppConfig        `mapstructure:"app"`
	Server     ServerConfig     `mapstructure:"server"`
	ClickHouse ClickHouseConfig `mapstructure:"clickhouse"`
	Kafka      KafkaConfig      `mapstructure:"kafka"`
	Log        LogConfig        `mapstructure:"log"`
}

var globalConfig *Config

func Load(configPath string) (*Config, error) {
	v := viper.New()
	v.SetConfigFile(configPath)
	v.SetConfigType("yaml")

	v.SetEnvPrefix("ADP")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()

	if err := v.ReadInConfig(); err != nil {
		return nil, fmt.Errorf("read config failed: %w", err)
	}

	cfg := &Config{}
	if err := v.Unmarshal(cfg); err != nil {
		return nil, fmt.Errorf("unmarshal config failed: %w", err)
	}

	globalConfig = cfg
	return cfg, nil
}

func Get() *Config {
	return globalConfig
}