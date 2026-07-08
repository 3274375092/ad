package logger

import (
	"os"

	"ad-platform/internal/config"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var L *zap.Logger

func ZapError(err error) zap.Field {
	return zap.Error(err)
}

func Init(cfg *config.LogConfig) error {
	level := zap.NewAtomicLevelAt(parseLevel(cfg.Level))

	encoderConfig := zapcore.EncoderConfig{
		TimeKey:        "ts",
		LevelKey:       "level",
		NameKey:        "logger",
		CallerKey:      "caller",
		MessageKey:     "msg",
		StacktraceKey:  "stacktrace",
		LineEnding:     zapcore.DefaultLineEnding,
		EncodeLevel:    zapcore.CapitalColorLevelEncoder,
		EncodeTime:     zapcore.ISO8601TimeEncoder,
		EncodeDuration: zapcore.SecondsDurationEncoder,
		EncodeCaller:   zapcore.ShortCallerEncoder,
	}

	var encoder zapcore.Encoder
	if cfg.Format == "json" {
		encoder = zapcore.NewJSONEncoder(encoderConfig)
	} else {
		encoder = zapcore.NewConsoleEncoder(encoderConfig)
	}

	var writeSyncer zapcore.WriteSyncer
	writeSyncer = zapcore.AddSync(os.Stdout)

	core := zapcore.NewCore(encoder, writeSyncer, level)
	L = zap.New(core, zap.AddCaller(), zap.AddCallerSkip(1))
	return nil
}

func Sync() error {
	if L != nil {
		return L.Sync()
	}
	return nil
}

func parseLevel(l string) zapcore.Level {
	switch l {
	case "debug":
		return zapcore.DebugLevel
	case "info":
		return zapcore.InfoLevel
	case "warn":
		return zapcore.WarnLevel
	case "error":
		return zapcore.ErrorLevel
	default:
		return zapcore.InfoLevel
	}
}