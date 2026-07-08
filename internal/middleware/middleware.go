package middleware

import (
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"go.uber.org/zap"

	"ad-platform/pkg/logger"
)

// TraceID 注入 trace_id
func TraceID() gin.HandlerFunc {
	return func(c *gin.Context) {
		traceID := c.GetHeader("X-Trace-ID")
		if traceID == "" {
			traceID = uuid.NewString()
		}
		c.Set("trace_id", traceID)
		c.Writer.Header().Set("X-Trace-ID", traceID)
		c.Next()
	}
}

// AccessLog 请求日志
func AccessLog() gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		path := c.Request.URL.Path
		query := c.Request.URL.RawQuery

		c.Next()

		if logger.L == nil {
			return
		}

		cost := time.Since(start)
		status := c.Writer.Status()

		fields := []zap.Field{
			zap.String("trace_id", c.GetString("trace_id")),
			zap.String("method", c.Request.Method),
			zap.String("path", path),
			zap.String("query", query),
			zap.Int("status", status),
			zap.Duration("cost", cost),
			zap.String("ip", c.ClientIP()),
		}

		if status >= 500 {
			logger.L.Error("http", fields...)
		} else if status >= 400 {
			logger.L.Warn("http", fields...)
		} else {
			logger.L.Info("http", fields...)
		}
	}
}

// Recover panic 恢复
func Recover() gin.HandlerFunc {
	return func(c *gin.Context) {
		defer func() {
			if err := recover(); err != nil {
				if logger.L != nil {
					logger.L.Error("panic recovered",
						zap.Any("error", err),
						zap.String("path", c.Request.URL.Path),
					)
				}
				c.AbortWithStatusJSON(500, gin.H{
					"code":    500,
					"message": "internal server error",
				})
			}
		}()
		c.Next()
	}
}