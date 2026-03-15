package common

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
	"gopkg.in/natefinch/lumberjack.v2"
	"gorm.io/gorm/logger"
)

var Logger *zap.Logger

type traceIDKeyType struct{}

var traceIDKey = traceIDKeyType{}

// InjectTraceID 将生成的traceID注入到context中
func InjectTraceID(ctx context.Context, traceID string) context.Context {
	return context.WithValue(ctx, traceIDKey, traceID)
}

// ExtractTraceID 从context中提取 traceID
func ExtractTraceID(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	if val, ok := ctx.Value(traceIDKey).(string); ok {
		return val
	}
	return ""
}

// WithContext 返回一个携带了TraceID字段的Zap Logger实例
func WithContext(ctx context.Context) *zap.Logger {
	traceID := ExtractTraceID(ctx)
	if traceID != "" {
		return Logger.With(zap.String("trace_id", traceID))
	}
	return Logger
}

// InitLogger 初始化全局日志
func InitLogger(logPath string, maxSize, maxBackups, maxAge int, level string) {
	writeSyncer := zapcore.AddSync(&lumberjack.Logger{
		Filename:   logPath,
		MaxSize:    maxSize,
		MaxBackups: maxBackups,
		MaxAge:     maxAge,
		Compress:   false,
	})

	encoderConfig := zap.NewProductionEncoderConfig()
	encoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	encoder := zapcore.NewJSONEncoder(encoderConfig)

	var l zapcore.Level
	if err := l.UnmarshalText([]byte(level)); err != nil {
		l = zapcore.InfoLevel
	}

	core := zapcore.NewTee(
		zapcore.NewCore(encoder, writeSyncer, l),
		zapcore.NewCore(encoder, zapcore.AddSync(os.Stdout), l),
	)

	Logger = zap.New(core, zap.AddCaller())
}

// grpc拦截器实现请求监控+日志记录
func ZapGrpcLogger() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp interface{}, err error) {
		startTime := time.Now()

		traceID := uuid.New().String()
		newCtx := InjectTraceID(ctx, traceID)

		resp, err = handler(newCtx, req)

		duration := time.Since(startTime)
		statusCode := status.Code(err)

		// 使用 WithContext 提取 trace_id 打印拦截器日志
		reqLogger := WithContext(newCtx)

		if err != nil {
			reqLogger.Error("gRPC Request Failed",
				zap.String("method", info.FullMethod),
				zap.Any("request", req),
				zap.String("code", statusCode.String()),
				zap.Error(err),
				zap.Duration("duration", duration),
			)
		} else {
			reqLogger.Info("gRPC Request Success",
				zap.String("method", info.FullMethod),
				zap.String("code", statusCode.String()),
				zap.Duration("duration", duration),
			)
		}
		return resp, err
	}
}

// --- GORM 日志适配器 ---
type ZapGormLogger struct {
	LogLevel                  logger.LogLevel
	SlowThreshold             time.Duration
	IgnoreRecordNotFoundError bool
}

func (l ZapGormLogger) LogMode(level logger.LogLevel) logger.Interface {
	newlogger := l
	newlogger.LogLevel = level
	return &newlogger
}

func (l ZapGormLogger) Info(ctx context.Context, msg string, data ...interface{}) {
	if l.LogLevel >= logger.Info {
		WithContext(ctx).Sugar().Infof(msg, data...)
	}
}

func (l ZapGormLogger) Warn(ctx context.Context, msg string, data ...interface{}) {
	if l.LogLevel >= logger.Warn {
		WithContext(ctx).Sugar().Warnf(msg, data...)
	}
}

func (l ZapGormLogger) Error(ctx context.Context, msg string, data ...interface{}) {
	if l.LogLevel >= logger.Error {
		WithContext(ctx).Sugar().Errorf(msg, data...)
	}
}

func (l ZapGormLogger) Trace(ctx context.Context, begin time.Time, fc func() (sql string, rowsAffected int64), err error) {
	if l.LogLevel <= logger.Silent {
		return
	}
	elapsed := time.Since(begin)
	sql, rows := fc()
	reqLogger := WithContext(ctx)

	if err != nil && (!l.IgnoreRecordNotFoundError || err != logger.ErrRecordNotFound) {
		reqLogger.Error("SQL Error", zap.Error(err), zap.Duration("elapsed", elapsed), zap.Int64("rows", rows), zap.String("sql", sql))
		return
	}
	if l.SlowThreshold != 0 && elapsed > l.SlowThreshold && l.LogLevel >= logger.Warn {
		reqLogger.Warn("SQL Slow Query", zap.Duration("elapsed", elapsed), zap.Int64("rows", rows), zap.String("sql", sql))
		return
	}
	if l.LogLevel == logger.Info {
		reqLogger.Info("SQL Trace", zap.Duration("elapsed", elapsed), zap.Int64("rows", rows), zap.String("sql", sql))
	}
}

// --- Redis 日志适配器 ---
type RedisZapLogger struct{}

// 实现logger internal.Logging接口：
func (l *RedisZapLogger) Printf(ctx context.Context, format string, v ...interface{}) {
	msg := fmt.Sprintf(format, v...)
	WithContext(ctx).Warn("Redis Internal Log", zap.String("msg", msg))
}
