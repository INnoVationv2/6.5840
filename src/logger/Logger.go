package logger

import (
	"fmt"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"
	"io"
	"os"
	"sync"
	"time"
)

var (
	logger *zap.Logger
	once   sync.Once // 确保日志初始化只执行一次
)

func init() {
	fmt.Println("Initializing logger...")

	// 配置日志文件
	once.Do(func() {
		customTimeEncoder := func(t time.Time, enc zapcore.PrimitiveArrayEncoder) {
			enc.AppendString(t.Format("2006-01-02 15:04:05.000"))
		}

		encoderConfig := zapcore.EncoderConfig{
			TimeKey:        "time",                         // 时间字段的键名
			LevelKey:       "level",                        // 日志级别字段的键名
			NameKey:        "logger",                       // Logger 名称字段的键名
			CallerKey:      "caller",                       // 调用者字段的键名
			MessageKey:     "msg",                          // 消息字段的键名
			StacktraceKey:  "stacktrace",                   // 堆栈跟踪字段的键名
			LineEnding:     zapcore.DefaultLineEnding,      // 换行符
			EncodeLevel:    zapcore.CapitalLevelEncoder,    // 日志级别大写
			EncodeTime:     customTimeEncoder,              // 使用自定义时间编码器
			EncodeDuration: zapcore.SecondsDurationEncoder, // 持续时间以秒为单位
			EncodeCaller:   zapcore.ShortCallerEncoder,     // 调用者使用短文件路径
		}
		encoder := zapcore.NewJSONEncoder(encoderConfig)

		file, err := os.OpenFile("./raft.log", os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
		if err != nil {
			panic(err)
		}
		raftLog := zapcore.AddSync(io.MultiWriter(
			file,
			&lumberjack.Logger{
				Filename:   "./raft.log",
				MaxSize:    1000,
				MaxBackups: 0,
				MaxAge:     0,
				Compress:   true,
			},
		))

		// 创建核心日志器
		core := zapcore.NewCore(encoder, raftLog, zapcore.DebugLevel)
		logger = zap.New(core)
	})
}

func Info(format string, a ...interface{}) {
	logger.Info(fmt.Sprintf(format, a...))
}

func Debug(format string, a ...interface{}) {
	msg := fmt.Sprintf(format, a...)
	logger.Warn(msg)
}

func Error(format string, a ...interface{}) {
	logger.Debug(fmt.Sprintf(format, a...))
}
