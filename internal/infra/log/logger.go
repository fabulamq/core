package log

import (
	"context"
	"github.com/sirupsen/logrus"
	prefixed "github.com/x-cray/logrus-prefixed-formatter"
	"github.com/zeusmq/internal/infra/tracer"
	"os"
	"sync"
)

type (
	Fields interface {
		get() map[string]interface{}
	}
	Common     map[string]interface{}
	Sensitive  map[string]interface{}
	FillFields func(ctx context.Context, f Common) Common
)

func (f Common) get() map[string]interface{} {
	return f
}

func (f Sensitive) get() map[string]interface{} {
	return f
}

var (
	logger        *logrus.Logger
	once          sync.Once
	defaultFields FillFields
	logSensitive  bool
)

func init() {
	defaultFields = func(ctx context.Context, f Common) Common {
		f["ch"] = inject(ctx, "ch")
		f["id"] = inject(ctx, "id")
		return f
	}
}

//GetCorrelationId get the correlation id from context
func inject(ctx context.Context, key string) string {
	if v := ctx.Value(key); v != nil {
		return v.(string)
	}
	return ""
}

func loggerInit() *logrus.Logger {
	once.Do(func() {
		logger = &logrus.Logger{
			Out:   os.Stderr,
			Level: logrus.DebugLevel,
			Formatter: &prefixed.TextFormatter{
				DisableColors:   false,
				TimestampFormat: "2006-01-02 15:04:05",
				FullTimestamp:   true,
				ForceFormatting: true,
			},
		}
	})

	return logger
}

func output(ctx context.Context, fieldsArr []Fields, err error) *logrus.Entry {
	f := defaultFields(ctx, make(Common, 0))

	for _, fields := range fieldsArr {
		if _, isSensitive := fields.(Sensitive); isSensitive && !logSensitive {
			continue
		}
		for k, v := range fields.get() {
			f[k] = v
		}
	}

	//f["stack"] = tracer.Generate(2).Stack()

	if err != nil {
		f["error"] = err.Error()
		errWithTrace, ok := err.(tracer.ErrorWithTrace)
		if ok {
			f["stack_error"] = errWithTrace.Trace()
		}
	}

	return loggerInit().WithFields(logrus.Fields(f))
}

func Info(ctx context.Context, message string, fields ...Fields) {
	output(ctx, fields, nil).Info(message)
}

func Warn(ctx context.Context, message string, err error, fields ...Fields) {
	output(ctx, fields, err).Warn(message)
}

func Error(ctx context.Context, message string, err error, fields ...Fields) {
	output(ctx, fields, err).Error(message)
}

func Fatal(ctx context.Context, message string, fields ...Fields) {
	output(ctx, fields, nil).Fatal(message)
}

func Trace(ctx context.Context, message string, fields ...Fields) {
	output(ctx, fields, nil).Trace(message)
}
