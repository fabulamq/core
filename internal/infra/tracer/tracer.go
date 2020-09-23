package tracer

import (
	"context"
	"fmt"
	zipkintracer "github.com/openzipkin-contrib/zipkin-go-opentracing"
	"github.com/openzipkin/zipkin-go"
	"github.com/openzipkin/zipkin-go/reporter"
	zipkinhttp "github.com/openzipkin/zipkin-go/reporter/http"
	"runtime"
	"strings"
	"time"

	"github.com/opentracing/opentracing-go"
)

const (
	CorrelationIdKey = "pismo_correlation_id"
)

var zipkinAPM *ZipkinAPM

type (
	ZipkinAPM struct {
		Name     string
		Host     string
		Version  string
		Tracer   *zipkin.Tracer
		Reporter reporter.Reporter
	}

	ZipkinSpan struct {
		Name            string
		Resource        string
		CorrelationId   string
		Start           time.Time
		Error           bool
		OpenTracingSpan opentracing.Span
		APM             *ZipkinAPM
	}
)

func NewZipkinAPM(name, host, version string) *ZipkinAPM {
	return &ZipkinAPM{Name: name, Host: host, Version: version}
}

//StartAPM initialize the APM
func (apm *ZipkinAPM) StartAPM() error {
	var err error
	apm.Reporter = zipkinhttp.NewReporter(apm.Host, zipkinhttp.BatchInterval(time.Second*3))

	// create our local service endpoint
	endpoint, err := zipkin.NewEndpoint(apm.Name, "0.0.0.0:50053")
	if err != nil {
		return err
	}

	apm.Tracer, err = zipkin.NewTracer(
		apm.Reporter,
		zipkin.WithLocalEndpoint(endpoint),
		zipkin.WithTraceID128Bit(true),
	)

	if err != nil {
		return err
	}
	// optionally set as Global OpenTracing tracer instance
	opentracing.SetGlobalTracer(zipkintracer.Wrap(apm.Tracer))
	zipkinAPM = apm
	return nil
}

//StopAPM stop the APM
func (apm *ZipkinAPM) StopAPM() error {
	if err := apm.Reporter.Close(); err != nil {
		return err
	}
	return nil
}

//ContextWithCorrelationId append to context the correlation id
func ContextWithCorrelationId(ctx context.Context, correlationId string) context.Context {
	return context.WithValue(ctx, CorrelationIdKey, correlationId)
}

//GetCorrelationId get the correlation id from context
func GetCorrelationId(ctx context.Context) string {
	if v := ctx.Value(CorrelationIdKey); v != nil {
		return v.(string)
	}
	return ""
}

type TracePackageFunc struct {
	packageName string
	funcName    string
	line        int
}

func (e TracePackageFunc) Stack() string {
	return fmt.Sprintf("%s.%s:%d", e.packageName, e.funcName, e.line)
}

func (e TracePackageFunc) Location() string {
	return fmt.Sprintf("%s.%s", e.packageName, e.funcName)
}

func Generate(stack int) *TracePackageFunc {
	pc, _, line, ok := runtime.Caller(1 + stack)
	if ok {
		funcName := runtime.FuncForPC(pc).Name()
		lastSlash := strings.LastIndexByte(funcName, '/')
		if lastSlash < 0 {
			lastSlash = 0
		}
		lastDot := strings.LastIndexByte(funcName[lastSlash:], '.') + lastSlash
		return &TracePackageFunc{
			packageName: strings.Trim(funcName[lastSlash:lastDot], "/"),
			funcName:    funcName[lastDot+1:],
			line:        line,
		}
	}
	return nil
}
