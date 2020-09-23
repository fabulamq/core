package tracer

type ErrorWithTrace interface {
	Trace() string
}
