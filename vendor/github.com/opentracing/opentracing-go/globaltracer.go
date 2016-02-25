package opentracing

var (
	globalTracer Tracer = NoopTracer{}
)

// InitGlobalTracer sets the [singleton] opentracing.Tracer returned by
// GlobalTracer(). Those who use GlobalTracer (rather than directly manage an
// opentracing.Tracer instance) should call InitGlobalTracer as early as possible in
// main(), prior to calling the `StartSpan` (etc) global funcs below. Prior to
// calling `InitGlobalTracer`, any Spans started via the `StartSpan` (etc)
// globals are noops.
func InitGlobalTracer(tracer Tracer) {
	globalTracer = tracer
}

// GlobalTracer returns the global singleton `Tracer` implementation.
// Before `InitGlobalTracer()` is called, the `GlobalTracer()` is a noop
// implementation that drops all data handed to it.
func GlobalTracer() Tracer {
	return globalTracer
}

// StartSpan defers to `Tracer.StartSpan`. See `GlobalTracer()`.
func StartSpan(operationName string) Span {
	return globalTracer.StartSpan(operationName)
}
