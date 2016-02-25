package opentracing

import "golang.org/x/net/context"

type contextKey struct{}

var activeSpanKey = contextKey{}

// ContextWithSpan returns a new `context.Context` that holds a reference to
// the given `Span`.
//
// The second return value is simply the `span` passed in:
// this can save some typing and is only provided as a convenience.
func ContextWithSpan(ctx context.Context, span Span) (context.Context, Span) {
	return context.WithValue(ctx, activeSpanKey, span), span
}

// BackgroundContextWithSpan is a convenience wrapper around
// `ContextWithSpan(context.BackgroundContext(), ...)`.
//
// The second return value is simply the `span` passed in:
// this can save some typing and is only provided as a convenience.
func BackgroundContextWithSpan(span Span) (context.Context, Span) {
	return context.WithValue(context.Background(), activeSpanKey, span), span
}

// SpanFromContext returns the `Span` previously associated with `ctx`, or
// `nil` if no such `Span` could be found.
func SpanFromContext(ctx context.Context) Span {
	val := ctx.Value(activeSpanKey)
	if span, ok := val.(Span); ok {
		return span
	}
	return nil
}
