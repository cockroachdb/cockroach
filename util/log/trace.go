package log

import (
	opentracing "github.com/opentracing/opentracing-go"
	"golang.org/x/net/context"
)

// Trace looks for an opentracing.Trace in the context and logs the given
// message to it on success.
func Trace(ctx context.Context, msg string) {
	sp := opentracing.SpanFromContext(ctx)
	if sp != nil {
		sp.LogEvent(msg)
	}
}
