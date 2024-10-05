// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package log

import (
	"context"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/util/log/channel"
	"github.com/cockroachdb/cockroach/pkg/util/log/severity"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/logtags"
	"golang.org/x/net/trace"
)

// ctxEventLogKey is an empty type for the handle associated with the
// ctxEventLog value (see context.Value).
type ctxEventLogKey struct{}

// ctxEventLog is used for contexts to keep track of an EventLog.
type ctxEventLog struct {
	syncutil.Mutex
	eventLog trace.EventLog
}

func (el *ctxEventLog) finish() {
	el.Lock()
	defer el.Unlock()
	if el.eventLog != nil {
		el.eventLog.Finish()
		el.eventLog = nil
	}
}

func embedCtxEventLog(ctx context.Context, el *ctxEventLog) context.Context {
	return context.WithValue(ctx, ctxEventLogKey{}, el)
}

// withEventLogInternal embeds a trace.EventLog in the context, causing future
// logging and event calls to go to the EventLog. The current context must not
// have an existing open span.
func withEventLogInternal(ctx context.Context, eventLog trace.EventLog) context.Context {
	if tracing.SpanFromContext(ctx) != nil {
		panic("event log under span")
	}
	return embedCtxEventLog(ctx, &ctxEventLog{eventLog: eventLog})
}

// WithEventLog creates and embeds a trace.EventLog in the context, causing
// future logging and event calls to go to the EventLog. The current context
// must not have an existing open span.
func WithEventLog(ctx context.Context, family, title string) context.Context {
	return withEventLogInternal(ctx, trace.NewEventLog(family, title))
}

var _ = WithEventLog

// WithNoEventLog creates a context which no longer has an embedded event log.
func WithNoEventLog(ctx context.Context) context.Context {
	return withEventLogInternal(ctx, nil)
}

func eventLogFromCtx(ctx context.Context) *ctxEventLog {
	if val := ctx.Value(ctxEventLogKey{}); val != nil {
		return val.(*ctxEventLog)
	}
	return nil
}

// FinishEventLog closes the event log in the context (see WithEventLog).
// Concurrent and subsequent calls to record events are allowed.
func FinishEventLog(ctx context.Context) {
	if el := eventLogFromCtx(ctx); el != nil {
		el.finish()
	}
}

// getSpanOrEventLog returns the current Span. If there is no Span, it returns
// the current ctxEventLog. If neither (or the Span is "black hole"), returns
// false.
func getSpanOrEventLog(ctx context.Context) (*tracing.Span, *ctxEventLog, bool) {
	if sp := tracing.SpanFromContext(ctx); sp != nil {
		if !sp.IsVerbose() && !sp.Tracer().HasExternalSink() {
			return nil, nil, false
		}
		return sp, nil, true
	}
	if el := eventLogFromCtx(ctx); el != nil {
		return nil, el, true
	}
	return nil, nil, false
}

// eventInternal is the common code for logging an event to trace and/or
// event logs. Entries passed to this method may or may not be
// redactable (via the `logEntry.redactable` flag). However, if
// sp.Redactable() is true then they will be printed as redactable by
// `Recordf`. I the entry is not redactable, the entirety of the message
// will be marked redactable, otherwise fine-grainer redaction will
// remain in the result.
func eventInternal(sp *tracing.Span, el *ctxEventLog, isErr bool, entry *logEntry) {
	if sp != nil {
		sp.Recordf("%s", entry)
		// TODO(obs-inf): figure out a way to signal that this is an error. We could
		// use a different "error" key (provided it shows up in LightStep). Things
		// like NetTraceIntegrator would need to be modified to understand the
		// difference. We could also set a special Tag on the span. See
		// #8827 for more discussion.
		_ = isErr
		return
	}

	// No span, record to event log instead.
	//
	// TODO(obs-inf): making this either/or doesn't seem useful, but it
	// is the status quo, and the callers only pass one of the two even
	// if they have both.
	el.Lock()
	defer el.Unlock()
	if el.eventLog != nil {
		if isErr {
			el.eventLog.Errorf("%s", entry)
		} else {
			el.eventLog.Printf("%s", entry)
		}
	}
}

// formatTags appends the tags to a strings.Builder. If there are no tags,
// returns false.
func formatTags(ctx context.Context, brackets bool, buf *strings.Builder) bool {
	tags := logtags.FromContext(ctx)
	if tags == nil {
		return false
	}
	if brackets {
		buf.WriteByte('[')
	}
	tags.FormatToString(buf)
	if brackets {
		buf.WriteString("] ")
	}
	return true
}

// Event looks for a tracing span in the context and logs the given message to
// it. If no span is found, it looks for an EventLog in the context and logs the
// message to it. If neither is found, does nothing.
func Event(ctx context.Context, msg string) {
	sp, el, ok := getSpanOrEventLog(ctx)
	if !ok {
		// Nothing to log. Skip the work.
		return
	}

	// Format the tracing event and add it to the trace.
	entry := makeUnstructuredEntry(ctx,
		severity.INFO, /* unused for trace events */
		channel.DEV,   /* unused for trace events */
		1,             /* depth */
		sp.Redactable(),
		msg)
	eventInternal(sp, el, false /* isErr */, &entry)
}

// Eventf looks for a tracing span in the context and formats and logs
// the given message to it. If no span is found, it looks for an EventLog in
// the context and logs the message to it. If neither is found, does nothing.
func Eventf(ctx context.Context, format string, args ...interface{}) {
	sp, el, ok := getSpanOrEventLog(ctx)
	if !ok {
		// Nothing to log. Skip the work.
		return
	}

	// Format the tracing event and add it to the trace.
	entry := makeUnstructuredEntry(ctx,
		severity.INFO, /* unused for trace events */
		channel.DEV,   /* unused for trace events */
		1,             /* depth */
		sp.Redactable(),
		format, args...)
	eventInternal(sp, el, false /* isErr */, &entry)
}

func vEventf(
	ctx context.Context,
	isErr bool,
	depth int,
	level Level,
	ch Channel,
	format string,
	args ...interface{},
) {
	if VDepth(level, 1+depth) {
		// Log the message (which also logs an event).
		sev := severity.INFO
		if isErr {
			sev = severity.ERROR
		}
		logfDepth(ctx, 1+depth, sev, ch, format, args...)
	} else {
		sp, el, ok := getSpanOrEventLog(ctx)
		if !ok {
			// Nothing to log. Skip the work.
			return
		}
		entry := makeUnstructuredEntry(ctx,
			severity.INFO, /* unused for trace events */
			channel.DEV,   /* unused for trace events */
			depth+1,
			sp.Redactable(),
			format, args...)
		eventInternal(sp, el, isErr, &entry)
	}
}

// VEvent either logs a message to the DEV channel (which also outputs to the
// active trace or event log) or to the trace/event log alone, depending on
// whether the specified verbosity level is active.
func VEvent(ctx context.Context, level Level, msg string) {
	vEventf(ctx, false /* isErr */, 1, level, channel.DEV, msg)
}

// VEventf either logs a message to the DEV channel (which also outputs to the
// active trace or event log) or to the trace/event log alone, depending on
// whether the specified verbosity level is active.
func VEventf(ctx context.Context, level Level, format string, args ...interface{}) {
	vEventf(ctx, false /* isErr */, 1, level, channel.DEV, format, args...)
}

// VEventfDepth performs the same as VEventf but checks the verbosity level
// at the given depth in the call stack.
func VEventfDepth(ctx context.Context, depth int, level Level, format string, args ...interface{}) {
	vEventf(ctx, false /* isErr */, 1+depth, level, channel.DEV, format, args...)
}

// VErrEvent either logs an error message to the DEV channel (which also outputs
// to the active trace or event log) or to the trace/event log alone, depending
// on whether the specified verbosity level is active.
func VErrEvent(ctx context.Context, level Level, msg string) {
	vEventf(ctx, true /* isErr */, 1, level, channel.DEV, msg)
}

// VErrEventf either logs an error message to the DEV Channel (which also outputs
// to the active trace or event log) or to the trace/event log alone, depending
// on whether the specified verbosity level is active.
func VErrEventf(ctx context.Context, level Level, format string, args ...interface{}) {
	vEventf(ctx, true /* isErr */, 1, level, channel.DEV, format, args...)
}

// VErrEventfDepth performs the same as VErrEventf but checks the verbosity
// level at the given depth in the call stack.
func VErrEventfDepth(
	ctx context.Context, depth int, level Level, format string, args ...interface{},
) {
	vEventf(ctx, true /* isErr */, 1+depth, level, channel.DEV, format, args...)
}

var _ = VErrEventfDepth // silence unused warning

// HasSpanOrEvent returns true if the context has a span or event that should
// be logged to.
func HasSpanOrEvent(ctx context.Context) bool {
	_, _, ok := getSpanOrEventLog(ctx)
	return ok
}
