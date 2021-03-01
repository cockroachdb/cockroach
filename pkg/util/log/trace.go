// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package log

import (
	"context"
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/util/log/channel"
	"github.com/cockroachdb/cockroach/pkg/util/log/logpb"
	"github.com/cockroachdb/cockroach/pkg/util/log/severity"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/logtags"
	"github.com/cockroachdb/redact"
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
	if el.eventLog != nil {
		el.eventLog.Finish()
		el.eventLog = nil
	}
	el.Unlock()
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

// eventInternal is the common code for logging an event. If no args are given,
// the format is treated as a pre-formatted string.
//
// Note that when called from a logging function, this is taking the log
// message as input after introduction of redaction markers.  This
// means the message may or may not contain markers already depending
// of the configuration of --redactable-logs.
func eventInternal(sp *tracing.Span, el *ctxEventLog, isErr bool, entry logpb.Entry) {
	var msg string
	if len(entry.Tags) == 0 && len(entry.File) == 0 && !entry.Redactable {
		// Shortcut.
		msg = entry.Message
	} else {
		var buf strings.Builder
		if len(entry.File) != 0 {
			buf.WriteString(entry.File)
			buf.WriteByte(':')
			// TODO(knz): The "canonical" way to represent a file/line prefix
			// is: <file>:<line>: msg
			// with a colon between the line number and the message.
			// However, some location filter deep inside SQL doesn't
			// understand a colon after the line number.
			buf.WriteString(strconv.FormatInt(entry.Line, 10))
			buf.WriteByte(' ')
		}
		if len(entry.Tags) > 0 {
			buf.WriteByte('[')
			buf.WriteString(entry.Tags)
			buf.WriteString("] ")
		}
		buf.WriteString(entry.Message)
		msg = buf.String()

		if entry.Redactable {
			// This is true when eventInternal is called from logfDepth(),
			// ie. a regular log call. In this case, the tags and message may contain
			// redaction markers. We remove them here.
			msg = redact.RedactableString(msg).StripMarkers()
		}
	}

	if sp != nil {
		sp.Record(msg)
		// if isErr {
		// 	// TODO(radu): figure out a way to signal that this is an error. We
		// 	// could use a different "error" key (provided it shows up in
		// 	// LightStep). Things like NetTraceIntegrator would need to be modified
		// 	// to understand the difference. We could also set a special Tag or
		// 	// Baggage on the span. See #8827 for more discussion.
		// }
	} else {
		el.Lock()
		if el.eventLog != nil {
			if isErr {
				el.eventLog.Errorf("%s", msg)
			} else {
				el.eventLog.Printf("%s", msg)
			}
		}
		el.Unlock()
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

// Event looks for an opentracing.Trace in the context and logs the given
// message to it. If no Trace is found, it looks for an EventLog in the context
// and logs the message to it. If neither is found, does nothing.
func Event(ctx context.Context, msg string) {
	sp, el, ok := getSpanOrEventLog(ctx)
	if !ok {
		// Nothing to log. Skip the work.
		return
	}

	// Format the tracing event and add it to the trace.
	entry := MakeLegacyEntry(ctx,
		severity.INFO, /* unused for trace events */
		channel.DEV,   /* unused for trace events */
		1,             /* depth */
		// redactable is false because we want to flatten the data in traces
		// -- we don't have infrastructure yet for trace redaction.
		false, /* redactable */
		"")
	entry.Message = msg
	eventInternal(sp, el, false /* isErr */, entry)
}

// Eventf looks for an opentracing.Trace in the context and formats and logs
// the given message to it. If no Trace is found, it looks for an EventLog in
// the context and logs the message to it. If neither is found, does nothing.
func Eventf(ctx context.Context, format string, args ...interface{}) {
	sp, el, ok := getSpanOrEventLog(ctx)
	if !ok {
		// Nothing to log. Skip the work.
		return
	}

	// Format the tracing event and add it to the trace.
	entry := MakeLegacyEntry(ctx,
		severity.INFO, /* unused for trace events */
		channel.DEV,   /* unused for trace events */
		1,             /* depth */
		// redactable is false because we want to flatten the data in traces
		// -- we don't have infrastructure yet for trace redaction.
		false, /* redactable */
		format, args...)
	eventInternal(sp, el, false /* isErr */, entry)
}

func vEventf(
	ctx context.Context, isErr bool, depth int, level Level, format string, args ...interface{},
) {
	if VDepth(level, 1+depth) {
		// Log the message (which also logs an event).
		sev := severity.INFO
		if isErr {
			sev = severity.ERROR
		}
		logfDepth(ctx, 1+depth, sev, channel.DEV, format, args...)
	} else {
		sp, el, ok := getSpanOrEventLog(ctx)
		if !ok {
			// Nothing to log. Skip the work.
			return
		}
		entry := MakeLegacyEntry(ctx,
			severity.INFO, /* unused for trace events */
			channel.DEV,   /* unused for trace events */
			depth+1,
			// redactable is false because we want to flatten the data in traces
			// -- we don't have infrastructure yet for trace redaction.
			false, /* redactable */
			format, args...)
		eventInternal(sp, el, isErr, entry)
	}
}

// VEvent either logs a message to the DEV channel (which also outputs to the
// active trace or event log) or to the trace/event log alone, depending on
// whether the specified verbosity level is active.
func VEvent(ctx context.Context, level Level, msg string) {
	vEventf(ctx, false /* isErr */, 1, level, msg)
}

// VEventf either logs a message to the DEV channel (which also outputs to the
// active trace or event log) or to the trace/event log alone, depending on
// whether the specified verbosity level is active.
func VEventf(ctx context.Context, level Level, format string, args ...interface{}) {
	vEventf(ctx, false /* isErr */, 1, level, format, args...)
}

// VEventfDepth performs the same as VEventf but checks the verbosity level
// at the given depth in the call stack.
func VEventfDepth(ctx context.Context, depth int, level Level, format string, args ...interface{}) {
	vEventf(ctx, false /* isErr */, 1+depth, level, format, args...)
}

// VErrEvent either logs an error message to the DEV channel (which also outputs
// to the active trace or event log) or to the trace/event log alone, depending
// on whether the specified verbosity level is active.
func VErrEvent(ctx context.Context, level Level, msg string) {
	vEventf(ctx, true /* isErr */, 1, level, msg)
}

// VErrEventf either logs an error message to the DEV Channel (which also outputs
// to the active trace or event log) or to the trace/event log alone, depending
// on whether the specified verbosity level is active.
func VErrEventf(ctx context.Context, level Level, format string, args ...interface{}) {
	vEventf(ctx, true /* isErr */, 1, level, format, args...)
}

// VErrEventfDepth performs the same as VErrEventf but checks the verbosity
// level at the given depth in the call stack.
func VErrEventfDepth(
	ctx context.Context, depth int, level Level, format string, args ...interface{},
) {
	vEventf(ctx, true /* isErr */, 1+depth, level, format, args...)
}

var _ = VErrEventfDepth // silence unused warning

// HasSpanOrEvent returns true if the context has a span or event that should
// be logged to.
func HasSpanOrEvent(ctx context.Context) bool {
	_, _, ok := getSpanOrEventLog(ctx)
	return ok
}
