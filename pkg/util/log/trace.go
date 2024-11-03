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
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/logtags"
)

// getSpan returns the current Span or nil if the Span doesn't exist or is a
// "black hole".
func getSpan(ctx context.Context) *tracing.Span {
	if sp := tracing.SpanFromContext(ctx); sp != nil {
		if !sp.IsVerbose() && !sp.Tracer().HasExternalSink() {
			return nil
		}
		return sp
	}
	return nil
}

// eventInternal is the common code for logging an event to a trace. Entries
// passed to this method may or may not be redactable (via the
// `logEntry.redactable` flag). However, if sp.Redactable() is true then they
// will be printed as redactable by `Recordf`. I the entry is not redactable,
// the entirety of the message will be marked redactable, otherwise
// fine-grainer redaction will remain in the result.
func eventInternal(sp *tracing.Span, isErr bool, entry *logEntry) {
	sp.Recordf("%s", entry)
	// TODO(obs-inf): figure out a way to signal that this is an error. We could
	// use a different "error" key (provided it shows up in LightStep). Things
	// like NetTraceIntegrator would need to be modified to understand the
	// difference. We could also set a special Tag on the span. See
	// #8827 for more discussion.
	_ = isErr
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
// it.
func Event(ctx context.Context, msg string) {
	sp := getSpan(ctx)
	if sp == nil {
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
	eventInternal(sp, false /* isErr */, &entry)
}

// Eventf looks for a tracing span in the context and formats and logs the
// given message to it.
func Eventf(ctx context.Context, format string, args ...interface{}) {
	sp := getSpan(ctx)
	if sp == nil {
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
	eventInternal(sp, false /* isErr */, &entry)
}

// NOTE: we maintain a vEvent function separate from vEventf, instead of having
// all VEvent callers invoke vEventf directly, so that the heap allocation from
// the `msg` parameter escaping when packed into a vararg slice is not incurred
// on the no-op path.
func vEvent(ctx context.Context, isErr bool, depth int, level Level, ch Channel, msg string) {
	if VDepth(level, 1+depth) || getSpan(ctx) != nil {
		vEventf(ctx, isErr, 1+depth, level, ch, "%s", msg)
	}
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
	} else if sp := getSpan(ctx); sp != nil {
		entry := makeUnstructuredEntry(ctx,
			severity.INFO, /* unused for trace events */
			channel.DEV,   /* unused for trace events */
			depth+1,
			sp.Redactable(),
			format, args...)
		eventInternal(sp, isErr, &entry)
	}
}

// VEvent either logs a message to the DEV channel (which also outputs to the
// active trace) or to the trace alone, depending on whether the specified
// verbosity level is active.
func VEvent(ctx context.Context, level Level, msg string) {
	vEvent(ctx, false /* isErr */, 1, level, channel.DEV, msg)
}

// VEventf either logs a message to the DEV channel (which also outputs to the
// active trace) or to the trace alone, depending on whether the specified
// verbosity level is active.
func VEventf(ctx context.Context, level Level, format string, args ...interface{}) {
	vEventf(ctx, false /* isErr */, 1, level, channel.DEV, format, args...)
}

// VEventfDepth performs the same as VEventf but checks the verbosity level
// at the given depth in the call stack.
func VEventfDepth(ctx context.Context, depth int, level Level, format string, args ...interface{}) {
	vEventf(ctx, false /* isErr */, 1+depth, level, channel.DEV, format, args...)
}

// VErrEvent either logs an error message to the DEV channel (which also
// outputs to the active trace) or to the trace alone, depending on whether
// the specified verbosity level is active.
func VErrEvent(ctx context.Context, level Level, msg string) {
	vEvent(ctx, true /* isErr */, 1, level, channel.DEV, msg)
}

// VErrEventf either logs an error message to the DEV Channel (which also
// outputs to the active trace) or to the trace alone, depending on whether
// the specified verbosity level is active.
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

// HasSpan returns true if the context has a span that should be logged to.
func HasSpan(ctx context.Context) bool {
	return getSpan(ctx) != nil
}
