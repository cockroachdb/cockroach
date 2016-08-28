// Copyright 2016 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.
//
// Author: Tobias Schottdorf (tobias.schottdorf@gmail.com)

package log

import (
	"fmt"

	opentracing "github.com/opentracing/opentracing-go"
	"golang.org/x/net/context"
	"golang.org/x/net/trace"
)

// ctxEventLogKey is an empty type for the handle associated with the
// ctxEventLog value (see context.Value).
type ctxEventLogKey struct{}

// ctxEventLog is used for contexts to keep track of an EventLog.
type ctxEventLog struct {
	eventLog trace.EventLog
}

// WithEventLog embeds a trace.EventLog in the context, causing future logging
// and event calls to go to the EventLog. The current context must not have an
// open span already.
func WithEventLog(ctx context.Context, eventLog trace.EventLog) context.Context {
	if opentracing.SpanFromContext(ctx) != nil {
		panic("event log under span")
	}
	val := &ctxEventLog{eventLog: eventLog}
	return context.WithValue(ctx, ctxEventLogKey{}, val)
}

func eventLogFromCtx(ctx context.Context) *ctxEventLog {
	if val := ctx.Value(ctxEventLogKey{}); val != nil {
		return val.(*ctxEventLog)
	}
	return nil
}

var noopTracer opentracing.NoopTracer

// Event looks for an opentracing.Trace in the context and logs the given
// message to it. If no Trace is found, it looks for an EventLog in the context
// and logs the message to it. If neither is found, does nothing.
func Event(ctx context.Context, msg string) {
	sp := opentracing.SpanFromContext(ctx)
	if sp != nil {
		sp.LogEvent(msg)
	} else if el := eventLogFromCtx(ctx); el != nil {
		el.eventLog.Printf("%s", msg)
	}
}

// Eventf looks for an opentracing.Trace in the context and formats and logs
// the given message to it. If no Trace is found, it looks for an EventLog in
// the context and logs the message to it. If neither is found, does nothing.
func Eventf(ctx context.Context, format string, args ...interface{}) {
	if sp := opentracing.SpanFromContext(ctx); sp != nil {
		if sp.Tracer() != noopTracer {
			sp.LogEvent(fmt.Sprintf(format, args...))
		}
	} else if el := eventLogFromCtx(ctx); el != nil {
		el.eventLog.Printf(format, args...)
	}
}

// ErrEvent looks for an opentracing.Trace in the context and logs the given
// message to it. If no Trace is found, it looks for an EventLog in the context
// and logs the message to it (as an error). If neither is found, does nothing.
func ErrEvent(ctx context.Context, msg string) {
	if sp := opentracing.SpanFromContext(ctx); sp != nil {
		// TODO(radu): figure out a way to signal that this is an error. We could
		// use LogEventWithPayload and pass an error or special sentinel as the
		// payload. Things like NetTraceIntegrator would need to be modified to
		// understand the difference. We could also set a special Tag or Baggage on
		// the span. See #8827 for more discussion.
		sp.LogEvent(msg)
	} else if el := eventLogFromCtx(ctx); el != nil {
		el.eventLog.Errorf("%s", msg)
	}
}

// ErrEventf looks for an opentracing.Trace in the context and formats and logs
// the given message to it. If no Trace is found, it looks for an EventLog in
// the context and formats and logs the message to it (as an error). If neither
// is found, does nothing.
func ErrEventf(ctx context.Context, format string, args ...interface{}) {
	if sp := opentracing.SpanFromContext(ctx); sp != nil {
		if sp.Tracer() != noopTracer {
			// TODO(radu): see TODO for ErrEvent.
			sp.LogEvent(fmt.Sprintf(format, args...))
		}
	} else if el := eventLogFromCtx(ctx); el != nil {
		el.eventLog.Printf(format, args...)
	}
}

// VEvent either logs a message to the log files (which also outputs to the
// active trace or event log) or to the trace/event log alone, depending on
// whether the specified verbosity level is active.
func VEvent(level level, ctx context.Context, msg string) {
	if V(level) {
		Info(ctx, msg)
	} else {
		Event(ctx, msg)
	}
}

// VEventf either logs a message to the log files (which also outputs to the
// active trace or event log) or to the trace/event log alone, depending on
// whether the specified verbosity level is active.
func VEventf(level level, ctx context.Context, format string, args ...interface{}) {
	if V(level) {
		Infof(ctx, format, args...)
	} else {
		Eventf(ctx, format, args...)
	}
}

// Trace is a deprecated alias for Event.
func Trace(ctx context.Context, msg string) {
	Event(ctx, msg)
}

// Tracef is a deprecated alias for Eventf.
func Tracef(ctx context.Context, format string, args ...interface{}) {
	Eventf(ctx, format, args...)
}

// VTracef is a deprecated alias for VEventf.
func VTracef(level level, ctx context.Context, format string, args ...interface{}) {
	VEventf(level, ctx, format, args...)
}
