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
)

var noopTracer opentracing.NoopTracer

// Trace looks for an opentracing.Trace in the context and logs the given
// message to it on success.
func Trace(ctx context.Context, msg string) {
	sp := opentracing.SpanFromContext(ctx)
	if sp != nil && sp.Tracer() != noopTracer {
		sp.LogEvent(msg)
	}
}

// Tracef looks for an opentracing.Trace in the context and formats and logs
// the given message to it on success.
func Tracef(ctx context.Context, format string, args ...interface{}) {
	sp := opentracing.SpanFromContext(ctx)
	if sp != nil && sp.Tracer() != noopTracer {
		sp.LogEvent(fmt.Sprintf(format, args...))
	}
}

// VTrace either logs a message to the log files (which also outputs to the
// active trace) or logs to the trace alone depending on whether the specified
// verbosity level is active.
func VTrace(level level, ctx context.Context, msg string) {
	if V(level) {
		Info(ctx, msg)
	} else {
		Trace(ctx, msg)
	}
}

var _ = VTrace // TODO(peter): silence unused error, remove when used

// VTracef either logs a message to the log files (which also outputs to the
// active trace) or logs to the trace alone depending on whether the specified
// verbosity level is active.
func VTracef(level level, ctx context.Context, format string, args ...interface{}) {
	if V(level) {
		Infof(ctx, format, args...)
	} else {
		Tracef(ctx, format, args...)
	}
}
