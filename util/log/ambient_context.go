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
// Author: Radu Berinde

package log

import (
	opentracing "github.com/opentracing/opentracing-go"
	"golang.org/x/net/trace"

	"golang.org/x/net/context"
)

// AmbientContext is a helper type used to "annotate" context.Contexts with log tags
// or a Tracer or EventLog.
type AmbientContext struct {
	// Tracer is used to open spans (see AnnotateCtxWithSpan).
	Tracer opentracing.Tracer

	// EventLog will be embedded into contexts that don't already have an event
	// log or an open span (if not nil).
	EventLog trace.EventLog

	// backgroundCtx is used as a cached value for when we annotate
	// context.Background or context.TODO.
	//
	// It is also used to store the log tags. TODO(radu): going through this
	// context to get/set tags is doing things backwards. We should operate on
	// *logTag directly.
	backgroundCtx context.Context
}

// EmptyAmbientContext creates an empty ambient context.
func EmptyAmbientContext() AmbientContext {
	return AmbientContext{backgroundCtx: context.Background()}
}

// AddLogTag adds a tag; see WithLogTag.
func (ac *AmbientContext) AddLogTag(name string, value interface{}) {
	ac.backgroundCtx = WithLogTag(ac.backgroundCtx, name, value)
}

// AddLogTagInt adds an integer tag; see WithLogTagInt.
func (ac *AmbientContext) AddLogTagInt(name string, value int) {
	ac.backgroundCtx = WithLogTagInt(ac.backgroundCtx, name, value)
}

// AddLogTagInt64 adds an integer tag; see WithLogTagInt64.
func (ac *AmbientContext) AddLogTagInt64(name string, value int64) {
	ac.backgroundCtx = WithLogTagInt64(ac.backgroundCtx, name, value)
}

// AddLogTagStr adds a string tag; see WithLogTagStr.
func (ac *AmbientContext) AddLogTagStr(name string, value string) {
	ac.backgroundCtx = WithLogTagStr(ac.backgroundCtx, name, value)
}

// AnnotateCtx annotates a given context with the information in AmbientContext:
//  - the EventLog is embedded in the context if the context doesn't already
//    have en event log or an open trace.
//  - the log tags in AmbientContext are added (if ctx doesn't already have them).
//
// For background operations, context.Background() should be passed; however, in
// that case it is strongly recommended to open a span if possible (using
// AnnotateCtxWithSpan).
func (ac *AmbientContext) AnnotateCtx(ctx context.Context) context.Context {
	if ctx == context.Background() || ctx == context.TODO() {
		// Fast path to avoid allocations in this case. The result is equivalent to
		// what the rest of the function does.
		return ac.backgroundCtx
	}

	if ac.EventLog != nil && opentracing.SpanFromContext(ctx) == nil && eventLogFromCtx(ctx) == nil {
		ctx = withEventLogInternal(ctx, ac.EventLog)
	}
	return WithLogTagsFromCtx(ctx, ac.backgroundCtx)
}

// AnnotateCtxWithSpan annotates the given context with the information in
// AmbientContext (see AnnotateCtx) and opens a span.
//
// If the given context has a span, the new span is a child of that span.
// Otherwise, the Tracer in AmbientContext is used to create a new root span.
//
// The caller is responsible for closing the span (via Span.Finish).
func (ac *AmbientContext) AnnotateCtxWithSpan(
	ctx context.Context, opName string,
) (context.Context, opentracing.Span) {
	ctx = ac.AnnotateCtx(ctx)

	var span opentracing.Span
	if parentSpan := opentracing.SpanFromContext(ctx); parentSpan != nil {
		tracer := parentSpan.Tracer()
		span = tracer.StartSpan(opName, opentracing.ChildOf(parentSpan.Context()))
	} else {
		if ac.Tracer == nil {
			panic("no tracer in AmbientContext for root span")
		}
		span = ac.Tracer.StartSpan(opName)
	}
	return opentracing.ContextWithSpan(ctx, span), span
}

// TODO(radu): remove once they start getting used.
var _ = AmbientContext{}.Tracer
var _ = (*AmbientContext).AddLogTagInt
var _ = (*AmbientContext).AddLogTagInt64
var _ = (*AmbientContext).AddLogTagStr
