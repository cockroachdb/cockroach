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
	otlog "github.com/opentracing/opentracing-go/log"
	"golang.org/x/net/context"
	"golang.org/x/net/trace"
)

// AmbientContext is a helper type used to "annotate" context.Contexts with log
// tags and a Tracer or EventLog. It is intended to be embedded into various
// server components.
//
// Example:
//   type SomeServer struct {
//     log.AmbientContext
//     ...
//   }
//
//   ac := AmbientContext{Tracer: tracing.NewTracer()}
//   ac.AddLogTag("n", 1)
//
//   s := &SomeServer{
//     AmbientContext: ac
//     ...
//   }
//
//   // on an operation with context ctx
//   ctx = s.AnnotateCtx(ctx)
//   ...
//
//   // run a worker
//   s.stopper.RunWorker(func() {
//     ctx := s.AnnotateCtx(context.Background())
//     ...
//   })
//
//   // start a background operation
//   ctx, span := s.AnnotateCtxWithSpan(context.Background(), "some-op")
//   defer span.Finish()
//   ...
type AmbientContext struct {
	// Tracer is used to open spans (see AnnotateCtxWithSpan).
	Tracer opentracing.Tracer

	// eventLog will be embedded into contexts that don't already have an event
	// log or an open span (if not nil).
	eventLog *ctxEventLog

	tags *logTag

	// Cached annotated version of context.{TODO,Background}, to avoid annotating
	// these contexts repeatedly.
	backgroundCtx context.Context
}

func (ac *AmbientContext) addTag(field otlog.Field) {
	ac.tags = &logTag{Field: field, parent: ac.tags}
	ac.refreshCache()
}

// AddLogTag adds a tag; see WithLogTag.
func (ac *AmbientContext) AddLogTag(name string, value interface{}) {
	ac.addTag(otlog.Object(name, value))
	ac.refreshCache()
}

// AddLogTagInt adds an integer tag; see WithLogTagInt.
func (ac *AmbientContext) AddLogTagInt(name string, value int) {
	ac.addTag(otlog.Int(name, value))
	ac.refreshCache()
}

// AddLogTagInt64 adds an integer tag; see WithLogTagInt64.
func (ac *AmbientContext) AddLogTagInt64(name string, value int64) {
	ac.addTag(otlog.Int64(name, value))
	ac.refreshCache()
}

// AddLogTagStr adds a string tag; see WithLogTagStr.
func (ac *AmbientContext) AddLogTagStr(name string, value string) {
	ac.addTag(otlog.String(name, value))
	ac.refreshCache()
}

// SetEventLog sets up an event log. Annotated contexts log into this event log
// (unless there's an open Span).
func (ac *AmbientContext) SetEventLog(family, title string) {
	ac.eventLog = &ctxEventLog{eventLog: trace.NewEventLog(family, title)}
	ac.refreshCache()
}

// FinishEventLog closes the event log. Concurrent and subsequent calls to
// record events from contexts that use this event log embedded are allowed.
func (ac *AmbientContext) FinishEventLog() {
	ac.eventLog.finish()
}

func (ac *AmbientContext) refreshCache() {
	ac.backgroundCtx = ac.annotateCtxInternal(context.Background())
}

// AnnotateCtx annotates a given context with the information in AmbientContext:
//  - the EventLog is embedded in the context if the context doesn't already
//    have en event log or an open trace.
//  - the log tags in AmbientContext are added (if ctx doesn't already have
//  them). If the tags already exist, the values from the AmbientContext
//  overwrite the existing values, but the order of the tags might change.
//
// For background operations, context.Background() should be passed; however, in
// that case it is strongly recommended to open a span if possible (using
// AnnotateCtxWithSpan).
func (ac *AmbientContext) AnnotateCtx(ctx context.Context) context.Context {
	switch ctx {
	case context.TODO(), context.Background():
		// NB: context.TODO and context.Background are identical except for their
		// names.
		if ac.backgroundCtx != nil {
			return ac.backgroundCtx
		}
		return ctx
	default:
		return ac.annotateCtxInternal(ctx)
	}
}

// ResetAndAnnotateCtx annotates a given context with the information in
// AmbientContext, but unlike AnnotateCtx, it drops all log tags in the
// supplied context before adding the ones from the AmbientContext.
func (ac *AmbientContext) ResetAndAnnotateCtx(ctx context.Context) context.Context {
	switch ctx {
	case context.TODO(), context.Background():
		// NB: context.TODO and context.Background are identical except for their
		// names.
		if ac.backgroundCtx != nil {
			return ac.backgroundCtx
		}
		return ctx
	default:
		if ac.eventLog != nil && opentracing.SpanFromContext(ctx) == nil && eventLogFromCtx(ctx) == nil {
			ctx = embedCtxEventLog(ctx, ac.eventLog)
		}
		if ac.tags != nil {
			ctx = context.WithValue(ctx, contextTagKeyType{}, ac.tags)
		}
		return ctx
	}
}

func (ac *AmbientContext) annotateCtxInternal(ctx context.Context) context.Context {
	if ac.eventLog != nil && opentracing.SpanFromContext(ctx) == nil && eventLogFromCtx(ctx) == nil {
		ctx = embedCtxEventLog(ctx, ac.eventLog)
	}
	if ac.tags != nil {
		ctx = augmentTagChain(ctx, ac.tags)
	}
	return ctx
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
	switch ctx {
	case context.TODO(), context.Background():
		// NB: context.TODO and context.Background are identical except for their
		// names.
		if ac.backgroundCtx != nil {
			ctx = ac.backgroundCtx
		}
	default:
		if ac.tags != nil {
			ctx = augmentTagChain(ctx, ac.tags)
		}
	}

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
