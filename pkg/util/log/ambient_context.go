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

	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/logtags"
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
	Tracer *tracing.Tracer

	// eventLog will be embedded into contexts that don't already have an event
	// log or an open span (if not nil).
	eventLog *ctxEventLog

	// The buffer.
	//
	// NB: this should not be returned to the caller, to avoid other mutations
	// leaking in. If we return this to the caller, it should be in immutable
	// form.
	tags *logtags.Buffer

	// Cached annotated version of context.{TODO,Background}, to avoid annotating
	// these contexts repeatedly.
	backgroundCtx context.Context
}

// AddLogTag adds a tag to the ambient context.
func (ac *AmbientContext) AddLogTag(name string, value interface{}) {
	ac.tags = ac.tags.Add(name, value)
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
//    have an event log or an open trace.
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
		if ac.eventLog != nil && tracing.SpanFromContext(ctx) == nil && eventLogFromCtx(ctx) == nil {
			ctx = embedCtxEventLog(ctx, ac.eventLog)
		}
		if ac.tags != nil {
			ctx = logtags.WithTags(ctx, ac.tags)
		}
		return ctx
	}
}

func (ac *AmbientContext) annotateCtxInternal(ctx context.Context) context.Context {
	if ac.eventLog != nil && tracing.SpanFromContext(ctx) == nil && eventLogFromCtx(ctx) == nil {
		ctx = embedCtxEventLog(ctx, ac.eventLog)
	}
	if ac.tags != nil {
		ctx = logtags.AddTags(ctx, ac.tags)
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
) (context.Context, *tracing.Span) {
	switch ctx {
	case context.TODO(), context.Background():
		// NB: context.TODO and context.Background are identical except for their
		// names.
		if ac.backgroundCtx != nil {
			ctx = ac.backgroundCtx
		}
	default:
		if ac.tags != nil {
			ctx = logtags.AddTags(ctx, ac.tags)
		}
	}

	return tracing.EnsureChildSpan(ctx, ac.Tracer, opName)
}
