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
//   ac := log.MakeAmbientContext(tracing.NewTracer())
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
	// The AmbientContext should not be constructed manually; instead,
	// use the constructor functions in this package.
	p privateAmbientContext
}

type privateAmbientContext struct {
	// tracer is used to open spans (see AnnotateCtxWithSpan).
	tracer *tracing.Tracer

	// serverIDs will be embedded into contexts that don't already have
	// one.
	serverIDs ServerIdentificationPayload

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

// Tracer retrieves the tracer associated with the ambient context.
func (ac *AmbientContext) Tracer() *tracing.Tracer {
	return ac.p.tracer
}

// AddLogTag adds a tag to the ambient context.
func (ac *AmbientContext) AddLogTag(name string, value interface{}) {
	ac.p.tags = ac.p.tags.Add(name, value)
	ac.p.refreshCache()
}

// SetEventLog sets up an event log. Annotated contexts log into this event log
// (unless there's an open Span).
func (ac *AmbientContext) SetEventLog(family, title string) {
	ac.p.eventLog = &ctxEventLog{eventLog: trace.NewEventLog(family, title)}
	ac.p.refreshCache()
}

// FinishEventLog closes the event log. Concurrent and subsequent calls to
// record events from contexts that use this event log embedded are allowed.
func (ac *AmbientContext) FinishEventLog() {
	ac.p.eventLog.finish()
}

func (ac *privateAmbientContext) refreshCache() {
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
		if ac.p.backgroundCtx != nil {
			return ac.p.backgroundCtx
		}
		return ctx
	default:
		return ac.p.annotateCtxInternal(ctx)
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
		if ac.p.backgroundCtx != nil {
			return ac.p.backgroundCtx
		}
		return ctx
	default:
		if ac.p.eventLog != nil && tracing.SpanFromContext(ctx) == nil && eventLogFromCtx(ctx) == nil {
			ctx = embedCtxEventLog(ctx, ac.p.eventLog)
		}
		if ac.p.tags != nil {
			ctx = logtags.WithTags(ctx, ac.p.tags)
		}
		if ac.p.serverIDs != nil {
			ctx = context.WithValue(ctx, ServerIdentificationContextKey{}, ac.p.serverIDs)
		}
		return ctx
	}
}

func (ac *privateAmbientContext) annotateCtxInternal(ctx context.Context) context.Context {
	if ac.eventLog != nil && tracing.SpanFromContext(ctx) == nil && eventLogFromCtx(ctx) == nil {
		ctx = embedCtxEventLog(ctx, ac.eventLog)
	}
	if ac.tags != nil {
		ctx = logtags.AddTags(ctx, ac.tags)
	}
	if ac.serverIDs != nil && ctx.Value(ServerIdentificationContextKey{}) == nil {
		ctx = context.WithValue(ctx, ServerIdentificationContextKey{}, ac.serverIDs)
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
		if ac.p.backgroundCtx != nil {
			ctx = ac.p.backgroundCtx
		}
	default:
		if ac.p.tags != nil {
			ctx = logtags.AddTags(ctx, ac.p.tags)
		}
		if ac.p.serverIDs != nil && ctx.Value(ServerIdentificationContextKey{}) == nil {
			ctx = context.WithValue(ctx, ServerIdentificationContextKey{}, ac.p.serverIDs)
		}
	}

	return tracing.EnsureChildSpan(ctx, ac.p.tracer, opName)
}

// MakeClientAmbientContext creates an AmbientContext for use by
// client commands.
func MakeClientAmbientContext(tracer *tracing.Tracer) AmbientContext {
	return AmbientContext{p: privateAmbientContext{tracer: tracer}}
}

// MakeDummyAmbientContext creates an AmbientContext for use in tests.
func MakeDummyAmbientContext(tracer *tracing.Tracer) AmbientContext {
	return AmbientContext{p: privateAmbientContext{tracer: tracer}}
}

// MakeServerAmbientContext creates an AmbientContext for use by
// server processes.
func MakeServerAmbientContext(
	tracer *tracing.Tracer, idProvider ServerIdentificationPayload,
) AmbientContext {
	// TODO(knz): add the server identifier containers here as mandatory
	// arguments.
	return AmbientContext{p: privateAmbientContext{tracer: tracer, serverIDs: idProvider}}
}
