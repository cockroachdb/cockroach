// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package log

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/base/serverident"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/logtags"
)

// AmbientContext is a helper type used to "annotate" context.Contexts with
// log tags and a Tracer. It is intended to be embedded into various server
// components.
//
// Example:
//
//	type SomeServer struct {
//	  log.AmbientContext
//	  ...
//	}
//
//	ac := AmbientContext{Tracer: tracing.NewTracer()}
//	ac.AddLogTag("n", 1)
//
//	s := &SomeServer{
//	  AmbientContext: ac
//	  ...
//	}
//
//	// on an operation with context ctx
//	ctx = s.AnnotateCtx(ctx)
//	...
//
//	// run a worker
//	s.stopper.RunWorker(func() {
//	  ctx := s.AnnotateCtx(context.Background())
//	  ...
//	})
//
//	// start a background operation
//	ctx, span := s.AnnotateCtxWithSpan(context.Background(), "some-op")
//	defer span.Finish()
//	...
type AmbientContext struct {
	// Tracer is used to open spans (see AnnotateCtxWithSpan).
	Tracer *tracing.Tracer

	// ServerIDs will be embedded into contexts that don't already have
	// one.
	ServerIDs serverident.ServerIdentificationPayload

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

func (ac *AmbientContext) refreshCache() {
	ac.backgroundCtx = ac.annotateCtxInternal(context.Background())
}

// AnnotateCtx annotates a given context with the information in AmbientContext:
//   - the log tags in AmbientContext are added (if ctx doesn't already have
//     them). If the tags already exist, the values from the AmbientContext
//     overwrite the existing values, but the order of the tags might change.
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
		ctx = logtags.WithTags(ctx, ac.tags)
		if ac.ServerIDs != nil {
			ctx = serverident.ContextWithServerIdentification(ctx, ac.ServerIDs)
		}
		return ctx
	}
}

func (ac *AmbientContext) annotateCtxInternal(ctx context.Context) context.Context {
	if ac.tags != nil {
		ctx = logtags.AddTags(ctx, ac.tags)
	}
	if ac.ServerIDs != nil && serverident.ServerIdentificationFromContext(ctx) == nil {
		ctx = serverident.ContextWithServerIdentification(ctx, ac.ServerIDs)
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
		if ac.ServerIDs != nil && serverident.ServerIdentificationFromContext(ctx) == nil {
			ctx = serverident.ContextWithServerIdentification(ctx, ac.ServerIDs)
		}
	}

	return tracing.EnsureChildSpan(ctx, ac.Tracer, opName)
}

// MakeTestingAmbientContext creates an AmbientContext for use in tests,
// when a test does not have sufficient details to instantiate a fully
// fledged server AmbientContext.
func MakeTestingAmbientContext(tracer *tracing.Tracer) AmbientContext {
	return AmbientContext{Tracer: tracer}
}

// MakeTestingAmbientCtxWithNewTracer is like MakeTestingAmbientContext() but it
// also instantiates a new tracer.
//
// TODO(andrei): Remove this API.
//
// This is generally a bad idea because it creates a Tracer under the
// hood, and if this Tracer ends up actually being used, chances are
// it's going to crash because it'll end up mixing with some other
// Tracer. When a test uses multiple tracers and does not crash,
// either it's because one of the tracers is not being used (in which
// case it'd be clearer to share the one being used using
// MakeTestingAmbientContext), or, by happenstance, the trace in
// question doesn't end up having more than one span. This is very
// brittle.
func MakeTestingAmbientCtxWithNewTracer() AmbientContext {
	return MakeTestingAmbientContext(tracing.NewTracer())
}

// MakeServerAmbientContext creates an AmbientContext for use by
// server processes.
func MakeServerAmbientContext(
	tracer *tracing.Tracer, idProvider serverident.ServerIdentificationPayload,
) AmbientContext {
	return AmbientContext{Tracer: tracer, ServerIDs: idProvider}
}
