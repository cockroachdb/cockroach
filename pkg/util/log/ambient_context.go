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

	// The log tag buffer specific to this ambient context.
	//
	// NB: this should not be returned to the caller, to avoid other mutations
	// leaking in. If we return this to the caller, it should be in immutable
	// form.
	privateTags *logtags.Buffer

	// The log tag buffer shared across ambient contexts, across all
	// copies of ambient contexts from the first where shared tags were
	// defined.
	//
	// NB: We are avoiding atomic.Value or syncutil.RWMutex here out of
	// concern for performance. This is OK with regards to race
	// conditions considering the following:
	//
	// 1. the initialization of the **logtags.Buffer pointer, from nil to non-nil.
	// 2. the initialization/update of the tags stored as *logtags.Buffer.
	// 3. accesses to the shared tags.
	//
	// Both initializations (1) and (2) are race free, under the
	// assumption that calls to AddSharedLogTag() and
	// InitializeSharedLogTags() are not performed concurrently on a
	// given AmbientContext copy-family. This is true of server
	// initialization inside CockroachDB.
	//
	// Accesses can be concurrent, which is safe: concurrent reads
	// of a pointer are fine.
	sharedTags **logtags.Buffer

	// Cached annotated version of context.{TODO,Background}, to avoid annotating
	// these contexts repeatedly.
	backgroundCtx context.Context
}

// AddLogTag adds a tag to the ambient context.
// This tag is not shared with other AmbientContext instances.
func (ac *AmbientContext) AddLogTag(name string, value interface{}) {
	ac.privateTags = ac.privateTags.Add(name, value)
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
			// This is a cached background ctx with the private tags already
			// embedded.
			// Note that if there are private tags, ac.backgroundCtx is
			// guaranteed non-nil because it was populated by AddLogTag().
			ctx = ac.backgroundCtx
		}
	default:
		// Add the tracing information and the private tags of any.
		ctx = ac.annotateCtxInternal(ctx)
	}

	// Add the shared tags, if any.
	// This can't be cached in ac.backgroundCtx since the shared
	// tags might be updated in a different AmbientContext instance.
	if ac.sharedTags != nil && *ac.sharedTags != nil {
		ctx = logtags.AddTags(ctx, *ac.sharedTags)
	}
	return ctx
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
			// This is a cached background ctx with the private tags already
			// embedded.
			// Note that if there are private tags, ac.backgroundCtx is
			// guaranteed non-nil because it was populated by AddLogTag().
			ctx = ac.backgroundCtx
		}
		// In any case, add the shared tags if any.
		// This can't be cached in ac.backgroundCtx since the shared
		// tags might be updated in a different AmbientContext instance.
		if ac.sharedTags != nil && *ac.sharedTags != nil {
			ctx = logtags.AddTags(ctx, *ac.sharedTags)
		}
	default:
		if ac.eventLog != nil && tracing.SpanFromContext(ctx) == nil && eventLogFromCtx(ctx) == nil {
			ctx = embedCtxEventLog(ctx, ac.eventLog)
		}
		if ac.privateTags != nil {
			// We are "resetting" the context in this method, so
			// we use WithTags() which ignores any pre-existing tags.
			// This is also why we can't use annotateCtxInternal() here.
			ctx = logtags.WithTags(ctx, ac.privateTags)
			// *Add* the shared tags, if any. We're adding here to not
			// overwrite the private tags.
			if ac.sharedTags != nil && *ac.sharedTags != nil {
				ctx = logtags.AddTags(ctx, *ac.sharedTags)
			}
		} else {
			// *Replace* the tags with shared tags, if any.
			if ac.sharedTags != nil && *ac.sharedTags != nil {
				ctx = logtags.WithTags(ctx, *ac.sharedTags)
			}
		}
	}
	return ctx
}

func (ac *AmbientContext) annotateCtxInternal(ctx context.Context) context.Context {
	if ac.eventLog != nil && tracing.SpanFromContext(ctx) == nil && eventLogFromCtx(ctx) == nil {
		ctx = embedCtxEventLog(ctx, ac.eventLog)
	}
	if ac.privateTags != nil {
		// Combine the private tags with those inside the context.
		ctx = logtags.AddTags(ctx, ac.privateTags)
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
			// This is a cached background ctx with the private tags already
			// embedded.
			// Note that if there are private tags, ac.backgroundCtx is
			// guaranteed non-nil because it was populated by AddLogTag().
			ctx = ac.backgroundCtx
		}
	default:
		if ac.privateTags != nil {
			ctx = logtags.AddTags(ctx, ac.privateTags)
		}
	}

	// In any case, add the shared tags if any.
	// This can't be cached in ac.backgroundCtx since the shared
	// tags might be updated in a different AmbientContext instance.
	if ac.sharedTags != nil && *ac.sharedTags == nil {
		ctx = logtags.AddTags(ctx, *ac.sharedTags)
	}

	return tracing.EnsureChildSpan(ctx, ac.Tracer, opName)
}

// InitializeSharedLogTags initializes the AmbientContext such that
// AddSharedLogTag() can be called either on this instance of
// AmbientContext or any other that has been copied from it by value.
//
// It's possible to skip a call to InitializeSharedLogTags() if the
// first call to AddSharedLogTag() on the original AmbientContext is
// guaranteed to not be concurrent with any other call.
func (ac *AmbientContext) InitializeSharedLogTags() {
	if ac.sharedTags != nil {
		panic("already initialized")
	}
	var emptyBuf *logtags.Buffer
	ac.sharedTags = &emptyBuf
}

// AddSharedLogTag adds a logging tag shared with all copy-descendents
// of this AmbientContext. The tag is also shared with all
// copy-ancestors of this AmbientContext up to the first ancestor that
// also called AddSharedLogTag() or InitializeSharedLogTags().
//
// It is incorrect to have multiple calls of AddSharedLogTag()
// concurrent with each other.
func (ac *AmbientContext) AddSharedLogTag(name string, value interface{}) {
	if ac.sharedTags == nil {
		ac.InitializeSharedLogTags()
	}
	*ac.sharedTags = (*ac.sharedTags).Add(name, value)
}
