// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tracing

import "context"

type activeSpanKey struct{}

// noCtx is a singleton that we use internally to unify code paths that only
// optionally take a Context. The specific construction here does not matter,
// the only thing we need is that no outside caller could ever pass this
// context in (i.e. we can't use context.Background() and the like).
var noCtx context.Context = &struct{ context.Context }{context.Background()}

// SpanFromContext returns the *Span contained in the Context, if any.
func SpanFromContext(ctx context.Context) *Span {
	val := ctx.Value(activeSpanKey{})
	if sp, ok := val.(*Span); ok {
		return sp
	}
	return nil
}

// optimizedContext is an implementation of context.Context special
// cased to carry a Span under activeSpanKey{}. By making an explicit
// type we unlock optimizations that save allocations by allocating
// the optimizedContext together with the Span it eventually carries.
type optimizedContext struct {
	context.Context
	sp *Span
}

func (ctx *optimizedContext) Value(k interface{}) interface{} {
	if k == (interface{}(activeSpanKey{})) {
		return ctx.sp
	}
	return ctx.Context.Value(k)
}

// maybeWrapCtx returns a Context wrapping the Span, with two exceptions:
// 1. if ctx==noCtx, it's a noop
// 2. if ctx contains the noop Span, and sp is also the noop Span, elide
//    allocating a new Context.
//
// If a non-nil octx is passed in, it forms the returned Context. This can
// avoid allocations if the caller is able to allocate octx together with
// the Span, as is commonly possible when StartSpanCtx is used.
func maybeWrapCtx(ctx context.Context, octx *optimizedContext, sp *Span) (context.Context, *Span) {
	if ctx == noCtx {
		return noCtx, sp
	}
	// NB: we check sp != nil explicitly because some callers want to remove a
	// Span from a Context, and thus pass nil.
	if sp != nil && sp.i.isNoop() {
		// If the context originally had the noop span, and we would now be wrapping
		// the noop span in it again, we don't have to wrap at all and can save an
		// allocation.
		//
		// Note that applying this optimization for a nontrivial ctxSp would
		// constitute a bug: A real, non-recording span might later start recording.
		// Besides, the caller expects to get their own span, and will .Finish() it,
		// leading to an extra, premature call to Finish().
		if ctxSp := SpanFromContext(ctx); ctxSp != nil && ctxSp.i.isNoop() {
			return ctx, sp
		}
	}
	if octx != nil {
		octx.Context = ctx
		octx.sp = sp
		return octx, sp
	}
	return context.WithValue(ctx, activeSpanKey{}, sp), sp
}

// ContextWithSpan returns a Context wrapping the supplied Span.
func ContextWithSpan(ctx context.Context, sp *Span) context.Context {
	ctx, _ = maybeWrapCtx(ctx, nil /* octx */, sp)
	return ctx
}
