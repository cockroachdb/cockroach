// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package stop

import (
	"context"
	"fmt"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/util/growstack"
	"github.com/cockroachdb/cockroach/pkg/util/quotapool"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
)

var handlePool = sync.Pool{New: func() interface{} {
	return &Handle{}
}}

type Handle struct {
	s        *Stopper
	taskName string
	spanOpt  SpanOption
	alloc    *quotapool.IntAlloc // possibly nil

	// The below fields are allocated only in Activate, i.e. on the async
	// goroutine.
	sp     *tracing.Span // possibly nil (but nil is functional)
	region region
}

func (hdl *Handle) Activate(ctx context.Context) (context.Context, func(context.Context, *Handle)) {
	growstack.Grow()

	// If the caller has a span, the task gets a child span.
	//
	// Because we're in the spawned async goroutine, the parent span might get
	// Finish()ed by then. That's okay, if the parent goes away without waiting
	// for the child, it will not collect the child anyway.
	switch hdl.spanOpt {
	case FollowsFromSpan:
		ctx, hdl.sp = tracing.ForkSpan(ctx, hdl.taskName)
	case ChildSpan:
		ctx, hdl.sp = tracing.ChildSpan(ctx, hdl.taskName)
	case SterileRootSpan:
		ctx, hdl.sp = hdl.s.tracer.StartSpanCtx(ctx, hdl.taskName, tracing.WithSterile())
	default:
		panic(fmt.Sprintf("unsupported SpanOption: %v", hdl.spanOpt))
	}
	hdl.region = hdl.s.startRegion(ctx, hdl.taskName)
	// NB: it's tempting for ergonomics to make `release` a method on `Handle` and
	// to return `hdl.release` here, but that allocates.
	return ctx, release
}

func release(ctx context.Context, hdl *Handle) {
	hdl.s.recover(ctx)
	hdl.region.End()
	hdl.sp.Finish()
	if hdl.alloc != nil {
		hdl.alloc.Release()
	}
	hdl.s.runPostlude()
	*hdl = Handle{}
	handlePool.Put(hdl)
}
