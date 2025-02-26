// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package stop

import (
	"context"
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
	sp       *tracing.Span       // possibly nil (but nil is functional)

	// The below fields are allocated only in Activate, i.e. on the async
	// goroutine.
	region region
}

type activeHandle Handle

type ActiveHandle interface {
	Release(ctx context.Context)
	stopperHandleMarker() // makes it easy to navigate to impl
}

func (hdl *Handle) Activate(ctx context.Context) ActiveHandle {
	growstack.Grow()

	hdl.region = hdl.s.startRegion(ctx, hdl.taskName)
	// NB: it's tempting for ergonomics to make `release` a method on `Handle` and
	// to return `hdl.release` here, but that allocates.
	return (*activeHandle)(hdl)
}

func (ah *activeHandle) stopperHandleMarker() {}

// Release must be called in a defer.
func (ah *activeHandle) Release(ctx context.Context) {
	hdl := (*Handle)(ah)
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
