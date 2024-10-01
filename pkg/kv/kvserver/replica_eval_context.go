// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver

import (
	"context"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/batcheval"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/spanset"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// todoSpanSet is a placeholder value for callsites that need to pass a properly
// populated SpanSet (with according protection by the spanlatch manager) but fail
// to do so at the time of writing.
//
// See https://github.com/cockroachdb/cockroach/issues/19851.
//
// Do not introduce new uses of this.
var todoSpanSet = &spanset.SpanSet{}

var evalContextPool = sync.Pool{
	New: func() interface{} {
		return &evalContextImpl{}
	},
}

// evalContextImpl implements the batcheval.EvalContext interface.
type evalContextImpl struct {
	*Replica
	// NB: We cannot use the emptiness of `closedTS` to determine whether the
	// closed timestamp was elided during the creation of this eval context, so we
	// track it separately.
	closedTSElided bool
	closedTS       hlc.Timestamp
	ah             kvpb.AdmissionHeader
}

func newEvalContextImpl(
	ctx context.Context,
	r *Replica,
	requiresClosedTSOlderThanStorageSnap bool,
	ah kvpb.AdmissionHeader,
) (ec *evalContextImpl) {
	var closedTS hlc.Timestamp
	if requiresClosedTSOlderThanStorageSnap {
		// We elide this call to get the replica's current closed timestamp unless
		// the request requires it, in order to avoid redundant mutex contention.
		closedTS = r.GetCurrentClosedTimestamp(ctx)
	}

	ec = evalContextPool.Get().(*evalContextImpl)
	*ec = evalContextImpl{
		Replica:        r,
		closedTSElided: !requiresClosedTSOlderThanStorageSnap,
		closedTS:       closedTS,
		ah:             ah,
	}
	return ec
}

// GetClosedTimestampOlderThanStorageSnapshot implements the EvalContext
// interface.
func (ec *evalContextImpl) GetClosedTimestampOlderThanStorageSnapshot() hlc.Timestamp {
	if ec.closedTSElided {
		panic("closed timestamp was elided during eval context creation; does the" +
			" request set the requiresClosedTimestamp flag?")
	}
	return ec.closedTS
}

// Release implements the EvalContext interface.
func (ec *evalContextImpl) Release() {
	*ec = evalContextImpl{}
	evalContextPool.Put(ec)
}

// AdmissionHeader implements the EvalContext interface.
func (ec *evalContextImpl) AdmissionHeader() kvpb.AdmissionHeader {
	return ec.ah
}

var _ batcheval.EvalContext = &evalContextImpl{}

// NewReplicaEvalContext returns a batcheval.EvalContext to use for command
// evaluation. The supplied SpanSet will be ignored except for race builds, in
// which case state access is asserted against it. A SpanSet must always be
// passed.
// The caller must call rec.Release() once done with the evaluation context in
// order to return its memory back to a sync.Pool.
func NewReplicaEvalContext(
	ctx context.Context,
	r *Replica,
	ss *spanset.SpanSet,
	requiresClosedTSOlderThanStorageSnap bool,
	ah kvpb.AdmissionHeader,
) (rec batcheval.EvalContext) {
	if ss == nil {
		log.Fatalf(r.AnnotateCtx(context.Background()), "can't create a ReplicaEvalContext with assertions but no SpanSet")
	}

	rec = newEvalContextImpl(ctx, r, requiresClosedTSOlderThanStorageSnap, ah)
	if util.RaceEnabled {
		return &SpanSetReplicaEvalContext{
			i:  rec,
			ss: *ss,
		}
	}
	return rec
}
