// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvserver

import (
	"context"

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

// evalContextImpl implements the batcheval.EvalContext interface.
type evalContextImpl struct {
	*Replica
	// NB: We cannot use the emptiness of `closedTS` to determine whether the
	// closed timestamp was elided during the creation of this eval context, so we
	// track it separately.
	closedTSElided bool
	closedTS       hlc.Timestamp
}

func newEvalContextImpl(r *Replica, requireClosedTS bool) *evalContextImpl {
	var closedTS hlc.Timestamp
	if requireClosedTS {
		// We elide this call to get the replica's current closed timestamp unless
		// the request requires it, in order to avoid redundant mutex contention.
		closedTS = r.GetCurrentClosedTimestamp(context.Background())
	}

	return &evalContextImpl{
		Replica:        r,
		closedTSElided: !requireClosedTS,
		closedTS:       closedTS,
	}
}

// GetClosedTimestamp implements the EvalContext interface.
func (ec *evalContextImpl) GetClosedTimestamp() hlc.Timestamp {
	if ec.closedTSElided {
		panic("closed timestamp was elided during eval context creation; does the" +
			" request set the requiresClosedTimestamp flag?")
	}
	return ec.closedTS
}

var _ batcheval.EvalContext = &evalContextImpl{}

// NewReplicaEvalContext returns a batcheval.EvalContext to use for command
// evaluation. The supplied SpanSet will be ignored except for race builds, in
// which case state access is asserted against it. A SpanSet must always be
// passed.
func NewReplicaEvalContext(
	r *Replica, ss *spanset.SpanSet, requireClosedTS bool,
) batcheval.EvalContext {
	if ss == nil {
		log.Fatalf(r.AnnotateCtx(context.Background()), "can't create a ReplicaEvalContext with assertions but no SpanSet")
	}

	ec := newEvalContextImpl(r, requireClosedTS)
	if util.RaceEnabled {
		return &SpanSetReplicaEvalContext{
			i:  ec,
			ss: *ss,
		}
	}
	return ec
}
