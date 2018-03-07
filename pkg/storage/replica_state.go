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

package storage

import (
	"context"

	opentracing "github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage/abortspan"
	"github.com/cockroachdb/cockroach/pkg/storage/batcheval"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/storage/stateloader"
	"github.com/cockroachdb/cockroach/pkg/storage/storagebase"
	"github.com/cockroachdb/cockroach/pkg/storage/txnwait"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// writeInitialReplicaState sets up a new Range, but without writing an
// associated Raft state (which must be written separately via
// synthesizeRaftState before instantiating a Replica). The main task is to
// persist a ReplicaState which does not start from zero but presupposes a few
// entries already having applied. The supplied MVCCStats are used for the Stats
// field after adjusting for persisting the state itself, and the updated stats
// are returned.
func writeInitialReplicaState(
	ctx context.Context,
	st *cluster.Settings,
	eng engine.ReadWriter,
	ms enginepb.MVCCStats,
	desc roachpb.RangeDescriptor,
	lease roachpb.Lease,
	gcThreshold hlc.Timestamp,
	txnSpanGCThreshold hlc.Timestamp,
) (enginepb.MVCCStats, error) {
	rsl := stateloader.Make(st, desc.RangeID)

	var s storagebase.ReplicaState
	s.TruncatedState = &roachpb.RaftTruncatedState{
		Term:  raftInitialLogTerm,
		Index: raftInitialLogIndex,
	}
	s.RaftAppliedIndex = s.TruncatedState.Index
	s.Desc = &roachpb.RangeDescriptor{
		RangeID: desc.RangeID,
	}
	s.Stats = &ms
	s.Lease = &lease
	s.GCThreshold = &gcThreshold
	s.TxnSpanGCThreshold = &txnSpanGCThreshold

	if existingLease, err := rsl.LoadLease(ctx, eng); err != nil {
		return enginepb.MVCCStats{}, errors.Wrap(err, "error reading lease")
	} else if (existingLease != roachpb.Lease{}) {
		log.Fatalf(ctx, "expected trivial lease, but found %+v", existingLease)
	}

	if existingGCThreshold, err := rsl.LoadGCThreshold(ctx, eng); err != nil {
		return enginepb.MVCCStats{}, errors.Wrap(err, "error reading GCThreshold")
	} else if (*existingGCThreshold != hlc.Timestamp{}) {
		log.Fatalf(ctx, "expected trivial GChreshold, but found %+v", existingGCThreshold)
	}

	if existingTxnSpanGCThreshold, err := rsl.LoadTxnSpanGCThreshold(ctx, eng); err != nil {
		return enginepb.MVCCStats{}, errors.Wrap(err, "error reading TxnSpanGCThreshold")
	} else if (*existingTxnSpanGCThreshold != hlc.Timestamp{}) {
		log.Fatalf(ctx, "expected trivial TxnSpanGCThreshold, but found %+v", existingTxnSpanGCThreshold)
	}

	newMS, err := rsl.Save(ctx, eng, s)
	if err != nil {
		return enginepb.MVCCStats{}, err
	}

	return newMS, nil
}

// writeInitialState calls writeInitialReplicaState followed by
// synthesizeRaftState. It is typically called during bootstrap. The supplied
// MVCCStats are used for the Stats field after adjusting for persisting the
// state itself, and the updated stats are returned.
func writeInitialState(
	ctx context.Context,
	st *cluster.Settings,
	eng engine.ReadWriter,
	ms enginepb.MVCCStats,
	desc roachpb.RangeDescriptor,
	lease roachpb.Lease,
	gcThreshold hlc.Timestamp,
	txnSpanGCThreshold hlc.Timestamp,
) (enginepb.MVCCStats, error) {
	newMS, err := writeInitialReplicaState(ctx, st, eng, ms, desc, lease, gcThreshold, txnSpanGCThreshold)
	if err != nil {
		return enginepb.MVCCStats{}, err
	}
	if err := stateloader.Make(st, desc.RangeID).SynthesizeRaftState(ctx, eng); err != nil {
		return enginepb.MVCCStats{}, err
	}
	return newMS, nil
}

// ClusterSettings returns the node's ClusterSettings.
func (r *Replica) ClusterSettings() *cluster.Settings {
	return r.store.cfg.Settings
}

// In-memory state, immutable fields, and debugging methods are accessed directly.

// StoreID returns the Replica's StoreID.
func (r *Replica) StoreID() roachpb.StoreID {
	return r.store.StoreID()
}

// EvalKnobs returns the EvalContext's Knobs.
func (r *Replica) EvalKnobs() batcheval.TestingKnobs {
	return r.store.cfg.TestingKnobs.EvalKnobs
}

// Tracer returns the Replica's Tracer.
func (r *Replica) Tracer() opentracing.Tracer {
	return r.store.Tracer()
}

// Clock returns the hlc clock shared by this replica.
func (r *Replica) Clock() *hlc.Clock {
	return r.store.Clock()
}

// DB returns the Replica's client DB.
func (r *Replica) DB() *client.DB {
	return r.store.DB()
}

// Engine returns the Replica's underlying Engine. In most cases the
// evaluation Batch should be used instead.
func (r *Replica) Engine() engine.Engine {
	return r.store.Engine()
}

// AbortSpan returns the Replica's AbortSpan.
func (r *Replica) AbortSpan() *abortspan.AbortSpan {
	// Despite its name, the AbortSpan doesn't hold on-disk data in
	// memory. It just provides methods that take a Batch, so SpanSet
	// declarations are enforced there.
	return r.abortSpan
}

// GetLimiters returns the Replica's limiters.
func (r *Replica) GetLimiters() *batcheval.Limiters {
	return &r.store.limiters
}

// GetTxnWaitQueue returns the Replica's txnwait.Queue.
func (r *Replica) GetTxnWaitQueue() *txnwait.Queue {
	return r.txnWaitQueue
}

// GetTerm returns the term of the given index in the raft log.
func (r *Replica) GetTerm(i uint64) (uint64, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.raftTermRLocked(i)
}

// GetRangeID returns the Range ID.
func (r *Replica) GetRangeID() roachpb.RangeID {
	return r.RangeID
}

// GetGCThreshold returns the GC threshold.
func (r *Replica) GetGCThreshold() hlc.Timestamp {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return *r.mu.state.GCThreshold
}

// GetTxnSpanGCThreshold returns the time of the replica's last transaction span
// GC.
func (r *Replica) GetTxnSpanGCThreshold() hlc.Timestamp {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return *r.mu.state.TxnSpanGCThreshold
}
