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
	opentracing "github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage/abortspan"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/storage/stateloader"
	"github.com/cockroachdb/cockroach/pkg/storage/storagebase"
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

// ReplicaEvalContext is the interface through which command
// evaluation accesses the in-memory state of a Replica. Any state
// that corresponds to (mutable) on-disk data must be registered in
// the SpanSet if one is given.
type ReplicaEvalContext struct {
	repl *Replica
	ss   *SpanSet
}

// ClusterSettings returns the node's ClusterSettings.
func (rec ReplicaEvalContext) ClusterSettings() *cluster.Settings {
	return rec.repl.store.cfg.Settings
}

func (rec *ReplicaEvalContext) makeReplicaStateLoader() stateloader.StateLoader {
	return stateloader.Make(rec.ClusterSettings(), rec.RangeID())
}

// In-memory state, immutable fields, and debugging methods are accessed directly.

// NodeID returns the Replica's NodeID.
func (rec ReplicaEvalContext) NodeID() roachpb.NodeID {
	return rec.repl.NodeID()
}

// StoreID returns the Replica's StoreID.
func (rec ReplicaEvalContext) StoreID() roachpb.StoreID {
	return rec.repl.store.StoreID()
}

// RangeID returns the Replica's RangeID.
func (rec ReplicaEvalContext) RangeID() roachpb.RangeID {
	return rec.repl.RangeID
}

// IsFirstRange returns true if this replica is the first range in the
// system.
func (rec ReplicaEvalContext) IsFirstRange() bool {
	return rec.repl.IsFirstRange()
}

// String returns a string representation of the Replica.
func (rec ReplicaEvalContext) String() string {
	return rec.repl.String()
}

// StoreTestingKnobs returns the Replica's StoreTestingKnobs.
func (rec ReplicaEvalContext) StoreTestingKnobs() StoreTestingKnobs {
	return rec.repl.store.cfg.TestingKnobs
}

// Tracer returns the Replica's Tracer.
func (rec ReplicaEvalContext) Tracer() opentracing.Tracer {
	return rec.repl.store.Tracer()
}

// DB returns the Replica's client DB.
func (rec ReplicaEvalContext) DB() *client.DB {
	return rec.repl.store.DB()
}

// Engine returns the Replica's underlying Engine. In most cases the
// evaluation Batch should be used instead.
func (rec ReplicaEvalContext) Engine() engine.Engine {
	return rec.repl.store.Engine()
}

// AbortSpan returns the Replica's AbortSpan.
func (rec ReplicaEvalContext) AbortSpan() *abortspan.AbortSpan {
	// Despite its name, the AbortSpan doesn't hold on-disk data in
	// memory. It just provides methods that take a Batch, so SpanSet
	// declarations are enforced there.
	return rec.repl.abortSpan
}

// pushTxnQueue returns the Replica's pushTxnQueue.
func (rec ReplicaEvalContext) pushTxnQueue() *pushTxnQueue {
	return rec.repl.pushTxnQueue
}

// FirstIndex returns the oldest index in the raft log.
func (rec ReplicaEvalContext) FirstIndex() (uint64, error) {
	return rec.repl.GetFirstIndex()
}

// Term returns the term of the given entry in the raft log.
func (rec ReplicaEvalContext) Term(i uint64) (uint64, error) {
	rec.repl.mu.RLock()
	defer rec.repl.mu.RUnlock()
	return rec.repl.raftTermRLocked(i)
}

// Fields backed by on-disk data must be registered in the SpanSet.

// Desc returns the Replica's RangeDescriptor.
func (rec ReplicaEvalContext) Desc() (*roachpb.RangeDescriptor, error) {
	rec.repl.mu.RLock()
	defer rec.repl.mu.RUnlock()
	if rec.ss != nil {
		if err := rec.ss.checkAllowed(SpanReadOnly,
			roachpb.Span{Key: keys.RangeDescriptorKey(rec.repl.mu.state.Desc.StartKey)},
		); err != nil {
			return nil, err
		}
	}
	return rec.repl.mu.state.Desc, nil
}

// ContainsKey returns true if the given key is within the Replica's range.
//
// TODO(bdarnell): Replace this method with one on Desc(). See comment
// on Replica.ContainsKey.
func (rec ReplicaEvalContext) ContainsKey(key roachpb.Key) (bool, error) {
	rec.repl.mu.RLock()
	defer rec.repl.mu.RUnlock()
	if rec.ss != nil {
		if err := rec.ss.checkAllowed(SpanReadOnly,
			roachpb.Span{Key: keys.RangeDescriptorKey(rec.repl.mu.state.Desc.StartKey)},
		); err != nil {
			return false, err
		}
	}
	return containsKey(*rec.repl.mu.state.Desc, key), nil
}

// GetMVCCStats returns the Replica's MVCCStats.
func (rec ReplicaEvalContext) GetMVCCStats() (enginepb.MVCCStats, error) {
	if rec.ss != nil {
		if err := rec.ss.checkAllowed(SpanReadOnly,
			roachpb.Span{Key: keys.RangeStatsKey(rec.RangeID())},
		); err != nil {
			return enginepb.MVCCStats{}, err
		}
	}
	return rec.repl.GetMVCCStats(), nil
}

// GCThreshold returns the GC threshold of the Range, typically updated when
// keys are garbage collected. Reads and writes at timestamps <= this time will
// not be served.
func (rec ReplicaEvalContext) GCThreshold() (hlc.Timestamp, error) {
	if rec.ss != nil {
		if err := rec.ss.checkAllowed(SpanReadOnly,
			roachpb.Span{Key: keys.RangeLastGCKey(rec.RangeID())},
		); err != nil {
			return hlc.Timestamp{}, err
		}
	}
	rec.repl.mu.RLock()
	defer rec.repl.mu.RUnlock()
	return *rec.repl.mu.state.GCThreshold, nil
}

// TxnSpanGCThreshold returns the time of the Replica's last
// transaction span GC.
func (rec ReplicaEvalContext) TxnSpanGCThreshold() (hlc.Timestamp, error) {
	if rec.ss != nil {
		if err := rec.ss.checkAllowed(SpanReadOnly,
			roachpb.Span{Key: keys.RangeTxnSpanGCThresholdKey(rec.RangeID())},
		); err != nil {
			return hlc.Timestamp{}, err
		}
	}
	rec.repl.mu.RLock()
	defer rec.repl.mu.RUnlock()
	return *rec.repl.mu.state.TxnSpanGCThreshold, nil
}

// GetLastReplicaGCTimestamp returns the last time the Replica was
// considered for GC.
func (rec ReplicaEvalContext) GetLastReplicaGCTimestamp(
	ctx context.Context,
) (hlc.Timestamp, error) {
	if rec.ss != nil {
		if err := rec.ss.checkAllowed(SpanReadOnly,
			roachpb.Span{Key: keys.RangeLastReplicaGCTimestampKey(rec.RangeID())},
		); err != nil {
			return hlc.Timestamp{}, err
		}
	}
	return rec.repl.getLastReplicaGCTimestamp(ctx)
}

// GetLease returns the Replica's current and next lease (if any).
func (rec ReplicaEvalContext) GetLease() (roachpb.Lease, *roachpb.Lease, error) {
	if rec.ss != nil {
		if err := rec.ss.checkAllowed(SpanReadOnly,
			roachpb.Span{Key: keys.RangeLeaseKey(rec.RangeID())},
		); err != nil {
			return roachpb.Lease{}, nil, err
		}
	}
	lease, nextLease := rec.repl.getLease()
	return lease, nextLease, nil
}
