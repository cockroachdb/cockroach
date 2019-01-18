// Copyright 2017 The Cockroach Authors.
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

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage/abortspan"
	"github.com/cockroachdb/cockroach/pkg/storage/batcheval"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/storage/spanset"
	"github.com/cockroachdb/cockroach/pkg/storage/storagebase"
	"github.com/cockroachdb/cockroach/pkg/storage/txnwait"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

// todoSpanSet is a placeholder value for callsites that need to pass a properly
// populated SpanSet (with according protection by the spanlatch manager) but fail
// to do so at the time of writing.
//
// See https://github.com/cockroachdb/cockroach/issues/19851.
//
// Do not introduce new uses of this.
var todoSpanSet = &spanset.SpanSet{}

// NewReplicaEvalContext returns a batcheval.EvalContext to use for command
// evaluation. The supplied SpanSet will be ignored except for race builds, in
// which case state access is asserted against it. A SpanSet must always be
// passed.
func NewReplicaEvalContext(r *Replica, ss *spanset.SpanSet) batcheval.EvalContext {
	if ss == nil {
		log.Fatalf(r.AnnotateCtx(context.Background()), "can't create a ReplicaEvalContext with assertions but no SpanSet")
	}
	if util.RaceEnabled {
		return &SpanSetReplicaEvalContext{
			i:  (*ReplicaEvalContext)(r),
			ss: *ss,
		}
	}
	return (*ReplicaEvalContext)(r)
}

type ReplicaEvalContext Replica

func (r *ReplicaEvalContext) String() string {
	return (*Replica)(r).String()
}

// ClusterSettings returns the node's ClusterSettings.
func (r *ReplicaEvalContext) ClusterSettings() *cluster.Settings {
	return r.store.cfg.Settings
}

// EvalKnobs returns the EvalContext's Knobs.
func (r *ReplicaEvalContext) EvalKnobs() storagebase.BatchEvalTestingKnobs {
	return r.store.cfg.TestingKnobs.EvalKnobs
}

// Engine returns the Replica's underlying Engine. In most cases the
// evaluation Batch should be used instead.
func (r *ReplicaEvalContext) Engine() engine.Engine {
	return r.store.Engine()
}

// Clock returns the hlc clock shared by this replica.
func (r *ReplicaEvalContext) Clock() *hlc.Clock {
	return r.store.Clock()
}

// DB returns the Replica's client DB.
func (r *ReplicaEvalContext) DB() *client.DB {
	return r.store.DB()
}

// AbortSpan returns the Replica's AbortSpan.
func (r *ReplicaEvalContext) AbortSpan() *abortspan.AbortSpan {
	// Despite its name, the AbortSpan doesn't hold on-disk data in
	// memory. It just provides methods that take a Batch, so SpanSet
	// declarations are enforced there.
	return r.abortSpan
}

// GetTxnWaitQueue returns the Replica's txnwait.Queue.
func (r *ReplicaEvalContext) GetTxnWaitQueue() *txnwait.Queue {
	return r.txnWaitQueue
}

// GetLimiters returns the Replica's limiters.
func (r *ReplicaEvalContext) GetLimiters() *batcheval.Limiters {
	return &r.store.limiters
}

// NodeID returns the ID of the node this replica belongs to.
func (r *ReplicaEvalContext) NodeID() roachpb.NodeID {
	return r.store.nodeDesc.NodeID
}

// StoreID returns the Replica's StoreID.
func (r *ReplicaEvalContext) StoreID() roachpb.StoreID {
	return r.store.StoreID()
}

// GetRangeID returns the Range ID.
func (r *ReplicaEvalContext) GetRangeID() roachpb.RangeID {
	return r.RangeID
}

// IsFirstRange returns true if this is the first range.
func (r *ReplicaEvalContext) IsFirstRange() bool {
	return r.RangeID == 1
}

// GetFirstIndex is the same function as (*Replica).raftFirstIndexLocked without
// the lock.
func (r *ReplicaEvalContext) GetFirstIndex() (uint64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	// NB: I think this is only called from log truncation decision making
	// otherwise, so there's an opportunity for further refactoring.
	return (*Replica)(r).raftFirstIndexLocked()
}

// GetMVCCStats returns a copy of the MVCC stats object for this range.
// This accessor is thread-safe, but provides no guarantees about its
// synchronization with any concurrent writes.
func (r *ReplicaEvalContext) GetMVCCStats() enginepb.MVCCStats {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return *r.mu.state.Stats
}

// GetSplitQPS returns the Replica's queries/s request rate.
// The value returned represents the QPS recorded at the time of the
// last request which can be found using GetLastRequestTime().
// NOTE: This should only be used for load based splitting, only
// works when the load based splitting cluster setting is enabled.
//
// Use QueriesPerSecond() for current QPS stats for all other purposes.
func (r *ReplicaEvalContext) GetSplitQPS() float64 {
	r.splitMu.Lock()
	defer r.splitMu.Unlock()
	return r.splitMu.qps
}

// GetLastReplicaGCTimestamp reads the timestamp at which the replica was
// last checked for removal by the replica gc queue.
func (r *ReplicaEvalContext) GetLastReplicaGCTimestamp(ctx context.Context) (hlc.Timestamp, error) {
	key := keys.RangeLastReplicaGCTimestampKey(r.RangeID)
	var timestamp hlc.Timestamp
	_, err := engine.MVCCGetProto(ctx, r.store.Engine(), key, hlc.Timestamp{}, &timestamp,
		engine.MVCCGetOptions{})
	if err != nil {
		return hlc.Timestamp{}, err
	}
	return timestamp, nil
}

// GetTerm returns the term of the given index in the raft log.
func (r *ReplicaEvalContext) GetTerm(i uint64) (uint64, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return (*Replica)(r).raftTermRLocked(i)
}

// GetLeaseAppliedIndex returns the lease index of the last applied command.
func (r *ReplicaEvalContext) GetLeaseAppliedIndex() uint64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.mu.state.LeaseAppliedIndex
}

// GetReplicaDescriptor returns the replica for this range from the range
// descriptor. Returns a *RangeNotFoundError if the replica is not found.
// No other errors are returned.
func (r *ReplicaEvalContext) GetReplicaDescriptor() (roachpb.ReplicaDescriptor, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return (*Replica)(r).getReplicaDescriptorRLocked()
}

// Desc returns the authoritative range descriptor, acquiring a replica lock in
// the process.
func (r *ReplicaEvalContext) Desc() *roachpb.RangeDescriptor {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.mu.state.Desc
}

// ContainsKey returns whether this range contains the specified key.
//
// TODO(bdarnell): This is not the same as RangeDescriptor.ContainsKey.
func (r *ReplicaEvalContext) ContainsKey(key roachpb.Key) bool {
	return storagebase.ContainsKey(*r.Desc(), key)
}

func (r *ReplicaEvalContext) CanCreateTxnRecord(
	txnID uuid.UUID, txnKey []byte, txnMinTSUpperBound hlc.Timestamp,
) (ok bool, minCommitTS hlc.Timestamp, reason roachpb.TransactionAbortedReason) {
	return CanCreateTxnRecord(r.store.tsCache, r.GetTxnSpanGCThreshold(), txnID, txnKey, txnMinTSUpperBound)
}

// GetGCThreshold returns the GC threshold.
func (r *ReplicaEvalContext) GetGCThreshold() hlc.Timestamp {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return *r.mu.state.GCThreshold
}

// GetTxnSpanGCThreshold returns the time of the replica's last transaction span
// GC.
func (r *ReplicaEvalContext) GetTxnSpanGCThreshold() hlc.Timestamp {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return *r.mu.state.TxnSpanGCThreshold
}

// GetLease returns the lease and, if available, the proposed next lease.
func (r *ReplicaEvalContext) GetLease() (roachpb.Lease, roachpb.Lease) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return (*Replica)(r).getLeaseRLocked()
}
