// Copyright 2016 The Cockroach Authors.
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

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/abortspan"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/batcheval"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/readsummary/rspb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/spanset"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

// SpanSetReplicaEvalContext is a testing-only implementation of
// ReplicaEvalContext which verifies that access to state is registered in the
// SpanSet if one is given.
type SpanSetReplicaEvalContext struct {
	i  batcheval.EvalContext
	ss spanset.SpanSet
}

var _ batcheval.EvalContext = &SpanSetReplicaEvalContext{}

// AbortSpan returns the abort span.
func (rec *SpanSetReplicaEvalContext) AbortSpan() *abortspan.AbortSpan {
	return rec.i.AbortSpan()
}

// EvalKnobs returns the batch evaluation Knobs.
func (rec *SpanSetReplicaEvalContext) EvalKnobs() kvserverbase.BatchEvalTestingKnobs {
	return rec.i.EvalKnobs()
}

// StoreID returns the StoreID.
func (rec *SpanSetReplicaEvalContext) StoreID() roachpb.StoreID {
	return rec.i.StoreID()
}

// GetRangeID returns the RangeID.
func (rec *SpanSetReplicaEvalContext) GetRangeID() roachpb.RangeID {
	return rec.i.GetRangeID()
}

// ClusterSettings returns the cluster settings.
func (rec *SpanSetReplicaEvalContext) ClusterSettings() *cluster.Settings {
	return rec.i.ClusterSettings()
}

// Clock returns the Replica's clock.
func (rec *SpanSetReplicaEvalContext) Clock() *hlc.Clock {
	return rec.i.Clock()
}

// GetConcurrencyManager returns the concurrency.Manager.
func (rec *SpanSetReplicaEvalContext) GetConcurrencyManager() concurrency.Manager {
	return rec.i.GetConcurrencyManager()
}

// NodeID returns the NodeID.
func (rec *SpanSetReplicaEvalContext) NodeID() roachpb.NodeID {
	return rec.i.NodeID()
}

// GetNodeLocality returns the node locality.
func (rec *SpanSetReplicaEvalContext) GetNodeLocality() roachpb.Locality {
	return rec.i.GetNodeLocality()
}

// GetFirstIndex returns the first index.
func (rec *SpanSetReplicaEvalContext) GetFirstIndex() uint64 {
	return rec.i.GetFirstIndex()
}

// GetTerm returns the term for the given index in the Raft log.
func (rec *SpanSetReplicaEvalContext) GetTerm(i uint64) (uint64, error) {
	return rec.i.GetTerm(i)
}

// GetLeaseAppliedIndex returns the lease index of the last applied command.
func (rec *SpanSetReplicaEvalContext) GetLeaseAppliedIndex() uint64 {
	return rec.i.GetLeaseAppliedIndex()
}

// IsFirstRange returns true iff the replica belongs to the first range.
func (rec *SpanSetReplicaEvalContext) IsFirstRange() bool {
	return rec.i.IsFirstRange()
}

// Desc returns the Replica's RangeDescriptor.
func (rec SpanSetReplicaEvalContext) Desc() *roachpb.RangeDescriptor {
	desc := rec.i.Desc()
	rec.ss.AssertAllowed(spanset.SpanReadOnly,
		roachpb.Span{Key: keys.RangeDescriptorKey(desc.StartKey)},
	)
	return desc
}

// ContainsKey returns true if the given key is within the Replica's range.
//
// TODO(bdarnell): Replace this method with one on Desc(). See comment
// on Replica.ContainsKey.
func (rec SpanSetReplicaEvalContext) ContainsKey(key roachpb.Key) bool {
	desc := rec.Desc() // already asserts
	return kvserverbase.ContainsKey(desc, key)
}

// GetMVCCStats returns the Replica's MVCCStats.
func (rec SpanSetReplicaEvalContext) GetMVCCStats() enginepb.MVCCStats {
	// Thanks to commutativity, the spanlatch manager does not have to serialize
	// on the MVCCStats key. This means that the key is not included in SpanSet
	// declarations, so there's nothing to assert here.
	return rec.i.GetMVCCStats()
}

// GetMaxSplitQPS returns the Replica's maximum queries/s rate for splitting and
// merging purposes.
func (rec SpanSetReplicaEvalContext) GetMaxSplitQPS(ctx context.Context) (float64, bool) {
	return rec.i.GetMaxSplitQPS(ctx)
}

// GetLastSplitQPS returns the Replica's most recent queries/s rate for
// splitting and merging purposes.
func (rec SpanSetReplicaEvalContext) GetLastSplitQPS(ctx context.Context) float64 {
	return rec.i.GetLastSplitQPS(ctx)
}

// CanCreateTxnRecord determines whether a transaction record can be created
// for the provided transaction information. See Replica.CanCreateTxnRecord
// for details about its arguments, return values, and preconditions.
func (rec SpanSetReplicaEvalContext) CanCreateTxnRecord(
	ctx context.Context, txnID uuid.UUID, txnKey []byte, txnMinTS hlc.Timestamp,
) (bool, roachpb.TransactionAbortedReason) {
	rec.ss.AssertAllowed(spanset.SpanReadOnly,
		roachpb.Span{Key: keys.TransactionKey(txnKey, txnID)},
	)
	return rec.i.CanCreateTxnRecord(ctx, txnID, txnKey, txnMinTS)
}

// MinTxnCommitTS determines the minimum timestamp at which a transaction with
// the provided ID and key can commit. See Replica.MinTxnCommitTS for details
// about its arguments, return values, and preconditions.
func (rec SpanSetReplicaEvalContext) MinTxnCommitTS(
	ctx context.Context, txnID uuid.UUID, txnKey []byte,
) hlc.Timestamp {
	rec.ss.AssertAllowed(spanset.SpanReadOnly,
		roachpb.Span{Key: keys.TransactionKey(txnKey, txnID)},
	)
	return rec.i.MinTxnCommitTS(ctx, txnID, txnKey)
}

// GetGCThreshold returns the GC threshold of the Range, typically updated when
// keys are garbage collected. Reads and writes at timestamps <= this time will
// not be served.
func (rec SpanSetReplicaEvalContext) GetGCThreshold() hlc.Timestamp {
	rec.ss.AssertAllowed(spanset.SpanReadOnly,
		roachpb.Span{Key: keys.RangeGCThresholdKey(rec.GetRangeID())},
	)
	return rec.i.GetGCThreshold()
}

// ExcludeDataFromBackup returns whether the replica is to be excluded from a
// backup.
func (rec SpanSetReplicaEvalContext) ExcludeDataFromBackup() bool {
	return rec.i.ExcludeDataFromBackup()
}

// String implements Stringer.
func (rec SpanSetReplicaEvalContext) String() string {
	return rec.i.String()
}

// GetLastReplicaGCTimestamp returns the last time the Replica was
// considered for GC.
func (rec SpanSetReplicaEvalContext) GetLastReplicaGCTimestamp(
	ctx context.Context,
) (hlc.Timestamp, error) {
	if err := rec.ss.CheckAllowed(spanset.SpanReadOnly,
		roachpb.Span{Key: keys.RangeLastReplicaGCTimestampKey(rec.GetRangeID())},
	); err != nil {
		return hlc.Timestamp{}, err
	}
	return rec.i.GetLastReplicaGCTimestamp(ctx)
}

// GetLease returns the Replica's current and next lease (if any).
func (rec SpanSetReplicaEvalContext) GetLease() (roachpb.Lease, roachpb.Lease) {
	rec.ss.AssertAllowed(spanset.SpanReadOnly,
		roachpb.Span{Key: keys.RangeLeaseKey(rec.GetRangeID())},
	)
	return rec.i.GetLease()
}

// GetRangeInfo is part of the EvalContext interface.
func (rec SpanSetReplicaEvalContext) GetRangeInfo(ctx context.Context) roachpb.RangeInfo {
	// Do the latching checks and ignore the results.
	rec.Desc()
	rec.GetLease()

	return rec.i.GetRangeInfo(ctx)
}

// GetCurrentReadSummary is part of the EvalContext interface.
func (rec *SpanSetReplicaEvalContext) GetCurrentReadSummary(ctx context.Context) rspb.ReadSummary {
	// To capture a read summary over the range, all keys must be latched for
	// writing to prevent any concurrent reads or writes.
	desc := rec.i.Desc()
	rec.ss.AssertAllowed(spanset.SpanReadWrite, roachpb.Span{
		Key:    keys.MakeRangeKeyPrefix(desc.StartKey),
		EndKey: keys.MakeRangeKeyPrefix(desc.EndKey),
	})
	rec.ss.AssertAllowed(spanset.SpanReadWrite, roachpb.Span{
		Key:    desc.StartKey.AsRawKey(),
		EndKey: desc.EndKey.AsRawKey(),
	})
	return rec.i.GetCurrentReadSummary(ctx)
}

// GetCurrentClosedTimestamp is part of the EvalContext interface.
func (rec *SpanSetReplicaEvalContext) GetCurrentClosedTimestamp(ctx context.Context) hlc.Timestamp {
	return rec.i.GetCurrentClosedTimestamp(ctx)
}

// GetClosedTimestampOlderThanStorageSnapshot is part of the EvalContext
// interface.
func (rec *SpanSetReplicaEvalContext) GetClosedTimestampOlderThanStorageSnapshot() hlc.Timestamp {
	return rec.i.GetClosedTimestampOlderThanStorageSnapshot()
}

// RevokeLease stops the replica from using its current lease.
func (rec *SpanSetReplicaEvalContext) RevokeLease(ctx context.Context, seq roachpb.LeaseSequence) {
	rec.i.RevokeLease(ctx, seq)
}

// WatchForMerge arranges to block all requests until the in-progress merge
// completes.
func (rec *SpanSetReplicaEvalContext) WatchForMerge(ctx context.Context) error {
	return rec.i.WatchForMerge(ctx)
}

// GetResponseMemoryAccount implements the batcheval.EvalContext interface.
func (rec *SpanSetReplicaEvalContext) GetResponseMemoryAccount() *mon.BoundAccount {
	return rec.i.GetResponseMemoryAccount()
}

// GetMaxBytes implements the batcheval.EvalContext interface.
func (rec *SpanSetReplicaEvalContext) GetMaxBytes() int64 {
	return rec.i.GetMaxBytes()
}

// GetEngineCapacity implements the batcheval.EvalContext interface.
func (rec *SpanSetReplicaEvalContext) GetEngineCapacity() (roachpb.StoreCapacity, error) {
	return rec.i.GetEngineCapacity()
}

// GetApproximateDiskBytes implements the batcheval.EvalContext interface.
func (rec *SpanSetReplicaEvalContext) GetApproximateDiskBytes(
	from, to roachpb.Key,
) (uint64, error) {
	return rec.i.GetApproximateDiskBytes(from, to)
}

// Release implements the batcheval.EvalContext interface.
func (rec *SpanSetReplicaEvalContext) Release() { rec.i.Release() }
