// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package batcheval

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/abortspan"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/readsummary/rspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/limit"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"golang.org/x/time/rate"
)

// Limiters is the collection of per-store limits used during cmd evaluation.
type Limiters struct {
	BulkIOWriteRate                      *rate.Limiter
	ConcurrentExportRequests             limit.ConcurrentRequestLimiter
	ConcurrentAddSSTableRequests         limit.ConcurrentRequestLimiter
	ConcurrentAddSSTableAsWritesRequests limit.ConcurrentRequestLimiter
	// concurrentRangefeedIters is a semaphore used to limit the number of
	// rangefeeds in the "catch-up" state across the store. The "catch-up" state
	// is a temporary state at the beginning of a rangefeed which is expensive
	// because it uses an engine iterator.
	ConcurrentRangefeedIters         limit.ConcurrentRequestLimiter
	ConcurrentScanInterleavedIntents limit.ConcurrentRequestLimiter
}

// EvalContext is the interface through which command evaluation accesses the
// underlying state.
type EvalContext interface {
	fmt.Stringer
	ClusterSettings() *cluster.Settings
	EvalKnobs() kvserverbase.BatchEvalTestingKnobs

	Clock() *hlc.Clock
	AbortSpan() *abortspan.AbortSpan
	GetConcurrencyManager() concurrency.Manager

	NodeID() roachpb.NodeID
	StoreID() roachpb.StoreID
	GetRangeID() roachpb.RangeID
	GetNodeLocality() roachpb.Locality

	IsFirstRange() bool
	GetFirstIndex() (uint64, error)
	GetTerm(uint64) (uint64, error)
	GetLeaseAppliedIndex() uint64

	Desc() *roachpb.RangeDescriptor
	ContainsKey(key roachpb.Key) bool

	// CanCreateTxnRecord determines whether a transaction record can be created
	// for the provided transaction information. See Replica.CanCreateTxnRecord
	// for details about its arguments, return values, and preconditions.
	CanCreateTxnRecord(
		ctx context.Context, txnID uuid.UUID, txnKey []byte, txnMinTS hlc.Timestamp,
	) (ok bool, minCommitTS hlc.Timestamp, reason roachpb.TransactionAbortedReason)

	// GetMVCCStats returns a snapshot of the MVCC stats for the range.
	// If called from a command that declares a read/write span on the
	// entire range, the stats will be consistent with the data that is
	// visible to the batch. Otherwise, it may return inconsistent
	// results due to concurrent writes.
	GetMVCCStats() enginepb.MVCCStats

	// GetMaxSplitQPS returns the Replicas maximum queries/s request rate over a
	// configured retention period.
	//
	// NOTE: This should not be used when the load based splitting cluster setting
	// is disabled.
	GetMaxSplitQPS() (float64, bool)

	// GetLastSplitQPS returns the Replica's most recent queries/s request rate.
	//
	// NOTE: This should not be used when the load based splitting cluster setting
	// is disabled.
	//
	// TODO(nvanbenschoten): remove this method in v22.1.
	GetLastSplitQPS() float64

	GetGCThreshold() hlc.Timestamp
	ExcludeDataFromBackup() bool
	GetLastReplicaGCTimestamp(context.Context) (hlc.Timestamp, error)
	GetLease() (roachpb.Lease, roachpb.Lease)
	GetRangeInfo(context.Context) roachpb.RangeInfo

	// GetCurrentReadSummary returns a new ReadSummary reflecting all reads
	// served by the range to this point. The method requires a write latch
	// across all keys in the range (see declareAllKeys), because it will only
	// return a meaningful summary if the caller has serialized with all other
	// requests on the range.
	GetCurrentReadSummary(ctx context.Context) rspb.ReadSummary

	// GetClosedTimestamp returns the current closed timestamp on the range.
	// It is expected that a caller will have performed some action (either
	// calling RevokeLease or WatchForMerge) to freeze further progression of
	// the closed timestamp before calling this method.
	GetClosedTimestamp(ctx context.Context) hlc.Timestamp

	GetExternalStorage(ctx context.Context, dest roachpb.ExternalStorage) (cloud.ExternalStorage, error)
	GetExternalStorageFromURI(ctx context.Context, uri string, user security.SQLUsername) (cloud.ExternalStorage,
		error)

	// RevokeLease stops the replica from using its current lease, if that lease
	// matches the provided lease sequence. All future calls to leaseStatus on
	// this node with the current lease will now return a PROSCRIBED status.
	RevokeLease(context.Context, roachpb.LeaseSequence)

	// WatchForMerge arranges to block all requests until the in-progress merge
	// completes. Returns an error if no in-progress merge is detected.
	WatchForMerge(ctx context.Context) error

	// GetResponseMemoryAccount returns a memory account to be used when
	// generating BatchResponses. Currently only used for MVCC scans, and only
	// non-nil on those paths (a nil account is safe to use since it functions
	// as an unlimited account).
	GetResponseMemoryAccount() *mon.BoundAccount
}

// MockEvalCtx is a dummy implementation of EvalContext for testing purposes.
// For technical reasons, the interface is implemented by a wrapper .EvalContext().
type MockEvalCtx struct {
	ClusterSettings    *cluster.Settings
	Desc               *roachpb.RangeDescriptor
	StoreID            roachpb.StoreID
	Clock              *hlc.Clock
	Stats              enginepb.MVCCStats
	QPS                float64
	AbortSpan          *abortspan.AbortSpan
	GCThreshold        hlc.Timestamp
	Term, FirstIndex   uint64
	CanCreateTxn       func() (bool, hlc.Timestamp, roachpb.TransactionAbortedReason)
	Lease              roachpb.Lease
	CurrentReadSummary rspb.ReadSummary
	ClosedTimestamp    hlc.Timestamp
	RevokedLeaseSeq    roachpb.LeaseSequence
}

// EvalContext returns the MockEvalCtx as an EvalContext. It will reflect future
// modifications to the underlying MockEvalContext.
func (m *MockEvalCtx) EvalContext() EvalContext {
	return &mockEvalCtxImpl{MockEvalCtx: m}
}

type mockEvalCtxImpl struct {
	// Hide the fields of MockEvalCtx which have names that conflict with some
	// of the interface methods.
	*MockEvalCtx
}

func (m *mockEvalCtxImpl) String() string {
	return "mock"
}
func (m *mockEvalCtxImpl) ClusterSettings() *cluster.Settings {
	return m.MockEvalCtx.ClusterSettings
}
func (m *mockEvalCtxImpl) EvalKnobs() kvserverbase.BatchEvalTestingKnobs {
	return kvserverbase.BatchEvalTestingKnobs{}
}
func (m *mockEvalCtxImpl) Clock() *hlc.Clock {
	return m.MockEvalCtx.Clock
}
func (m *mockEvalCtxImpl) AbortSpan() *abortspan.AbortSpan {
	return m.MockEvalCtx.AbortSpan
}
func (m *mockEvalCtxImpl) GetConcurrencyManager() concurrency.Manager {
	panic("unimplemented")
}
func (m *mockEvalCtxImpl) NodeID() roachpb.NodeID {
	panic("unimplemented")
}
func (m *mockEvalCtxImpl) GetNodeLocality() roachpb.Locality {
	panic("unimplemented")
}
func (m *mockEvalCtxImpl) StoreID() roachpb.StoreID {
	return m.MockEvalCtx.StoreID
}
func (m *mockEvalCtxImpl) GetRangeID() roachpb.RangeID {
	return m.MockEvalCtx.Desc.RangeID
}
func (m *mockEvalCtxImpl) IsFirstRange() bool {
	panic("unimplemented")
}
func (m *mockEvalCtxImpl) GetFirstIndex() (uint64, error) {
	return m.FirstIndex, nil
}
func (m *mockEvalCtxImpl) GetTerm(uint64) (uint64, error) {
	return m.Term, nil
}
func (m *mockEvalCtxImpl) GetLeaseAppliedIndex() uint64 {
	panic("unimplemented")
}
func (m *mockEvalCtxImpl) Desc() *roachpb.RangeDescriptor {
	return m.MockEvalCtx.Desc
}
func (m *mockEvalCtxImpl) ContainsKey(key roachpb.Key) bool {
	return false
}
func (m *mockEvalCtxImpl) GetMVCCStats() enginepb.MVCCStats {
	return m.Stats
}
func (m *mockEvalCtxImpl) GetMaxSplitQPS() (float64, bool) {
	return m.QPS, true
}
func (m *mockEvalCtxImpl) GetLastSplitQPS() float64 {
	return m.QPS
}
func (m *mockEvalCtxImpl) CanCreateTxnRecord(
	context.Context, uuid.UUID, []byte, hlc.Timestamp,
) (bool, hlc.Timestamp, roachpb.TransactionAbortedReason) {
	return m.CanCreateTxn()
}
func (m *mockEvalCtxImpl) GetGCThreshold() hlc.Timestamp {
	return m.GCThreshold
}
func (m *mockEvalCtxImpl) ExcludeDataFromBackup() bool {
	return false
}
func (m *mockEvalCtxImpl) GetLastReplicaGCTimestamp(context.Context) (hlc.Timestamp, error) {
	panic("unimplemented")
}
func (m *mockEvalCtxImpl) GetLease() (roachpb.Lease, roachpb.Lease) {
	return m.Lease, roachpb.Lease{}
}
func (m *mockEvalCtxImpl) GetRangeInfo(ctx context.Context) roachpb.RangeInfo {
	return roachpb.RangeInfo{Desc: *m.Desc(), Lease: m.Lease}
}
func (m *mockEvalCtxImpl) GetCurrentReadSummary(ctx context.Context) rspb.ReadSummary {
	return m.CurrentReadSummary
}
func (m *mockEvalCtxImpl) GetClosedTimestamp(ctx context.Context) hlc.Timestamp {
	return m.ClosedTimestamp
}
func (m *mockEvalCtxImpl) GetExternalStorage(
	ctx context.Context, dest roachpb.ExternalStorage,
) (cloud.ExternalStorage, error) {
	panic("unimplemented")
}
func (m *mockEvalCtxImpl) GetExternalStorageFromURI(
	ctx context.Context, uri string, user security.SQLUsername,
) (cloud.ExternalStorage, error) {
	panic("unimplemented")
}
func (m *mockEvalCtxImpl) RevokeLease(_ context.Context, seq roachpb.LeaseSequence) {
	m.RevokedLeaseSeq = seq
}
func (m *mockEvalCtxImpl) WatchForMerge(ctx context.Context) error {
	panic("unimplemented")
}
func (m *mockEvalCtxImpl) GetResponseMemoryAccount() *mon.BoundAccount {
	// No limits.
	return nil
}
