// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package batcheval

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/abortspan"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/intentresolver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/readsummary/rspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
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
	ConcurrentRangefeedIters limit.ConcurrentRequestLimiter
}

// EvalContext is the interface through which command evaluation accesses the
// underlying state.
type EvalContext interface {
	fmt.Stringer
	ImmutableEvalContext

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
	GetFirstIndex() kvpb.RaftIndex
	GetTerm(index kvpb.RaftIndex) (kvpb.RaftTerm, error)
	GetLeaseAppliedIndex() kvpb.LeaseAppliedIndex

	Desc() *roachpb.RangeDescriptor
	ContainsKey(key roachpb.Key) bool

	// CanCreateTxnRecord determines whether a transaction record can be created
	// for the provided transaction information. See Replica.CanCreateTxnRecord
	// for details about its arguments, return values, and preconditions.
	CanCreateTxnRecord(
		ctx context.Context, txnID uuid.UUID, txnKey []byte, txnMinTS hlc.Timestamp,
	) (ok bool, reason kvpb.TransactionAbortedReason)

	// MinTxnCommitTS determines the minimum timestamp at which a transaction with
	// the provided ID and key can commit. See Replica.MinTxnCommitTS for details
	// about its arguments, return values, and preconditions.
	MinTxnCommitTS(ctx context.Context, txnID uuid.UUID, txnKey []byte) hlc.Timestamp

	// GetMVCCStats returns a snapshot of the MVCC stats for the range.
	// If called from a command that declares a read/write span on the
	// entire range, the stats will be consistent with the data that is
	// visible to the batch. Otherwise, it may return inconsistent
	// results due to concurrent writes.
	GetMVCCStats() enginepb.MVCCStats

	// GetMaxSplitQPS returns the Replica's maximum queries/s request rate over
	// a configured retention period.
	//
	// NOTE: This should not be used when the load based splitting cluster setting
	// is disabled.
	GetMaxSplitQPS(context.Context) (float64, bool)

	// GetMaxSplitCPU returns the Replica's maximum request cpu/s rate over a
	// configured retention period.
	//
	// NOTE: This should not be used when the load based splitting cluster setting
	// is disabled.
	GetMaxSplitCPU(context.Context) (float64, bool)

	GetGCThreshold() hlc.Timestamp
	ExcludeDataFromBackup(context.Context, roachpb.Span) (bool, error)
	GetLastReplicaGCTimestamp(context.Context) (hlc.Timestamp, error)
	GetLease() (roachpb.Lease, roachpb.Lease)
	GetRangeLeaseDuration() time.Duration
	GetRangeInfo(context.Context) roachpb.RangeInfo

	// GetCurrentReadSummary returns a new ReadSummary reflecting all reads
	// served by the range to this point. The method requires a write latch
	// across all keys in the range (see declareAllKeys), because it will only
	// return a meaningful summary if the caller has serialized with all other
	// requests on the range.
	GetCurrentReadSummary(ctx context.Context) rspb.ReadSummary

	// RevokeLease stops the replica from using its current lease, if that lease
	// matches the provided lease sequence. All future calls to leaseStatus on
	// this node with the current lease will now return a PROSCRIBED status.
	RevokeLease(context.Context, roachpb.LeaseSequence)

	// WatchForMerge arranges to block all requests until the in-progress merge
	// completes. Returns an error if no in-progress merge is detected.
	WatchForMerge(ctx context.Context) error

	// GetResponseMemoryAccount returns a memory account to be used when
	// generating BatchResponses. Currently only used for MVCC scans, and only
	// non-nil on those paths (a nil account is replaced with standalone
	// unlimited one later).
	GetResponseMemoryAccount() *mon.BoundAccount

	GetMaxBytes(context.Context) int64

	// GetEngineCapacity returns the store's underlying engine capacity; other
	// StoreCapacity fields not related to engine capacity are not populated.
	GetEngineCapacity() (roachpb.StoreCapacity, error)

	// GetApproximateDiskBytes returns an approximate measure of bytes in the store
	// in the specified key range.
	GetApproximateDiskBytes(from, to roachpb.Key) (uint64, error)

	// GetCurrentClosedTimestamp returns the current closed timestamp on the
	// range. It is expected that a caller will have performed some action (either
	// calling RevokeLease or WatchForMerge) to freeze further progression of the
	// closed timestamp before calling this method.
	GetCurrentClosedTimestamp(ctx context.Context) hlc.Timestamp

	// AdmissionHeader returns the AdmissionHeader specified for the batch.
	AdmissionHeader() kvpb.AdmissionHeader

	// Release returns the memory allocated by the EvalContext implementation to a
	// sync.Pool.
	Release()
}

// ImmutableEvalContext is like EvalContext, but it encapsulates state that
// needs to be immutable during the course of command evaluation.
type ImmutableEvalContext interface {
	// GetClosedTimestampOlderThanStorageSnapshot returns the closed timestamp
	// that was active before the state of the storage engine was pinned.
	GetClosedTimestampOlderThanStorageSnapshot() hlc.Timestamp
}

// MockEvalCtx is a dummy implementation of EvalContext for testing purposes.
// For technical reasons, the interface is implemented by a wrapper .EvalContext().
type MockEvalCtx struct {
	ClusterSettings        *cluster.Settings
	Desc                   *roachpb.RangeDescriptor
	StoreID                roachpb.StoreID
	NodeID                 roachpb.NodeID
	Clock                  *hlc.Clock
	Stats                  enginepb.MVCCStats
	QPS                    float64
	CPU                    float64
	AbortSpan              *abortspan.AbortSpan
	GCThreshold            hlc.Timestamp
	Term                   kvpb.RaftTerm
	FirstIndex             kvpb.RaftIndex
	CanCreateTxnRecordFn   func() (bool, kvpb.TransactionAbortedReason)
	MinTxnCommitTSFn       func() hlc.Timestamp
	LastReplicaGCTimestamp hlc.Timestamp
	Lease                  roachpb.Lease
	RangeLeaseDuration     time.Duration
	CurrentReadSummary     rspb.ReadSummary
	ConcurrencyManager     concurrency.Manager
	ClosedTimestamp        hlc.Timestamp
	RevokedLeaseSeq        roachpb.LeaseSequence
	MaxBytes               int64
	ApproxDiskBytes        uint64
	EvalKnobs              kvserverbase.BatchEvalTestingKnobs
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
	return m.MockEvalCtx.EvalKnobs
}
func (m *mockEvalCtxImpl) Clock() *hlc.Clock {
	return m.MockEvalCtx.Clock
}
func (m *mockEvalCtxImpl) AbortSpan() *abortspan.AbortSpan {
	return m.MockEvalCtx.AbortSpan
}
func (m *mockEvalCtxImpl) GetConcurrencyManager() concurrency.Manager {
	if m.ConcurrencyManager != nil {
		return m.ConcurrencyManager
	} else {
		panic("ConcurrencyManager not configured")
	}
}
func (m *mockEvalCtxImpl) NodeID() roachpb.NodeID {
	return m.MockEvalCtx.NodeID
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
func (m *mockEvalCtxImpl) GetFirstIndex() kvpb.RaftIndex {
	return m.FirstIndex
}
func (m *mockEvalCtxImpl) GetTerm(kvpb.RaftIndex) (kvpb.RaftTerm, error) {
	return m.Term, nil
}
func (m *mockEvalCtxImpl) GetLeaseAppliedIndex() kvpb.LeaseAppliedIndex {
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
func (m *mockEvalCtxImpl) GetMaxSplitQPS(context.Context) (float64, bool) {
	return m.QPS, true
}
func (m *mockEvalCtxImpl) GetMaxSplitCPU(context.Context) (float64, bool) {
	return m.CPU, true
}
func (m *mockEvalCtxImpl) CanCreateTxnRecord(
	context.Context, uuid.UUID, []byte, hlc.Timestamp,
) (bool, kvpb.TransactionAbortedReason) {
	if m.CanCreateTxnRecordFn == nil {
		return true, 0
	}
	return m.CanCreateTxnRecordFn()
}
func (m *mockEvalCtxImpl) MinTxnCommitTS(
	ctx context.Context, txnID uuid.UUID, txnKey []byte,
) hlc.Timestamp {
	if m.MinTxnCommitTSFn == nil {
		return hlc.Timestamp{}
	}
	return m.MinTxnCommitTSFn()
}
func (m *mockEvalCtxImpl) GetGCThreshold() hlc.Timestamp {
	return m.GCThreshold
}
func (m *mockEvalCtxImpl) ExcludeDataFromBackup(context.Context, roachpb.Span) (bool, error) {
	return false, nil
}
func (m *mockEvalCtxImpl) GetLastReplicaGCTimestamp(context.Context) (hlc.Timestamp, error) {
	return m.LastReplicaGCTimestamp, nil
}
func (m *mockEvalCtxImpl) GetLease() (roachpb.Lease, roachpb.Lease) {
	return m.Lease, roachpb.Lease{}
}
func (m *mockEvalCtxImpl) GetRangeLeaseDuration() time.Duration {
	return m.RangeLeaseDuration
}
func (m *mockEvalCtxImpl) GetRangeInfo(ctx context.Context) roachpb.RangeInfo {
	return roachpb.RangeInfo{Desc: *m.Desc(), Lease: m.Lease}
}
func (m *mockEvalCtxImpl) GetCurrentReadSummary(ctx context.Context) rspb.ReadSummary {
	return m.CurrentReadSummary
}
func (m *mockEvalCtxImpl) GetClosedTimestampOlderThanStorageSnapshot() hlc.Timestamp {
	return m.ClosedTimestamp
}
func (m *mockEvalCtxImpl) GetCurrentClosedTimestamp(ctx context.Context) hlc.Timestamp {
	return m.ClosedTimestamp
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
func (m *mockEvalCtxImpl) GetMaxBytes(context.Context) int64 {
	if m.MaxBytes != 0 {
		return m.MaxBytes
	}
	return math.MaxInt64
}
func (m *mockEvalCtxImpl) GetEngineCapacity() (roachpb.StoreCapacity, error) {
	return roachpb.StoreCapacity{Available: 1, Capacity: 1}, nil
}
func (m *mockEvalCtxImpl) GetApproximateDiskBytes(from, to roachpb.Key) (uint64, error) {
	return m.ApproxDiskBytes, nil
}

func (m *mockEvalCtxImpl) AdmissionHeader() kvpb.AdmissionHeader {
	return kvpb.AdmissionHeader{}
}

func (m *mockEvalCtxImpl) Release() {}

type noopIntentResolver struct{}

func (m *noopIntentResolver) PushTransaction(
	ctx context.Context, txn *enginepb.TxnMeta, h kvpb.Header, pushType kvpb.PushTxnType,
) (*roachpb.Transaction, bool, *concurrency.Error) {
	panic("unimplemented")
}

func (m *noopIntentResolver) ResolveIntent(
	ctx context.Context, intent roachpb.LockUpdate, opts intentresolver.ResolveOptions,
) *concurrency.Error {
	panic("unimplemented")
}

func (m *noopIntentResolver) ResolveIntents(
	ctx context.Context, intents []roachpb.LockUpdate, opts intentresolver.ResolveOptions,
) *concurrency.Error {
	panic("unimplemented")
}
