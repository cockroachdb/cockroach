// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package concurrency

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// verifiableLockTable is a lock table that is able to verify structural and
// correctness properties.
type verifiableLockTable interface {
	lockTable
	// verify ensures structural and correctness properties hold for each of the
	// locks stored in the lock table. Verification is expensive and should only
	// be performed for test builds.
	verify()

	// verifyKey ensures structural and correctness properties hold for all locks
	// stored in the lock table for the given key.
	verifyKey(roachpb.Key)
}

type verifyingLockTable struct {
	lt verifiableLockTable
}

var _ lockTable = &verifyingLockTable{}

// maybeWrapInVerifyingLockTable wraps the supplied lock table to perform
// verification for test-only builds.
func maybeWrapInVerifyingLockTable(lt lockTable) lockTable {
	// Verification methods are called while holding mutexes, and the algorithm is
	// quite slow, so this can falsely trip the deadlock detector. Enable the
	// checks only in non-deadlock test builds.
	//
	// See https://github.com/cockroachdb/cockroach/issues/161496.
	if buildutil.CrdbTestBuild && !syncutil.DeadlockEnabled {
		return &verifyingLockTable{lt: lt.(verifiableLockTable)}
	}
	return lt
}

// Enable implements the lockTable interface.
func (v verifyingLockTable) Enable(sequence roachpb.LeaseSequence) {
	defer v.lt.verify()
	v.lt.Enable(sequence)
}

// Clear implements the lockTable interface.
func (v verifyingLockTable) Clear(disable bool) {
	defer v.lt.verify()
	v.lt.Clear(disable)
}

// ClearGE implements the lockTable interface.
func (v verifyingLockTable) ClearGE(key roachpb.Key) []roachpb.LockAcquisition {
	defer v.lt.verify()
	return v.lt.ClearGE(key)
}

// ScanAndEnqueue implements the lockTable interface.
func (v verifyingLockTable) ScanAndEnqueue(
	ctx context.Context, req Request, guard lockTableGuard,
) (lockTableGuard, *Error) {
	defer v.lt.verify()
	return v.lt.ScanAndEnqueue(ctx, req, guard)
}

// ScanOptimistic implements the lockTable interface.
func (v verifyingLockTable) ScanOptimistic(req Request) lockTableGuard {
	defer v.lt.verify()
	return v.lt.ScanOptimistic(req)
}

// Dequeue implements the lockTable interface.
func (v verifyingLockTable) Dequeue(ctx context.Context, guard lockTableGuard) {
	defer v.lt.verify()
	v.lt.Dequeue(ctx, guard)
}

// AddDiscoveredLock implements the lockTable interface.
func (v verifyingLockTable) AddDiscoveredLock(
	ctx context.Context,
	foundLock *roachpb.Lock,
	seq roachpb.LeaseSequence,
	consultTxnStatusCache bool,
	guard lockTableGuard,
) (bool, error) {
	defer v.lt.verifyKey(foundLock.Key)
	return v.lt.AddDiscoveredLock(ctx, foundLock, seq, consultTxnStatusCache, guard)
}

// AcquireLock implements the lockTable interface.
func (v verifyingLockTable) AcquireLock(ctx context.Context, acq *roachpb.LockAcquisition) error {
	defer v.lt.verifyKey(acq.Key)
	return v.lt.AcquireLock(ctx, acq)
}

// MarkIneligibleForExport implements the lockTable interface.
func (v verifyingLockTable) MarkIneligibleForExport(
	ctx context.Context, acq *roachpb.LockAcquisition,
) error {
	return v.lt.MarkIneligibleForExport(ctx, acq)
}

// UpdateLocks implements the lockTable interface.
func (v verifyingLockTable) UpdateLocks(ctx context.Context, up *roachpb.LockUpdate) error {
	defer v.lt.verify()
	return v.lt.UpdateLocks(ctx, up)
}

// PushedTransactionUpdated implements the lockTable interface.
func (v verifyingLockTable) PushedTransactionUpdated(
	txn *roachpb.Transaction, clockObs roachpb.ObservedTimestamp,
) {
	v.lt.PushedTransactionUpdated(txn, clockObs)
}

// QueryLockTableState implements the lockTable interface.
func (v verifyingLockTable) QueryLockTableState(
	span roachpb.Span, opts QueryLockTableOptions,
) ([]roachpb.LockStateInfo, QueryLockTableResumeState) {
	return v.lt.QueryLockTableState(span, opts)
}

func (v verifyingLockTable) ExportUnreplicatedLocks(
	span roachpb.Span, exporter func(*roachpb.LockAcquisition) bool,
) {
	v.lt.ExportUnreplicatedLocks(span, exporter)
}

// Metrics implements the lockTable interface.
func (v verifyingLockTable) Metrics() LockTableMetrics {
	return v.lt.Metrics()
}

// String implements the lockTable interface.
func (v verifyingLockTable) String() string {
	return v.lt.String()
}

// SetMaxLockTableSize implements the lockTable interface.
func (v verifyingLockTable) SetMaxLockTableSize(maxKeysLocked int64) {
	v.lt.SetMaxLockTableSize(maxKeysLocked)
}
