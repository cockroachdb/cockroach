// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package concurrency

import (
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
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
	if buildutil.CrdbTestBuild {
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

// ScanAndEnqueue implements the lockTable interface.
func (v verifyingLockTable) ScanAndEnqueue(
	req Request, guard lockTableGuard,
) (lockTableGuard, *Error) {
	defer v.lt.verify()
	return v.lt.ScanAndEnqueue(req, guard)
}

// ScanOptimistic implements the lockTable interface.
func (v verifyingLockTable) ScanOptimistic(req Request) lockTableGuard {
	defer v.lt.verify()
	return v.lt.ScanOptimistic(req)
}

// Dequeue implements the lockTable interface.
func (v verifyingLockTable) Dequeue(guard lockTableGuard) {
	defer v.lt.verify()
	v.lt.Dequeue(guard)
}

// AddDiscoveredLock implements the lockTable interface.
func (v verifyingLockTable) AddDiscoveredLock(
	foundLock *roachpb.Lock,
	seq roachpb.LeaseSequence,
	consultTxnStatusCache bool,
	guard lockTableGuard,
) (bool, error) {
	defer v.lt.verifyKey(foundLock.Key)
	return v.lt.AddDiscoveredLock(foundLock, seq, consultTxnStatusCache, guard)
}

// AcquireLock implements the lockTable interface.
func (v verifyingLockTable) AcquireLock(acq *roachpb.LockAcquisition) error {
	defer v.lt.verifyKey(acq.Key)
	return v.lt.AcquireLock(acq)
}

// UpdateLocks implements the lockTable interface.
func (v verifyingLockTable) UpdateLocks(up *roachpb.LockUpdate) error {
	defer v.lt.verify()
	return v.lt.UpdateLocks(up)
}

// PushedTransactionUpdated implements the lockTable interface.
func (v verifyingLockTable) PushedTransactionUpdated(txn *roachpb.Transaction) {
	v.lt.PushedTransactionUpdated(txn)
}

// QueryLockTableState implements the lockTable interface.
func (v verifyingLockTable) QueryLockTableState(
	span roachpb.Span, opts QueryLockTableOptions,
) ([]roachpb.LockStateInfo, QueryLockTableResumeState) {
	return v.lt.QueryLockTableState(span, opts)
}

// Metrics implements the lockTable interface.
func (v verifyingLockTable) Metrics() LockTableMetrics {
	return v.lt.Metrics()
}

// String implements the lockTable interface.
func (v verifyingLockTable) String() string {
	return v.lt.String()
}

// TestingSetMaxLocks implements the lockTable interface.
func (v verifyingLockTable) TestingSetMaxLocks(maxKeysLocked int64) {
	v.lt.TestingSetMaxLocks(maxKeysLocked)
}
