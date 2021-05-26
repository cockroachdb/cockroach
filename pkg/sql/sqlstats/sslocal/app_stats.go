// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sslocal

import (
	"context"
	"sync/atomic"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/execstats"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// TODO(arul): The fields on stmtKey should really be immutable fields on
// stmtStats which are set once (on first addition to the map). Instead, we
// should use stmtID (which is a hashed string of the fields below) as the
// stmtKey.
type stmtKey struct {
	anonymizedStmt string
	failed         bool
	implicitTxn    bool
	database       string
}

func (s stmtKey) String() string {
	if s.failed {
		return "!" + s.anonymizedStmt
	}
	return s.anonymizedStmt
}

func (s stmtKey) size() int64 {
	return int64(unsafe.Sizeof(s)) + int64(len(s.anonymizedStmt)) + int64(len(s.database))
}

const invalidStmtID = 0

// appStats holds per-application statistics.
type appStats struct {
	// TODO(arul): This can be refactored to have a RWLock instead, and have all
	// usages acquire a read lock whenever appropriate. See #55285.
	syncutil.Mutex

	// sqlStats is the pointer to the parent sqlStats struct that stores this
	// appStats.
	sqlStats *sqlStats

	// acc is the memory account that tracks memory allocations related to stmts
	// and txns within this appStats struct.
	// Since currently we do not destroy the appStats struct when we perform
	// reset, we never close this account.
	acc mon.BoundAccount

	st    *cluster.Settings
	stmts map[stmtKey]*stmtStats

	// TODO(azhng): This is behind a struct level mutex. However,
	//  transactionCounts itself already has a mutex lock. This means we can
	//  refactor this struct out and put rest of the fields behind a `mu` field.
	txnCounts transactionCounts
	txns      map[sqlstats.TransactionFingerprintID]*txnStats
}

type txnStats struct {
	statementIDs []roachpb.StmtID

	mu struct {
		syncutil.Mutex

		data roachpb.TransactionStatistics
	}
}

func (t *txnStats) sizeUnsafe() int64 {
	const txnStatsShallowSize = int64(unsafe.Sizeof(txnStats{}))
	stmtIDsSize := int64(cap(t.statementIDs)) *
		int64(unsafe.Sizeof(roachpb.StmtID(0)))

	// t.mu.data might contain pointer types, so we subtract its shallow size
	// and include the actual size.
	dataSize := -int64(unsafe.Sizeof(roachpb.TransactionStatistics{})) +
		int64(t.mu.data.Size())

	return txnStatsShallowSize + stmtIDsSize + dataSize
}

// stmtStats holds per-statement statistics.
type stmtStats struct {
	// ID is the statementID constructed using the stmtKey fields.
	ID roachpb.StmtID

	// data contains all fields that are modified when new statements matching
	// the stmtKey are executed, and therefore must be protected by a mutex.
	mu struct {
		syncutil.Mutex

		// distSQLUsed records whether the last instance of this statement used
		// distribution.
		distSQLUsed bool

		// vectorized records whether the last instance of this statement used
		// vectorization.
		vectorized bool

		// fullScan records whether the last instance of this statement used a
		// full table index scan.
		fullScan bool

		// database records the database from the session the statement
		// was executed from
		database string

		data roachpb.StatementStatistics
	}
}

func (s *stmtStats) sizeUnsafe() int64 {
	const stmtStatsShallowSize = int64(unsafe.Sizeof(stmtStats{}))
	databaseNameSize := int64(len(s.mu.database))

	// s.mu.data might contain pointer tyeps, so we subtract its shallow size and
	// include the actual size.
	dataSize := -int64(unsafe.Sizeof(roachpb.StatementStatistics{})) +
		int64(s.mu.data.Size())

	return stmtStatsShallowSize + databaseNameSize + dataSize
}

func (s *stmtStats) recordExecStats(stats execstats.QueryLevelStats) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.mu.data.ExecStats.Count++
	count := s.mu.data.ExecStats.Count
	s.mu.data.ExecStats.NetworkBytes.Record(count, float64(stats.NetworkBytesSent))
	s.mu.data.ExecStats.MaxMemUsage.Record(count, float64(stats.MaxMemUsage))
	s.mu.data.ExecStats.ContentionTime.Record(count, stats.ContentionTime.Seconds())
	s.mu.data.ExecStats.NetworkMessages.Record(count, float64(stats.NetworkMessages))
	s.mu.data.ExecStats.MaxDiskUsage.Record(count, float64(stats.MaxDiskUsage))
}

type transactionCounts struct {
	mu struct {
		syncutil.Mutex
		// TODO(arul): Can we rename this without breaking stuff?
		roachpb.TxnStats
	}
}

// getStatsForStmt retrieves the per-stmt stat object. Regardless of if a valid
// stat object is returned or not, we always return the correct stmtID
// for the given stmt.
func (a *appStats) getStatsForStmt(
	anonymizedStmt string, implicitTxn bool, database string, failed bool, createIfNonexistent bool,
) (stats *stmtStats, key stmtKey, stmtID roachpb.StmtID, created bool, throttled bool) {
	// Extend the statement key with various characteristics, so
	// that we use separate buckets for the different situations.
	key = stmtKey{
		anonymizedStmt: anonymizedStmt,
		failed:         failed,
		implicitTxn:    implicitTxn,
		database:       database,
	}

	// We first try and see if we can get by without creating a new entry for this
	// key, as this allows us to not construct the statementID from scratch (which
	// is an expensive operation)
	stats, _, _ = a.getStatsForStmtWithKey(key, invalidStmtID, false /* createIfNonexistent */)
	if stats == nil {
		stmtID = constructStatementIDFromStmtKey(key)
		stats, created, throttled = a.getStatsForStmtWithKey(key, stmtID, createIfNonexistent)
		return stats, key, stmtID, created, throttled
	}
	return stats, key, stats.ID, false /* created */, false /* throttled */
}

func (a *appStats) getStatsForStmtWithKey(
	key stmtKey, stmtID roachpb.StmtID, createIfNonexistent bool,
) (stats *stmtStats, created, throttled bool) {
	a.Lock()
	defer a.Unlock()

	// Retrieve the per-statement statistic object, and create it if it
	// doesn't exist yet.
	stats, ok := a.stmts[key]
	if !ok && createIfNonexistent {
		// We check if we have reached the limit of unique fingerprints we can
		// store.
		limit := a.sqlStats.uniqueStmtFingerprintLimit.Get(&a.sqlStats.st.SV)
		incrementedFingerprintCount :=
			atomic.AddInt64(&a.sqlStats.atomic.uniqueStmtFingerprintCount, int64(1) /* delta */)

		// Abort if we have exceeded limit of unique statement fingerprints.
		if incrementedFingerprintCount > limit {
			atomic.AddInt64(&a.sqlStats.atomic.uniqueStmtFingerprintCount, -int64(1) /* delta */)
			return stats, false /* created */, true /* throttled */
		}

		stats = &stmtStats{}
		stats.ID = stmtID
		a.stmts[key] = stats
		return stats, true /* created */, false /* throttled */
	}
	return stats, false /* created */, false /* throttled */
}

func (a *appStats) getStatsForTxnWithKey(
	key sqlstats.TransactionFingerprintID, stmtIDs []roachpb.StmtID, createIfNonexistent bool,
) (stats *txnStats, created, throttled bool) {
	a.Lock()
	defer a.Unlock()
	// Retrieve the per-transaction statistic object, and create it if it doesn't
	// exist yet.
	stats, ok := a.txns[key]
	if !ok && createIfNonexistent {
		limit := a.sqlStats.uniqueTxnFingerprintLimit.Get(&a.sqlStats.st.SV)
		incrementedFingerprintCount :=
			atomic.AddInt64(&a.sqlStats.atomic.uniqueTxnFingerprintCount, int64(1) /* delta */)

		// If we have exceeded limit of fingerprint count, decrement the counter
		// and abort.
		if incrementedFingerprintCount > limit {
			atomic.AddInt64(&a.sqlStats.atomic.uniqueTxnFingerprintCount, -int64(1) /* delta */)
			return nil /* stats */, false /* created */, true /* throttled */
		}
		stats = &txnStats{}
		stats.statementIDs = stmtIDs
		a.txns[key] = stats
		return stats, true /* created */, false /* throttled */
	}
	return stats, false /* created */, false /* throttled */
}

// Add combines one appStats into another. Add manages locks on a, so taking
// a lock on a will cause a deadlock.
func (a *appStats) Add(ctx context.Context, other *appStats) (err error) {
	other.Lock()
	statMap := make(map[stmtKey]*stmtStats)
	for k, v := range other.stmts {
		statMap[k] = v
	}
	other.Unlock()

	// Copy the statement stats for each statement key.
	for k, v := range statMap {
		v.mu.Lock()
		statCopy := &stmtStats{}
		statCopy.mu.data = v.mu.data
		v.mu.Unlock()
		statCopy.ID = v.ID
		statMap[k] = statCopy
	}

	// Merge the statement stats.
	for k, v := range statMap {
		s, created, throttled := a.getStatsForStmtWithKey(k, v.ID, true /* createIfNonexistent */)

		// If we have reached the limit of fingerprints, we skip this fingerprint.
		// No cleanup necessary.
		if throttled {
			continue
		}

		s.mu.Lock()

		// If we created a new entry for the fingerprint, we check if we have
		// exceeded our memory budget.
		if created {
			estimatedAllocBytes := s.sizeUnsafe() + k.size() + 8 /* stmtKey hash */
			// We still want to continue this loop to merge stats that are already
			// present in our map that do not require allocation.
			a.Lock()
			if latestErr := a.acc.Grow(ctx, estimatedAllocBytes); latestErr != nil {
				s.mu.Unlock()
				// Instead of combining errors, we track the latest error occurred
				// in this method. This is because currently the only type of error we
				// can generate in this function is out of memory errors. Also since we
				// do not abort after encountering such errors, combining many same
				// errors is not helpful.
				err = latestErr
				delete(a.stmts, k)
				a.Unlock()
				continue
			}
			a.Unlock()
		}

		// Note that we don't need to take a lock on v because
		// no other thread knows about v yet.
		s.mu.data.Add(&v.mu.data)
		s.mu.Unlock()
	}

	// Do what we did above for the statMap for the txn Map now.
	other.Lock()
	txnMap := make(map[sqlstats.TransactionFingerprintID]*txnStats)
	for k, v := range other.txns {
		txnMap[k] = v
	}
	other.Unlock()

	// Copy the transaction stats for each txn key
	for k, v := range txnMap {
		v.mu.Lock()
		txnCopy := &txnStats{}
		txnCopy.mu.data = v.mu.data
		v.mu.Unlock()
		txnCopy.statementIDs = v.statementIDs
		txnMap[k] = txnCopy
	}

	// Merge the txn stats
	for k, v := range txnMap {
		// We don't check if we have created a new entry here because we have
		// already accounted for all the memory that we will be allocating in this
		// function.
		t, created, throttled := a.getStatsForTxnWithKey(k, v.statementIDs, true /* createIfNonExistent */)

		// If we have reached the unique fingerprint limit, we skip adding the
		// current fingerprint. No cleanup is necessary.
		if throttled {
			continue
		}

		t.mu.Lock()
		if created {
			estimatedAllocBytes := t.sizeUnsafe() + k.Size() + 8 /* TransactionFingerprintID hash */
			// We still want to continue this loop to merge stats that are already
			// present in our map that do not require allocation.
			a.Lock()
			if latestErr := a.acc.Grow(ctx, estimatedAllocBytes); latestErr != nil {
				t.mu.Unlock()
				// We only track the latest error. See comment above for explanation.
				err = latestErr
				delete(a.txns, k)
				a.Unlock()
				continue
			}
			a.Unlock()
		}

		t.mu.data.Add(&v.mu.data)
		t.mu.Unlock()
	}

	// Create a copy of the other's transactions statistics.
	other.txnCounts.mu.Lock()
	txnStats := other.txnCounts.mu.TxnStats
	other.txnCounts.mu.Unlock()

	// Merge the transaction stats.
	a.txnCounts.mu.Lock()
	a.txnCounts.mu.TxnStats.Add(txnStats)
	a.txnCounts.mu.Unlock()

	return err
}

func (s *transactionCounts) getStats() (
	txnCount int64,
	txnTimeAvg float64,
	txnTimeVar float64,
	committedCount int64,
	implicitCount int64,
) {
	s.mu.Lock()
	defer s.mu.Unlock()
	txnCount = s.mu.TxnCount
	txnTimeAvg = s.mu.TxnTimeSec.Mean
	txnTimeVar = s.mu.TxnTimeSec.GetVariance(txnCount)
	committedCount = s.mu.CommittedCount
	implicitCount = s.mu.ImplicitCount
	return txnCount, txnTimeAvg, txnTimeVar, committedCount, implicitCount
}

func (s *transactionCounts) recordTransactionCounts(
	txnTimeSec float64, commit bool, implicit bool,
) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mu.TxnCount++
	s.mu.TxnTimeSec.Record(s.mu.TxnCount, txnTimeSec)
	if commit {
		s.mu.CommittedCount++
	}
	if implicit {
		s.mu.ImplicitCount++
	}
}

func (a *appStats) recordTransactionCounts(txnTimeSec float64, commit bool, implicit bool) {
	if !sqlstats.TxnStatsEnable.Get(&a.st.SV) {
		return
	}
	a.txnCounts.recordTransactionCounts(txnTimeSec, commit, implicit)
}

// shouldSaveLogicalPlanDescription returns whether we should save the sample
// logical plan for a fingerprint (represented implicitly by the corresponding
// stmtStats object). stats is nil if it is the first time we see the
// fingerprint. We use `logicalPlanCollectionPeriod` to assess how frequently to
// sample logical plans.
func (a *appStats) shouldSaveLogicalPlanDescription(stats *stmtStats) bool {
	if !sqlstats.SampleLogicalPlans.Get(&a.st.SV) {
		return false
	}
	if stats == nil {
		// Save logical plan the first time we see new statement fingerprint.
		return true
	}
	now := timeutil.Now()
	period := sqlstats.LogicalPlanCollectionPeriod.Get(&a.st.SV)
	stats.mu.Lock()
	defer stats.mu.Unlock()
	timeLastSampled := stats.mu.data.SensitiveInfo.MostRecentPlanTimestamp
	return now.Sub(timeLastSampled) >= period
}
