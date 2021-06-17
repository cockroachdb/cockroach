// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package sqlstats is a subsystem that is responsible for tracking the
// statistics of statements and transactions.

package ssmemstorage

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"sync/atomic"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/execstats"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// TODO(arul): The fields on stmtKey should really be immutable fields on
// stmtStats which are set once (on first addition to the map). Instead, we
// should use stmtFingerprintID (which is a hashed string of the fields below) as the
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

const invalidStmtFingerprintID = 0

// Container holds per-application statement and transaction statistics.
type Container struct {
	st *cluster.Settings

	// uniqueStmtFingerprintLimit is the limit on number of unique statement
	// fingerprints we can store in memory.
	uniqueStmtFingerprintLimit *settings.IntSetting

	// uniqueTxnFingerprintLimit is the limit on number of unique transaction
	// fingerprints we can store in memory.
	uniqueTxnFingerprintLimit *settings.IntSetting

	atomic struct {
		// uniqueStmtFingerprintCount is the number of unique statement fingerprints
		// we are storing in memory.
		uniqueStmtFingerprintCount *int64

		// uniqueTxnFingerprintCount is the number of unique transaction fingerprints
		// we are storing in memory.
		uniqueTxnFingerprintCount *int64
	}

	mu struct {
		// TODO(arul): This can be refactored to have a RWLock instead, and have all
		// usages acquire a read lock whenever appropriate. See #55285.
		syncutil.Mutex

		// acc is the memory account that tracks memory allocations related to stmts
		// and txns within this Container struct.
		// Since currently we do not destroy the Container struct when we perform
		// reset, we never close this account.
		acc mon.BoundAccount

		stmts map[stmtKey]*stmtStats
		txns  map[roachpb.TransactionFingerprintID]*txnStats
	}

	txnCounts transactionCounts
}

var _ sqlstats.Writer = &Container{}

// New returns a new instance of Container.
func New(
	st *cluster.Settings,
	uniqueStmtFingerprintLimit *settings.IntSetting,
	uniqueTxnFingerprintLimit *settings.IntSetting,
	uniqueStmtFingerprintCount *int64,
	uniqueTxnFingerprintCount *int64,
	mon *mon.BytesMonitor,
) *Container {
	s := &Container{
		st:                         st,
		uniqueStmtFingerprintLimit: uniqueStmtFingerprintLimit,
		uniqueTxnFingerprintLimit:  uniqueTxnFingerprintLimit,
	}

	s.mu.acc = mon.MakeBoundAccount()
	s.mu.stmts = make(map[stmtKey]*stmtStats)
	s.mu.txns = make(map[roachpb.TransactionFingerprintID]*txnStats)

	s.atomic.uniqueStmtFingerprintCount = uniqueStmtFingerprintCount
	s.atomic.uniqueTxnFingerprintCount = uniqueTxnFingerprintCount

	return s
}

// IterateStatementStats iterates through the stored statement statistics
// stored in this Container.
func (s *Container) IterateStatementStats(
	appName string, orderedKey bool, visitor sqlstats.StatementVisitor,
) error {
	var stmtKeys stmtList
	s.mu.Lock()
	for k := range s.mu.stmts {
		stmtKeys = append(stmtKeys, k)
	}
	s.mu.Unlock()
	if orderedKey {
		sort.Sort(stmtKeys)
	}

	for _, stmtKey := range stmtKeys {
		stmtFingerprintID := constructStatementFingerprintIDFromStmtKey(stmtKey)
		statementStats, _, _ :=
			s.getStatsForStmtWithKey(stmtKey, invalidStmtFingerprintID, false /* createIfNonexistent */)

		// If the key is not found (and we expected to find it), the table must
		// have been cleared between now and the time we read all the keys. In
		// that case we simply skip this key as there are no metrics to report.
		if statementStats == nil {
			continue
		}

		statementStats.mu.Lock()
		data := statementStats.mu.data
		distSQLUsed := statementStats.mu.distSQLUsed
		vectorized := statementStats.mu.vectorized
		fullScan := statementStats.mu.fullScan
		database := statementStats.mu.database
		statementStats.mu.Unlock()

		collectedStats := roachpb.CollectedStatementStatistics{
			Key: roachpb.StatementStatisticsKey{
				Query:       stmtKey.anonymizedStmt,
				DistSQL:     distSQLUsed,
				Opt:         true,
				Vec:         vectorized,
				ImplicitTxn: stmtKey.implicitTxn,
				FullScan:    fullScan,
				Failed:      stmtKey.failed,
				App:         appName,
				Database:    database,
			},
			ID:    stmtFingerprintID,
			Stats: data,
		}

		err := visitor(&collectedStats)
		if err != nil {
			return fmt.Errorf("sql stats iteration abort: %s", err)
		}
	}
	return nil
}

// IterateTransactionStats iterates through the stored transaction statistics
// stored in this Container.
func (s *Container) IterateTransactionStats(
	appName string, orderedKey bool, visitor sqlstats.TransactionVisitor,
) error {
	// Retrieve the transaction keys and optionally sort them.
	var txnKeys txnList
	s.mu.Lock()
	for k := range s.mu.txns {
		txnKeys = append(txnKeys, k)
	}
	s.mu.Unlock()
	if orderedKey {
		sort.Sort(txnKeys)
	}

	// Now retrieve the per-stmt stats proper.
	for _, txnKey := range txnKeys {
		// We don't want to create the key if it doesn't exist, so it's okay to
		// pass nil for the statementFingerprintIDs, as they are only set when a key is
		// constructed.
		txnStats, _, _ := s.getStatsForTxnWithKey(txnKey, nil /* stmtFingerprintIDs */, false /* createIfNonexistent */)
		// If the key is not found (and we expected to find it), the table must
		// have been cleared between now and the time we read all the keys. In
		// that case we simply skip this key as there are no metrics to report.
		if txnStats == nil {
			continue
		}

		txnStats.mu.Lock()
		collectedStats := roachpb.CollectedTransactionStatistics{
			StatementFingerprintIDs: txnStats.statementFingerprintIDs,
			App:                     appName,
			Stats:                   txnStats.mu.data,
		}
		txnStats.mu.Unlock()

		err := visitor(txnKey, &collectedStats)
		if err != nil {
			return fmt.Errorf("sql stats iteration abort: %s", err)
		}
	}
	return nil
}

// IterateAggregatedTransactionStats iterates through the stored aggregated
// transaction statistics stored in this Container.
func (s *Container) IterateAggregatedTransactionStats(
	appName string, visitor sqlstats.AggregatedTransactionVisitor,
) error {
	var txnStat roachpb.TxnStats
	s.txnCounts.mu.Lock()
	txnStat = s.txnCounts.mu.TxnStats
	s.txnCounts.mu.Unlock()

	err := visitor(appName, &txnStat)
	if err != nil {
		return fmt.Errorf("sql stats iteration abort: %s", err)
	}

	return nil
}

// GetStatementStats implements sqlstats.Provider interface.
func (s *Container) GetStatementStats(
	key *roachpb.StatementStatisticsKey,
) (*roachpb.CollectedStatementStatistics, error) {
	statementStats, _, stmtFingerprintID, _, _ := s.getStatsForStmt(
		key.Query, key.ImplicitTxn, key.Database, key.Failed, false /* createIfNonexistent */)

	if statementStats == nil {
		return nil, errors.Errorf("no stats found for the provided key")
	}

	statementStats.mu.Lock()
	defer statementStats.mu.Unlock()
	data := statementStats.mu.data

	collectedStats := &roachpb.CollectedStatementStatistics{
		ID:    stmtFingerprintID,
		Key:   *key,
		Stats: data,
	}

	return collectedStats, nil
}

// GetTransactionStats implements sqlstats.Provider interface.
func (s *Container) GetTransactionStats(
	appName string, key roachpb.TransactionFingerprintID,
) (*roachpb.CollectedTransactionStatistics, error) {
	txnStats, _, _ :=
		s.getStatsForTxnWithKey(key, nil /* stmtFingerprintIDs */, false /* createIfNonexistent */)

	if txnStats == nil {
		return nil, errors.Errorf("no stats found for the provided key")
	}

	txnStats.mu.Lock()
	defer txnStats.mu.Unlock()
	data := txnStats.mu.data

	collectedStats := &roachpb.CollectedTransactionStatistics{
		StatementFingerprintIDs: txnStats.statementFingerprintIDs,
		App:                     appName,
		Stats:                   data,
	}

	return collectedStats, nil
}

type txnStats struct {
	statementFingerprintIDs []roachpb.StmtFingerprintID

	mu struct {
		syncutil.Mutex

		data roachpb.TransactionStatistics
	}
}

func (t *txnStats) sizeUnsafe() int64 {
	const txnStatsShallowSize = int64(unsafe.Sizeof(txnStats{}))
	stmtFingerprintIDsSize := int64(cap(t.statementFingerprintIDs)) *
		int64(unsafe.Sizeof(roachpb.StmtFingerprintID(0)))

	// t.mu.data might contain pointer types, so we subtract its shallow size
	// and include the actual size.
	dataSize := -int64(unsafe.Sizeof(roachpb.TransactionStatistics{})) +
		int64(t.mu.data.Size())

	return txnStatsShallowSize + stmtFingerprintIDsSize + dataSize
}

// stmtStats holds per-statement statistics.
type stmtStats struct {
	// ID is the statementFingerprintID constructed using the stmtKey fields.
	ID roachpb.StmtFingerprintID

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

// getStatsForStmt retrieves the per-stmt stat object. Regardless of if a valid
// stat object is returned or not, we always return the correct stmtFingerprintID
// for the given stmt.
func (s *Container) getStatsForStmt(
	anonymizedStmt string, implicitTxn bool, database string, failed bool, createIfNonexistent bool,
) (
	stats *stmtStats,
	key stmtKey,
	stmtFingerprintID roachpb.StmtFingerprintID,
	created bool,
	throttled bool,
) {
	// Extend the statement key with various characteristics, so
	// that we use separate buckets for the different situations.
	key = stmtKey{
		anonymizedStmt: anonymizedStmt,
		failed:         failed,
		implicitTxn:    implicitTxn,
		database:       database,
	}

	// We first try and see if we can get by without creating a new entry for this
	// key, as this allows us to not construct the statementFingerprintID from scratch (which
	// is an expensive operation)
	stats, _, _ = s.getStatsForStmtWithKey(key, invalidStmtFingerprintID, false /* createIfNonexistent */)
	if stats == nil {
		stmtFingerprintID = constructStatementFingerprintIDFromStmtKey(key)
		stats, created, throttled = s.getStatsForStmtWithKey(key, stmtFingerprintID, createIfNonexistent)
		return stats, key, stmtFingerprintID, created, throttled
	}
	return stats, key, stats.ID, false /* created */, false /* throttled */
}

func (s *Container) getStatsForStmtWithKey(
	key stmtKey, stmtFingerprintID roachpb.StmtFingerprintID, createIfNonexistent bool,
) (stats *stmtStats, created, throttled bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Retrieve the per-statement statistic object, and create it if it
	// doesn't exist yet.
	stats, ok := s.mu.stmts[key]
	if !ok && createIfNonexistent {
		// We check if we have reached the limit of unique fingerprints we can
		// store.
		limit := s.uniqueStmtFingerprintLimit.Get(&s.st.SV)
		incrementedFingerprintCount :=
			atomic.AddInt64(s.atomic.uniqueStmtFingerprintCount, int64(1) /* delts */)

		// Abort if we have exceeded limit of unique statement fingerprints.
		if incrementedFingerprintCount > limit {
			atomic.AddInt64(s.atomic.uniqueStmtFingerprintCount, -int64(1) /* delts */)
			return stats, false /* created */, true /* throttled */
		}

		stats = &stmtStats{}
		stats.ID = stmtFingerprintID
		s.mu.stmts[key] = stats
		return stats, true /* created */, false /* throttled */
	}
	return stats, false /* created */, false /* throttled */
}

func (s *Container) getStatsForTxnWithKey(
	key roachpb.TransactionFingerprintID,
	stmtFingerprintIDs []roachpb.StmtFingerprintID,
	createIfNonexistent bool,
) (stats *txnStats, created, throttled bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	// Retrieve the per-transaction statistic object, and create it if it doesn't
	// exist yet.
	stats, ok := s.mu.txns[key]
	if !ok && createIfNonexistent {
		limit := s.uniqueTxnFingerprintLimit.Get(&s.st.SV)
		incrementedFingerprintCount :=
			atomic.AddInt64(s.atomic.uniqueTxnFingerprintCount, int64(1) /* delts */)

		// If we have exceeded limit of fingerprint count, decrement the counter
		// and abort.
		if incrementedFingerprintCount > limit {
			atomic.AddInt64(s.atomic.uniqueTxnFingerprintCount, -int64(1) /* delts */)
			return nil /* stats */, false /* created */, true /* throttled */
		}
		stats = &txnStats{}
		stats.statementFingerprintIDs = stmtFingerprintIDs
		s.mu.txns[key] = stats
		return stats, true /* created */, false /* throttled */
	}
	return stats, false /* created */, false /* throttled */
}

// SaveToLog saves the existing statement stats into the info log.
func (s *Container) SaveToLog(ctx context.Context, appName string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.mu.stmts) == 0 {
		return
	}
	var buf bytes.Buffer
	for key, stats := range s.mu.stmts {
		stats.mu.Lock()
		json, err := json.Marshal(stats.mu.data)
		s.mu.Unlock()
		if err != nil {
			log.Errorf(ctx, "error while marshaling stats for %q // %q: %v", appName, key.String(), err)
			continue
		}
		fmt.Fprintf(&buf, "%q: %s\n", key.String(), json)
	}
	log.Infof(ctx, "statistics for %q:\n%s", appName, buf.String())
}

// Clear clears the data stored in this Container.
func (s *Container) Clear(ctx context.Context) {
	s.mu.Lock()
	defer s.mu.Unlock()

	atomic.AddInt64(s.atomic.uniqueStmtFingerprintCount, int64(-len(s.mu.stmts)))
	atomic.AddInt64(s.atomic.uniqueTxnFingerprintCount, int64(-len(s.mu.txns)))

	// Clear the map, to release the memory; make the new map somewhat already
	// large for the likely future workload.
	s.mu.stmts = make(map[stmtKey]*stmtStats, len(s.mu.stmts)/2)
	s.mu.txns = make(map[roachpb.TransactionFingerprintID]*txnStats, len(s.mu.txns)/2)

	s.mu.acc.Empty(ctx)
}

// Add combines one Container into another. Add manages locks on a, so taking
// a lock on a will cause a deadlock.
func (s *Container) Add(ctx context.Context, other *Container) (err error) {
	other.mu.Lock()
	statMap := make(map[stmtKey]*stmtStats)
	for k, v := range other.mu.stmts {
		statMap[k] = v
	}
	other.mu.Unlock()

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
		stats, created, throttled := s.getStatsForStmtWithKey(k, v.ID, true /* createIfNonexistent */)

		// If we have reached the limit of fingerprints, we skip this fingerprint.
		// No cleanup necessary.
		if throttled {
			continue
		}

		stats.mu.Lock()

		// If we created a new entry for the fingerprint, we check if we have
		// exceeded our memory budget.
		if created {
			estimatedAllocBytes := stats.sizeUnsafe() + k.size() + 8 /* stmtKey hash */
			// We still want to continue this loop to merge stats that are already
			// present in our map that do not require allocation.
			s.mu.Lock()
			if latestErr := s.mu.acc.Grow(ctx, estimatedAllocBytes); latestErr != nil {
				stats.mu.Unlock()
				// Instead of combining errors, we track the latest error occurred
				// in this method. This is because currently the only type of error we
				// can generate in this function is out of memory errors. Also since we
				// do not abort after encountering such errors, combining many same
				// errors is not helpful.
				err = latestErr
				delete(s.mu.stmts, k)
				s.mu.Unlock()
				continue
			}
			s.mu.Unlock()
		}

		// Note that we don't need to take a lock on v because
		// no other thread knows about v yet.
		stats.mu.data.Add(&v.mu.data)
		stats.mu.Unlock()
	}

	// Do what we did above for the statMap for the txn Map now.
	other.mu.Lock()
	txnMap := make(map[roachpb.TransactionFingerprintID]*txnStats)
	for k, v := range other.mu.txns {
		txnMap[k] = v
	}
	other.mu.Unlock()

	// Copy the transaction stats for each txn key
	for k, v := range txnMap {
		v.mu.Lock()
		txnCopy := &txnStats{}
		txnCopy.mu.data = v.mu.data
		v.mu.Unlock()
		txnCopy.statementFingerprintIDs = v.statementFingerprintIDs
		txnMap[k] = txnCopy
	}

	// Merge the txn stats
	for k, v := range txnMap {
		// We don't check if we have created a new entry here because we have
		// already accounted for all the memory that we will be allocating in this
		// function.
		t, created, throttled := s.getStatsForTxnWithKey(k, v.statementFingerprintIDs, true /* createIfNonExistent */)

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
			s.mu.Lock()
			if latestErr := s.mu.acc.Grow(ctx, estimatedAllocBytes); latestErr != nil {
				t.mu.Unlock()
				// We only track the latest error. See comment above for explanation.
				err = latestErr
				delete(s.mu.txns, k)
				s.mu.Unlock()
				continue
			}
			s.mu.Unlock()
		}

		t.mu.data.Add(&v.mu.data)
		t.mu.Unlock()
	}

	// Create a copy of the other's transactions statistics.
	other.txnCounts.mu.Lock()
	txnStats := other.txnCounts.mu.TxnStats
	other.txnCounts.mu.Unlock()

	// Merge the transaction stats.
	s.txnCounts.mu.Lock()
	s.txnCounts.mu.TxnStats.Add(txnStats)
	s.txnCounts.mu.Unlock()

	return err
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

// shouldSaveLogicalPlanDescription returns whether we should save the sample
// logical plan for a fingerprint (represented implicitly by the corresponding
// stmtStats object). stats is nil if it is the first time we see the
// fingerprint. We use `logicalPlanCollectionPeriod` to assess how frequently to
// sample logical plans.
func (s *Container) shouldSaveLogicalPlanDescription(stats *stmtStats) bool {
	if !sqlstats.SampleLogicalPlans.Get(&s.st.SV) {
		return false
	}
	if stats == nil {
		// Save logical plan the first time we see new statement fingerprint.
		return true
	}
	now := timeutil.Now()
	period := sqlstats.LogicalPlanCollectionPeriod.Get(&s.st.SV)
	stats.mu.Lock()
	defer stats.mu.Unlock()
	timeLastSampled := stats.mu.data.SensitiveInfo.MostRecentPlanTimestamp
	return now.Sub(timeLastSampled) >= period
}

type transactionCounts struct {
	mu struct {
		syncutil.Mutex
		// TODO(arul): Can we rename this without breaking stuff?
		roachpb.TxnStats
	}
}

func constructStatementFingerprintIDFromStmtKey(key stmtKey) roachpb.StmtFingerprintID {
	return roachpb.ConstructStatementFingerprintID(
		key.anonymizedStmt, key.failed, key.implicitTxn, key.database,
	)
}
