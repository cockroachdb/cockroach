// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package sqlstats is a subsystem that is responsible for tracking the
// statistics of statements and transactions.

package ssmemstorage

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"time"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/appstatspb"
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
	sampledPlanKey
	planHash                 uint64
	transactionFingerprintID appstatspb.TransactionFingerprintID
}

// sampledPlanKey is used by the Optimizer to determine if we should build a full EXPLAIN plan.
type sampledPlanKey struct {
	stmtNoConstants string
	implicitTxn     bool
	database        string
}

func (p sampledPlanKey) size() int64 {
	return int64(unsafe.Sizeof(p)) + int64(len(p.stmtNoConstants)) + int64(len(p.database))
}

func (s stmtKey) String() string {
	return s.stmtNoConstants
}

func (s stmtKey) size() int64 {
	return s.sampledPlanKey.size() + int64(unsafe.Sizeof(invalidStmtFingerprintID))
}

const invalidStmtFingerprintID = 0

// Container holds per-application statement and transaction statistics.
type Container struct {
	st      *cluster.Settings
	appName string

	// uniqueServerCount is a server level counter of all the unique fingerprints
	uniqueServerCount *SQLStatsAtomicCounters

	// acc is the memory account that tracks memory allocations related to stmts
	// and txns within this Container struct.
	// Since currently we do not destroy the Container struct when we perform
	// reset, we never close this account.
	acc *mon.ConcurrentBoundAccount

	mu struct {
		syncutil.Mutex

		stmts map[stmtKey]*stmtStats
		txns  map[appstatspb.TransactionFingerprintID]*txnStats

		// sampledStatementCache records if the statement has been sampled via
		// tracing. sampledPlanKey is used as the key to the map as it does not
		// use transactionFingerprintID which is not available at the time of
		// sampling decision.
		sampledStatementCache map[sampledPlanKey]struct{}
	}

	txnCounts transactionCounts
	mon       *mon.BytesMonitor

	knobs *sqlstats.TestingKnobs
}

// New returns a new instance of Container.
func New(
	st *cluster.Settings,
	uniqueServerCount *SQLStatsAtomicCounters,
	mon *mon.BytesMonitor,
	appName string,
	knobs *sqlstats.TestingKnobs,
) *Container {
	s := &Container{
		st:                st,
		appName:           appName,
		mon:               mon,
		knobs:             knobs,
		uniqueServerCount: uniqueServerCount,
	}

	if mon != nil {
		s.acc = mon.MakeConcurrentBoundAccount()
	}

	s.mu.stmts = make(map[stmtKey]*stmtStats)
	s.mu.txns = make(map[appstatspb.TransactionFingerprintID]*txnStats)
	s.mu.sampledStatementCache = make(map[sampledPlanKey]struct{})

	return s
}

func (s *Container) IterateAggregatedTransactionStats(
	_ context.Context, _ sqlstats.IteratorOptions, visitor sqlstats.AggregatedTransactionVisitor,
) error {
	txnStat := func() appstatspb.TxnStats {
		s.txnCounts.mu.Lock()
		defer s.txnCounts.mu.Unlock()
		return s.txnCounts.mu.TxnStats
	}()

	err := visitor(s.appName, &txnStat)
	if err != nil {
		return errors.Wrap(err, "sql stats iteration abort")
	}

	return nil
}

// StmtStatsIterator returns an instance of StmtStatsIterator.
func (s *Container) StmtStatsIterator(options sqlstats.IteratorOptions) StmtStatsIterator {
	return NewStmtStatsIterator(s, options)
}

// TxnStatsIterator returns an instance of TxnStatsIterator.
func (s *Container) TxnStatsIterator(options sqlstats.IteratorOptions) TxnStatsIterator {
	return NewTxnStatsIterator(s, options)
}

func (s *Container) IterateStatementStats(
	ctx context.Context, options sqlstats.IteratorOptions, visitor sqlstats.StatementVisitor,
) error {
	iter := s.StmtStatsIterator(options)

	for iter.Next() {
		if err := visitor(ctx, iter.Cur()); err != nil {
			return err
		}
	}

	return nil
}

func (s *Container) IterateTransactionStats(
	ctx context.Context, options sqlstats.IteratorOptions, visitor sqlstats.TransactionVisitor,
) error {
	iter := s.TxnStatsIterator(options)

	for iter.Next() {
		stats := iter.Cur()
		if err := visitor(ctx, stats); err != nil {
			return err
		}
	}

	return nil
}

// NewTempContainerFromExistingStmtStats creates a new Container by ingesting a slice
// of serverpb.StatementsResponse_CollectedStatementStatistics sorted by
// Key.KeyData.App field.
// It consumes the first chunk of the slice where
// all entries in the chunk contains the identical appName. The remaining
// slice is returned as the result.
// It returns a nil slice once all entries in statistics are consumed.
func NewTempContainerFromExistingStmtStats(
	statistics []serverpb.StatementsResponse_CollectedStatementStatistics,
) (
	container *Container,
	remaining []serverpb.StatementsResponse_CollectedStatementStatistics,
	err error,
) {
	if len(statistics) == 0 {
		return nil, statistics, nil
	}

	appName := statistics[0].Key.KeyData.App

	container = New(
		nil, /* st */
		nil, /* uniqueServerCount */
		nil, /* mon */
		appName,
		nil, /* knobs */
	)

	for i := range statistics {
		if currentAppName := statistics[i].Key.KeyData.App; currentAppName != appName {
			return container, statistics[i:], nil
		}
		key := stmtKey{
			sampledPlanKey: sampledPlanKey{
				stmtNoConstants: statistics[i].Key.KeyData.Query,
				implicitTxn:     statistics[i].Key.KeyData.ImplicitTxn,
				database:        statistics[i].Key.KeyData.Database,
			},
			planHash:                 statistics[i].Key.KeyData.PlanHash,
			transactionFingerprintID: statistics[i].Key.KeyData.TransactionFingerprintID,
		}
		stmtStats, _, throttled :=
			container.tryCreateStatsForStmtWithKeyLocked(key, statistics[i].ID)
		if throttled {
			return nil /* container */, nil /* remaining */, ErrFingerprintLimitReached
		}

		// This handles all the statistics fields.
		stmtStats.mu.data.Add(&statistics[i].Stats)

		// Setting all metadata fields.
		if stmtStats.mu.data.SensitiveInfo.LastErr == "" {
			stmtStats.mu.data.SensitiveInfo.LastErr = statistics[i].Stats.SensitiveInfo.LastErr
		}

		if stmtStats.mu.data.SensitiveInfo.MostRecentPlanTimestamp.Before(statistics[i].Stats.SensitiveInfo.MostRecentPlanTimestamp) {
			stmtStats.mu.data.SensitiveInfo.MostRecentPlanDescription = statistics[i].Stats.SensitiveInfo.MostRecentPlanDescription
			stmtStats.mu.data.SensitiveInfo.MostRecentPlanTimestamp = statistics[i].Stats.SensitiveInfo.MostRecentPlanTimestamp
		}

		stmtStats.mu.vectorized = statistics[i].Key.KeyData.Vec
		stmtStats.mu.distSQLUsed = statistics[i].Key.KeyData.DistSQL
		stmtStats.mu.fullScan = statistics[i].Key.KeyData.FullScan
		stmtStats.mu.database = statistics[i].Key.KeyData.Database
		stmtStats.mu.querySummary = statistics[i].Key.KeyData.QuerySummary
	}

	return container, nil /* remaining */, nil /* err */
}

func (s *Container) MaybeLogDiscardMessage(ctx context.Context) {
	s.uniqueServerCount.maybeLogDiscardMessage(ctx)
}

// NewTempContainerFromExistingTxnStats creates a new Container by ingesting a slice
// of CollectedTransactionStatistics sorted by .StatsData.App field.
// It consumes the first chunk of the slice where all entries in the chunk
// contains the identical appName. The remaining slice is returned as the result.
// It returns a nil slice once all entries in statistics are consumed.
func NewTempContainerFromExistingTxnStats(
	statistics []serverpb.StatementsResponse_ExtendedCollectedTransactionStatistics,
) (
	container *Container,
	remaining []serverpb.StatementsResponse_ExtendedCollectedTransactionStatistics,
	err error,
) {
	if len(statistics) == 0 {
		return nil, statistics, nil
	}

	appName := statistics[0].StatsData.App

	container = New(
		nil, /* st */
		nil, /* uniqueServerCount */
		nil, /* mon */
		appName,
		nil, /* knobs */
	)

	for i := range statistics {
		if currentAppName := statistics[i].StatsData.App; currentAppName != appName {
			return container, statistics[i:], nil
		}
		// Since we just created the container and haven't exposed it yet, we
		// don't need to take a lock on it.
		txnStats, _, throttled := container.tryCreateStatsForTxnWithKey(
			statistics[i].StatsData.TransactionFingerprintID,
			statistics[i].StatsData.StatementFingerprintIDs)
		if throttled {
			return nil /* container */, nil /* remaining */, ErrFingerprintLimitReached
		}
		// No need for a lock here given that we're the only ones who has access
		// to this txnStats object.
		txnStats.mu.data.Add(&statistics[i].StatsData.Stats)
	}

	return container, nil /* remaining */, nil /* err */
}

func (s *Container) NewApplicationStatsWithInheritedOptions() *Container {
	return New(
		s.st,
		// There is no need to constraint txn fingerprint limit since in temporary
		// container, there will never be more than one transaction fingerprint.
		nil, // uniqueServerCount
		s.mon,
		s.appName,
		s.knobs,
	)
}

type txnStats struct {
	statementFingerprintIDs []appstatspb.StmtFingerprintID

	mu struct {
		syncutil.Mutex

		data appstatspb.TransactionStatistics
	}
}

func (t *txnStats) sizeUnsafeLocked() int64 {
	t.mu.AssertHeld()
	const txnStatsShallowSize = int64(unsafe.Sizeof(txnStats{}))
	stmtFingerprintIDsSize := int64(cap(t.statementFingerprintIDs)) *
		int64(unsafe.Sizeof(appstatspb.StmtFingerprintID(0)))

	// t.mu.data might contain pointer types, so we subtract its shallow size
	// and include the actual size.
	dataSize := -int64(unsafe.Sizeof(appstatspb.TransactionStatistics{})) +
		int64(t.mu.data.Size())

	return txnStatsShallowSize + stmtFingerprintIDsSize + dataSize
}

// stmtStats holds per-statement statistics.
type stmtStats struct {
	// ID is the statementFingerprintID constructed using the stmtKey fields.
	ID appstatspb.StmtFingerprintID

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
		// was executed from.
		database string

		// querySummary records a summarized format of the query statement.
		querySummary string

		data appstatspb.StatementStatistics
	}
}

func (s *stmtStats) sizeUnsafeLocked() int64 {
	const stmtStatsShallowSize = int64(unsafe.Sizeof(stmtStats{}))
	databaseNameSize := int64(len(s.mu.database))

	// s.mu.data might contain pointer tyeps, so we subtract its shallow size and
	// include the actual size.
	dataSize := -int64(unsafe.Sizeof(appstatspb.StatementStatistics{})) +
		int64(s.mu.data.Size())

	return stmtStatsShallowSize + databaseNameSize + dataSize
}

func (s *stmtStats) recordExecStatsLocked(stats execstats.QueryLevelStats) {
	s.mu.data.ExecStats.Count++
	count := s.mu.data.ExecStats.Count
	s.mu.data.ExecStats.NetworkBytes.Record(count, float64(stats.NetworkBytesSent))
	s.mu.data.ExecStats.MaxMemUsage.Record(count, float64(stats.MaxMemUsage))
	s.mu.data.ExecStats.ContentionTime.Record(count, stats.ContentionTime.Seconds())
	s.mu.data.ExecStats.NetworkMessages.Record(count, float64(stats.NetworkMessages))
	s.mu.data.ExecStats.MaxDiskUsage.Record(count, float64(stats.MaxDiskUsage))
	s.mu.data.ExecStats.CPUSQLNanos.Record(count, float64(stats.CPUTime.Nanoseconds()))

	s.mu.data.ExecStats.MVCCIteratorStats.StepCount.Record(count, float64(stats.MvccSteps))
	s.mu.data.ExecStats.MVCCIteratorStats.StepCountInternal.Record(count, float64(stats.MvccStepsInternal))
	s.mu.data.ExecStats.MVCCIteratorStats.SeekCount.Record(count, float64(stats.MvccSeeks))
	s.mu.data.ExecStats.MVCCIteratorStats.SeekCountInternal.Record(count, float64(stats.MvccSeeksInternal))
	s.mu.data.ExecStats.MVCCIteratorStats.BlockBytes.Record(count, float64(stats.MvccBlockBytes))
	s.mu.data.ExecStats.MVCCIteratorStats.BlockBytesInCache.Record(count, float64(stats.MvccBlockBytesInCache))
	s.mu.data.ExecStats.MVCCIteratorStats.KeyBytes.Record(count, float64(stats.MvccKeyBytes))
	s.mu.data.ExecStats.MVCCIteratorStats.ValueBytes.Record(count, float64(stats.MvccValueBytes))
	s.mu.data.ExecStats.MVCCIteratorStats.PointCount.Record(count, float64(stats.MvccPointCount))
	s.mu.data.ExecStats.MVCCIteratorStats.PointsCoveredByRangeTombstones.Record(count, float64(stats.MvccPointsCoveredByRangeTombstones))
	s.mu.data.ExecStats.MVCCIteratorStats.RangeKeyCount.Record(count, float64(stats.MvccRangeKeyCount))
	s.mu.data.ExecStats.MVCCIteratorStats.RangeKeyContainedPoints.Record(count, float64(stats.MvccRangeKeyContainedPoints))
	s.mu.data.ExecStats.MVCCIteratorStats.RangeKeySkippedPoints.Record(count, float64(stats.MvccRangeKeySkippedPoints))
}

// getStatsForStmtWithKey returns an instance of stmtStats.
func (s *Container) getStatsForStmtWithKey(key stmtKey) (stats *stmtStats) {
	s.mu.Lock()
	defer s.mu.Unlock()
	stats = s.mu.stmts[key]
	return stats
}

// tryCreateStatsForStmtWithKey attemps to insert a new stmtStats entry with
// the given stmtKey and stmtFingerprintID. If the stmtKey exists already
// we'll return the existing stmtStats object. If the provided stmtFingerprintID
// is 0, we'll also compute the fingerprintID from the stmtKey.
// we'll construct it from the stmtKey.
func (s *Container) tryCreateStatsForStmtWithKey(
	key stmtKey, stmtFingerprintID appstatspb.StmtFingerprintID,
) (stats *stmtStats, created, throttled bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.tryCreateStatsForStmtWithKeyLocked(key, stmtFingerprintID)
}

func (s *Container) tryCreateStatsForStmtWithKeyLocked(
	key stmtKey, stmtFingerprintID appstatspb.StmtFingerprintID,
) (stats *stmtStats, created, throttled bool) {
	// Retrieve the per-statement statistic object, and create it if it
	// doesn't exist yet.
	stats, ok := s.mu.stmts[key]
	if ok {
		return stats, false /* created */, false /* throttled */
	}

	// If the uniqueStmtFingerprintCount is nil, then we don't check for
	// fingerprint limit.
	if s.uniqueServerCount != nil && !s.uniqueServerCount.tryAddStmtFingerprint() {
		return stats, false /* created */, true /* throttled */
	}

	// Otherwise try to create a new entry.
	if stmtFingerprintID == 0 {
		stmtFingerprintID = constructStatementFingerprintIDFromStmtKey(key)
	}

	stats = &stmtStats{}
	stats.ID = stmtFingerprintID
	s.mu.stmts[key] = stats
	s.mu.sampledStatementCache[key.sampledPlanKey] = struct{}{}

	return stats, true /* created */, false /* throttled */
}

func (s *Container) getStatsForTxnWithKey(key appstatspb.TransactionFingerprintID) *txnStats {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.mu.txns[key]
}

func (s *Container) tryCreateStatsForTxnWithKey(
	key appstatspb.TransactionFingerprintID, stmtFingerprintIDs []appstatspb.StmtFingerprintID,
) (stats *txnStats, created, throttled bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	stats, ok := s.mu.txns[key]
	if ok {
		return stats, false /* created */, false /* throttled */
	}

	// If the uniqueTxnFingerprintCount is nil, then we don't check for
	// fingerprint limit.
	if s.uniqueServerCount != nil && !s.uniqueServerCount.tryAddTxnFingerprint() {
		return nil /* stats */, false /* created */, true /* throttled */
	}
	stats = &txnStats{}
	stats.statementFingerprintIDs = stmtFingerprintIDs
	s.mu.txns[key] = stats
	return stats, true /* created */, false /* throttled */
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
		json, err := func() ([]byte, error) {
			stats.mu.Lock()
			defer stats.mu.Unlock()
			return json.Marshal(stats.mu.data)
		}()
		if err != nil {
			log.Errorf(ctx, "error while marshaling stats for %q // %q: %v", appName, key.String(), err)
			continue
		}
		fmt.Fprintf(&buf, "%q: %s\n", key.String(), json)
	}
	log.Infof(ctx, "statistics for %q:\n%s", appName, buf.String())
}

// DrainStats returns all collected statement and transaction stats in memory to the caller and clears SQL stats
// make sure that new arriving stats won't be interfering with existing one.
func (s *Container) DrainStats(
	ctx context.Context,
) ([]*appstatspb.CollectedStatementStatistics, []*appstatspb.CollectedTransactionStatistics) {
	statementStats := make([]*appstatspb.CollectedStatementStatistics, 0)
	var stmts map[stmtKey]*stmtStats

	transactionStats := make([]*appstatspb.CollectedTransactionStatistics, 0)
	var txns map[appstatspb.TransactionFingerprintID]*txnStats

	func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		stmts = s.mu.stmts
		txns = s.mu.txns
		// Reset statementStats and transactions after they're assigned to local variables.
		s.clearLocked(ctx)
	}()

	var data appstatspb.StatementStatistics
	var distSQLUsed, vectorized, fullScan bool
	var database, querySummary string

	for key, stmt := range stmts {
		func() {
			stmt.mu.Lock()
			defer stmt.mu.Unlock()
			data = stmt.mu.data
			distSQLUsed = stmt.mu.distSQLUsed
			vectorized = stmt.mu.vectorized
			fullScan = stmt.mu.fullScan
			database = stmt.mu.database
			querySummary = stmt.mu.querySummary
		}()

		statementStats = append(statementStats, &appstatspb.CollectedStatementStatistics{
			Key: appstatspb.StatementStatisticsKey{
				Query:                    key.stmtNoConstants,
				QuerySummary:             querySummary,
				DistSQL:                  distSQLUsed,
				Vec:                      vectorized,
				ImplicitTxn:              key.implicitTxn,
				FullScan:                 fullScan,
				App:                      s.appName,
				Database:                 database,
				PlanHash:                 key.planHash,
				TransactionFingerprintID: key.transactionFingerprintID,
			},
			ID:    stmt.ID,
			Stats: data,
		})
	}

	for key, txn := range txns {
		var stats appstatspb.TransactionStatistics
		func() {
			txn.mu.Lock()
			defer txn.mu.Unlock()
			stats = txn.mu.data
		}()
		transactionStats = append(transactionStats, &appstatspb.CollectedTransactionStatistics{
			StatementFingerprintIDs:  txn.statementFingerprintIDs,
			App:                      s.appName,
			Stats:                    stats,
			TransactionFingerprintID: key,
		})
	}
	return statementStats, transactionStats
}

// Clear clears the data stored in this Container and prepare the Container
// for reuse.
func (s *Container) Clear(ctx context.Context) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.clearLocked(ctx)
}

func (s *Container) clearLocked(ctx context.Context) {
	// We must call freeLocked before clearing the containers as freeLocked
	// reads the size of each container to reset the counters.
	s.freeLocked(ctx)

	// Clear the map, to release the memory; make the new map somewhat already
	// large for the likely future workload.
	s.mu.stmts = make(map[stmtKey]*stmtStats, len(s.mu.stmts)/2)
	s.mu.txns = make(map[appstatspb.TransactionFingerprintID]*txnStats, len(s.mu.txns)/2)
	s.mu.sampledStatementCache = make(map[sampledPlanKey]struct{}, len(s.mu.sampledStatementCache)/2)
	if s.knobs != nil && s.knobs.OnAfterClear != nil {
		s.knobs.OnAfterClear()
	}
}

// Free frees the accounted resources from the Container. The Container is
// presumed to be no longer in use and its actual allocated memory will
// eventually be GC'd.
func (s *Container) Free(ctx context.Context) {
	s.acc.Clear(ctx)

	s.mu.Lock()
	defer s.mu.Unlock()
	s.freeLocked(ctx)
}

func (s *Container) freeLocked(ctx context.Context) {
	if s.uniqueServerCount != nil {
		s.uniqueServerCount.freeByCnt(int64(len(s.mu.stmts)), int64(len(s.mu.txns)))
	}
}

// Add combines one Container into another. Add manages locks on a, so taking
// a lock on a will cause a deadlock.
func (s *Container) Add(ctx context.Context, other *Container) (err error) {
	statMap := func() map[stmtKey]*stmtStats {
		other.mu.Lock()
		defer other.mu.Unlock()

		statMap := make(map[stmtKey]*stmtStats)
		for k, v := range other.mu.stmts {
			statMap[k] = v
		}
		return statMap
	}()

	// Copy the statement stats for each statement key.
	for k, v := range statMap {
		statCopy := func() *stmtStats {
			v.mu.Lock()
			defer v.mu.Unlock()
			statCopy := &stmtStats{}
			statCopy.mu.data = v.mu.data
			return statCopy
		}()
		statCopy.ID = v.ID
		statMap[k] = statCopy
	}

	// Merge the statement stats.
	for k, v := range statMap {
		stats, created, throttled := s.tryCreateStatsForStmtWithKey(k, v.ID)
		// If we have reached the limit of fingerprints, we skip this fingerprint.
		// No cleanup necessary.
		if throttled {
			continue
		}

		func() {
			stats.mu.Lock()
			defer stats.mu.Unlock()

			// If we created a new entry for the fingerprint, we check if we have
			// exceeded our memory budget.
			if created {
				estimatedAllocBytes := stats.sizeUnsafeLocked() + k.size() + 8 /* stmtKey hash */
				// We still want to continue this loop to merge stats that are already
				// present in our map that do not require allocation.
				if latestErr := func() error {
					s.mu.Lock()
					defer s.mu.Unlock()
					growErr := s.acc.Grow(ctx, estimatedAllocBytes)
					if growErr != nil {
						delete(s.mu.stmts, k)
					}
					return growErr
				}(); latestErr != nil {
					// Instead of combining errors, we track the latest error occurred
					// in this method. This is because currently the only type of error we
					// can generate in this function is out of memory errors. Also since we
					// do not abort after encountering such errors, combining many same
					// errors is not helpful.
					err = latestErr
					return
				}
			}

			// Note that we don't need to take a lock on v because
			// no other thread knows about v yet.
			stats.mu.data.Add(&v.mu.data)
		}()
	}

	// Do what we did above for the statMap for the txn Map now.
	txnMap := func() map[appstatspb.TransactionFingerprintID]*txnStats {
		other.mu.Lock()
		defer other.mu.Unlock()
		txnMap := make(map[appstatspb.TransactionFingerprintID]*txnStats)
		for k, v := range other.mu.txns {
			txnMap[k] = v
		}
		return txnMap
	}()

	// Copy the transaction stats for each txn key
	for k, v := range txnMap {
		txnCopy := func() *txnStats {
			v.mu.Lock()
			defer v.mu.Unlock()
			txnCopy := &txnStats{}
			txnCopy.mu.data = v.mu.data
			return txnCopy
		}()
		txnCopy.statementFingerprintIDs = v.statementFingerprintIDs
		txnMap[k] = txnCopy
	}

	// Merge the txn stats
	for k, v := range txnMap {
		// We don't check if we have created a new entry here because we have
		// already accounted for all the memory that we will be allocating in this
		// function.
		t, created, throttled := s.tryCreateStatsForTxnWithKey(k, v.statementFingerprintIDs)

		// If we have reached the unique fingerprint limit, we skip adding the
		// current fingerprint. No cleanup is necessary.
		if throttled {
			continue
		}

		func() {
			t.mu.Lock()
			defer t.mu.Unlock()

			if created {
				estimatedAllocBytes := t.sizeUnsafeLocked() + k.Size() + 8 /* TransactionFingerprintID hash */
				// We still want to continue this loop to merge stats that are already
				// present in our map that do not require allocation.
				if latestErr := func() error {
					growErr := s.acc.Grow(ctx, estimatedAllocBytes)
					if growErr != nil {
						delete(s.mu.txns, k)
					}
					return growErr
				}(); latestErr != nil {
					// We only track the latest error. See comment above for explanation.
					err = latestErr
					return
				}
			}

			// Note that we don't need to take a lock on v because
			// no other thread knows about v yet.
			t.mu.data.Add(&v.mu.data)
		}()
	}

	// Create a copy of the other's transactions statistics.
	txnStats := func() appstatspb.TxnStats {
		other.txnCounts.mu.Lock()
		defer other.txnCounts.mu.Unlock()
		return other.txnCounts.mu.TxnStats
	}()

	// Merge the transaction stats.
	func(txnStats appstatspb.TxnStats) {
		s.txnCounts.mu.Lock()
		defer s.txnCounts.mu.Unlock()
		s.txnCounts.mu.TxnStats.Add(txnStats)
	}(txnStats)

	return err
}

func (s *Container) getTimeNow() time.Time {
	if s.knobs != nil && s.knobs.StubTimeNow != nil {
		return s.knobs.StubTimeNow()
	}

	return timeutil.Now()
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

type transactionCounts struct {
	mu struct {
		syncutil.Mutex
		// TODO(arul): Can we rename this without breaking stuff?
		appstatspb.TxnStats
	}
}

func constructStatementFingerprintIDFromStmtKey(key stmtKey) appstatspb.StmtFingerprintID {
	return appstatspb.ConstructStatementFingerprintID(
		key.stmtNoConstants, key.implicitTxn, key.database,
	)
}
