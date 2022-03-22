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
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
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
	sampledPlanKey
	planHash                 uint64
	transactionFingerprintID roachpb.TransactionFingerprintID
}

// sampledPlanKey is used by the Optimizer to determine if we should build a full EXPLAIN plan.
type sampledPlanKey struct {
	anonymizedStmt string
	failed         bool
	implicitTxn    bool
	database       string
}

func (p sampledPlanKey) size() int64 {
	return int64(unsafe.Sizeof(p)) + int64(len(p.anonymizedStmt)) + int64(len(p.database))
}

func (s stmtKey) String() string {
	if s.failed {
		return "!" + s.anonymizedStmt
	}
	return s.anonymizedStmt
}

func (s stmtKey) size() int64 {
	return s.sampledPlanKey.size() + int64(unsafe.Sizeof(invalidStmtFingerprintID))
}

const invalidStmtFingerprintID = 0

// Container holds per-application statement and transaction statistics.
type Container struct {
	st      *cluster.Settings
	appName string

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

		// sampledPlanMetadataCache records when was the last time the plan was
		// sampled. This data structure uses a subset of stmtKey as the key into
		// in-memory dictionary in order to allow lookup for whether a plan has been
		// sampled for a statement without needing to know the statement's
		// transaction fingerprintID.
		sampledPlanMetadataCache map[sampledPlanKey]time.Time
	}

	txnCounts transactionCounts
	mon       *mon.BytesMonitor

	knobs *sqlstats.TestingKnobs
}

var _ sqlstats.ApplicationStats = &Container{}

// New returns a new instance of Container.
func New(
	st *cluster.Settings,
	uniqueStmtFingerprintLimit *settings.IntSetting,
	uniqueTxnFingerprintLimit *settings.IntSetting,
	uniqueStmtFingerprintCount *int64,
	uniqueTxnFingerprintCount *int64,
	mon *mon.BytesMonitor,
	appName string,
	knobs *sqlstats.TestingKnobs,
) *Container {
	s := &Container{
		st:                         st,
		appName:                    appName,
		uniqueStmtFingerprintLimit: uniqueStmtFingerprintLimit,
		uniqueTxnFingerprintLimit:  uniqueTxnFingerprintLimit,
		mon:                        mon,
		knobs:                      knobs,
	}

	if mon != nil {
		s.mu.acc = mon.MakeBoundAccount()
	}

	s.mu.stmts = make(map[stmtKey]*stmtStats)
	s.mu.txns = make(map[roachpb.TransactionFingerprintID]*txnStats)
	s.mu.sampledPlanMetadataCache = make(map[sampledPlanKey]time.Time)

	s.atomic.uniqueStmtFingerprintCount = uniqueStmtFingerprintCount
	s.atomic.uniqueTxnFingerprintCount = uniqueTxnFingerprintCount

	return s
}

// IterateAggregatedTransactionStats implements sqlstats.ApplicationStats
// interface.
func (s *Container) IterateAggregatedTransactionStats(
	_ context.Context, _ *sqlstats.IteratorOptions, visitor sqlstats.AggregatedTransactionVisitor,
) error {
	var txnStat roachpb.TxnStats
	s.txnCounts.mu.Lock()
	txnStat = s.txnCounts.mu.TxnStats
	s.txnCounts.mu.Unlock()

	err := visitor(s.appName, &txnStat)
	if err != nil {
		return errors.Wrap(err, "sql stats iteration abort")
	}

	return nil
}

// StmtStatsIterator returns an instance of StmtStatsIterator.
func (s *Container) StmtStatsIterator(options *sqlstats.IteratorOptions) *StmtStatsIterator {
	return NewStmtStatsIterator(s, options)
}

// TxnStatsIterator returns an instance of TxnStatsIterator.
func (s *Container) TxnStatsIterator(options *sqlstats.IteratorOptions) *TxnStatsIterator {
	return NewTxnStatsIterator(s, options)
}

// IterateStatementStats implements sqlstats.Provider interface.
func (s *Container) IterateStatementStats(
	ctx context.Context, options *sqlstats.IteratorOptions, visitor sqlstats.StatementVisitor,
) error {
	iter := s.StmtStatsIterator(options)

	for iter.Next() {
		if err := visitor(ctx, iter.Cur()); err != nil {
			return err
		}
	}

	return nil
}

// IterateTransactionStats implements sqlstats.Provider interface.
func (s *Container) IterateTransactionStats(
	ctx context.Context, options *sqlstats.IteratorOptions, visitor sqlstats.TransactionVisitor,
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
		nil, /* uniqueStmtFingerprintLimit */
		nil, /* uniqueTxnFingerprintLimit */
		nil, /* uniqueStmtFingerprintCount */
		nil, /* uniqueTxnFingerprintCount */
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
				anonymizedStmt: statistics[i].Key.KeyData.Query,
				failed:         statistics[i].Key.KeyData.Failed,
				implicitTxn:    statistics[i].Key.KeyData.ImplicitTxn,
				database:       statistics[i].Key.KeyData.Database,
			},
			planHash:                 statistics[i].Key.KeyData.PlanHash,
			transactionFingerprintID: statistics[i].Key.KeyData.TransactionFingerprintID,
		}
		stmtStats, _, throttled :=
			container.getStatsForStmtWithKeyLocked(key, statistics[i].ID, true /* createIfNonexistent */)
		if throttled {
			return nil /* container */, nil /* remaining */, ErrFingerprintLimitReached
		}

		// This handles all the statistics fields.
		stmtStats.mu.data.Add(&statistics[i].Stats)

		// Setting all metadata fields.
		if stmtStats.mu.data.SensitiveInfo.LastErr == "" && key.failed {
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
		nil, /* uniqueStmtFingerprintLimit */
		nil, /* uniqueTxnFingerprintLimit */
		nil, /* uniqueStmtFingerprintCount */
		nil, /* uniqueTxnFingerprintCount */
		nil, /* mon */
		appName,
		nil, /* knobs */
	)

	for i := range statistics {
		if currentAppName := statistics[i].StatsData.App; currentAppName != appName {
			return container, statistics[i:], nil
		}
		txnStats, _, throttled :=
			container.getStatsForTxnWithKeyLocked(
				statistics[i].StatsData.TransactionFingerprintID,
				statistics[i].StatsData.StatementFingerprintIDs,
				true /* createIfNonexistent */)
		if throttled {
			return nil /* container */, nil /* remaining */, ErrFingerprintLimitReached
		}
		txnStats.mu.data.Add(&statistics[i].StatsData.Stats)
	}

	return container, nil /* remaining */, nil /* err */
}

// NewApplicationStatsWithInheritedOptions implements the
// sqlstats.ApplicationStats interface.
func (s *Container) NewApplicationStatsWithInheritedOptions() sqlstats.ApplicationStats {
	var (
		uniqueStmtFingerprintCount int64
		uniqueTxnFingerprintCount  int64
	)
	s.mu.Lock()
	defer s.mu.Unlock()
	return New(
		s.st,
		sqlstats.MaxSQLStatsStmtFingerprintsPerExplicitTxn,
		// There is no need to constraint txn fingerprint limit since in temporary
		// container, there will never be more than one transaction fingerprint.
		nil, // uniqueTxnFingerprintLimit,
		&uniqueStmtFingerprintCount,
		&uniqueTxnFingerprintCount,
		s.mon,
		s.appName,
		s.knobs,
	)
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

func (t *txnStats) mergeStats(stats *roachpb.TransactionStatistics) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.mu.data.Add(stats)
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
		// was executed from.
		database string

		// querySummary records a summarized format of the query statement.
		querySummary string

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

func (s *stmtStats) mergeStatsLocked(statistics *roachpb.CollectedStatementStatistics) {
	// This handles all the statistics fields.
	s.mu.data.Add(&statistics.Stats)

	// Setting all metadata fields.
	if s.mu.data.SensitiveInfo.LastErr == "" && statistics.Key.Failed {
		s.mu.data.SensitiveInfo.LastErr = statistics.Stats.SensitiveInfo.LastErr
	}

	if s.mu.data.SensitiveInfo.MostRecentPlanTimestamp.Before(statistics.Stats.SensitiveInfo.MostRecentPlanTimestamp) {
		s.mu.data.SensitiveInfo.MostRecentPlanDescription = statistics.Stats.SensitiveInfo.MostRecentPlanDescription
		s.mu.data.SensitiveInfo.MostRecentPlanTimestamp = statistics.Stats.SensitiveInfo.MostRecentPlanTimestamp
	}

	s.mu.vectorized = statistics.Key.Vec
	s.mu.distSQLUsed = statistics.Key.DistSQL
	s.mu.fullScan = statistics.Key.FullScan
	s.mu.database = statistics.Key.Database
}

// getStatsForStmt retrieves the per-stmt stat object. Regardless of if a valid
// stat object is returned or not, we always return the correct stmtFingerprintID
// for the given stmt.
func (s *Container) getStatsForStmt(
	anonymizedStmt string,
	implicitTxn bool,
	database string,
	failed bool,
	planHash uint64,
	transactionFingerprintID roachpb.TransactionFingerprintID,
	createIfNonexistent bool,
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
		sampledPlanKey: sampledPlanKey{
			anonymizedStmt: anonymizedStmt,
			failed:         failed,
			implicitTxn:    implicitTxn,
			database:       database,
		},
		planHash:                 planHash,
		transactionFingerprintID: transactionFingerprintID,
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

// getStatsForStmtWithKey returns an instance of stmtStats.
// If createIfNonexistent flag is set to true, then a new entry is created in
// the Container if it does not yet exist.
func (s *Container) getStatsForStmtWithKey(
	key stmtKey, stmtFingerprintID roachpb.StmtFingerprintID, createIfNonexistent bool,
) (stats *stmtStats, created, throttled bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.getStatsForStmtWithKeyLocked(key, stmtFingerprintID, createIfNonexistent)
}

func (s *Container) getStatsForStmtWithKeyLocked(
	key stmtKey, stmtFingerprintID roachpb.StmtFingerprintID, createIfNonexistent bool,
) (stats *stmtStats, created, throttled bool) {
	// Retrieve the per-statement statistic object, and create it if it
	// doesn't exist yet.
	stats, ok := s.mu.stmts[key]
	if !ok && createIfNonexistent {
		// If the uniqueStmtFingerprintCount is nil, then we don't check for
		// fingerprint limit.
		if s.atomic.uniqueStmtFingerprintCount != nil {
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
		}
		stats = &stmtStats{}
		stats.ID = stmtFingerprintID
		s.mu.stmts[key] = stats
		s.mu.sampledPlanMetadataCache[key.sampledPlanKey] = s.getTimeNow()

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

	return s.getStatsForTxnWithKeyLocked(key, stmtFingerprintIDs, createIfNonexistent)
}

func (s *Container) getStatsForTxnWithKeyLocked(
	key roachpb.TransactionFingerprintID,
	stmtFingerprintIDs []roachpb.StmtFingerprintID,
	createIfNonexistent bool,
) (stats *txnStats, created, throttled bool) {
	// Retrieve the per-transaction statistic object, and create it if it doesn't
	// exist yet.
	stats, ok := s.mu.txns[key]
	if !ok && createIfNonexistent {
		// If the uniqueTxnFingerprintCount is nil, then we don't check for
		// fingerprint limit.
		if s.atomic.uniqueTxnFingerprintCount != nil {
			limit := s.uniqueTxnFingerprintLimit.Get(&s.st.SV)
			incrementedFingerprintCount :=
				atomic.AddInt64(s.atomic.uniqueTxnFingerprintCount, int64(1) /* delts */)

			// If we have exceeded limit of fingerprint count, decrement the counter
			// and abort.
			if incrementedFingerprintCount > limit {
				atomic.AddInt64(s.atomic.uniqueTxnFingerprintCount, -int64(1) /* delts */)
				return nil /* stats */, false /* created */, true /* throttled */
			}
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

// Clear clears the data stored in this Container and prepare the Container
// for reuse.
func (s *Container) Clear(ctx context.Context) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.freeLocked(ctx)

	// Clear the map, to release the memory; make the new map somewhat already
	// large for the likely future workload.
	s.mu.stmts = make(map[stmtKey]*stmtStats, len(s.mu.stmts)/2)
	s.mu.txns = make(map[roachpb.TransactionFingerprintID]*txnStats, len(s.mu.txns)/2)
	s.mu.sampledPlanMetadataCache = make(map[sampledPlanKey]time.Time, len(s.mu.sampledPlanMetadataCache)/2)
}

// Free frees the accounted resources from the Container. The Container is
// presumed to be no longer in use and its actual allocated memory will
// eventually be GC'd.
func (s *Container) Free(ctx context.Context) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.freeLocked(ctx)
}

func (s *Container) freeLocked(ctx context.Context) {
	atomic.AddInt64(s.atomic.uniqueStmtFingerprintCount, int64(-len(s.mu.stmts)))
	atomic.AddInt64(s.atomic.uniqueTxnFingerprintCount, int64(-len(s.mu.txns)))

	s.mu.acc.Clear(ctx)
}

// MergeApplicationStatementStats implements the sqlstats.ApplicationStats interface.
func (s *Container) MergeApplicationStatementStats(
	ctx context.Context,
	other sqlstats.ApplicationStats,
	transformer func(*roachpb.CollectedStatementStatistics),
) (discardedStats uint64) {
	if err := other.IterateStatementStats(
		ctx,
		&sqlstats.IteratorOptions{},
		func(ctx context.Context, statistics *roachpb.CollectedStatementStatistics) error {
			if transformer != nil {
				transformer(statistics)
			}
			key := stmtKey{
				sampledPlanKey: sampledPlanKey{
					anonymizedStmt: statistics.Key.Query,
					failed:         statistics.Key.Failed,
					implicitTxn:    statistics.Key.ImplicitTxn,
					database:       statistics.Key.Database,
				},
				planHash:                 statistics.Key.PlanHash,
				transactionFingerprintID: statistics.Key.TransactionFingerprintID,
			}

			stmtStats, _, throttled :=
				s.getStatsForStmtWithKey(key, statistics.ID, true /* createIfNoneExistent */)
			if throttled {
				discardedStats++
				return nil
			}

			stmtStats.mu.Lock()
			defer stmtStats.mu.Unlock()

			stmtStats.mergeStatsLocked(statistics)
			planLastSampled := s.getLogicalPlanLastSampled(key.sampledPlanKey)
			if planLastSampled.Before(stmtStats.mu.data.SensitiveInfo.MostRecentPlanTimestamp) {
				s.setLogicalPlanLastSampled(key.sampledPlanKey, stmtStats.mu.data.SensitiveInfo.MostRecentPlanTimestamp)
			}

			return nil
		},
	); err != nil {
		// Calling Iterate.*Stats() function with a visitor function that does not
		// return error should not cause any error.
		panic(
			errors.NewAssertionErrorWithWrappedErrf(err, "unexpected error returned when iterating through application stats"),
		)
	}

	return discardedStats
}

// MergeApplicationTransactionStats implements the sqlstats.ApplicationStats interface.
func (s *Container) MergeApplicationTransactionStats(
	ctx context.Context, other sqlstats.ApplicationStats,
) (discardedStats uint64) {
	if err := other.IterateTransactionStats(
		ctx,
		&sqlstats.IteratorOptions{},
		func(ctx context.Context, statistics *roachpb.CollectedTransactionStatistics) error {
			txnStats, _, throttled :=
				s.getStatsForTxnWithKey(
					statistics.TransactionFingerprintID,
					statistics.StatementFingerprintIDs,
					true, /* createIfNonexistent */
				)

			if throttled {
				discardedStats++
				return nil
			}

			txnStats.mergeStats(&statistics.Stats)
			return nil
		}); err != nil {
		// Calling Iterate.*Stats() function with a visitor function that does not
		// return error should not cause any error.
		panic(
			errors.NewAssertionErrorWithWrappedErrf(err, "unexpected error returned when iterating through application stats"),
		)
	}

	return discardedStats
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

func (s *Container) getLogicalPlanLastSampled(key sampledPlanKey) time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()

	lastSampled, found := s.mu.sampledPlanMetadataCache[key]
	if !found {
		return time.Time{}
	}

	return lastSampled
}

func (s *Container) setLogicalPlanLastSampled(key sampledPlanKey, time time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mu.sampledPlanMetadataCache[key] = time
}

// shouldSaveLogicalPlanDescription returns whether we should save the sample
// logical plan based on the time it was last sampled. We use
// `logicalPlanCollectionPeriod` to assess how frequently to sample logical plans.
func (s *Container) shouldSaveLogicalPlanDescription(lastSampled time.Time) bool {
	if !sqlstats.SampleLogicalPlans.Get(&s.st.SV) {
		return false
	}
	now := s.getTimeNow()
	period := sqlstats.LogicalPlanCollectionPeriod.Get(&s.st.SV)
	return now.Sub(lastSampled) >= period
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
