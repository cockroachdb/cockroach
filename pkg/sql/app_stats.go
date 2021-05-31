// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/execstats"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
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

// txnKey is the hashed string constructed using the individual statement IDs
// that comprise the transaction.
type txnKey uint64

func (t txnKey) size() int64 {
	return 8
}

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

	st        *cluster.Settings
	stmts     map[stmtKey]*stmtStats
	txnCounts transactionCounts
	txns      map[txnKey]*txnStats
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

// stmtStatsEnable determines whether to collect per-statement
// statistics.
var stmtStatsEnable = settings.RegisterBoolSetting(
	"sql.metrics.statement_details.enabled", "collect per-statement query statistics", true,
).WithPublic()

// TxnStatsNumStmtIDsToRecord limits the number of statementIDs stored for in
// transactions statistics for a single transaction. This defaults to 1000, and
// currently is non-configurable (hidden setting).
var TxnStatsNumStmtIDsToRecord = settings.RegisterIntSetting(
	"sql.metrics.transaction_details.max_statement_ids",
	"max number of statement IDs to store for transaction statistics",
	1000,
	settings.PositiveInt,
)

// txnStatsEnable determines whether to collect per-application transaction
// statistics.
var txnStatsEnable = settings.RegisterBoolSetting(
	"sql.metrics.transaction_details.enabled", "collect per-application transaction statistics", true,
).WithPublic()

// sqlStatsCollectionLatencyThreshold specifies the minimum amount of time
// consumed by a SQL statement before it is collected for statistics reporting.
var sqlStatsCollectionLatencyThreshold = settings.RegisterDurationSetting(
	"sql.metrics.statement_details.threshold",
	"minimum execution time to cause statement statistics to be collected. "+
		"If configured, no transaction stats are collected.",
	0,
).WithPublic()

var dumpStmtStatsToLogBeforeReset = settings.RegisterBoolSetting(
	"sql.metrics.statement_details.dump_to_logs",
	"dump collected statement statistics to node logs when periodically cleared",
	false,
).WithPublic()

var sampleLogicalPlans = settings.RegisterBoolSetting(
	"sql.metrics.statement_details.plan_collection.enabled",
	"periodically save a logical plan for each fingerprint",
	true,
).WithPublic()

var logicalPlanCollectionPeriod = settings.RegisterDurationSetting(
	"sql.metrics.statement_details.plan_collection.period",
	"the time until a new logical plan is collected",
	5*time.Minute,
	settings.NonNegativeDuration,
).WithPublic()

var maxMemSQLStatsStmtFingerprints = settings.RegisterIntSetting(
	"sql.metrics.max_mem_stmt_fingerprints",
	"the maximum number of statement fingerprints stored in memory",
	100000,
).WithPublic()

var maxMemSQLStatsTxnFingerprints = settings.RegisterIntSetting(
	"sql.metrics.max_mem_txn_fingerprints",
	"the maximum number of transaction fingerprints stored in memory",
	100000,
).WithPublic()

var maxMemReportedSQLStatsStmtFingerprints = settings.RegisterIntSetting(
	"sql.metrics.max_mem_reported_stmt_fingerprints",
	"the maximum number of reported statement fingerprints stored in memory",
	100000,
).WithPublic()

var maxMemReportedSQLStatsTxnFingerprints = settings.RegisterIntSetting(
	"sql.metrics.max_mem_reported_txn_fingerprints",
	"the maximum number of reported transaction fingerprints stored in memory",
	100000,
).WithPublic()

// recordStatement saves per-statement statistics.
//
// samplePlanDescription can be nil, as these are only sampled periodically
// per unique fingerprint.
// recordStatement always returns a valid stmtID corresponding to the given
// stmt regardless of whether the statement is actually recorded or not.
//
// If the statement is not actually recorded due to either:
// 1. the memory budget has been exceeded
// 2. the unique statement fingerprint limit has been exceeded
// and error is being returned.
// Note: This error is only related to the operation of recording the statement
// statistics into in-memory structs. It is unrelated to the stmtErr in the
// arguments.
func (a *appStats) recordStatement(
	ctx context.Context,
	stmt *Statement,
	samplePlanDescription *roachpb.ExplainTreePlanNode,
	distSQLUsed bool,
	vectorized bool,
	implicitTxn bool,
	fullScan bool,
	automaticRetryCount int,
	numRows int,
	stmtErr error,
	parseLat, planLat, runLat, svcLat, ovhLat float64,
	stats topLevelQueryStats,
	planner *planner,
) (roachpb.StmtID, error) {
	createIfNonExistent := true
	// If the statement is below the latency threshold, or stats aren't being
	// recorded we don't need to create an entry in the stmts map for it. We do
	// still need stmtID for transaction level metrics tracking.
	t := sqlStatsCollectionLatencyThreshold.Get(&a.st.SV)
	if !stmtStatsEnable.Get(&a.st.SV) || (t > 0 && t.Seconds() >= svcLat) {
		createIfNonExistent = false
	}

	// Get the statistics object.
	s, statementKey, stmtID, created, throttled := a.getStatsForStmt(
		stmt.AnonymizedStr, implicitTxn, planner.SessionData().Database,
		stmtErr, createIfNonExistent,
	)

	// This means we have reached the limit of unique fingerprints. We don't
	// record anything and abort the operation.
	if throttled {
		return stmtID, errors.New("unique fingerprint limit has been reached")
	}

	// This statement was below the latency threshold or sql stats aren't being
	// recorded. Either way, we don't need to record anything in the stats object
	// for this statement, though we do need to return the statement ID for
	// transaction level metrics collection.
	if !createIfNonExistent {
		return stmtID, nil
	}

	// Retrieve the list of all nodes which the statement was executed on.
	var nodes []int64
	if planner.instrumentation.sp != nil {
		trace := planner.instrumentation.sp.GetRecording()
		// ForEach returns nodes in order.
		execinfrapb.ExtractNodesFromSpans(trace).ForEach(func(i int) {
			nodes = append(nodes, int64(i))
		})
	}

	// Collect the per-statement statistics.
	s.mu.Lock()
	defer s.mu.Unlock()

	s.mu.data.Count++
	if stmtErr != nil {
		s.mu.data.SensitiveInfo.LastErr = stmtErr.Error()
	}
	// Only update MostRecentPlanDescription if we sampled a new PlanDescription.
	if samplePlanDescription != nil {
		s.mu.data.SensitiveInfo.MostRecentPlanDescription = *samplePlanDescription
		s.mu.data.SensitiveInfo.MostRecentPlanTimestamp = timeutil.Now()
	}
	if automaticRetryCount == 0 {
		s.mu.data.FirstAttemptCount++
	} else if int64(automaticRetryCount) > s.mu.data.MaxRetries {
		s.mu.data.MaxRetries = int64(automaticRetryCount)
	}
	s.mu.data.SQLType = stmt.AST.StatementType().String()
	s.mu.data.NumRows.Record(s.mu.data.Count, float64(numRows))
	s.mu.data.ParseLat.Record(s.mu.data.Count, parseLat)
	s.mu.data.PlanLat.Record(s.mu.data.Count, planLat)
	s.mu.data.RunLat.Record(s.mu.data.Count, runLat)
	s.mu.data.ServiceLat.Record(s.mu.data.Count, svcLat)
	s.mu.data.OverheadLat.Record(s.mu.data.Count, ovhLat)
	s.mu.data.BytesRead.Record(s.mu.data.Count, float64(stats.bytesRead))
	s.mu.data.RowsRead.Record(s.mu.data.Count, float64(stats.rowsRead))
	s.mu.data.LastExecTimestamp = timeutil.Now()
	s.mu.data.Nodes = util.CombineUniqueInt64(s.mu.data.Nodes, nodes)
	// Note that some fields derived from tracing statements (such as
	// BytesSentOverNetwork) are not updated here because they are collected
	// on-demand.
	// TODO(asubiotto): Record the aforementioned fields here when always-on
	//  tracing is a thing.
	s.mu.vectorized = vectorized
	s.mu.distSQLUsed = distSQLUsed
	s.mu.fullScan = fullScan
	s.mu.database = planner.CurrentDatabase()

	if created {
		// stats size + stmtKey size + hash of the statementKey
		estimatedMemoryAllocBytes := s.sizeUnsafe() + statementKey.size() + 8
		a.Lock()
		defer a.Unlock()
		// We attempt to account for all the memory we used. If we have exceeded our
		// memory budget, delete the entry that we just created and report the error.
		if err := a.acc.Grow(ctx, estimatedMemoryAllocBytes); err != nil {
			delete(a.stmts, statementKey)
			return s.ID, err
		}
	}

	return s.ID, nil
}

// getStatsForStmt retrieves the per-stmt stat object. Regardless of if a valid
// stat object is returned or not, we always return the correct stmtID
// for the given stmt.
func (a *appStats) getStatsForStmt(
	anonymizedStmt string, implicitTxn bool, database string, stmtErr error, createIfNonexistent bool,
) (stats *stmtStats, key stmtKey, stmtID roachpb.StmtID, created bool, throttled bool) {
	// Extend the statement key with various characteristics, so
	// that we use separate buckets for the different situations.
	key = stmtKey{
		anonymizedStmt: anonymizedStmt,
		failed:         stmtErr != nil,
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
	key txnKey, stmtIDs []roachpb.StmtID, createIfNonexistent bool,
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
	txnMap := make(map[txnKey]*txnStats)
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
			estimatedAllocBytes := t.sizeUnsafe() + k.size() + 8 /* txnKey hash */
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

func anonymizeStmt(ast tree.Statement) string {
	if ast == nil {
		return ""
	}
	return tree.AsStringWithFlags(ast, tree.FmtHideConstants)
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
	txnTimeSec float64, ev txnEvent, implicit bool,
) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mu.TxnCount++
	s.mu.TxnTimeSec.Record(s.mu.TxnCount, txnTimeSec)
	if ev == txnCommit {
		s.mu.CommittedCount++
	}
	if implicit {
		s.mu.ImplicitCount++
	}
}

func (a *appStats) recordTransactionCounts(txnTimeSec float64, ev txnEvent, implicit bool) {
	if !txnStatsEnable.Get(&a.st.SV) {
		return
	}
	a.txnCounts.recordTransactionCounts(txnTimeSec, ev, implicit)
}

// recordTransaction saves per-transaction statistics
func (a *appStats) recordTransaction(
	ctx context.Context,
	key txnKey,
	retryCount int64,
	statementIDs []roachpb.StmtID,
	serviceLat time.Duration,
	retryLat time.Duration,
	commitLat time.Duration,
	numRows int,
	collectedExecStats bool,
	execStats execstats.QueryLevelStats,
	rowsRead int64,
	bytesRead int64,
) error {
	if !txnStatsEnable.Get(&a.st.SV) {
		return nil
	}
	// Do not collect transaction statistics if the stats collection latency
	// threshold is set, since our transaction UI relies on having stats for every
	// statement in the transaction.
	t := sqlStatsCollectionLatencyThreshold.Get(&a.st.SV)
	if t > 0 {
		return nil
	}

	// Get the statistics object.
	s, created, throttled := a.getStatsForTxnWithKey(key, statementIDs, true /* createIfNonexistent */)

	if throttled {
		return errors.New("unique fingerprint limit has been reached")
	}

	// Collect the per-transaction statistics.
	s.mu.Lock()
	defer s.mu.Unlock()

	// If we have created a new entry successfully, we check if we have reached
	// the memory limit. If we have, then we delete the newly created entry and
	// return the memory allocation error.
	// If the entry is not created, this means we have reached the limit of unique
	// fingerprints for this app. We also abort the operation and return an error.
	if created {
		estimatedMemAllocBytes :=
			s.sizeUnsafe() + key.size() + 8 /* hash of transaction key */
		a.Lock()
		if err := a.acc.Grow(ctx, estimatedMemAllocBytes); err != nil {
			delete(a.txns, key)
			a.Unlock()
			return err
		}
		a.Unlock()
	}

	s.mu.data.Count++

	s.mu.data.NumRows.Record(s.mu.data.Count, float64(numRows))
	s.mu.data.ServiceLat.Record(s.mu.data.Count, serviceLat.Seconds())
	s.mu.data.RetryLat.Record(s.mu.data.Count, retryLat.Seconds())
	s.mu.data.CommitLat.Record(s.mu.data.Count, commitLat.Seconds())
	if retryCount > s.mu.data.MaxRetries {
		s.mu.data.MaxRetries = retryCount
	}
	s.mu.data.RowsRead.Record(s.mu.data.Count, float64(rowsRead))
	s.mu.data.BytesRead.Record(s.mu.data.Count, float64(bytesRead))

	if collectedExecStats {
		s.mu.data.ExecStats.Count++
		s.mu.data.ExecStats.NetworkBytes.Record(s.mu.data.ExecStats.Count, float64(execStats.NetworkBytesSent))
		s.mu.data.ExecStats.MaxMemUsage.Record(s.mu.data.ExecStats.Count, float64(execStats.MaxMemUsage))
		s.mu.data.ExecStats.ContentionTime.Record(s.mu.data.ExecStats.Count, execStats.ContentionTime.Seconds())
		s.mu.data.ExecStats.NetworkMessages.Record(s.mu.data.ExecStats.Count, float64(execStats.NetworkMessages))
		s.mu.data.ExecStats.MaxDiskUsage.Record(s.mu.data.ExecStats.Count, float64(execStats.MaxDiskUsage))
	}

	return nil
}

// shouldSaveLogicalPlanDescription returns whether we should save the sample
// logical plan for a fingerprint (represented implicitly by the corresponding
// stmtStats object). stats is nil if it is the first time we see the
// fingerprint. We use `logicalPlanCollectionPeriod` to assess how frequently to
// sample logical plans.
func (a *appStats) shouldSaveLogicalPlanDescription(stats *stmtStats) bool {
	if !sampleLogicalPlans.Get(&a.st.SV) {
		return false
	}
	if stats == nil {
		// Save logical plan the first time we see new statement fingerprint.
		return true
	}
	now := timeutil.Now()
	period := logicalPlanCollectionPeriod.Get(&a.st.SV)
	stats.mu.Lock()
	defer stats.mu.Unlock()
	timeLastSampled := stats.mu.data.SensitiveInfo.MostRecentPlanTimestamp
	return now.Sub(timeLastSampled) >= period
}

// sqlStats carries per-application statistics for all applications.
type sqlStats struct {
	st *cluster.Settings

	// uniqueStmtFingerprintLimit is the limit on number of unique statement
	// fingerprints we can store in memory.
	uniqueStmtFingerprintLimit *settings.IntSetting

	// uniqueTxnFingerprintLimit is the limit on number of unique transaction
	// fingerprints we can store in memory
	uniqueTxnFingerprintLimit *settings.IntSetting

	mu struct {
		syncutil.Mutex

		mon *mon.BytesMonitor

		// lastReset is the time at which the app containers were reset.
		lastReset time.Time
		// apps is the container for all the per-application statistics objects.
		apps map[string]*appStats
	}

	atomic struct {
		// uniqueStmtFingerprintCount is the number of unique statement fingerprints
		// we are storing in memory.
		uniqueStmtFingerprintCount int64

		// uniqueTxnFingerprintCount is the number of unique transaction fingerprints
		// we are storing in memory.
		uniqueTxnFingerprintCount int64
	}
}

func newSQLStats(
	st *cluster.Settings,
	uniqueStmtFingerprintLimit *settings.IntSetting,
	uniqueTxnFingerprintLimit *settings.IntSetting,
	curMemBytesCount *metric.Gauge,
	maxMemBytesHist *metric.Histogram,
	parentMon *mon.BytesMonitor,
) *sqlStats {
	monitor := mon.NewMonitor(
		"sqlStats",
		mon.MemoryResource,
		curMemBytesCount,
		maxMemBytesHist,
		-1, /* increment */
		math.MaxInt64,
		st,
	)
	s := &sqlStats{
		st:                         st,
		uniqueStmtFingerprintLimit: uniqueStmtFingerprintLimit,
		uniqueTxnFingerprintLimit:  uniqueTxnFingerprintLimit,
	}
	s.mu.apps = make(map[string]*appStats)
	s.mu.mon = monitor
	s.mu.mon.Start(context.Background(), parentMon, mon.BoundAccount{})
	return s
}

func (s *sqlStats) getStatsForApplication(appName string) *appStats {
	s.mu.Lock()
	defer s.mu.Unlock()
	if a, ok := s.mu.apps[appName]; ok {
		return a
	}
	a := &appStats{
		st:       s.st,
		sqlStats: s,
		acc:      s.mu.mon.MakeBoundAccount(),
		stmts:    make(map[stmtKey]*stmtStats),
		txns:     make(map[txnKey]*txnStats),
	}
	s.mu.apps[appName] = a
	return a
}

// resetAndMaybeDumpStats clears all the stored per-app, per-statement and
// per-transaction statistics. If target s not nil, then the stats in s will be
// flushed into target.
func (s *sqlStats) resetAndMaybeDumpStats(ctx context.Context, target *sqlStats) (err error) {
	// Note: we do not clear the entire s.apps map here. We would need
	// to do so to prevent problems with a runaway client running `SET
	// APPLICATION_NAME=...` with a different name every time.  However,
	// any ongoing open client session at the time of the reset has
	// cached a pointer to its appStats struct and would thus continue
	// to report its stats in an object now invisible to the target tools
	// (virtual table, marshaling, etc.). It's a judgement call, but
	// for now we prefer to see more data and thus not clear the map, at
	// the risk of seeing the map grow unboundedly with the number of
	// different application_names seen so far.

	// appStatsCopy will hold a snapshot of the stats being cleared
	// to dump into target.
	var appStatsCopy map[string]*appStats

	s.mu.Lock()

	if target != nil {
		appStatsCopy = make(map[string]*appStats, len(s.mu.apps))
	}

	// Clear the per-apps maps manually,
	// because any SQL session currently open has cached the
	// pointer to its appStats object and will continue to
	// accumulate data using that until it closes (or changes its
	// application_name).
	for appName, a := range s.mu.apps {
		a.Lock()

		// Save the existing data to logs.
		// TODO(knz/dt): instead of dumping the stats to the log, save
		// them in a SQL table so they can be inspected by the DBA and/or
		// the UI.
		if dumpStmtStatsToLogBeforeReset.Get(&a.st.SV) {
			dumpStmtStats(ctx, appName, a.stmts)
		}

		// Only save a copy of a if we need to dump a copy of the stats.
		if target != nil {
			aCopy := &appStats{
				st:       a.st,
				sqlStats: s,
				stmts:    a.stmts,
				txns:     a.txns,
			}
			appStatsCopy[appName] = aCopy
		}

		atomic.AddInt64(&s.atomic.uniqueStmtFingerprintCount, int64(-len(a.stmts)))
		atomic.AddInt64(&s.atomic.uniqueTxnFingerprintCount, int64(-len(a.txns)))

		// Clear the map, to release the memory; make the new map somewhat already
		// large for the likely future workload.
		a.stmts = make(map[stmtKey]*stmtStats, len(a.stmts)/2)
		a.txns = make(map[txnKey]*txnStats, len(a.txns)/2)

		a.acc.Empty(ctx)
		a.Unlock()
	}
	s.mu.lastReset = timeutil.Now()
	s.mu.Unlock()

	// Dump the copied stats into target.
	if target != nil {
		for k, v := range appStatsCopy {
			stats := target.getStatsForApplication(k)
			// Add manages locks for itself, so we don't need to guard it with locks.
			latestErr := stats.Add(ctx, v)
			// If we run out of memory budget, appStats.Add() will merge stats in v
			// with all the existing stats. However it will discard rest of the stats
			// in v that requires memory allocation. We do not wish to short circuit
			// here because we want to still try our best to merge all the stats that
			// we can.
			if latestErr != nil {
				err = latestErr
			}
		}
	}

	return err
}

func (s *sqlStats) getLastReset() time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.mu.lastReset
}

// Save the existing data for an application to the info log.
func dumpStmtStats(ctx context.Context, appName string, stats map[stmtKey]*stmtStats) {
	if len(stats) == 0 {
		return
	}
	var buf bytes.Buffer
	for key, s := range stats {
		s.mu.Lock()
		json, err := json.Marshal(s.mu.data)
		s.mu.Unlock()
		if err != nil {
			log.Errorf(ctx, "error while marshaling stats for %q // %q: %v", appName, key.String(), err)
			continue
		}
		fmt.Fprintf(&buf, "%q: %s\n", key.String(), json)
	}
	log.Infof(ctx, "statistics for %q:\n%s", appName, buf.String())
}

func constructStatementIDFromStmtKey(key stmtKey) roachpb.StmtID {
	return roachpb.ConstructStatementID(
		key.anonymizedStmt, key.failed, key.implicitTxn, key.database,
	)
}

func scrubStmtStatKey(vt VirtualTabler, key string) (string, bool) {
	// Re-parse the statement to obtain its AST.
	stmt, err := parser.ParseOne(key)
	if err != nil {
		return "", false
	}

	// Re-format to remove most names.
	f := tree.NewFmtCtx(tree.FmtAnonymize)
	f.SetReformatTableNames(hideNonVirtualTableNameFunc(vt))
	f.FormatNode(stmt.AST)
	return f.CloseAndGetString(), true
}

func (s *sqlStats) getScrubbedStmtStats(
	vt *VirtualSchemaHolder,
) []roachpb.CollectedStatementStatistics {
	return s.getStmtStats(vt, true /* scrub */)
}

func (s *sqlStats) getUnscrubbedStmtStats(
	vt *VirtualSchemaHolder,
) []roachpb.CollectedStatementStatistics {
	return s.getStmtStats(vt, false /* scrub */)
}

func (s *sqlStats) getUnscrubbedTxnStats() []roachpb.CollectedTransactionStatistics {
	s.mu.Lock()
	defer s.mu.Unlock()
	var ret []roachpb.CollectedTransactionStatistics
	for appName, a := range s.mu.apps {
		a.Lock()
		// guesstimate that we'll need apps*(transactions-per-app)
		if cap(ret) == 0 {
			ret =
				make([]roachpb.CollectedTransactionStatistics, 0, len(a.txns)*len(s.mu.apps))
		}
		for _, stats := range a.txns {
			stats.mu.Lock()
			data := stats.mu.data
			stats.mu.Unlock()

			ret = append(ret, roachpb.CollectedTransactionStatistics{
				StatementIDs: stats.statementIDs,
				App:          appName,
				Stats:        data,
			})
		}
		a.Unlock()
	}
	return ret
}

func (s *sqlStats) getStmtStats(
	vt *VirtualSchemaHolder, scrub bool,
) []roachpb.CollectedStatementStatistics {
	s.mu.Lock()
	defer s.mu.Unlock()
	var ret []roachpb.CollectedStatementStatistics
	salt := ClusterSecret.Get(&s.st.SV)
	for appName, a := range s.mu.apps {
		a.Lock()
		if cap(ret) == 0 {
			// guesstimate that we'll need apps*(queries-per-app).
			ret =
				make([]roachpb.CollectedStatementStatistics, 0, len(a.stmts)*len(s.mu.apps))
		}
		for q, stats := range a.stmts {
			maybeScrubbed := q.anonymizedStmt
			maybeHashedAppName := appName
			ok := true
			if scrub {
				maybeScrubbed, ok = scrubStmtStatKey(vt, q.anonymizedStmt)
				if !strings.HasPrefix(appName, catconstants.ReportableAppNamePrefix) {
					maybeHashedAppName = HashForReporting(salt, appName)
				}
			}

			if ok {
				stats.mu.Lock()
				data := stats.mu.data
				distSQLUsed := stats.mu.distSQLUsed
				vectorized := stats.mu.vectorized
				fullScan := stats.mu.fullScan
				database := stats.mu.database
				stats.mu.Unlock()

				k := roachpb.StatementStatisticsKey{
					Query:       maybeScrubbed,
					DistSQL:     distSQLUsed,
					Opt:         true,
					Vec:         vectorized,
					ImplicitTxn: q.implicitTxn,
					FullScan:    fullScan,
					Failed:      q.failed,
					App:         maybeHashedAppName,
					Database:    database,
				}

				if scrub {
					// Quantize the counts to avoid leaking information that way.
					quantizeCounts(&data)
					data.SensitiveInfo = data.SensitiveInfo.GetScrubbedCopy()
				}

				ret = append(ret, roachpb.CollectedStatementStatistics{
					Key:   k,
					ID:    stats.ID,
					Stats: data,
				})
			}
		}
		a.Unlock()
	}
	return ret
}

// quantizeCounts ensures that the counts are bucketed into "simple" values.
func quantizeCounts(d *roachpb.StatementStatistics) {
	oldCount := d.Count
	newCount := telemetry.Bucket10(oldCount)
	d.Count = newCount
	// The SquaredDiffs values are meant to enable computing the variance
	// via the formula variance = squareddiffs / (count - 1).
	// Since we're adjusting the count, we must re-compute a value
	// for SquaredDiffs that keeps the same variance with the new count.
	oldCountMinusOne := float64(oldCount - 1)
	newCountMinusOne := float64(newCount - 1)
	d.NumRows.SquaredDiffs = (d.NumRows.SquaredDiffs / oldCountMinusOne) * newCountMinusOne
	d.ParseLat.SquaredDiffs = (d.ParseLat.SquaredDiffs / oldCountMinusOne) * newCountMinusOne
	d.PlanLat.SquaredDiffs = (d.PlanLat.SquaredDiffs / oldCountMinusOne) * newCountMinusOne
	d.RunLat.SquaredDiffs = (d.RunLat.SquaredDiffs / oldCountMinusOne) * newCountMinusOne
	d.ServiceLat.SquaredDiffs = (d.ServiceLat.SquaredDiffs / oldCountMinusOne) * newCountMinusOne
	d.OverheadLat.SquaredDiffs = (d.OverheadLat.SquaredDiffs / oldCountMinusOne) * newCountMinusOne

	d.MaxRetries = telemetry.Bucket10(d.MaxRetries)

	d.FirstAttemptCount = int64((float64(d.FirstAttemptCount) / float64(oldCount)) * float64(newCount))
}

// FailedHashedValue is used as a default return value for when HashForReporting
// cannot hash a value correctly.
const FailedHashedValue = "unknown"

// HashForReporting 1-way hashes values for use in stat reporting. The secret
// should be the cluster.secret setting.
func HashForReporting(secret, appName string) string {
	// If no secret is provided, we cannot irreversibly hash the value, so return
	// a default value.
	if len(secret) == 0 {
		return FailedHashedValue
	}
	hash := hmac.New(sha256.New, []byte(secret))
	if _, err := hash.Write([]byte(appName)); err != nil {
		panic(errors.NewAssertionErrorWithWrappedErrf(err,
			`"It never returns an error." -- https://golang.org/pkg/hash`))
	}
	return hex.EncodeToString(hash.Sum(nil)[:4])
}
