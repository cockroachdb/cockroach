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
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

type stmtKey struct {
	stmt        string
	failed      bool
	distSQLUsed bool
	vectorized  bool
	implicitTxn bool
}

// appStats holds per-application statistics.
type appStats struct {
	syncutil.Mutex

	st    *cluster.Settings
	stmts map[stmtKey]*stmtStats
	txns  transactionStats
}

// stmtStats holds per-statement statistics.
type stmtStats struct {
	syncutil.Mutex

	data roachpb.StatementStatistics
}

// transactionStats holds per-application transaction statistics.
type transactionStats struct {
	mu struct {
		syncutil.Mutex
		roachpb.TxnStats
	}
}

// stmtStatsEnable determines whether to collect per-statement
// statistics.
var stmtStatsEnable = settings.RegisterPublicBoolSetting(
	"sql.metrics.statement_details.enabled", "collect per-statement query statistics", true,
)

// txnStatsEnable determines whether to collect per-application transaction
// statistics.
var txnStatsEnable = settings.RegisterPublicBoolSetting(
	"sql.metrics.transaction_details.enabled", "collect per-application transaction statistics", true,
)

// sqlStatsCollectionLatencyThreshold specifies the minimum amount of time
// consumed by a SQL statement before it is collected for statistics reporting.
var sqlStatsCollectionLatencyThreshold = settings.RegisterPublicDurationSetting(
	"sql.metrics.statement_details.threshold",
	"minimum execution time to cause statistics to be collected",
	0,
)

var dumpStmtStatsToLogBeforeReset = settings.RegisterPublicBoolSetting(
	"sql.metrics.statement_details.dump_to_logs",
	"dump collected statement statistics to node logs when periodically cleared",
	false,
)

var sampleLogicalPlans = settings.RegisterPublicBoolSetting(
	"sql.metrics.statement_details.plan_collection.enabled",
	"periodically save a logical plan for each fingerprint",
	true,
)

var logicalPlanCollectionPeriod = settings.RegisterPublicNonNegativeDurationSetting(
	"sql.metrics.statement_details.plan_collection.period",
	"the time until a new logical plan is collected",
	5*time.Minute,
)

func (s stmtKey) String() string {
	return s.flags() + s.stmt
}

func (s stmtKey) flags() string {
	var b bytes.Buffer
	if s.failed {
		b.WriteByte('!')
	}
	if s.distSQLUsed {
		b.WriteByte('+')
	}
	return b.String()
}

// recordStatement saves per-statement statistics.
//
// samplePlanDescription can be nil, as these are only sampled periodically per unique fingerprint.
func (a *appStats) recordStatement(
	stmt *Statement,
	samplePlanDescription *roachpb.ExplainTreePlanNode,
	distSQLUsed bool,
	vectorized bool,
	implicitTxn bool,
	automaticRetryCount int,
	numRows int,
	err error,
	parseLat, planLat, runLat, svcLat, ovhLat float64,
	bytesRead, rowsRead int64,
) {
	if !stmtStatsEnable.Get(&a.st.SV) {
		return
	}

	if t := sqlStatsCollectionLatencyThreshold.Get(&a.st.SV); t > 0 && t.Seconds() >= svcLat {
		return
	}

	// Get the statistics object.
	s := a.getStatsForStmt(
		stmt, distSQLUsed, vectorized, implicitTxn,
		err, true, /* createIfNonexistent */
	)

	// Collect the per-statement statistics.
	s.Lock()
	s.data.Count++
	if err != nil {
		s.data.SensitiveInfo.LastErr = err.Error()
	}
	// Only update MostRecentPlanDescription if we sampled a new PlanDescription.
	if samplePlanDescription != nil {
		s.data.SensitiveInfo.MostRecentPlanDescription = *samplePlanDescription
		s.data.SensitiveInfo.MostRecentPlanTimestamp = timeutil.Now()
	}
	if automaticRetryCount == 0 {
		s.data.FirstAttemptCount++
	} else if int64(automaticRetryCount) > s.data.MaxRetries {
		s.data.MaxRetries = int64(automaticRetryCount)
	}
	s.data.NumRows.Record(s.data.Count, float64(numRows))
	s.data.ParseLat.Record(s.data.Count, parseLat)
	s.data.PlanLat.Record(s.data.Count, planLat)
	s.data.RunLat.Record(s.data.Count, runLat)
	s.data.ServiceLat.Record(s.data.Count, svcLat)
	s.data.OverheadLat.Record(s.data.Count, ovhLat)
	s.data.BytesRead = bytesRead
	s.data.RowsRead = rowsRead
	s.Unlock()
}

// getStatsForStmt retrieves the per-stmt stat object.
func (a *appStats) getStatsForStmt(
	stmt *Statement,
	distSQLUsed bool,
	vectorized bool,
	implicitTxn bool,
	err error,
	createIfNonexistent bool,
) *stmtStats {
	// Extend the statement key with various characteristics, so
	// that we use separate buckets for the different situations.
	key := stmtKey{
		failed:      err != nil,
		distSQLUsed: distSQLUsed,
		vectorized:  vectorized,
		implicitTxn: implicitTxn,
	}
	if stmt.AnonymizedStr != "" {
		// Use the cached anonymized string.
		key.stmt = stmt.AnonymizedStr
	} else {
		key.stmt = anonymizeStmt(stmt.AST)
	}

	return a.getStatsForStmtWithKey(key, createIfNonexistent)
}

func (a *appStats) getStatsForStmtWithKey(key stmtKey, createIfNonexistent bool) *stmtStats {
	a.Lock()
	// Retrieve the per-statement statistic object, and create it if it
	// doesn't exist yet.
	s, ok := a.stmts[key]
	if !ok && createIfNonexistent {
		s = &stmtStats{}
		a.stmts[key] = s
	}
	a.Unlock()
	return s
}

// Add combines one appStats into another. Add manages locks on a, so taking
// a lock on a will cause a deadlock.
func (a *appStats) Add(other *appStats) {
	other.Lock()
	statMap := make(map[stmtKey]*stmtStats)
	for k, v := range other.stmts {
		statMap[k] = v
	}
	other.Unlock()

	// Copy the statement stats for each statement key.
	for k, v := range statMap {
		v.Lock()
		statCopy := &stmtStats{data: v.data}
		v.Unlock()
		statMap[k] = statCopy
	}

	// Merge the statement stats.
	for k, v := range statMap {
		s := a.getStatsForStmtWithKey(k, true)
		s.Lock()
		// Note that we don't need to take a lock on v because
		// no other thread knows about v yet.
		s.data.Add(&v.data)
		s.Unlock()
	}

	// Create a copy of the other's transactions statistics.
	other.txns.mu.Lock()
	txnStats := other.txns.mu.TxnStats
	other.txns.mu.Unlock()

	// Merge the transaction stats.
	a.txns.mu.Lock()
	a.txns.mu.TxnStats.Add(txnStats)
	a.txns.mu.Unlock()
}

func anonymizeStmt(ast tree.Statement) string {
	return tree.AsStringWithFlags(ast, tree.FmtHideConstants)
}

func (s *transactionStats) getStats() (
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

func (s *transactionStats) recordTransaction(txnTimeSec float64, ev txnEvent, implicit bool) {
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

func (a *appStats) recordTransaction(txnTimeSec float64, ev txnEvent, implicit bool) {
	if !txnStatsEnable.Get(&a.st.SV) {
		return
	}
	a.txns.recordTransaction(txnTimeSec, ev, implicit)
}

// shouldSaveLogicalPlanDescription returns whether we should save this as a
// sample logical plan for its corresponding fingerprint. We use
// `logicalPlanCollectionPeriod` to assess how frequently to sample logical
// plans.
func (a *appStats) shouldSaveLogicalPlanDescription(
	stmt *Statement, useDistSQL bool, vectorized bool, implicitTxn bool, err error,
) bool {
	if !sampleLogicalPlans.Get(&a.st.SV) {
		return false
	}
	stats := a.getStatsForStmt(
		stmt, useDistSQL, vectorized, implicitTxn,
		err, false, /* createIfNonexistent */
	)
	if stats == nil {
		// Save logical plan the first time we see new statement fingerprint.
		return true
	}
	now := timeutil.Now()
	period := logicalPlanCollectionPeriod.Get(&a.st.SV)
	stats.Lock()
	defer stats.Unlock()
	timeLastSampled := stats.data.SensitiveInfo.MostRecentPlanTimestamp
	return now.Sub(timeLastSampled) >= period
}

// sqlStats carries per-application statistics for all applications.
type sqlStats struct {
	syncutil.Mutex

	st *cluster.Settings
	// lastReset is the time at which the app containers were reset.
	lastReset time.Time
	// apps is the container for all the per-application statistics objects.
	apps map[string]*appStats
}

func (s *sqlStats) getStatsForApplication(appName string) *appStats {
	s.Lock()
	defer s.Unlock()
	if a, ok := s.apps[appName]; ok {
		return a
	}
	a := &appStats{
		st:    s.st,
		stmts: make(map[stmtKey]*stmtStats),
	}
	s.apps[appName] = a
	return a
}

// resetAndMaybeDumpStats clears all the stored per-app and per-statement
// statistics. If target s not nil, then the stats in s will be flushed
// into target.
func (s *sqlStats) resetAndMaybeDumpStats(ctx context.Context, target *sqlStats) {
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

	s.Lock()

	if target != nil {
		appStatsCopy = make(map[string]*appStats, len(s.apps))
	}

	// Clear the per-apps maps manually,
	// because any SQL session currently open has cached the
	// pointer to its appStats object and will continue to
	// accumulate data using that until it closes (or changes its
	// application_name).
	for appName, a := range s.apps {
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
			aCopy := &appStats{st: a.st, stmts: a.stmts}
			appStatsCopy[appName] = aCopy
		}

		// Clear the map, to release the memory; make the new map somewhat already
		// large for the likely future workload.
		a.stmts = make(map[stmtKey]*stmtStats, len(a.stmts)/2)
		a.Unlock()
	}
	s.lastReset = timeutil.Now()
	s.Unlock()

	// Dump the copied stats into target.
	if target != nil {
		for k, v := range appStatsCopy {
			stats := target.getStatsForApplication(k)
			// Add manages locks for itself, so we don't need to guard it with locks.
			stats.Add(v)
		}
	}
}

func (s *sqlStats) getLastReset() time.Time {
	s.Lock()
	defer s.Unlock()
	return s.lastReset
}

// Save the existing data for an application to the info log.
func dumpStmtStats(ctx context.Context, appName string, stats map[stmtKey]*stmtStats) {
	if len(stats) == 0 {
		return
	}
	var buf bytes.Buffer
	for key, s := range stats {
		s.Lock()
		json, err := json.Marshal(s.data)
		s.Unlock()
		if err != nil {
			log.Errorf(ctx, "error while marshaling stats for %q // %q: %v", appName, key.String(), err)
			continue
		}
		fmt.Fprintf(&buf, "%q: %s\n", key.String(), json)
	}
	log.Infof(ctx, "Statistics for %q:\n%s", appName, buf.String())
}

func scrubStmtStatKey(vt VirtualTabler, key string) (string, bool) {
	// Re-parse the statement to obtain its AST.
	stmt, err := parser.ParseOne(key)
	if err != nil {
		return "", false
	}

	// Re-format to remove most names.
	f := tree.NewFmtCtx(tree.FmtAnonymize)

	reformatFn := func(ctx *tree.FmtCtx, tn *tree.TableName) {
		virtual, err := vt.getVirtualTableEntry(tn)
		if err != nil || virtual.desc == nil {
			ctx.WriteByte('_')
			return
		}
		// Virtual table: we want to keep the name; however
		// we need to scrub the database name prefix.
		newTn := *tn
		newTn.CatalogName = "_"

		ctx.WithFlags(tree.FmtParsable, func() {
			ctx.WithReformatTableNames(nil, func() {
				ctx.FormatNode(&newTn)
			})
		})
	}
	f.SetReformatTableNames(reformatFn)
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

func (s *sqlStats) getStmtStats(
	vt *VirtualSchemaHolder, scrub bool,
) []roachpb.CollectedStatementStatistics {
	s.Lock()
	defer s.Unlock()
	var ret []roachpb.CollectedStatementStatistics
	salt := ClusterSecret.Get(&s.st.SV)
	for appName, a := range s.apps {
		a.Lock()
		if cap(ret) == 0 {
			// guesstimate that we'll need apps*(queries-per-app).
			ret = make([]roachpb.CollectedStatementStatistics, 0, len(a.stmts)*len(s.apps))
		}
		for q, stats := range a.stmts {
			maybeScrubbed := q.stmt
			maybeHashedAppName := appName
			ok := true
			if scrub {
				maybeScrubbed, ok = scrubStmtStatKey(vt, q.stmt)
				if !strings.HasPrefix(appName, sqlbase.ReportableAppNamePrefix) {
					maybeHashedAppName = HashForReporting(salt, appName)
				}
			}
			if ok {
				k := roachpb.StatementStatisticsKey{
					Query:       maybeScrubbed,
					DistSQL:     q.distSQLUsed,
					Opt:         true,
					Vec:         q.vectorized,
					ImplicitTxn: q.implicitTxn,
					Failed:      q.failed,
					App:         maybeHashedAppName,
				}
				stats.Lock()
				data := stats.data
				stats.Unlock()

				if scrub {
					// Quantize the counts to avoid leaking information that way.
					quantizeCounts(&data)
					data.SensitiveInfo = data.SensitiveInfo.GetScrubbedCopy()
				}

				ret = append(ret, roachpb.CollectedStatementStatistics{Key: k, Stats: data})
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
