// Copyright 2017 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

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
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/pkg/errors"
)

type stmtKey struct {
	stmt        string
	failed      bool
	distSQLUsed bool
	optUsed     bool
}

// appStats holds per-application statistics.
type appStats struct {
	st *cluster.Settings

	syncutil.Mutex
	stmts map[stmtKey]*stmtStats
}

// stmtStats holds per-statement statistics.
type stmtStats struct {
	syncutil.Mutex

	data roachpb.StatementStatistics
}

// stmtStatsEnable determines whether to collect per-statement
// statistics.
var stmtStatsEnable = settings.RegisterBoolSetting(
	"sql.metrics.statement_details.enabled", "collect per-statement query statistics", true,
)

// sqlStatsCollectionLatencyThreshold specifies the minimum amount of time
// consumed by a SQL statement before it is collected for statistics reporting.
var sqlStatsCollectionLatencyThreshold = settings.RegisterDurationSetting(
	"sql.metrics.statement_details.threshold",
	"minimum execution time to cause statistics to be collected",
	0,
)

var dumpStmtStatsToLogBeforeReset = settings.RegisterBoolSetting(
	"sql.metrics.statement_details.dump_to_logs",
	"dump collected statement statistics to node logs when periodically cleared",
	false,
)

var sampleLogicalPlans = settings.RegisterBoolSetting(
	"sql.metrics.statement_details.sample_logical_plans",
	"periodically save a logical plan for each fingerprint",
	true,
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
	if !s.optUsed {
		b.WriteByte('-')
	}
	return b.String()
}

// saveFingerprintPlanOnceEvery is the number of queries for a given fingerprint that go by before
// we save the plan again.
const saveFingerprintPlanOnceEvery = 1000

// recordStatement saves per-statement statistics.
//
// samplePlanDescription can be nil, as these are only sampled periodically per unique fingerprint.
func (a *appStats) recordStatement(
	stmt Statement,
	samplePlanDescription *roachpb.ExplainTreePlanNode,
	distSQLUsed bool,
	optUsed bool,
	automaticRetryCount int,
	numRows int,
	err error,
	parseLat, planLat, runLat, svcLat, ovhLat float64,
) {
	if a == nil || !stmtStatsEnable.Get(&a.st.SV) {
		return
	}

	if t := sqlStatsCollectionLatencyThreshold.Get(&a.st.SV); t > 0 && t.Seconds() >= svcLat {
		return
	}

	// Get the statistics object.
	s := a.getStatsForStmt(stmt, distSQLUsed, optUsed, err, true /* createIfNonexistent */)

	// Collect the per-statement statistics.
	s.Lock()
	s.data.Count++
	if err != nil {
		s.data.SensitiveInfo.LastErr = err.Error()
	}
	// Only update MostRecentPlanDescription if we sampled a new PlanDescription.
	if samplePlanDescription != nil {
		s.data.SensitiveInfo.MostRecentPlanDescription = *samplePlanDescription
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
	s.Unlock()
}

// getStatsForStmt retrieves the per-stmt stat object.
func (a *appStats) getStatsForStmt(
	stmt Statement, distSQLUsed bool, optimizerUsed bool, err error, createIfNonexistent bool,
) *stmtStats {
	// Extend the statement key with various characteristics, so
	// that we use separate buckets for the different situations.
	key := stmtKey{failed: err != nil, distSQLUsed: distSQLUsed, optUsed: optimizerUsed}
	if stmt.AnonymizedStr != "" {
		// Use the cached anonymized string.
		key.stmt = stmt.AnonymizedStr
	} else {
		key.stmt = anonymizeStmt(stmt)
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

func anonymizeStmt(stmt Statement) string {
	return tree.AsStringWithFlags(stmt.AST, tree.FmtHideConstants)
}

// sqlStats carries per-application statistics for all applications on
// each node.
type sqlStats struct {
	st *cluster.Settings
	syncutil.Mutex

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
	a := &appStats{st: s.st, stmts: make(map[stmtKey]*stmtStats)}
	s.apps[appName] = a
	return a
}

// resetStats clears all the stored per-app and per-statement
// statistics.
func (s *sqlStats) resetStats(ctx context.Context) {
	// Note: we do not clear the entire s.apps map here. We would need
	// to do so to prevent problems with a runaway client running `SET
	// APPLICATION_NAME=...` with a different name every time.  However,
	// any ongoing open client session at the time of the reset has
	// cached a pointer to its appStats struct and would thus continue
	// to report its stats in an object now invisible to the other tools
	// (virtual table, marshaling, etc.). It's a judgement call, but
	// for now we prefer to see more data and thus not clear the map, at
	// the risk of seeing the map grow unboundedly with the number of
	// different application_names seen so far.

	s.Lock()
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

		// Clear the map, to release the memory; make the new map somewhat
		// already large for the likely future workload.
		a.stmts = make(map[stmtKey]*stmtStats, len(a.stmts)/2)
		a.Unlock()
	}
	s.lastReset = timeutil.Now()
	s.Unlock()
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
	fmt.Fprintf(&buf, "Statistics for %q:\n", appName)
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
	log.Info(ctx, buf.String())
}

func scrubStmtStatKey(vt VirtualTabler, key string) (string, bool) {
	// Re-parse the statement to obtain its AST.
	stmt, err := parser.ParseOne(key)
	if err != nil {
		return "", false
	}

	// Re-format to remove most names.
	f := tree.NewFmtCtxWithBuf(tree.FmtAnonymize)
	f.WithReformatTableNames(
		func(ctx *tree.FmtCtx, tn *tree.TableName) {
			virtual, err := vt.getVirtualTableEntry(tn)
			if err != nil || virtual.desc == nil {
				ctx.WriteByte('_')
				return
			}
			// Virtual table: we want to keep the name; however
			// we need to scrub the database name prefix.
			newTn := *tn
			newTn.CatalogName = "_"
			keepNameCtx := ctx.CopyWithFlags(tree.FmtParsable)
			keepNameCtx.WithReformatTableNames(nil)
			keepNameCtx.FormatNode(&newTn)
		})
	f.FormatNode(stmt)
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

// InternalAppNamePrefix indicates that the application name is internal to
// CockroachDB and therefore can be reported without scrubbing. (Note this only
// applies to the application name itself. Query data is still scrubbed as
// usual.)
const InternalAppNamePrefix = "$ "

// DelegatedAppNamePrefix is added to a regular client application name
// for SQL queries that are ran internally on behalf of other SQL queries
// inside that application. The application name should be scrubbed in reporting.
const DelegatedAppNamePrefix = "$$ "

func (s *sqlStats) getStmtStats(
	vt *VirtualSchemaHolder, scrub bool,
) []roachpb.CollectedStatementStatistics {
	s.Lock()
	defer s.Unlock()
	var ret []roachpb.CollectedStatementStatistics
	salt := ClusterSecret.Get(&s.st.SV)
	for appName, a := range s.apps {
		if cap(ret) == 0 {
			// guesstimate that we'll need apps*(queries-per-app).
			ret = make([]roachpb.CollectedStatementStatistics, 0, len(a.stmts)*len(s.apps))
		}
		a.Lock()
		for q, stats := range a.stmts {
			maybeScrubbed := q.stmt
			maybeHashedAppName := appName
			ok := true
			if scrub {
				maybeScrubbed, ok = scrubStmtStatKey(vt, q.stmt)
				if !strings.HasPrefix(appName, InternalAppNamePrefix) {
					maybeHashedAppName = HashForReporting(salt, appName)
				}
			}
			if ok {
				k := roachpb.StatementStatisticsKey{
					Query:   maybeScrubbed,
					DistSQL: q.distSQLUsed,
					Opt:     q.optUsed,
					Failed:  q.failed,
					App:     maybeHashedAppName,
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
		panic(errors.Wrap(err, `"It never returns an error." -- https://golang.org/pkg/hash`))
	}
	return hex.EncodeToString(hash.Sum(nil)[:4])
}
