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
	"encoding/json"
	"fmt"
	"hash/fnv"
	"strconv"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/pkg/errors"
)

type stmtKey struct {
	stmt        string
	failed      bool
	distSQLUsed bool
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

func (a *appStats) recordStatement(
	stmt Statement,
	distSQLUsed bool,
	automaticRetryCount int,
	numRows int,
	err error,
	parseLat, planLat, runLat, svcLat, ovhLat float64,
) {
	if a == nil || !a.st.StmtStatsEnable.Get() {
		return
	}

	if t := a.st.SQLStatsCollectionLatencyThreshold.Get(); t > 0 && t.Seconds() >= svcLat {
		return
	}

	// Some statements like SET, SHOW etc are not useful to collect
	// stats about. Ignore them.
	if _, ok := stmt.AST.(parser.HiddenFromStats); ok {
		return
	}

	// Extend the statement key with a character that indicated whether
	// there was an error and/or whether the query was distributed, so
	// that we use separate buckets for the different situations.
	var buf bytes.Buffer

	parser.FormatNode(&buf, parser.FmtHideConstants, stmt.AST)

	// Get the statistics object.
	s := a.getStatsForStmt(stmtKey{stmt: buf.String(), failed: err != nil, distSQLUsed: distSQLUsed})

	// Collect the per-statement statistics.
	s.Lock()
	s.data.Count++
	if err != nil {
		s.data.LastErr = err.Error()
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
func (a *appStats) getStatsForStmt(key stmtKey) *stmtStats {
	a.Lock()
	// Retrieve the per-statement statistic object, and create it if it
	// doesn't exist yet.
	s, ok := a.stmts[key]
	if !ok {
		s = &stmtStats{}
		a.stmts[key] = s
	}
	a.Unlock()
	return s
}

// sqlStats carries per-application statistics for all applications on
// each node. It hangs off Executor.
type sqlStats struct {
	st *cluster.Settings
	syncutil.Mutex

	// apps is the container for all the per-application statistics
	// objects.
	apps map[string]*appStats
}

// resetApplicationName initializes both Session.mu.ApplicationName and
// the cached pointer to per-application statistics. It is meant to be
// used upon session initialization and upon SET APPLICATION_NAME.
func (s *Session) resetApplicationName(appName string) {
	s.mu.Lock()
	s.mu.ApplicationName = appName
	s.mu.Unlock()
	if s.sqlStats != nil {
		s.appStats = s.sqlStats.getStatsForApplication(appName)
	}
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
	// (virtual table, marshalling, etc.). It's a judgement call, but
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
		if a.st.DumpStmtStatsToLogBeforeReset.Get() {
			dumpStmtStats(ctx, appName, a.stmts)
		}

		// Clear the map, to release the memory; make the new map somewhat
		// already large for the likely future workload.
		a.stmts = make(map[stmtKey]*stmtStats, len(a.stmts)/2)
		a.Unlock()
	}
	s.Unlock()
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

func scrubStmtStatKey(vt virtualSchemaHolder, key string) (string, bool) {
	// Re-parse the statement to obtain its AST.
	stmt, err := parser.ParseOne(key)
	if err != nil {
		return "", false
	}

	// Re-format to remove most names.
	formatter := parser.FmtReformatTableNames(parser.FmtAnonymize,
		func(t *parser.NormalizableTableName, buf *bytes.Buffer, f parser.FmtFlags) {
			tn, err := t.Normalize()
			if err != nil {
				buf.WriteByte('_')
				return
			}
			virtual, err := vt.getVirtualTableEntry(tn)
			if err != nil || virtual.desc == nil {
				buf.WriteByte('_')
				return
			}
			// Virtual table: we want to keep the name.
			tn.Format(buf, parser.FmtParsable)
		})
	return parser.AsStringWithFlags(stmt, formatter), true
}

// GetScrubbedStmtStats returns the statement statistics by app, with the
// queries scrubbed of their identifiers. Any statements which cannot be
// scrubbed will be omitted from the returned map.
func (e *Executor) GetScrubbedStmtStats() []roachpb.CollectedStatementStatistics {
	var ret []roachpb.CollectedStatementStatistics
	vt := e.virtualSchemas
	e.sqlStats.Lock()
	for appName, a := range e.sqlStats.apps {
		if cap(ret) == 0 {
			// guesitmate that we'll need apps*(queries-per-app).
			ret = make([]roachpb.CollectedStatementStatistics, 0, len(a.stmts)*len(e.sqlStats.apps))
		}
		hashedApp := HashAppName(appName)
		a.Lock()
		for q, stats := range a.stmts {
			scrubbed, ok := scrubStmtStatKey(vt, q.stmt)
			if ok {
				k := roachpb.StatementStatisticsKey{
					Query:   scrubbed,
					DistSQL: q.distSQLUsed,
					Failed:  q.failed,
					App:     hashedApp,
				}
				stats.Lock()
				ret = append(ret, roachpb.CollectedStatementStatistics{Key: k, Stats: stats.data})
				stats.Unlock()
			}
		}
		a.Unlock()
	}
	e.sqlStats.Unlock()
	return ret
}

// HashAppName 1-way hashes an application names for use in stat reporting.
func HashAppName(appName string) string {
	hash := fnv.New64a()

	if _, err := hash.Write([]byte(appName)); err != nil {
		panic(errors.Wrap(err, `"It never returns an error." -- https://golang.org/pkg/hash`))
	}
	return strconv.Itoa(int(hash.Sum64()))
}

// ResetStatementStats resets the executor's collected statement statistics.
func (e *Executor) ResetStatementStats(ctx context.Context) {
	e.sqlStats.resetStats(ctx)
}

// FillUnimplementedErrorCounts fills the passed map with the executor's current
// counts of how often individual unimplemented features have been encountered.
func (e *Executor) FillUnimplementedErrorCounts(fill map[string]int64) {
	e.unimplementedErrors.Lock()
	for k, v := range e.unimplementedErrors.counts {
		fill[k] = v
	}
	e.unimplementedErrors.Unlock()
}

func (e *Executor) recordUnimplementedFeature(feature string) {
	if feature == "" {
		return
	}
	e.unimplementedErrors.Lock()
	if e.unimplementedErrors.counts == nil {
		e.unimplementedErrors.counts = make(map[string]int64)
	}
	e.unimplementedErrors.counts[feature]++
	e.unimplementedErrors.Unlock()
}

// ResetUnimplementedCounts resets counting of unimplemented errors.
func (e *Executor) ResetUnimplementedCounts() {
	e.unimplementedErrors.Lock()
	e.unimplementedErrors.counts = make(map[string]int64, len(e.unimplementedErrors.counts))
	e.unimplementedErrors.Unlock()
}
