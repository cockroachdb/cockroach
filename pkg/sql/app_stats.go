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
//

package sql

import (
	"bytes"
	"encoding/json"
	"fmt"
	"time"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// appStats holds per-application statistics.
type appStats struct {
	syncutil.Mutex

	stmts map[string]*stmtStats
}

// stmtStats holds per-statement statistics.
type stmtStats struct {
	syncutil.Mutex

	data StatementStatistics
}

// StmtStatsEnable determines whether to collect per-statement
// statistics.
var StmtStatsEnable = envutil.EnvOrDefaultBool(
	"COCKROACH_SQL_STMT_STATS_ENABLE", true,
)

// SQLStatsCollectionLatencyThreshold specifies the minimum amount of time
// consumed by a SQL statement before it is collected for statistics reporting.
var SQLStatsCollectionLatencyThreshold = envutil.EnvOrDefaultFloat(
	"COCKROACH_SQL_STATS_SVCLAT_THRESHOLD", 0,
)

func (a *appStats) recordStatement(
	stmt parser.Statement,
	distSQLUsed bool,
	automaticRetryCount int,
	numRows int,
	err error,
	parseLat, planLat, runLat, svcLat, ovhLat float64,
) {
	if a == nil || !StmtStatsEnable {
		return
	}

	if svcLat < SQLStatsCollectionLatencyThreshold {
		return
	}

	// Some statements like SET, SHOW etc are not useful to collect
	// stats about. Ignore them.
	if _, ok := stmt.(parser.HiddenFromStats); ok {
		return
	}

	// Extend the statement key with a character that indicated whether
	// there was an error and/or whether the query was distributed, so
	// that we use separate buckets for the different situations.
	var buf bytes.Buffer
	if err != nil {
		buf.WriteByte('!')
	}
	if distSQLUsed {
		buf.WriteByte('+')
	}
	parser.FormatNode(&buf, parser.FmtHideConstants, stmt)
	stmtKey := buf.String()

	// Get the statistics object.
	s := a.getStatsForStmt(stmtKey)

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
	s.data.NumRows.record(s.data.Count, float64(numRows))
	s.data.ParseLat.record(s.data.Count, parseLat)
	s.data.PlanLat.record(s.data.Count, planLat)
	s.data.RunLat.record(s.data.Count, runLat)
	s.data.ServiceLat.record(s.data.Count, svcLat)
	s.data.OverheadLat.record(s.data.Count, ovhLat)
	s.Unlock()
}

// Retrieve the variance of the values.
func (l *NumericStat) getVariance(count int64) float64 {
	return l.SquaredDiffs / (float64(count) - 1)
}

func (l *NumericStat) record(count int64, val float64) {
	delta := val - l.Mean
	l.Mean += delta / float64(count)
	l.SquaredDiffs += delta * (val - l.Mean)
}

// getStatsForStmt retrieves the per-stmt stat object.
func (a *appStats) getStatsForStmt(stmtKey string) *stmtStats {
	a.Lock()
	// Retrieve the per-statement statistic object, and create it if it
	// doesn't exist yet.
	s, ok := a.stmts[stmtKey]
	if !ok {
		s = &stmtStats{}
		a.stmts[stmtKey] = s
	}
	a.Unlock()
	return s
}

// sqlStats carries per-application statistics for all applications on
// each node. It hangs off Executor.
type sqlStats struct {
	syncutil.Mutex

	// apps is the container for all the per-application statistics
	// objects.
	apps map[string]*appStats
}

// resetApplicationName initializes both Session.ApplicationName and
// the cached pointer to per-application statistics. It is meant to be
// used upon session initialization and upon SET APPLICATION_NAME.
func (s *Session) resetApplicationName(appName string) {
	s.ApplicationName = appName
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
	a := &appStats{stmts: make(map[string]*stmtStats)}
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
		dumpStmtStats(ctx, appName, a.stmts)

		// Clear the map, to release the memory; make the new map somewhat
		// already large for the likely future workload.
		a.stmts = make(map[string]*stmtStats, len(a.stmts)/2)
		a.Unlock()
	}
	s.Unlock()
}

// Save the existing data for an application to the info log.
func dumpStmtStats(ctx context.Context, appName string, stats map[string]*stmtStats) {
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
			log.Errorf(ctx, "error while marshaling stats for %q // %q: %v", appName, key, err)
			continue
		}
		fmt.Fprintf(&buf, "%q: %s\n", key, json)
	}
	log.Info(ctx, buf.String())
}

// StmtStatsResetFrequency is the frequency at which per-app and
// per-statement statistics are cleared from memory, to avoid
// unlimited memory growth.
var StmtStatsResetFrequency = envutil.EnvOrDefaultDuration(
	"COCKROACH_SQL_STMT_STATS_RESET_INTERVAL", 1*time.Hour,
)

// startResetWorker ensures that the data is removed from memory
// periodically, so as to avoid memory blow-ups.
func (s *sqlStats) startResetWorker(stopper *stop.Stopper) {
	ctx := log.WithLogTag(context.Background(), "sql-stats", nil)
	stopper.RunWorker(ctx, func(ctx context.Context) {
		ticker := time.NewTicker(StmtStatsResetFrequency)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				s.resetStats(ctx)
			case <-stopper.ShouldStop():
				return
			}
		}
	})
}
