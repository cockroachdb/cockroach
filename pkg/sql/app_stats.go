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
	"time"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
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

	// count is the total number of times this statement was executed
	// since the begin of the reporting period.
	count int

	// firstAttemptCount collects the total number of times a first
	// attempt was executed (either the one time in explicitly committed
	// statements, or the first time in implicitly committed statements
	// with implicit retries).
	// The proportion of statements that could be executed without retry
	// can be computed as firstAttemptCount / count.
	// The cumulative number of retries can be computed with
	// count - firstAttemptCount.
	firstAttemptCount int

	// maxRetries collects the maximum observed number of automatic
	// retries in the reporting period.
	maxRetries int

	// lastErr collects the last error encountered.
	lastErr error

	// numRows collects the number of rows returned or observed.
	numRows numericStat

	// phase latencies.
	parseLat, planLat, runLat, execLat, ovhLat numericStat
}

// StmtStatsEnable determines whether to collect per-statement
// statistics.
var StmtStatsEnable = envutil.EnvOrDefaultBool(
	"COCKROACH_SQL_STMT_STATS_ENABLE", false,
)

func (a *appStats) recordStatement(
	stmt parser.Statement,
	distSQLUsed bool,
	automaticRetryCount int,
	numRows int,
	err error,
	parseLat, planLat, runLat, execLat, ovhLat float64,
) {
	if a == nil || !StmtStatsEnable {
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
	parser.FormatNode(&buf, parser.FmtSimple, stmt)
	stmtKey := buf.String()

	// Get the statistics object.
	s := a.getStatsForStmt(stmtKey)

	// Collect the per-statement statistics.
	s.Lock()
	s.count++
	if err != nil {
		s.lastErr = err
	}
	if automaticRetryCount == 0 {
		s.firstAttemptCount++
	} else if automaticRetryCount > s.maxRetries {
		s.maxRetries = automaticRetryCount
	}
	s.numRows.record(s.count, float64(numRows))
	s.parseLat.record(s.count, parseLat)
	s.planLat.record(s.count, planLat)
	s.runLat.record(s.count, runLat)
	s.execLat.record(s.count, execLat)
	s.ovhLat.record(s.count, ovhLat)
	s.Unlock()
}

// numericStat collect the running values until the mean and variance
// are required.
type numericStat struct {
	// running sum, to compute the average.
	sum float64
	// partial mean and square of differences, to compute the variance.
	// See https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Online_algorithm.
	mean    float64
	sqrDiff float64
}

// Retrieve the mean value.
func (l *numericStat) getMean(count int) float64 {
	return l.sum / float64(count)
}

// Retrieve the variance of the values.
func (l *numericStat) getVariance(count int) float64 {
	return l.sqrDiff / (float64(count) - 1)
}

func (l *numericStat) record(count int, val float64) {
	l.sum += val
	delta := val - l.mean
	l.mean += delta / float64(count)
	l.sqrDiff += delta * (val - l.mean)
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
	if s.planner.sqlStats != nil {
		s.appStats = s.planner.sqlStats.getStatsForApplication(appName)
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
func (s *sqlStats) resetStats() {
	s.Lock()
	// Even though we clear the top-level map completely below, we
	// also need to clear the per-apps maps manually before that,
	// because any SQL session currently open has cached the
	// pointer to its appStats object and will continue to
	// accumulate data using that until it closes (or changes its
	// application_name).
	for _, a := range s.apps {
		a.Lock()
		a.stmts = make(map[string]*stmtStats)
		a.Unlock()
	}

	// Clear everything.
	s.apps = make(map[string]*appStats)
	s.Unlock()
}

// StmtStatsResetFrequency is the frequency at which per-app and
// per-statement statistics are cleared from memory, to avoid
// unlimited memory growth. This mechanism may be removed once the
// statistics are suitably recorded and discarded like other metrics.
var StmtStatsResetFrequency = envutil.EnvOrDefaultDuration(
	"COCKROACH_SQL_STMT_STATS_RESET_INTERVAL", 24*time.Hour,
)

// getResetWorker retrieves a function suitable to start via
// stopper.RunWorker().
func (s *sqlStats) getResetWorker(stopper *stop.Stopper) func() {
	return func() {
		ticker := time.NewTicker(StmtStatsResetFrequency)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				s.resetStats()
			case <-stopper.ShouldStop():
				return
			}
		}
	}
}
