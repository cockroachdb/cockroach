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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// sqlStats carries per-application in-memory statistics for all applications.
type sqlStats struct {
	st *cluster.Settings

	// uniqueStmtFingerprintLimit is the limit on number of unique statement
	// fingerprints we can store in memory.
	uniqueStmtFingerprintLimit *settings.IntSetting

	// uniqueTxnFingerprintLimit is the limit on number of unique transaction
	// fingerprints we can store in memory.
	uniqueTxnFingerprintLimit *settings.IntSetting

	// resetInterval is the interval of how long before the in-memory stats are
	// being reset.
	resetInterval *settings.DurationSetting

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

	// flushTarget is another instance of sqlStats that, when the sqlStats resets
	// at the end of its reset interval, the sqlStats will dump all of the stats
	// into flushTarget if it is not nil.
	flushTarget *sqlStats
}

func newSQLStats(
	st *cluster.Settings,
	uniqueStmtFingerprintLimit *settings.IntSetting,
	uniqueTxnFingerprintLimit *settings.IntSetting,
	curMemBytesCount *metric.Gauge,
	maxMemBytesHist *metric.Histogram,
	parentMon *mon.BytesMonitor,
	resetInterval *settings.DurationSetting,
	flushTarget *sqlStats,
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
		resetInterval:              resetInterval,
		flushTarget:                flushTarget,
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
		txns:     make(map[sqlstats.TransactionFingerprintID]*txnStats),
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
		if sqlstats.DumpStmtStatsToLogBeforeReset.Get(&a.st.SV) {
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
		a.txns = make(map[sqlstats.TransactionFingerprintID]*txnStats, len(a.txns)/2)

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
