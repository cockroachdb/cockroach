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
	"context"
	"math"
	"time"

	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats/ssmemstorage"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// SQLStats carries per-application in-memory statistics for all applications.
type SQLStats struct {
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

		// lastReset is the last time at which the app containers were reset.
		lastReset time.Time
		// apps is the container for all the per-application statistics objects.
		apps map[string]*ssmemstorage.Container
	}

	atomic struct {
		// uniqueStmtFingerprintCount is the number of unique statement fingerprints
		// we are storing in memory.
		uniqueStmtFingerprintCount int64

		// uniqueTxnFingerprintCount is the number of unique transaction fingerprints
		// we are storing in memory.
		uniqueTxnFingerprintCount int64
	}

	// flushTarget is a Sink that, when the SQLStats resets at the end of its
	// reset interval, the SQLStats will dump all of the stats into if it is not
	// nil.
	flushTarget Sink
}

func newSQLStats(
	st *cluster.Settings,
	uniqueStmtFingerprintLimit *settings.IntSetting,
	uniqueTxnFingerprintLimit *settings.IntSetting,
	curMemBytesCount *metric.Gauge,
	maxMemBytesHist *metric.Histogram,
	parentMon *mon.BytesMonitor,
	resetInterval *settings.DurationSetting,
	flushTarget Sink,
) *SQLStats {
	monitor := mon.NewMonitor(
		"SQLStats",
		mon.MemoryResource,
		curMemBytesCount,
		maxMemBytesHist,
		-1, /* increment */
		math.MaxInt64,
		st,
	)
	s := &SQLStats{
		st:                         st,
		uniqueStmtFingerprintLimit: uniqueStmtFingerprintLimit,
		uniqueTxnFingerprintLimit:  uniqueTxnFingerprintLimit,
		resetInterval:              resetInterval,
		flushTarget:                flushTarget,
	}
	s.mu.apps = make(map[string]*ssmemstorage.Container)
	s.mu.mon = monitor
	s.mu.mon.Start(context.Background(), parentMon, mon.BoundAccount{})
	return s
}

func (s *SQLStats) getStatsForApplication(appName string) *ssmemstorage.Container {
	s.mu.Lock()
	defer s.mu.Unlock()
	if a, ok := s.mu.apps[appName]; ok {
		return a
	}
	a := ssmemstorage.New(
		s.st,
		s.uniqueStmtFingerprintLimit,
		s.uniqueTxnFingerprintLimit,
		&s.atomic.uniqueStmtFingerprintCount,
		&s.atomic.uniqueTxnFingerprintCount,
		s.mu.mon,
	)
	s.mu.apps[appName] = a
	return a
}

// resetAndMaybeDumpStats clears all the stored per-app, per-statement and
// per-transaction statistics. If target is not nil, then the stats in s will be
// flushed into target.
func (s *SQLStats) resetAndMaybeDumpStats(ctx context.Context, target Sink) (err error) {
	// Note: we do not clear the entire s.mu.apps map here. We would need
	// to do so to prevent problems with a runaway client running `SET
	// APPLICATION_NAME=...` with a different name every time.  However,
	// any ongoing open client session at the time of the reset has
	// cached a pointer to its appStats struct and would thus continue
	// to report its stats in an object now invisible to the target tools
	// (virtual table, marshaling, etc.). It's a judgement call, but
	// for now we prefer to see more data and thus not clear the map, at
	// the risk of seeing the map grow unboundedly with the number of
	// different application_names seen so far.

	s.mu.Lock()

	// Clear the per-apps maps manually,
	// because any SQL session currently open has cached the
	// pointer to its appStats object and will continue to
	// accumulate data using that until it closes (or changes its
	// application_name).
	for appName, statsContainer := range s.mu.apps {
		// Save the existing data to logs.
		// TODO(knz/dt): instead of dumping the stats to the log, save
		// them in a SQL table so they can be inspected by the DBA and/or
		// the UI.
		if sqlstats.DumpStmtStatsToLogBeforeReset.Get(&s.st.SV) {
			statsContainer.SaveToLog(ctx, appName)
		}

		if target != nil {
			lastErr := target.AddAppStats(ctx, appName, statsContainer)
			// If we run out of memory budget, Container.Add() will merge stats in
			// statsContainer with all the existing stats. However it will discard
			// rest of the stats in statsContainer that requires memory allocation.
			// We do not wish to short circuit here because we want to still try our
			// best to merge all the stats that we can.
			if lastErr != nil {
				err = lastErr
			}
		}

		statsContainer.Clear(ctx)
	}
	s.mu.lastReset = timeutil.Now()
	s.mu.Unlock()

	return err
}
