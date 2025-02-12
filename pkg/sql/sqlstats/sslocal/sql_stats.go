// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sslocal

import (
	"context"
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

var _ sqlstats.SSDrainer = &SQLStats{}

// SQLStats carries per-application in-memory statistics for all applications.
type SQLStats struct {
	st *cluster.Settings

	mu struct {
		syncutil.Mutex

		mon *mon.BytesMonitor

		// lastReset is the last time at which the app containers were reset.
		lastReset time.Time
		// apps is the container for all the per-application statistics objects.
		apps map[string]*ssmemstorage.Container
	}

	// Server level counter
	atomic *ssmemstorage.SQLStatsAtomicCounters

	// flushTarget is a Sink that, when the SQLStats resets at the end of its
	// reset interval, the SQLStats will dump all of the stats into if it is not
	// nil.
	flushTarget Sink

	knobs *sqlstats.TestingKnobs
}

func newSQLStats(
	st *cluster.Settings,
	uniqueStmtFingerprintLimit *settings.IntSetting,
	uniqueTxnFingerprintLimit *settings.IntSetting,
	curMemBytesCount *metric.Gauge,
	maxMemBytesHist metric.IHistogram,
	parentMon *mon.BytesMonitor,
	flushTarget Sink,
	knobs *sqlstats.TestingKnobs,
) *SQLStats {
	monitor := mon.NewMonitor(mon.Options{
		Name:       mon.MakeMonitorName("SQLStats"),
		CurCount:   curMemBytesCount,
		MaxHist:    maxMemBytesHist,
		Settings:   st,
		LongLiving: true,
	})
	s := &SQLStats{
		st:          st,
		flushTarget: flushTarget,
		knobs:       knobs,
	}
	s.atomic = ssmemstorage.NewSQLStatsAtomicCounters(
		st,
		uniqueStmtFingerprintLimit,
		uniqueTxnFingerprintLimit)
	s.mu.apps = make(map[string]*ssmemstorage.Container)
	s.mu.mon = monitor
	s.mu.mon.StartNoReserved(context.Background(), parentMon)
	return s
}

// GetTotalFingerprintCount returns total number of unique statement and
// transaction fingerprints stored in the current SQLStats.
func (s *SQLStats) GetTotalFingerprintCount() int64 {
	return s.atomic.GetTotalFingerprintCount()
}

// GetTotalFingerprintBytes returns the total amount of bytes currently
// allocated for storing statistics for both statement and transaction
// fingerprints.
func (s *SQLStats) GetTotalFingerprintBytes() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.mu.mon.AllocBytes()
}

func (s *SQLStats) GetCounters() *ssmemstorage.SQLStatsAtomicCounters {
	return s.atomic
}

func (s *SQLStats) getStatsForApplication(appName string) *ssmemstorage.Container {
	s.mu.Lock()
	defer s.mu.Unlock()
	if a, ok := s.mu.apps[appName]; ok {
		return a
	}
	a := ssmemstorage.New(
		s.st,
		s.atomic,
		s.mu.mon,
		appName,
		s.knobs,
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
	defer s.mu.Unlock()

	// Clear the per-apps maps manually,
	// because any SQL session currently open has cached the
	// pointer to its appStats object and will continue to
	// accumulate data using that until it closes (or changes its
	// application_name).
	for appName, statsContainer := range s.mu.apps {
		lastErr := s.MaybeDumpStatsToLog(ctx, appName, statsContainer, target)
		statsContainer.Clear(ctx)
		err = lastErr
	}
	s.mu.lastReset = timeutil.Now()

	return err
}

// MaybeDumpStatsToLog flushes stats into target If it is not nil.
func (s *SQLStats) MaybeDumpStatsToLog(
	ctx context.Context, appName string, container *ssmemstorage.Container, target Sink,
) (err error) {
	// Save the existing data to logs.
	// TODO(knz/dt): instead of dumping the stats to the log, save
	// them in a SQL table so they can be inspected by the DBA and/or
	// the UI.
	if sqlstats.DumpStmtStatsToLogBeforeReset.Get(&s.st.SV) {
		container.SaveToLog(ctx, appName)
	}

	if target != nil {
		lastErr := target.AddAppStats(ctx, appName, container)
		// If we run out of memory budget, Container.Add() will merge stats in
		// statsContainer with all the existing stats. However it will discard
		// rest of the stats in statsContainer that requires memory allocation.
		// We do not wish to short circuit here because we want to still try our
		// best to merge all the stats that we can.
		if lastErr != nil {
			err = lastErr
		}
	}
	return err
}

func (s *SQLStats) GetClusterSettings() *cluster.Settings {
	return s.st
}
