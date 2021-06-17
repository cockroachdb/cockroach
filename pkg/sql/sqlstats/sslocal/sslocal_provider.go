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
	"fmt"
	"sort"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats/ssmemstorage"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// New returns an instance of SQLStats.
func New(
	settings *cluster.Settings,
	maxStmtFingerprints *settings.IntSetting,
	maxTxnFingerprints *settings.IntSetting,
	curMemoryBytesCount *metric.Gauge,
	maxMemoryBytesHist *metric.Histogram,
	pool *mon.BytesMonitor,
	resetInterval *settings.DurationSetting,
	reportingSink Sink,
) *SQLStats {
	return newSQLStats(settings, maxStmtFingerprints, maxTxnFingerprints,
		curMemoryBytesCount, maxMemoryBytesHist, pool, resetInterval, reportingSink)
}

var _ sqlstats.Provider = &SQLStats{}

// Start implements sqlstats.Provider interface.
func (s *SQLStats) Start(ctx context.Context, stopper *stop.Stopper) {
	if s.resetInterval != nil {
		s.periodicallyClearSQLStats(ctx, stopper, s.resetInterval)
	}
	// Start a loop to clear SQL stats at the max reset interval. This is
	// to ensure that we always have some worker clearing SQL stats to avoid
	// continually allocating space for the SQL stats.
	s.periodicallyClearSQLStats(ctx, stopper, sqlstats.MaxSQLStatReset)
}

func (s *SQLStats) periodicallyClearSQLStats(
	ctx context.Context, stopper *stop.Stopper, resetInterval *settings.DurationSetting,
) {
	// We run a periodic async job to clean up the in-memory stats.
	_ = stopper.RunAsyncTask(ctx, "sql-stats-clearer", func(ctx context.Context) {
		var timer timeutil.Timer
		for {
			s.mu.Lock()
			last := s.mu.lastReset
			s.mu.Unlock()

			next := last.Add(resetInterval.Get(&s.st.SV))
			wait := next.Sub(timeutil.Now())
			if wait < 0 {
				err := s.Reset(ctx)
				if err != nil {
					if log.V(1) {
						log.Warningf(ctx, "reported SQL stats memory limit has been exceeded, some fingerprints stats are discarded: %s", err)
					}
				}
			} else {
				timer.Reset(wait)
				select {
				case <-stopper.ShouldQuiesce():
					return
				case <-timer.C:
					timer.Read = true
				}
			}
		}
	})
}

// GetWriterForApplication implements sqlstats.Provider interface.
func (s *SQLStats) GetWriterForApplication(appName string) sqlstats.Writer {
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

// GetLastReset implements sqlstats.Provider interface.
func (s *SQLStats) GetLastReset() time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.mu.lastReset
}

// IterateStatementStats implements sqlstats.Provider interface.
func (s *SQLStats) IterateStatementStats(
	_ context.Context, options *sqlstats.IteratorOptions, visitor sqlstats.StatementVisitor,
) error {
	appNames := s.getAppNames(options.SortedAppNames)

	for _, appName := range appNames {
		statsContainer := s.getStatsForApplication(appName)

		err := statsContainer.IterateStatementStats(appName, options.SortedKey, visitor)
		if err != nil {
			return fmt.Errorf("sql stats iteration abort: %s", err)
		}
	}
	return nil
}

// IterateTransactionStats implements sqlstats.Provider interface.
func (s *SQLStats) IterateTransactionStats(
	_ context.Context, options *sqlstats.IteratorOptions, visitor sqlstats.TransactionVisitor,
) error {
	appNames := s.getAppNames(options.SortedAppNames)

	for _, appName := range appNames {
		statsContainer := s.getStatsForApplication(appName)

		err := statsContainer.IterateTransactionStats(appName, options.SortedKey, visitor)
		if err != nil {
			return fmt.Errorf("sql stats iteration abort: %s", err)
		}
	}
	return nil
}

// IterateAggregatedTransactionStats implements sqlstats.Provider interface.
func (s *SQLStats) IterateAggregatedTransactionStats(
	_ context.Context,
	options *sqlstats.IteratorOptions,
	visitor sqlstats.AggregatedTransactionVisitor,
) error {
	appNames := s.getAppNames(options.SortedAppNames)

	for _, appName := range appNames {
		statsContainer := s.getStatsForApplication(appName)

		err := statsContainer.IterateAggregatedTransactionStats(appName, visitor)
		if err != nil {
			return fmt.Errorf("sql stats iteration abort: %s", err)
		}
	}

	return nil
}

// GetStatementStats implements sqlstats.Provider interface.
func (s *SQLStats) GetStatementStats(
	key *roachpb.StatementStatisticsKey,
) (*roachpb.CollectedStatementStatistics, error) {
	statsContainer := s.getStatsForApplication(key.App)
	if statsContainer == nil {
		return nil, errors.Errorf("no stats found for appName: %s", key.App)
	}

	return statsContainer.GetStatementStats(key)
}

// GetTransactionStats implements sqlstats.Provider interface.
func (s *SQLStats) GetTransactionStats(
	appName string, key roachpb.TransactionFingerprintID,
) (*roachpb.CollectedTransactionStatistics, error) {
	statsContainer := s.getStatsForApplication(appName)
	if statsContainer == nil {
		return nil, errors.Errorf("no stats found for appName: %s", appName)
	}

	return statsContainer.GetTransactionStats(appName, key)
}

// Reset implements sqlstats.Provider interface.
func (s *SQLStats) Reset(ctx context.Context) error {
	return s.resetAndMaybeDumpStats(ctx, s.flushTarget)
}

func (s *SQLStats) getAppNames(sorted bool) []string {
	var appNames []string
	s.mu.Lock()
	for n := range s.mu.apps {
		appNames = append(appNames, n)
	}
	s.mu.Unlock()
	if sorted {
		sort.Strings(appNames)
	}

	return appNames
}
