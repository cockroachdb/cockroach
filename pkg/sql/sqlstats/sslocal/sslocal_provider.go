// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sslocal

import (
	"context"
	"sort"
	"time"

	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/appstatspb"
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
	maxMemoryBytesHist metric.IHistogram,
	pool *mon.BytesMonitor,
	reportingSink Sink,
	knobs *sqlstats.TestingKnobs,
) *SQLStats {
	return newSQLStats(
		settings,
		maxStmtFingerprints,
		maxTxnFingerprints,
		curMemoryBytesCount,
		maxMemoryBytesHist,
		pool,
		reportingSink,
		knobs,
	)
}

// GetController returns a sqlstats.Controller responsible for the current
// SQLStats.
func (s *SQLStats) GetController(server serverpb.SQLStatusServer) *Controller {
	return NewController(s, server)
}

func (s *SQLStats) Start(ctx context.Context, stopper *stop.Stopper) {
	// We run a periodic async job to clean up the in-memory stats.
	_ = stopper.RunAsyncTask(ctx, "sql-stats-clearer", func(ctx context.Context) {
		var timer timeutil.Timer
		for {
			last := func() time.Time {
				s.mu.Lock()
				defer s.mu.Unlock()
				return s.mu.lastReset
			}()

			next := last.Add(sqlstats.MaxSQLStatReset.Get(&s.st.SV))
			wait := next.Sub(timeutil.Now())
			if wait < 0 {
				err := s.Reset(ctx)
				if err != nil {
					if log.V(1) {
						log.Warningf(ctx, "unexpected error: %s", err)
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

func (s *SQLStats) GetApplicationStats(appName string) *ssmemstorage.Container {
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

func (s *SQLStats) GetLastReset() time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.mu.lastReset
}

func (s *SQLStats) IterateStatementStats(
	ctx context.Context, options sqlstats.IteratorOptions, visitor sqlstats.StatementVisitor,
) error {
	iter := s.StmtStatsIterator(options)

	for iter.Next() {
		if err := visitor(ctx, iter.Cur()); err != nil {
			return err
		}
	}

	return nil
}

// DrainStats leverages the process of atomic pulling stats from in-memory storage and clearing in-memory stats.
// DrainStats allows to process pulled statements while new sql stats can be added to in-memory statistics.
func (s *SQLStats) DrainStats(
	ctx context.Context,
) (
	[]*appstatspb.CollectedStatementStatistics,
	[]*appstatspb.CollectedTransactionStatistics,
	int64,
) {
	stmtStats := make([]*appstatspb.CollectedStatementStatistics, 0)
	txnStats := make([]*appstatspb.CollectedTransactionStatistics, 0)
	apps := s.getAppNames(false)
	totalFingerprintCount := s.GetTotalFingerprintCount()

	for _, app := range apps {
		container := s.GetApplicationStats(app)
		if err := s.MaybeDumpStatsToLog(ctx, app, container, s.flushTarget); err != nil {
			log.Warningf(ctx, "failed to dump stats to log, %s", err.Error())
		}
		containerStmtStats, containerTxnStats := container.DrainStats(ctx)
		stmtStats = append(stmtStats, containerStmtStats...)
		txnStats = append(txnStats, containerTxnStats...)
	}
	return stmtStats, txnStats, totalFingerprintCount
}

// StmtStatsIterator returns an instance of sslocal.StmtStatsIterator for
// the current SQLStats.
func (s *SQLStats) StmtStatsIterator(options sqlstats.IteratorOptions) StmtStatsIterator {
	return NewStmtStatsIterator(s, options)
}

func (s *SQLStats) IterateTransactionStats(
	ctx context.Context, options sqlstats.IteratorOptions, visitor sqlstats.TransactionVisitor,
) error {
	iter := s.TxnStatsIterator(options)

	for iter.Next() {
		stats := iter.Cur()
		if err := visitor(ctx, stats); err != nil {
			return err
		}
	}

	return nil
}

// TxnStatsIterator returns an instance of sslocal.TxnStatsIterator for
// the current SQLStats.
func (s *SQLStats) TxnStatsIterator(options sqlstats.IteratorOptions) TxnStatsIterator {
	return NewTxnStatsIterator(s, options)
}

func (s *SQLStats) IterateAggregatedTransactionStats(
	ctx context.Context,
	options sqlstats.IteratorOptions,
	visitor sqlstats.AggregatedTransactionVisitor,
) error {
	appNames := s.getAppNames(options.SortedAppNames)

	for _, appName := range appNames {
		statsContainer := s.getStatsForApplication(appName)

		err := statsContainer.IterateAggregatedTransactionStats(ctx, options, visitor)
		if err != nil {
			return errors.Wrap(err, "sql stats iteration abort")
		}
	}

	return nil
}

func (s *SQLStats) Reset(ctx context.Context) error {
	return s.resetAndMaybeDumpStats(ctx, s.flushTarget)
}

func (s *SQLStats) getAppNames(sorted bool) []string {
	appNames := func() (appNames []string) {
		s.mu.Lock()
		defer s.mu.Unlock()
		for n := range s.mu.apps {
			appNames = append(appNames, n)
		}
		return appNames
	}()
	if sorted {
		sort.Strings(appNames)
	}

	return appNames
}
