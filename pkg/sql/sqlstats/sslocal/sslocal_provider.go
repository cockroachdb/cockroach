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
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/pkg/errors"
)

// New returns an local in-memory implementation of
// sqlstats.Provider.
// TODO(azhng): reportedSQLStats is an unfortunate hack to implement telemetry
//  reporting. As we move to persisted SQL stats, we would no longer need this
//  since we can directly report from our system table. So delete  the code
//  related to reported SQL stats once we have finished the migration.
func New(
	settings *cluster.Settings,
	maxStmtFingerprints *settings.IntSetting,
	maxTxnFingerprints *settings.IntSetting,
	curMemoryBytesCount *metric.Gauge,
	maxMemoryBytesHist *metric.Histogram,
	pool *mon.BytesMonitor,
	resetInterval *settings.DurationSetting,
	reportedProvider sqlstats.Provider,
) sqlstats.Provider {
	var reportedSQLStats *sqlStats
	if resetInterval != nil {
		var ok bool
		reportedSQLStats, ok = reportedProvider.(*sqlStats)
		if !ok {
			panic("reportedProvider should be an instance of sqlStats")
		}
	}
	return newSQLStats(settings, maxStmtFingerprints, maxTxnFingerprints,
		curMemoryBytesCount, maxMemoryBytesHist, pool, resetInterval, reportedSQLStats)
}

var _ sqlstats.Provider = &sqlStats{}

// Start implements sqlstats.Provider interface.
func (s *sqlStats) Start(ctx context.Context, stopper *stop.Stopper) {
	if s.resetInterval != nil {
		s.periodicallyClearSQLStats(ctx, stopper, s.resetInterval)
	}
	// Start a loop to clear SQL stats at the max reset interval. This is
	// to ensure that we always have some worker clearing SQL stats to avoid
	// continually allocating space for the SQL stats.
	s.periodicallyClearSQLStats(ctx, stopper, sqlstats.MaxSQLStatReset)
}

func (s *sqlStats) periodicallyClearSQLStats(
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
func (s *sqlStats) GetWriterForApplication(appName string) sqlstats.Writer {
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

// GetLastReset implements sqlstats.Provider interface.
func (s *sqlStats) GetLastReset() time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.mu.lastReset
}

// IterateStatementStats implements sqlstats.Provider interface.
func (s *sqlStats) IterateStatementStats(
	_ context.Context, options *sqlstats.IteratorOptions, visitor sqlstats.StatementVisitor,
) error {
	appNames := s.getAppNames(options.SortedAppNames)

	for _, appName := range appNames {
		appStats := s.getStatsForApplication(appName)

		// Retrieve the statement keys and optionally sort them.
		var stmtKeys stmtList
		appStats.Lock()
		for k := range appStats.stmts {
			stmtKeys = append(stmtKeys, k)
		}
		appStats.Unlock()
		if options.SortedKey {
			sort.Sort(stmtKeys)
		}

		// Now retrieve the per-stmt stats proper.
		for _, stmtKey := range stmtKeys {
			stmtID := constructStatementIDFromStmtKey(stmtKey)
			statementStats, _, _ :=
				appStats.getStatsForStmtWithKey(stmtKey, invalidStmtID, false /* createIfNonexistent */)

			// If the key is not found (and we expected to find it), the table must
			// have been cleared between now and the time we read all the keys. In
			// that case we simply skip this key as there are no metrics to report.
			if statementStats == nil {
				continue
			}

			statementStats.mu.Lock()
			data := statementStats.mu.data
			distSQLUsed := statementStats.mu.distSQLUsed
			vectorized := statementStats.mu.vectorized
			fullScan := statementStats.mu.fullScan
			database := statementStats.mu.database
			statementStats.mu.Unlock()

			collectedStats := roachpb.CollectedStatementStatistics{
				Key: roachpb.StatementStatisticsKey{
					Query:       stmtKey.anonymizedStmt,
					DistSQL:     distSQLUsed,
					Opt:         true,
					Vec:         vectorized,
					ImplicitTxn: stmtKey.implicitTxn,
					FullScan:    fullScan,
					Failed:      stmtKey.failed,
					App:         appName,
					Database:    database,
				},
				ID:    stmtID,
				Stats: data,
			}

			err := visitor(&collectedStats)
			if err != nil {
				return fmt.Errorf("sql stats iteration abort: %s", err)
			}
		}
	}
	return nil
}

// IterateTransactionStats implements sqlstats.Provider interface.
func (s *sqlStats) IterateTransactionStats(
	_ context.Context, options *sqlstats.IteratorOptions, visitor sqlstats.TransactionVisitor,
) error {
	appNames := s.getAppNames(options.SortedAppNames)

	for _, appName := range appNames {
		appStats := s.getStatsForApplication(appName)

		// Retrieve the transaction keys and optionally sort them.
		var txnKeys txnList
		appStats.Lock()
		for k := range appStats.txns {
			txnKeys = append(txnKeys, k)
		}
		appStats.Unlock()
		if options.SortedKey {
			sort.Sort(txnKeys)
		}

		// Now retrieve the per-stmt stats proper.
		for _, txnKey := range txnKeys {
			// We don't want to create the key if it doesn't exist, so it's okay to
			// pass nil for the statementIDs, as they are only set when a key is
			// constructed.
			txnStats, _, _ := appStats.getStatsForTxnWithKey(txnKey, nil /* stmtIDs */, false /* createIfNonexistent */)
			// If the key is not found (and we expected to find it), the table must
			// have been cleared between now and the time we read all the keys. In
			// that case we simply skip this key as there are no metrics to report.
			if txnStats == nil {
				continue
			}

			txnStats.mu.Lock()
			collectedStats := roachpb.CollectedTransactionStatistics{
				StatementIDs: txnStats.statementIDs,
				App:          appName,
				Stats:        txnStats.mu.data,
			}
			txnStats.mu.Unlock()

			err := visitor(txnKey, &collectedStats)
			if err != nil {
				return fmt.Errorf("sql stats iteration abort: %s", err)
			}
		}
	}
	return nil
}

// IterateAppLevelTransactionStats implements sqlstats.Provider interface.
func (s *sqlStats) IterateAppLevelTransactionStats(
	_ context.Context, options *sqlstats.IteratorOptions, visitor sqlstats.AppLevelTransactionVisitor,
) error {
	appNames := s.getAppNames(options.SortedAppNames)

	for _, appName := range appNames {
		appStats := s.getStatsForApplication(appName)

		var txnStat roachpb.TxnStats
		// TODO(azhng): We do not lock onto appStats here. See the struct field
		//  comment for details.
		appStats.txnCounts.mu.Lock()
		txnStat = appStats.txnCounts.mu.TxnStats
		appStats.txnCounts.mu.Unlock()

		err := visitor(appName, &txnStat)
		if err != nil {
			return fmt.Errorf("sql stats iteration abort: %s", err)
		}
	}

	return nil
}

// GetStatementStats implements sqlstats.Provider interface.
func (s *sqlStats) GetStatementStats(
	key *roachpb.StatementStatisticsKey,
) (*roachpb.CollectedStatementStatistics, error) {
	applicationStats := s.getStatsForApplication(key.App)
	if applicationStats == nil {
		return nil, errors.Errorf("no stats found for appName: %s", key.App)
	}

	statementStats, _, stmtID, _, _ := applicationStats.getStatsForStmt(
		key.Query, key.ImplicitTxn, key.Database, key.Failed, false /* createIfNonexistent */)

	if statementStats == nil {
		return nil, errors.Errorf("no stats found for the provided key")
	}

	statementStats.mu.Lock()
	defer statementStats.mu.Unlock()
	data := statementStats.mu.data

	collectedStats := &roachpb.CollectedStatementStatistics{
		ID:    stmtID,
		Key:   *key,
		Stats: data,
	}

	return collectedStats, nil
}

// GetTransactionStats implements sqlstats.Provider interface.
func (s *sqlStats) GetTransactionStats(
	appName string, key sqlstats.TransactionFingerprintID,
) (*roachpb.CollectedTransactionStatistics, error) {
	applicationStats := s.getStatsForApplication(appName)
	if applicationStats == nil {
		return nil, errors.Errorf("no stats found for appName: %s", appName)
	}

	txnStats, _, _ :=
		applicationStats.getStatsForTxnWithKey(key, nil /* stmtIDs */, false /* createIfNonexistent */)

	if txnStats == nil {
		return nil, errors.Errorf("no stats found for the provided key")
	}

	txnStats.mu.Lock()
	defer txnStats.mu.Unlock()
	data := txnStats.mu.data

	collectedStats := &roachpb.CollectedTransactionStatistics{
		StatementIDs: txnStats.statementIDs,
		App:          appName,
		Stats:        data,
	}

	return collectedStats, nil
}

// Reset implements sqlstats.Provider interface.
func (s *sqlStats) Reset(ctx context.Context) error {
	return s.resetAndMaybeDumpStats(ctx, s.flushTarget)
}

func (s *sqlStats) getAppNames(sorted bool) []string {
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
