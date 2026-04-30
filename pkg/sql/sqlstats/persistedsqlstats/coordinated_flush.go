// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package persistedsqlstats

import (
	"context"
	"maps"
	"slices"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/sql/appstatspb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/quotapool"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// initialMergeMapSizeHint is the cold-start hint for the first cycle in a
// process. Subsequent cycles use the previous cycle's observed sizes.
const initialMergeMapSizeHint = 500

// StatsCollector returns SQL stats from one or more sources, already merged
// in memory.
type StatsCollector interface {
	Collect(ctx context.Context) (CollectedStats, error)
}

// CollectedStats is the cluster-wide stmt and txn stats produced by a
// Collect call.
type CollectedStats struct {
	Statements   []*appstatspb.CollectedStatementStatistics
	Transactions []*appstatspb.CollectedTransactionStatistics
}

// Len returns the total of statement plus transaction entries.
func (s CollectedStats) Len() int {
	return len(s.Statements) + len(s.Transactions)
}

// StatsWriter persists already-merged stats.
type StatsWriter interface {
	Write(ctx context.Context, stats CollectedStats) error
}

// stmtMergeKey mirrors the primary key of system.statement_statistics
// (minus node_id). aggregatedTs / aggInterval are included so stats from
// distinct aggregation windows stay distinct when chunks straddle a
// boundary.
type stmtMergeKey struct {
	fingerprintID    appstatspb.StmtFingerprintID
	txnFingerprintID appstatspb.TransactionFingerprintID
	planHash         uint64
	app              string
	aggregatedTs     time.Time
	aggInterval      time.Duration
}

// txnMergeKey mirrors the primary key of system.transaction_statistics
// (minus node_id). See stmtMergeKey for window-disjoint reasoning.
type txnMergeKey struct {
	fingerprintID appstatspb.TransactionFingerprintID
	app           string
	aggregatedTs  time.Time
	aggInterval   time.Duration
}

// InstanceCollector drains every SQL instance's in-memory stats by
// invoking the cluster-fanout form of DrainSqlStatsStream and merging the
// per-source chunks into a single deduplicated result. The fanout, the
// per-instance failure tracking, and the concurrency controls all live
// in the server; this collector is a single-goroutine merge consumer.
type InstanceCollector struct {
	statusServer    serverpb.SQLStatusServer
	stmtMapSizeHint int
	txnMapSizeHint  int
}

// NewInstanceCollector constructs a collector. The size hints pre-size the
// merge maps to avoid grow-and-rehash on cluster-scale workloads; pass the
// previous cycle's observed sizes (or initialMergeMapSizeHint cold).
func NewInstanceCollector(
	statusServer serverpb.SQLStatusServer, stmtMapSizeHint, txnMapSizeHint int,
) *InstanceCollector {
	return &InstanceCollector{
		statusServer:    statusServer,
		stmtMapSizeHint: stmtMapSizeHint,
		txnMapSizeHint:  txnMapSizeHint,
	}
}

// mergeChunk merges one drain chunk into the stmt and txn maps.
func mergeChunk(
	chunk *serverpb.DrainStatsChunk,
	stmtMap map[stmtMergeKey]*appstatspb.CollectedStatementStatistics,
	txnMap map[txnMergeKey]*appstatspb.CollectedTransactionStatistics,
) {
	for _, stmt := range chunk.Statements {
		key := stmtMergeKey{
			fingerprintID:    stmt.ID,
			txnFingerprintID: stmt.Key.TransactionFingerprintID,
			planHash:         stmt.Key.PlanHash,
			app:              stmt.Key.App,
			aggregatedTs:     stmt.AggregatedTs,
			aggInterval:      stmt.AggregationInterval,
		}
		if existing, ok := stmtMap[key]; ok {
			existing.Stats.Add(&stmt.Stats)
		} else {
			stmtMap[key] = stmt
		}
	}
	for _, txn := range chunk.Transactions {
		key := txnMergeKey{
			fingerprintID: txn.TransactionFingerprintID,
			app:           txn.App,
			aggregatedTs:  txn.AggregatedTs,
			aggInterval:   txn.AggregationInterval,
		}
		if existing, ok := txnMap[key]; ok {
			existing.Stats.Add(&txn.Stats)
		} else {
			txnMap[key] = txn
		}
	}
}

// Collect implements StatsCollector. It drives a single cluster-fanout
// DrainSqlStatsStream call, merges every per-source chunk into the
// stmt/txn maps as it arrives, and reads the trailing summary to decide
// whether the cycle as a whole succeeded. Because there is exactly one
// goroutine reading the stream, no synchronization is needed around the
// merge maps.
func (c *InstanceCollector) Collect(ctx context.Context) (CollectedStats, error) {
	sink := &mergingSink{
		ctx:     ctx,
		stmtMap: make(map[stmtMergeKey]*appstatspb.CollectedStatementStatistics, c.stmtMapSizeHint),
		txnMap:  make(map[txnMergeKey]*appstatspb.CollectedTransactionStatistics, c.txnMapSizeHint),
	}
	if err := c.statusServer.DrainSqlStatsStream(&serverpb.DrainSqlStatsRequest{}, sink); err != nil {
		return CollectedStats{}, errors.Wrap(err, "drain sql stats")
	}

	out := CollectedStats{
		Statements:   slices.Collect(maps.Values(sink.stmtMap)),
		Transactions: slices.Collect(maps.Values(sink.txnMap)),
	}

	if sink.summary == nil {
		return out, errors.AssertionFailedf("drain sql stats: server omitted trailing summary")
	}
	var nFailed int
	var combinedErr error
	for _, outcome := range sink.summary.Nodes {
		if outcome.Error != "" {
			nFailed++
			combinedErr = errors.CombineErrors(combinedErr,
				errors.Newf("node %d: %s", outcome.NodeID, outcome.Error))
		}
	}
	if nFailed > 0 {
		log.Ops.Warningf(ctx, "coordinated flush: %d of %d instance drains failed",
			nFailed, len(sink.summary.Nodes))
	}
	return out, nil
}

// mergingSink satisfies serverpb.RPCStatus_DrainSqlStatsStream and merges
// each delivered chunk directly into stmtMap / txnMap. Summary-bearing
// chunks (sent as the final message of a cluster-fanout stream) are
// captured in summary instead and don't contribute to the merge.
type mergingSink struct {
	ctx     context.Context
	stmtMap map[stmtMergeKey]*appstatspb.CollectedStatementStatistics
	txnMap  map[txnMergeKey]*appstatspb.CollectedTransactionStatistics
	summary *serverpb.DrainStatsSummary
}

func (s *mergingSink) Context() context.Context { return s.ctx }

func (s *mergingSink) Send(chunk *serverpb.DrainStatsChunk) error {
	if chunk.Summary != nil {
		s.summary = chunk.Summary
		return nil
	}
	mergeChunk(chunk, s.stmtMap, s.txnMap)
	return nil
}

// FlushWriter batches merged stats and writes them via doFlushStmtStats /
// doFlushTxnStats. Per-batch writes are scheduled on stopper so they
// participate in graceful shutdown.
type FlushWriter struct {
	persistedStats *PersistedSQLStats
	stopper        *stop.Stopper
}

// NewFlushWriter constructs a writer.
func NewFlushWriter(persistedStats *PersistedSQLStats, stopper *stop.Stopper) *FlushWriter {
	return &FlushWriter{persistedStats: persistedStats, stopper: stopper}
}

// flushWriteParallelism caps how many UPSERT batches run concurrently
// against the persisted tables. Connection-pool / KV write concurrency are
// the real ceilings; 8 stays well under either.
const flushWriteParallelism = 8

// coordinatedFlushNodeID is the synthetic node_id every coordinated flush
// writes under.
const coordinatedFlushNodeID = 0

// Write implements StatsWriter. Returns the combined error of all
// per-batch failures; nil means every UPSERT succeeded.
func (w *FlushWriter) Write(ctx context.Context, stats CollectedStats) error {
	batchSize := int(SQLStatsFlushCoordinatedBatchSize.Get(&w.persistedStats.cfg.Settings.SV))
	cfg := w.persistedStats.cfg

	sem := quotapool.NewIntPool("sql-stats-flush-write", flushWriteParallelism)
	var (
		errMu       syncutil.Mutex
		combinedErr error
	)
	recordErr := func(err error) {
		errMu.Lock()
		defer errMu.Unlock()
		combinedErr = errors.CombineErrors(combinedErr, err)
	}

	var wg sync.WaitGroup
	runBatch := func(batchKind string, fn func(context.Context) error) {
		wg.Add(1)
		err := w.stopper.RunAsyncTaskEx(ctx, stop.TaskOpts{
			TaskName:   "sql-stats-flush-" + batchKind,
			Sem:        sem,
			WaitForSem: true,
		}, func(ctx context.Context) {
			defer wg.Done()
			if err := fn(ctx); err != nil {
				cfg.FlushesFailed.Inc(1)
				log.Dev.Warningf(ctx, "coordinated flush: %s batch: %v", batchKind, err)
				recordErr(err)
				return
			}
			cfg.FlushesSuccessful.Inc(1)
		})
		if err != nil {
			// Stopper quiescing: balance the counter and record the loss.
			wg.Done()
			cfg.FlushesFailed.Inc(1)
			log.Dev.Warningf(ctx, "coordinated flush: schedule %s batch: %v", batchKind, err)
			recordErr(err)
		}
	}

	for i := 0; i < len(stats.Statements); i += batchSize {
		end := i + batchSize
		if end > len(stats.Statements) {
			end = len(stats.Statements)
		}
		b := stats.Statements[i:end]
		runBatch("stmt", func(ctx context.Context) error {
			return doFlushStmtStats(ctx, b, coordinatedFlushNodeID, cfg.DB)
		})
	}
	for i := 0; i < len(stats.Transactions); i += batchSize {
		end := i + batchSize
		if end > len(stats.Transactions) {
			end = len(stats.Transactions)
		}
		b := stats.Transactions[i:end]
		runBatch("txn", func(ctx context.Context) error {
			return doFlushTxnStats(ctx, b, coordinatedFlushNodeID, cfg.DB)
		})
	}
	wg.Wait()
	return combinedErr
}

// RunCoordinatedFlush performs one coordinator cycle: collect stats from
// every SQL instance, merge in memory, and write to the persisted tables.
// Idempotent on retry: a second call sees only undrained stats, and the
// UPSERT path safely absorbs anything that was written.
//
// The caller is responsible for cadence: invoke once per
// sql.stats.flush.interval, only when CoordinatedFlushEnabled is true.
func (s *PersistedSQLStats) RunCoordinatedFlush(ctx context.Context, stopper *stop.Stopper) error {
	ctx, cancel := stopper.WithCancelOnQuiesce(ctx)
	defer cancel()

	if !SQLStatsFlushEnabled.Get(&s.cfg.Settings.SV) {
		return nil
	}

	stmtHint := int(s.coordinatedFlushHints.stmtMapSize.Load())
	if stmtHint == 0 {
		stmtHint = initialMergeMapSizeHint
	}
	txnHint := int(s.coordinatedFlushHints.txnMapSize.Load())
	if txnHint == 0 {
		txnHint = initialMergeMapSizeHint
	}

	flushBegin := timeutil.Now()
	collector := NewInstanceCollector(s.cfg.FanoutServer, stmtHint, txnHint)
	stats, err := collector.Collect(ctx)
	if err != nil {
		return err
	}

	// Last-write-wins: the coordinator is a sqlliveness singleton, so
	// concurrent cycles aren't expected and these are only hints.
	s.coordinatedFlushHints.stmtMapSize.Store(int64(len(stats.Statements)))
	s.coordinatedFlushHints.txnMapSize.Store(int64(len(stats.Transactions)))

	writer := NewFlushWriter(s, stopper)
	if err := writer.Write(ctx, stats); err != nil {
		return err
	}

	s.cfg.FlushedFingerprintCount.Inc(int64(stats.Len()))
	s.cfg.FlushLatency.RecordValue(timeutil.Since(flushBegin).Nanoseconds())

	// TODO(kyle.wong): the per-node flush loop signals
	// flushDoneMu.signalCh to wake the activity-update job. That signal is
	// in-process only, so it cannot reach the activity updater when its
	// sqlliveness lease is held by a different node from this coordinator.
	// Activity tables won't update in coordinated mode until either the
	// coordinator drives the activity update inline, or the activity
	// updater grows a cross-node trigger (e.g. system-table polling).
	return nil
}
