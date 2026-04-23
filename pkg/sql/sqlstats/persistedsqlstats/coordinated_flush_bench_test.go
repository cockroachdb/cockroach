// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package persistedsqlstats_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/appstatspb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats/persistedsqlstats"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats/ssmemstorage"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// fakeStatusServer is a minimal serverpb.SQLStatusServer implementation
// that only supports the cluster-fanout form of DrainSqlStatsStream
// (empty NodeID). It emits the precomputed chunks for every node in
// order, then a trailing summary marking each node as successful — the
// same shape the real server's drainSqlStatsClusterFanout produces.
type fakeStatusServer struct {
	serverpb.SQLStatusServer // embedded so unimplemented methods compile

	nodes  []roachpb.NodeID
	chunks map[roachpb.NodeID][]*serverpb.DrainStatsChunk
}

func (f *fakeStatusServer) DrainSqlStatsStream(
	req *serverpb.DrainSqlStatsRequest, stream serverpb.RPCStatus_DrainSqlStatsStream,
) error {
	if req.NodeID != "" {
		return errors.Newf("fakeStatusServer: only cluster-fanout (empty NodeID) is supported")
	}
	for _, nodeID := range f.nodes {
		for _, c := range f.chunks[nodeID] {
			if err := stream.Send(c); err != nil {
				return err
			}
		}
	}
	summary := &serverpb.DrainStatsSummary{
		Nodes: make([]serverpb.DrainStatsSummary_NodeOutcome, 0, len(f.nodes)),
	}
	for _, nodeID := range f.nodes {
		summary.Nodes = append(summary.Nodes, serverpb.DrainStatsSummary_NodeOutcome{NodeID: nodeID})
	}
	return stream.Send(&serverpb.DrainStatsChunk{Summary: summary})
}

// fingerprintMix controls how the synthetic per-node chunks are generated.
// uniqueFraction is the fraction of fingerprints unique to each node; the
// remainder is shared across all nodes (which is the common case driving the
// merge-Add hot path when GatewayNodeEnabled=false collapses everything to
// nodeID=0).
type fingerprintMix struct {
	stmtsPerNode   int
	txnsPerNode    int
	uniqueFraction float64 // 0.0 = fully overlapping, 1.0 = fully disjoint
	chunkSize      int
}

func buildSyntheticChunks(
	numNodes int, mix fingerprintMix,
) map[roachpb.NodeID][]*serverpb.DrainStatsChunk {
	out := make(map[roachpb.NodeID][]*serverpb.DrainStatsChunk, numNodes)
	now := time.Date(2026, 4, 23, 0, 0, 0, 0, time.UTC)

	uniqueStmts := int(float64(mix.stmtsPerNode) * mix.uniqueFraction)
	sharedStmts := mix.stmtsPerNode - uniqueStmts
	uniqueTxns := int(float64(mix.txnsPerNode) * mix.uniqueFraction)
	sharedTxns := mix.txnsPerNode - uniqueTxns

	for n := 0; n < numNodes; n++ {
		nodeID := roachpb.NodeID(n + 1)
		stmts := make([]*appstatspb.CollectedStatementStatistics, 0, mix.stmtsPerNode)
		txns := make([]*appstatspb.CollectedTransactionStatistics, 0, mix.txnsPerNode)

		// Shared stmts/txns: same fingerprintID across all nodes.
		for i := 0; i < sharedStmts; i++ {
			fpID := appstatspb.StmtFingerprintID(i + 1)
			stmts = append(stmts, &appstatspb.CollectedStatementStatistics{
				ID: fpID,
				Key: appstatspb.StatementStatisticsKey{
					App:                      "shared",
					PlanHash:                 uint64(i + 1),
					TransactionFingerprintID: appstatspb.TransactionFingerprintID(i + 1),
				},
				Stats:               sampleStmtStats(),
				AggregatedTs:        now,
				AggregationInterval: time.Hour,
			})
		}
		// Unique stmts: fingerprint includes nodeID so they don't collide.
		for i := 0; i < uniqueStmts; i++ {
			fpID := appstatspb.StmtFingerprintID(int64(nodeID)*1_000_000 + int64(i) + 1)
			stmts = append(stmts, &appstatspb.CollectedStatementStatistics{
				ID: fpID,
				Key: appstatspb.StatementStatisticsKey{
					App:                      fmt.Sprintf("unique_%d", nodeID),
					PlanHash:                 uint64(fpID),
					TransactionFingerprintID: appstatspb.TransactionFingerprintID(fpID),
				},
				Stats:               sampleStmtStats(),
				AggregatedTs:        now,
				AggregationInterval: time.Hour,
			})
		}
		// Shared txns.
		for i := 0; i < sharedTxns; i++ {
			fpID := appstatspb.TransactionFingerprintID(i + 1)
			txns = append(txns, &appstatspb.CollectedTransactionStatistics{
				App:                      "shared",
				TransactionFingerprintID: fpID,
				Stats:                    sampleTxnStats(),
				AggregatedTs:             now,
				AggregationInterval:      time.Hour,
			})
		}
		// Unique txns.
		for i := 0; i < uniqueTxns; i++ {
			fpID := appstatspb.TransactionFingerprintID(int64(nodeID)*1_000_000 + int64(i) + 1)
			txns = append(txns, &appstatspb.CollectedTransactionStatistics{
				App:                      fmt.Sprintf("unique_%d", nodeID),
				TransactionFingerprintID: fpID,
				Stats:                    sampleTxnStats(),
				AggregatedTs:             now,
				AggregationInterval:      time.Hour,
			})
		}

		// Pack into chunks.
		var chunks []*serverpb.DrainStatsChunk
		for i := 0; i < len(stmts); i += mix.chunkSize {
			end := i + mix.chunkSize
			if end > len(stmts) {
				end = len(stmts)
			}
			chunks = append(chunks, &serverpb.DrainStatsChunk{
				Statements: stmts[i:end],
			})
		}
		for i := 0; i < len(txns); i += mix.chunkSize {
			end := i + mix.chunkSize
			if end > len(txns) {
				end = len(txns)
			}
			chunks = append(chunks, &serverpb.DrainStatsChunk{
				Transactions: txns[i:end],
			})
		}
		out[nodeID] = chunks
	}
	return out
}

func sampleStmtStats() appstatspb.StatementStatistics {
	return appstatspb.StatementStatistics{
		Count:             1,
		FirstAttemptCount: 1,
		MaxRetries:        0,
		NumRows:           appstatspb.NumericStat{Mean: 10, SquaredDiffs: 1},
		ParseLat:          appstatspb.NumericStat{Mean: 0.001, SquaredDiffs: 0},
		PlanLat:           appstatspb.NumericStat{Mean: 0.005, SquaredDiffs: 0},
		RunLat:            appstatspb.NumericStat{Mean: 0.020, SquaredDiffs: 0},
		ServiceLat:        appstatspb.NumericStat{Mean: 0.026, SquaredDiffs: 0},
		OverheadLat:       appstatspb.NumericStat{Mean: 0, SquaredDiffs: 0},
		BytesRead:         appstatspb.NumericStat{Mean: 1024, SquaredDiffs: 0},
		RowsRead:          appstatspb.NumericStat{Mean: 100, SquaredDiffs: 0},
	}
}

func sampleTxnStats() appstatspb.TransactionStatistics {
	return appstatspb.TransactionStatistics{
		Count:      1,
		MaxRetries: 0,
		NumRows:    appstatspb.NumericStat{Mean: 10, SquaredDiffs: 1},
		ServiceLat: appstatspb.NumericStat{Mean: 0.030, SquaredDiffs: 0},
	}
}

func makeFakeStatusServer(numNodes int, mix fingerprintMix) *fakeStatusServer {
	chunks := buildSyntheticChunks(numNodes, mix)
	nodes := make([]roachpb.NodeID, 0, numNodes)
	for n := 0; n < numNodes; n++ {
		nodes = append(nodes, roachpb.NodeID(n+1))
	}
	return &fakeStatusServer{nodes: nodes, chunks: chunks}
}

// BenchmarkContainerDrainStats benchmarks ssmemstorage.Container.DrainStats
// directly — the per-node hot path that runs N times per coordinated flush.
// Slice pre-sizing in this function shows up here even though it doesn't
// surface in the merger benchmarks (which use synthetic chunks).
func BenchmarkContainerDrainStats(b *testing.B) {
	type spec struct {
		name             string
		stmtFingerprints int
		txnFingerprints  int
	}
	for _, s := range []spec{
		{name: "100fp", stmtFingerprints: 100, txnFingerprints: 30},
		{name: "1000fp", stmtFingerprints: 1000, txnFingerprints: 300},
		{name: "5000fp", stmtFingerprints: 5000, txnFingerprints: 1500},
	} {
		b.Run(s.name, func(b *testing.B) {
			ctx := context.Background()
			st := cluster.MakeTestingClusterSettings()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				container := ssmemstorage.New(st, nil /* uniqueServerCounts */, nil /* mon */, "bench" /* appName */, nil /* knobs */)
				for j := 0; j < s.stmtFingerprints; j++ {
					_ = container.RecordStatement(ctx, &sqlstats.RecordedStmtStats{
						Query:                    "SELECT 1",
						App:                      "bench",
						FingerprintID:            appstatspb.StmtFingerprintID(j + 1),
						PlanHash:                 uint64(j + 1),
						TransactionFingerprintID: appstatspb.TransactionFingerprintID(j + 1),
					})
				}
				for j := 0; j < s.txnFingerprints; j++ {
					_ = container.RecordTransaction(ctx, &sqlstats.RecordedTxnStats{
						FingerprintID: appstatspb.TransactionFingerprintID(j + 1),
					})
				}
				b.StartTimer()
				_, _ = container.DrainStats(ctx)
			}
		})
	}
}

// BenchmarkInstanceCollectorMerge measures Collect's merge path. The
// coordinator runs only when sqlstats.GatewayNodeEnabled is false (see
// CoordinatedFlushEnabled), so every source's stats collapse onto the
// same fingerprint key and the existing.Stats.Add path is the hot loop.
func BenchmarkInstanceCollectorMerge(b *testing.B) {
	type spec struct {
		name     string
		numNodes int
		mix      fingerprintMix
	}
	for _, s := range []spec{
		{
			name:     "small_3node_100fp",
			numNodes: 3,
			mix:      fingerprintMix{stmtsPerNode: 100, txnsPerNode: 30, uniqueFraction: 0.1, chunkSize: 1000},
		},
		{
			name:     "medium_10node_1000fp",
			numNodes: 10,
			mix:      fingerprintMix{stmtsPerNode: 1000, txnsPerNode: 300, uniqueFraction: 0.1, chunkSize: 1000},
		},
		{
			name:     "large_20node_5000fp",
			numNodes: 20,
			mix:      fingerprintMix{stmtsPerNode: 5000, txnsPerNode: 1500, uniqueFraction: 0.1, chunkSize: 1000},
		},
		{
			name:     "huge_50node_5000fp",
			numNodes: 50,
			mix:      fingerprintMix{stmtsPerNode: 5000, txnsPerNode: 1500, uniqueFraction: 0.1, chunkSize: 1000},
		},
	} {
		b.Run(s.name, func(b *testing.B) {
			ss := makeFakeStatusServer(s.numNodes, s.mix)
			ctx := context.Background()
			// Use the steady-state hint (matches what RunCoordinatedFlush
			// would feed in after the first cycle) so the bench measures
			// merge cost, not initial map growth.
			collector := persistedsqlstats.NewInstanceCollector(ss,
				s.mix.stmtsPerNode, s.mix.txnsPerNode)

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, err := collector.Collect(ctx)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkRunCoordinatedFlush_E2E exercises the entire coordinated flush
// path on a real multi-node testcluster: drain RPCs travel through gRPC,
// stats land in system.statement_statistics / system.transaction_statistics,
// and the per-node flush loop is suppressed so the coordinated flush owns
// the write path.
//
// This is the bench to validate optimizations that don't surface in the
// in-memory merger bench: write parallelism, local-node short-circuiting,
// and channel-vs-mutex fanin under real I/O latency.
//
// Cluster startup is ~5–10s so each sub-bench should run with low b.N
// (the 1x default works well for measuring a single end-to-end flush).
func BenchmarkRunCoordinatedFlush_E2E(b *testing.B) {
	skip.UnderShort(b)
	defer log.Scope(b).Close(b)

	type spec struct {
		name             string
		numNodes         int
		stmtFingerprints int
		txnFingerprints  int
	}
	specs := []spec{
		{name: "3node_1000fp", numNodes: 3, stmtFingerprints: 1000, txnFingerprints: 300},
		{name: "6node_1000fp", numNodes: 6, stmtFingerprints: 1000, txnFingerprints: 300},
		{name: "6node_5000fp", numNodes: 6, stmtFingerprints: 5000, txnFingerprints: 1500},
	}
	for _, s := range specs {
		b.Run(s.name, func(b *testing.B) {
			ctx := context.Background()
			tc := testcluster.StartTestCluster(b, s.numNodes, base.TestClusterArgs{
				ServerArgs: base.TestServerArgs{
					Knobs: base.TestingKnobs{
						SQLStatsKnobs: &sqlstats.TestingKnobs{
							SynchronousSQLStats: true,
						},
					},
				},
			})
			defer tc.Stopper().Stop(ctx)

			// Make sure no per-node flush loop drains stats from under us
			// between populate() and RunCoordinatedFlush(). The coordinated
			// setting causes per-node flush to early-return.
			sqlConn := sqlutils.MakeSQLRunner(tc.ServerConn(0))
			sqlConn.Exec(b, "SET CLUSTER SETTING sql.stats.flush.coordinated.enabled = 'true'")

			// Cache references so populate() doesn't re-look-them up each time.
			containers := make([]*ssmemstorage.Container, s.numNodes)
			for i := 0; i < s.numNodes; i++ {
				localStats := tc.Server(i).SQLServer().(*sql.Server).GetLocalSQLStatsProvider()
				containers[i] = localStats.GetApplicationStats(fmt.Sprintf("bench_e2e_%d", i))
			}

			persisted := tc.Server(0).SQLServer().(*sql.Server).GetSQLStatsProvider()
			stopper := tc.Server(0).Stopper()

			populate := func() {
				for i, c := range containers {
					app := fmt.Sprintf("bench_e2e_%d", i)
					for j := 0; j < s.stmtFingerprints; j++ {
						_ = c.RecordStatement(ctx, &sqlstats.RecordedStmtStats{
							Query:                    "SELECT _",
							App:                      app,
							FingerprintID:            appstatspb.StmtFingerprintID(j + 1),
							PlanHash:                 uint64(j + 1),
							TransactionFingerprintID: appstatspb.TransactionFingerprintID(j + 1),
						})
					}
					for j := 0; j < s.txnFingerprints; j++ {
						_ = c.RecordTransaction(ctx, &sqlstats.RecordedTxnStats{
							FingerprintID: appstatspb.TransactionFingerprintID(j + 1),
						})
					}
				}
			}

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				populate()
				b.StartTimer()
				if err := persisted.RunCoordinatedFlush(ctx, stopper); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
