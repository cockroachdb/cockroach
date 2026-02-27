// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql_test

import (
	"context"
	"fmt"
	"maps"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/sql/appstatspb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats/persistedsqlstats/sqlstatsutil"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/stretchr/testify/require"
)

// TestWorkloadIDPropagation verifies that SQL statements produce KV
// BatchRequests whose WorkloadID matches the statement's fingerprint_id
// in crdb_internal.cluster_statement_statistics.
func TestWorkloadIDPropagation(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	var mu syncutil.Mutex
	seenWorkloadIDs := make(map[uint64]struct{})

	testingRequestFilter := func(_ context.Context, ba *kvpb.BatchRequest) *kvpb.Error {
		if ba.Header.WorkloadID != 0 {
			mu.Lock()
			seenWorkloadIDs[ba.Header.WorkloadID] = struct{}{}
			mu.Unlock()
		}
		return nil
	}

	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			Store: &kvserver.StoreTestingKnobs{
				TestingRequestFilter: testingRequestFilter,
			},
			SQLStatsKnobs: &sqlstats.TestingKnobs{
				SynchronousSQLStats: true,
			},
		},
	})
	defer s.Stopper().Stop(ctx)

	runner := sqlutils.MakeSQLRunner(db)

	const appName = "workload_id_test"
	runner.Exec(t, "SET application_name = $1", appName)
	runner.Exec(t, "CREATE TABLE t (id INT PRIMARY KEY, v INT)")
	runner.Exec(t, "INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)")

	clearCaptured := func() {
		mu.Lock()
		seenWorkloadIDs = make(map[uint64]struct{})
		mu.Unlock()
	}

	getCaptured := func() map[uint64]struct{} {
		mu.Lock()
		defer mu.Unlock()
		result := make(map[uint64]struct{}, len(seenWorkloadIDs))
		maps.Copy(result, seenWorkloadIDs)
		return result
	}

	testCases := []struct {
		name           string
		query          string
		querySubstring string
	}{{
		name:           "select",
		query:          "SELECT * FROM t WHERE id = 1",
		querySubstring: "SELECT * FROM t WHERE",
	}, {
		name:           "insert",
		query:          "INSERT INTO t VALUES (10, 100)",
		querySubstring: "INSERT INTO t VALUES",
	}, {
		name:           "update",
		query:          "UPDATE t SET v = 200 WHERE id = 1",
		querySubstring: "UPDATE t SET v",
	}, {
		name:           "delete",
		query:          "DELETE FROM t WHERE id = 3",
		querySubstring: "DELETE FROM t WHERE",
	}}

	// assertWorkloadIDMatch verifies that at least one captured workload ID
	// matches a fingerprint from statement_statistics for the given query
	// substring.
	assertWorkloadIDMatch := func(
		t *testing.T,
		r *sqlutils.SQLRunner,
		app string,
		query string,
		querySubstring string,
		captured map[uint64]struct{},
	) {
		t.Helper()
		require.NotEmpty(t, captured,
			"expected non-zero workload IDs in KV batch requests for %q", query)

		rows := r.Query(t,
			`SELECT encode(fingerprint_id, 'hex')
			 FROM crdb_internal.cluster_statement_statistics
			 WHERE app_name = $1
			   AND strpos(metadata->>'query', $2) > 0`,
			app, querySubstring,
		)
		defer rows.Close()
		var hexFPs []string
		for rows.Next() {
			var hexFP string
			require.NoError(t, rows.Scan(&hexFP))
			hexFPs = append(hexFPs, hexFP)
		}
		require.NotEmpty(t, hexFPs,
			"no matching fingerprints in statement_statistics for %q", querySubstring)

		statsFingerprints := make(map[string]struct{}, len(hexFPs))
		for _, fp := range hexFPs {
			statsFingerprints[fp] = struct{}{}
		}
		found := false
		for wid := range captured {
			encoded := sqlstatsutil.EncodeStmtFingerprintIDToString(
				appstatspb.StmtFingerprintID(wid),
			)
			if _, ok := statsFingerprints[encoded]; ok {
				found = true
				break
			}
		}
		require.True(t, found,
			"no captured workload ID matched any fingerprint in statement_statistics; "+
				"stats fingerprints: %v, captured IDs: %s",
			hexFPs, capturedIDsString(captured))
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			clearCaptured()
			runner.Exec(t, tc.query)
			captured := getCaptured()
			assertWorkloadIDMatch(t, runner, appName, tc.query, tc.querySubstring, captured)
		})
	}

	// Test that the KV Streamer path (used by lookup joins) propagates the
	// workload ID.
	t.Run("streamer", func(t *testing.T) {
		runner.Exec(t, "CREATE TABLE t2 (id INT PRIMARY KEY, data INT)")
		runner.Exec(t, "INSERT INTO t2 VALUES (10, 100), (20, 200), (30, 300)")
		runner.Exec(t, "SET streamer_enabled = true")
		clearCaptured()
		runner.Exec(t, "SELECT count(*) FROM t INNER LOOKUP JOIN t2 ON t.v = t2.id")
		captured := getCaptured()
		assertWorkloadIDMatch(t, runner, appName,
			"SELECT count(*) FROM t INNER LOOKUP JOIN t2 ON t.v = t2.id",
			"LOOKUP JOIN t2", captured)
	})

	// Test that the columnar/vectorized scan path (cFetcher / ColBatchScan)
	// propagates the workload ID.
	t.Run("columnar_scan", func(t *testing.T) {
		runner.Exec(t, "SET vectorize = on")
		clearCaptured()
		runner.Exec(t, "SELECT * FROM t WHERE v > 0")
		captured := getCaptured()
		assertWorkloadIDMatch(t, runner, appName,
			"SELECT * FROM t WHERE v > 0",
			"SELECT * FROM t WHERE v", captured)
	})

	// Test that DistSQL remote flows propagate the workload ID through leaf
	// txn creation in distsql/server.go.
	t.Run("distsql_remote", func(t *testing.T) {
		var clusterMu syncutil.Mutex
		clusterSeenIDs := make(map[uint64]struct{})

		clusterFilter := func(_ context.Context, ba *kvpb.BatchRequest) *kvpb.Error {
			if ba.Header.WorkloadID != 0 {
				clusterMu.Lock()
				clusterSeenIDs[ba.Header.WorkloadID] = struct{}{}
				clusterMu.Unlock()
			}
			return nil
		}

		tc := serverutils.StartCluster(t, 3, base.TestClusterArgs{
			ServerArgs: base.TestServerArgs{
				Knobs: base.TestingKnobs{
					Store: &kvserver.StoreTestingKnobs{
						TestingRequestFilter: clusterFilter,
					},
					SQLStatsKnobs: &sqlstats.TestingKnobs{
						SynchronousSQLStats: true,
					},
				},
			},
		})
		defer tc.Stopper().Stop(ctx)

		clusterDB := tc.ServerConn(0)
		clusterRunner := sqlutils.MakeSQLRunner(clusterDB)

		const clusterAppName = "workload_id_test_distsql"
		clusterRunner.Exec(t, "SET application_name = $1", clusterAppName)
		clusterRunner.Exec(t, "CREATE TABLE td (id INT PRIMARY KEY, v INT)")
		clusterRunner.Exec(t, "INSERT INTO td SELECT generate_series(1, 30), generate_series(1, 30)")

		// Split the table into 3 ranges and relocate them to different nodes.
		clusterRunner.Exec(t, "ALTER TABLE td SPLIT AT VALUES (10), (20)")
		clusterRunner.ExecSucceedsSoon(t, fmt.Sprintf(
			"ALTER TABLE td EXPERIMENTAL_RELOCATE VALUES (ARRAY[%d], 1), (ARRAY[%d], 10), (ARRAY[%d], 20)",
			tc.Server(0).GetFirstStoreID(),
			tc.Server(1).GetFirstStoreID(),
			tc.Server(2).GetFirstStoreID(),
		))

		// Populate the range cache so the gateway knows where the ranges are.
		clusterRunner.Exec(t, "SELECT count(*) FROM td")

		// Force DistSQL to create remote flows with leaf txns.
		clusterRunner.Exec(t, "SET distsql = always")

		clusterMu.Lock()
		clusterSeenIDs = make(map[uint64]struct{})
		clusterMu.Unlock()

		clusterRunner.Exec(t, "SELECT * FROM td WHERE v > 0")

		clusterMu.Lock()
		captured := make(map[uint64]struct{}, len(clusterSeenIDs))
		maps.Copy(captured, clusterSeenIDs)
		clusterMu.Unlock()

		assertWorkloadIDMatch(t, clusterRunner, clusterAppName,
			"SELECT * FROM td WHERE v > 0",
			"SELECT * FROM td WHERE v", captured)
	})
}

// capturedIDsString formats captured workload IDs for diagnostic output.
func capturedIDsString(ids map[uint64]struct{}) string {
	parts := make([]string, 0, len(ids))
	for id := range ids {
		hex := sqlstatsutil.EncodeStmtFingerprintIDToString(appstatspb.StmtFingerprintID(id))
		parts = append(parts, fmt.Sprintf("%d(%s)", id, hex))
	}
	return strings.Join(parts, ", ")
}
