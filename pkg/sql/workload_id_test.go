// Copyright 2026 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/multitenant/tenantcapabilitiespb"
	"github.com/cockroachdb/cockroach/pkg/obs/workloadid"
	"github.com/cockroachdb/cockroach/pkg/sql/appstatspb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats/persistedsqlstats/sqlstatsutil"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/admission"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/stretchr/testify/require"
)

// TestWorkloadIDPropagation verifies that workload IDs (statement fingerprint
// IDs) are correctly propagated through three paths:
//
//  1. Transaction → KV BatchRequest headers (tested via TestingRequestFilter).
//  2. Row fetcher / KV streamer → admission control WorkInfo (tested via
//     WorkQueueAdmitInterceptor). This verifies that the fetcher and streamer
//     init args correctly pass workloadID to admission.
//  3. DistSQL remote flows → leaf transaction via EvalContext serialization
//     (tested via TestingRequestFilter on a multi-node cluster).
func TestWorkloadIDPropagation(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	var batchMu syncutil.Mutex
	seenBatchIDs := make(map[uint64]struct{})

	var admissionMu syncutil.Mutex
	seenAdmissionIDs := make(map[uint64]struct{})
	seenAdmissionTypes := make(map[uint64]workloadid.WorkloadType)

	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			Store: &kvserver.StoreTestingKnobs{
				TestingRequestFilter: func(_ context.Context, ba *kvpb.BatchRequest) *kvpb.Error {
					if ba.Header.WorkloadID != 0 {
						batchMu.Lock()
						seenBatchIDs[ba.Header.WorkloadID] = struct{}{}
						batchMu.Unlock()
					}
					return nil
				},
			},
			AdmissionControl: &admission.TestingKnobs{
				WorkQueueAdmitInterceptor: func(info admission.WorkInfo) {
					if info.WorkloadID != 0 {
						admissionMu.Lock()
						seenAdmissionIDs[info.WorkloadID] = struct{}{}
						seenAdmissionTypes[info.WorkloadID] = info.WorkloadType
						admissionMu.Unlock()
					}
				},
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

	clearBatchIDs := func() {
		batchMu.Lock()
		seenBatchIDs = make(map[uint64]struct{})
		batchMu.Unlock()
	}
	getBatchIDs := func() map[uint64]struct{} {
		batchMu.Lock()
		defer batchMu.Unlock()
		result := make(map[uint64]struct{}, len(seenBatchIDs))
		maps.Copy(result, seenBatchIDs)
		return result
	}
	clearAdmissionIDs := func() {
		admissionMu.Lock()
		seenAdmissionIDs = make(map[uint64]struct{})
		seenAdmissionTypes = make(map[uint64]workloadid.WorkloadType)
		admissionMu.Unlock()
	}
	getAdmissionIDs := func() map[uint64]struct{} {
		admissionMu.Lock()
		defer admissionMu.Unlock()
		result := make(map[uint64]struct{}, len(seenAdmissionIDs))
		maps.Copy(result, seenAdmissionIDs)
		return result
	}
	getAdmissionTypes := func() map[uint64]workloadid.WorkloadType {
		admissionMu.Lock()
		defer admissionMu.Unlock()
		result := make(map[uint64]workloadid.WorkloadType, len(seenAdmissionTypes))
		maps.Copy(result, seenAdmissionTypes)
		return result
	}

	// assertWorkloadIDMatch verifies that at least one captured workload ID
	// matches a fingerprint from statement_statistics for the given query
	// substring.
	assertWorkloadIDMatch := func(
		t *testing.T,
		r *sqlutils.SQLRunner,
		app string,
		querySubstring string,
		captured map[uint64]struct{},
	) {
		t.Helper()
		require.NotEmpty(t, captured, "expected non-zero workload IDs for %q", querySubstring)

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

	// Verify that txn.SetWorkloadInfo propagates to KV BatchRequest headers.
	// All fetcher/streamer variants share this same txn-level path, so a
	// single SELECT suffices.
	t.Run("batch_request", func(t *testing.T) {
		clearBatchIDs()
		runner.Exec(t, "SELECT * FROM t WHERE id = 1")
		captured := getBatchIDs()
		assertWorkloadIDMatch(t, runner, appName, "SELECT * FROM t WHERE", captured)
	})

	// Verify that fetcher and streamer init args correctly propagate
	// workloadID into admission control WorkInfo structs. This is distinct
	// from the batch_request test: the txn already carries workloadID
	// regardless of the fetcher, but the admission WorkInfo is only populated
	// if the fetcher/streamer received workloadID through its init args.
	t.Run("response_admission", func(t *testing.T) {
		// Row fetcher path: a basic scan goes through
		// txnKVFetcher.responseAdmissionQ.Admit() with WorkInfo.WorkloadID.
		t.Run("row_fetcher", func(t *testing.T) {
			clearAdmissionIDs()
			runner.Exec(t, "SELECT * FROM t WHERE v > 0")
			captured := getAdmissionIDs()
			assertWorkloadIDMatch(t, runner, appName,
				"SELECT * FROM t WHERE v", captured)
			// Verify WorkloadType is set (not UNKNOWN) for all captured IDs.
			for wid, wtype := range getAdmissionTypes() {
				require.NotEqual(t, workloadid.WorkloadTypeUnknown, wtype,
					"admission WorkInfo for workload ID %d has WorkloadType=UNKNOWN; "+
						"fetcher should propagate the type", wid)
			}
		})

		// Streamer path: a lookup join goes through the KV streamer's
		// responseAdmissionQ.Admit() with WorkInfo.WorkloadID.
		t.Run("streamer", func(t *testing.T) {
			runner.Exec(t, "CREATE TABLE IF NOT EXISTS t2 (id INT PRIMARY KEY, data INT)")
			runner.Exec(t, "INSERT INTO t2 VALUES (10, 100), (20, 200), (30, 300)")
			clearAdmissionIDs()
			runner.Exec(t, "SELECT count(*) FROM t INNER LOOKUP JOIN t2 ON t.v = t2.id")
			captured := getAdmissionIDs()
			assertWorkloadIDMatch(t, runner, appName, "LOOKUP JOIN t2", captured)
			// Verify WorkloadType is set (not UNKNOWN) for all captured IDs.
			for wid, wtype := range getAdmissionTypes() {
				require.NotEqual(t, workloadid.WorkloadTypeUnknown, wtype,
					"admission WorkInfo for workload ID %d has WorkloadType=UNKNOWN; "+
						"streamer should propagate the type", wid)
			}
		})
	})

	// Test that DistSQL remote flows propagate the workload ID through leaf
	// txn creation in distsql/server.go. This exercises a genuinely different
	// code path: the workloadID is serialized in the EvalContext protobuf,
	// transmitted to remote nodes, and set on the leaf transaction.
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
			ReplicationMode: base.ReplicationManual,
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

		if tc.DefaultTenantDeploymentMode().IsExternal() {
			tc.GrantTenantCapabilities(ctx, t, serverutils.TestTenantID(),
				map[tenantcapabilitiespb.ID]string{tenantcapabilitiespb.CanAdminRelocateRange: "true"})
		}

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
