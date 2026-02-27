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

	// getStatsFingerprintHexes returns all hex-encoded fingerprint_ids from
	// crdb_internal.cluster_statement_statistics for statements whose
	// fingerprint query contains the given substring under our app name.
	// Uses strpos instead of LIKE because fingerprint queries contain
	// underscores which are LIKE wildcards.
	getStatsFingerprintHexes := func(t *testing.T, querySubstring string) []string {
		t.Helper()
		rows := runner.Query(t,
			`SELECT encode(fingerprint_id, 'hex')
			 FROM crdb_internal.cluster_statement_statistics
			 WHERE app_name = $1
			   AND strpos(metadata->>'query', $2) > 0`,
			appName, querySubstring,
		)
		defer rows.Close()
		var hexFPs []string
		for rows.Next() {
			var hexFP string
			require.NoError(t, rows.Scan(&hexFP))
			hexFPs = append(hexFPs, hexFP)
		}
		return hexFPs
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

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			clearCaptured()
			runner.Exec(t, tc.query)
			captured := getCaptured()
			require.NotEmpty(t, captured,
				"expected non-zero workload IDs in KV batch requests for %q", tc.query)

			// Look up fingerprint_ids from statement_statistics for
			// statements matching our query pattern.
			hexFPs := getStatsFingerprintHexes(t, tc.querySubstring)
			require.NotEmpty(t, hexFPs,
				"no matching fingerprints in statement_statistics for %q", tc.querySubstring)

			// Verify that one of the captured workload IDs, when encoded the
			// same way as the fingerprint_id column, matches one of the
			// fingerprints.
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
		})
	}
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
