// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package bulkingest

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestSplitAndScatterSpans(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	// Create a 3-node test cluster
	tc := serverutils.StartCluster(t /* numNodes */, 3,
		base.TestClusterArgs{
			ServerArgs: base.TestServerArgs{
				DefaultTestTenant: base.TestControlsTenantsExplicitly,
			},
		})
	defer tc.Stopper().Stop(ctx)

	// Create a connection to the first node
	db := tc.Server(0).DB()
	sqlDB := tc.ServerConn(0)
	sqlRunner := sqlutils.MakeSQLRunner(sqlDB)

	// Create the test database and table
	sqlRunner.Exec(t, "CREATE DATABASE test")
	sqlRunner.Exec(t, "CREATE TABLE test.kv (key INT PRIMARY KEY, value INT)")

	// Get the table descriptor to build keys
	var tableID descpb.ID
	sqlRunner.QueryRow(t, `
		SELECT table_id FROM crdb_internal.tables 
		WHERE database_name = 'test' AND name = 'kv'
	`).Scan(&tableID)

	// Get the codec from the server
	testServer := tc.Server(0)
	codec := testServer.Codec()

	// Create spans for the table
	var spans []roachpb.Span
	testKeys := []int{100, 200, 300, 400, 500}

	for i := 0; i < len(testKeys)-1; i++ {
		startKey := codec.TablePrefix(uint32(tableID))
		startKey = keys.MakeFamilyKey(startKey, 0) // 0 is the default family ID
		startKey = encoding.EncodeVarintAscending(startKey, int64(testKeys[i]))

		endKey := codec.TablePrefix(uint32(tableID))
		endKey = keys.MakeFamilyKey(endKey, 0) // 0 is the default family ID
		endKey = encoding.EncodeVarintAscending(endKey, int64(testKeys[i+1]))

		spans = append(spans, roachpb.Span{
			Key:    startKey,
			EndKey: endKey,
		})
	}

	// Run split and scatter on the spans
	err := splitAndScatterSpans(ctx, db, spans)
	require.NoError(t, err)

	// Wait for splits to take effect
	// This is necessary because scatter is asynchronous
	waitForSplits(t, sqlRunner, len(spans)+1) // +1 for the initial range

	// Insert data to verify splits
	for _, key := range testKeys {
		sqlRunner.Exec(t, "INSERT INTO test.kv VALUES ($1, $2)", key, key*10)
	}

	// Verify that the splits are at the expected keys
	verifySplitPoints(t, sqlRunner, testKeys[:len(testKeys)-1], tableID)
}

// waitForSplits waits for the expected number of ranges to appear in the table.
func waitForSplits(t *testing.T, sqlRunner *sqlutils.SQLRunner, expectedRanges int) {
	t.Helper()

	require.Eventually(t, func() bool {
		var rangeCount int
		sqlRunner.QueryRow(t, `
			SELECT count(*) FROM [SHOW RANGES FROM TABLE test.kv]
		`).Scan(&rangeCount)
		return rangeCount >= expectedRanges
	}, 30*time.Second, 100*time.Millisecond,
		"timed out waiting for %d ranges", expectedRanges)
}

// verifySplitPoints checks that the splits are at the expected keys.
func verifySplitPoints(
	t *testing.T, sqlRunner *sqlutils.SQLRunner, expectedKeys []int, tableID descpb.ID,
) {
	t.Helper()

	// Query for the actual split points
	rows := sqlRunner.Query(t, `
		SELECT start_pretty 
		FROM [SHOW RANGES FROM TABLE test.kv] r
		JOIN crdb_internal.ranges ON r.range_id = crdb_internal.ranges.range_id
		WHERE start_pretty LIKE '/Table/%/0/%'
		ORDER BY start_pretty
	`)
	defer rows.Close()

	// Collect the actual split points
	var actualSplitPoints []string
	for rows.Next() {
		var splitPoint string
		require.NoError(t, rows.Scan(&splitPoint))
		actualSplitPoints = append(actualSplitPoints, splitPoint)
	}
	require.NoError(t, rows.Err())

	// We expect one split point for each key except possibly the first one
	// (which might be the start of the table)
	require.GreaterOrEqual(t, len(actualSplitPoints), len(expectedKeys)-1,
		"Not enough split points found")

	// Verify that each expected key (except possibly the first) appears in the split points
	for i := 1; i < len(expectedKeys); i++ {
		key := expectedKeys[i]
		found := false

		// Look for this key in the actual split points
		for _, splitPoint := range actualSplitPoints {
			if splitPoint == fmt.Sprintf("/Table/%d/0/%d", tableID, key) {
				found = true
				break
			}
		}

		require.True(t, found, "Expected split point for key %d not found", key)
	}
}
