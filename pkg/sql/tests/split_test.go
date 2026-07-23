// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// TestSplitOnTableBoundaries verifies that ranges get split
// as new tables get created.
func TestSplitOnTableBoundaries(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())

	// speeds up test
	{
		sysDB := sqlutils.MakeSQLRunner(s.SystemLayer().SQLConn(t))
		sysDB.Exec(t, `SET CLUSTER SETTING kv.closed_timestamp.target_duration = '100ms'`)
	}

	sqlConn := sqlutils.MakeSQLRunner(sqlDB)
	sqlConn.Exec(t, `CREATE DATABASE test`)

	// We split up to the largest allocated descriptor ID, if it's a table.
	// Ensure that no split happens if a database is created.
	var rangeCount int
	require.NoError(t, sqlDB.QueryRow("SELECT count(*) FROM [SHOW RANGES FROM DATABASE test]").Scan(&rangeCount))
	require.Equalf(t, rangeCount, 0, "expected 0 splits, found %d", rangeCount)

	// Let's create a table.
	sqlConn.Exec(t, `CREATE TABLE test.test (k INT PRIMARY KEY, v INT)`)

	// Get the KV key for the start of the table.
	tableID := sqlutils.QueryTableID(t, sqlDB, "test", "public", "test")
	expectedSplit := fmt.Sprintf("%s", s.ApplicationLayer().Codec().TablePrefix(tableID))

	// Wait for the span config update to trigger the split.
	testutils.SucceedsSoon(t, func() error {
		var rangeCount int
		require.NoError(t, sqlDB.QueryRow("SELECT count(*) FROM [SHOW RANGES FROM DATABASE test]").Scan(&rangeCount))
		if rangeCount != 1 {
			return errors.Errorf("expected 1 split, found %d", rangeCount)
		}

		// Verify the split key is at the table boundary.
		var startKey string
		require.NoError(t, sqlDB.QueryRow("SELECT start_key FROM [SHOW RANGES FROM DATABASE test]").Scan(&startKey))
		if expectedSplit != startKey {
			return errors.Errorf("expected %s found %s", expectedSplit, startKey)
		}
		return nil
	})
}
