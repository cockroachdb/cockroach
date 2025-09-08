// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package fingerprintutils_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/testutils/fingerprintutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

// TestFingerprintUtility asserts that the fingerprint helpers in the package can indeed catch
// fingerprint mismatches and return a proper error message.
func TestFingerprintUtility(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)
	db := tc.ServerConn(0)
	sql := sqlutils.MakeSQLRunner(db)

	startTime := hlc.Timestamp{WallTime: timeutil.Now().UnixNano()}
	setupDatabase := func(name string) {
		sql.Exec(t, fmt.Sprintf(`CREATE DATABASE %s`, name))
		tableName := name + ".t1"
		sql.Exec(t, fmt.Sprintf(`CREATE TABLE %s (id INT PRIMARY KEY, s STRING)`, tableName))
		sql.Exec(t, fmt.Sprintf(`INSERT INTO %s VALUES (1, 'x'),(2,'y')`, tableName))
	}
	setupDatabase("d1")
	setupDatabase("d2")

	// Fingerprints should only match on stripped fingerprint.
	fingerprintDatabases := func(optFuncs ...func(*fingerprintutils.FingerprintOption)) error {
		f1Stripped, err := fingerprintutils.FingerprintDatabase(ctx, db, "d1", optFuncs...)
		require.NoError(t, err)

		f2Stripped, err := fingerprintutils.FingerprintDatabase(ctx, db, "d2", optFuncs...)
		require.NoError(t, err)
		return fingerprintutils.CompareDatabaseFingerprints(f1Stripped, f2Stripped)
	}

	require.NoError(t, fingerprintDatabases(fingerprintutils.Stripped()))
	require.ErrorContains(t, fingerprintDatabases(fingerprintutils.StartTime(startTime)),
		`fingerprint mismatch on "t1" table`)

	// Ensure cluster stripped fingerprint mismatch occurs after adding a single row
	// Don't include system db, as it may naturally change over time.
	clusterFingerprint, err := fingerprintutils.FingerprintAllDatabases(ctx, db, false, fingerprintutils.Stripped())
	require.NoError(t, err)
	sql.Exec(t, `INSERT INTO d1.t1 VALUES (3,'z')`)

	clusterFingerprintAfterInsert, err := fingerprintutils.FingerprintAllDatabases(ctx, db, false, fingerprintutils.Stripped())
	require.NoError(t, err)
	require.ErrorContains(t, fingerprintutils.CompareMultipleDatabaseFingerprints(clusterFingerprint,
		clusterFingerprintAfterInsert), `fingerprint mismatch on "t1" table`)

	// Ensure one cannot run nonsensical fingerprint cmds.
	_, err = fingerprintutils.FingerprintDatabase(ctx, db, "d1", fingerprintutils.Stripped(), fingerprintutils.StartTime(startTime))
	require.ErrorContains(t, err, "cannot specify stripped and a start time")

	_, err = fingerprintutils.FingerprintDatabase(ctx, db, "d1", fingerprintutils.Stripped(), fingerprintutils.RevisionHistory())
	require.ErrorContains(t, err, "cannot specify stripped and revision history")
}

// TestFingerprintAllDatabasesWithAOST tests that FingerprintAllDatabases works
// correctly when using AOST (As Of System Time) even when the target
// database/tables are deleted after the AOST timestamp.
func TestFingerprintAllDatabasesWithAOST(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)
	db := tc.ServerConn(0)
	sql := sqlutils.MakeSQLRunner(db)

	rng, _ := randutil.NewTestRand()

	sql.Exec(t, `CREATE DATABASE test_aost`)
	sql.Exec(t, `CREATE TABLE test_aost.table_to_delete (id INT PRIMARY KEY, data STRING)`)
	sql.Exec(t, `INSERT INTO test_aost.table_to_delete VALUES (1, 'test_data')`)

	aostTimestamp := hlc.Timestamp{WallTime: timeutil.Now().UnixNano()}

	sql.Exec(t, `DROP TABLE test_aost.table_to_delete`)
	if rng.Intn(2) == 0 {
		sql.Exec(t, `DROP DATABASE test_aost`)
	}

	// FingerprintAllDatabases with AOST should still work and find the deleted table
	fingerprintsAOST, err := fingerprintutils.FingerprintAllDatabases(ctx, db, false,
		fingerprintutils.AOST(aostTimestamp))
	require.NoError(t, err)

	testAostFingerprints, exists := fingerprintsAOST["test_aost"]
	require.True(t, exists, "test_aost database should be found in fingerprints")

	// Verify that the deleted table is found in the AOST fingerprint
	tableFingerprint, exists := testAostFingerprints["table_to_delete"]
	require.True(t, exists, "table_to_delete should be found in AOST fingerprints")
	require.NotZero(t, tableFingerprint, "fingerprint should be non-zero")
}
