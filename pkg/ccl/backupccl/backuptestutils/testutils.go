// Copyright 2022 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package backuptestutils

import (
	"context"
	gosql "database/sql"
	"reflect"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	_ "github.com/cockroachdb/cockroach/pkg/ccl/backupccl/backupbase" // imported for cluster settings.
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/workload/bank"
	"github.com/cockroachdb/cockroach/pkg/workload/workloadsql"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
	"github.com/kr/pretty"
	"github.com/stretchr/testify/require"
)

const (
	// SingleNode is the size of a single node test cluster.
	SingleNode = 1
	// MultiNode is the size of a multi node test cluster.
	MultiNode = 3
)

// InitManualReplication calls tc.ToggleReplicateQueues(false).
//
// Note that the test harnesses that use this typically call
// tc.WaitForFullReplication before calling this method,
// so up-replication has usually already taken place.
func InitManualReplication(tc *testcluster.TestCluster) {
	tc.ToggleReplicateQueues(false)
}

// BackupDestinationTestSetup sets up a test cluster with the supplied
// arguments.
func BackupDestinationTestSetup(
	t testing.TB, clusterSize int, numAccounts int, init func(*testcluster.TestCluster),
) (tc *testcluster.TestCluster, sqlDB *sqlutils.SQLRunner, tempDir string, cleanup func()) {
	return backupRestoreTestSetupWithParams(t, clusterSize, numAccounts, init, base.TestClusterArgs{})
}

// TODO(ssd): This function is very similar a function in backupccl with the same name.
// We should only have 1.
func backupRestoreTestSetupWithParams(
	t testing.TB,
	clusterSize int,
	numAccounts int,
	init func(tc *testcluster.TestCluster),
	params base.TestClusterArgs,
) (tc *testcluster.TestCluster, sqlDB *sqlutils.SQLRunner, tempDir string, cleanup func()) {
	ctx := logtags.AddTag(context.Background(), "backup-restore-test-setup", nil)

	dir, dirCleanupFn := testutils.TempDir(t)
	params.ServerArgs.ExternalIODir = dir
	params.ServerArgs.UseDatabase = "data"
	params.ServerArgs.DisableDefaultTestTenant = true
	if len(params.ServerArgsPerNode) > 0 {
		for i := range params.ServerArgsPerNode {
			param := params.ServerArgsPerNode[i]
			param.ExternalIODir = dir
			param.UseDatabase = "data"
			param.DisableDefaultTestTenant = true
			params.ServerArgsPerNode[i] = param
		}
	}

	tc = testcluster.StartTestCluster(t, clusterSize, params)
	init(tc)

	const payloadSize = 100
	splits := 10
	if numAccounts == 0 {
		splits = 0
	}
	bankData := bank.FromConfig(numAccounts, numAccounts, payloadSize, splits)

	sqlDB = sqlutils.MakeSQLRunner(tc.Conns[0])

	// Set the max buffer size to something low to prevent backup/restore tests
	// from hitting OOM errors. If any test cares about this setting in
	// particular, they will override it inline after setting up the test cluster.
	sqlDB.Exec(t, `SET CLUSTER SETTING bulkio.backup.merge_file_buffer_size = '16MiB'`)

	sqlDB.Exec(t, `CREATE DATABASE data`)
	l := workloadsql.InsertsDataLoader{BatchSize: 1000, Concurrency: 4}
	if _, err := workloadsql.Setup(ctx, sqlDB.DB.(*gosql.DB), bankData, l); err != nil {
		t.Fatalf("%+v", err)
	}

	if err := tc.WaitForFullReplication(); err != nil {
		t.Fatal(err)
	}

	cleanupFn := func() {
		tc.Stopper().Stop(ctx) // cleans up in memory storage's auxiliary dirs
		dirCleanupFn()         // cleans up dir, which is the nodelocal:// storage
	}

	return tc, sqlDB, dir, cleanupFn
}

// BackupRestoreTestSetup creates and returns a pre-populated testing
// environment that can be used in backup and restore tests.
func BackupRestoreTestSetup(
	t testing.TB, clusterSize int, numAccounts int, init func(*testcluster.TestCluster),
) (tc *testcluster.TestCluster, sqlDB *sqlutils.SQLRunner, tempDir string, cleanup func()) {
	return backupRestoreTestSetupWithParams(t, clusterSize, numAccounts, init, base.TestClusterArgs{})
}

// VerifyBackupRestoreStatementResult conducts a Backup or Restore and verifies
// it was properly written to the jobs table
func VerifyBackupRestoreStatementResult(
	t *testing.T, sqlDB *sqlutils.SQLRunner, query string, args ...interface{},
) error {
	t.Helper()
	rows := sqlDB.Query(t, query, args...)

	columns, err := rows.Columns()
	if err != nil {
		return err
	}
	if e, a := columns, []string{
		"job_id", "status", "fraction_completed", "rows", "index_entries", "bytes",
	}; !reflect.DeepEqual(e, a) {
		return errors.Errorf("unexpected columns:\n%s", strings.Join(pretty.Diff(e, a), "\n"))
	}

	type job struct {
		id                int64
		status            string
		fractionCompleted float32
	}

	var expectedJob job
	var actualJob job
	var unused int64

	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return err
		}
		return errors.New("zero rows in result")
	}
	if err := rows.Scan(
		&actualJob.id, &actualJob.status, &actualJob.fractionCompleted, &unused, &unused, &unused,
	); err != nil {
		return err
	}
	if rows.Next() {
		return errors.New("more than one row in result")
	}

	sqlDB.QueryRow(t,
		`SELECT job_id, status, fraction_completed FROM crdb_internal.jobs WHERE job_id = $1`, actualJob.id,
	).Scan(
		&expectedJob.id, &expectedJob.status, &expectedJob.fractionCompleted,
	)
	require.Equal(t, expectedJob, actualJob, "result does not match system.jobs")

	return nil
}

// CheckForInvalidDescriptors returns an error if there exists any descriptors in
// the crdb_internal.invalid_objects virtual table.
func CheckForInvalidDescriptors(t testing.TB, sqlDB *gosql.DB) {
	// Ensure the connection to the database is still open.
	if err := sqlDB.Ping(); err != nil {
		t.Logf("Warning: Could not check for invalid descriptors: %v", err)
		return
	}
	// Because crdb_internal.invalid_objects is a virtual table, by default, the
	// query will take a lease on the database sqlDB is connected to and only run
	// the query on the given database. The "" prefix prevents this lease
	// acquisition and allows the query to fetch all descriptors in the cluster.
	rows, err := sqlDB.Query(`SELECT id, obj_name, error FROM "".crdb_internal.invalid_objects`)
	if err != nil {
		t.Fatal(err)
	}
	invalidIDs, err := sqlutils.RowsToDataDrivenOutput(rows)
	if err != nil {
		t.Error(err)
	}
	if invalidIDs != "" {
		t.Fatalf("the following descriptor ids are invalid\n%v", invalidIDs)
	}
	t.Log("no Invalid Descriptors")
}
