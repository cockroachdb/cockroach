// Copyright 2022 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package backuputils

import (
	"context"
	gosql "database/sql"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	_ "github.com/cockroachdb/cockroach/pkg/ccl/backupccl/backupbase" // imported for cluster settings.
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/workload/bank"
	"github.com/cockroachdb/cockroach/pkg/workload/workloadsql"
	"github.com/cockroachdb/logtags"
)

const (
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
	if len(params.ServerArgsPerNode) > 0 {
		for i := range params.ServerArgsPerNode {
			param := params.ServerArgsPerNode[i]
			param.ExternalIODir = dir
			param.UseDatabase = "data"
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
