// Copyright 2020 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package backupccl

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// A special jobs.Resumer that simulates interrupted resume by
// aborting resume after restore descriptors are published, and then
// resuming execution again.
var _ jobs.Resumer = &restartAfterPublishDescriptorsResumer{}

type restartAfterPublishDescriptorsResumer struct {
	t       *testing.T
	wrapped *restoreResumer
}

func (r *restartAfterPublishDescriptorsResumer) Resume(
	ctx context.Context, phs interface{}, resultsCh chan<- tree.Datums,
) error {
	e := errors.New("bail out")
	r.wrapped.testingKnobs.afterPublishingDescriptors = func() error {
		return e
	}
	require.Equal(r.t, e, r.wrapped.Resume(ctx, phs, resultsCh))
	r.wrapped.testingKnobs.afterPublishingDescriptors = nil
	return r.wrapped.Resume(ctx, phs, resultsCh)
}

func (r *restartAfterPublishDescriptorsResumer) OnFailOrCancel(
	ctx context.Context, phs interface{},
) error {
	return nil
}

func TestRestorePrivilegesChanged(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	dir, dirCleanupFn := testutils.TempDir(t)
	defer dirCleanupFn()
	params := base.TestClusterArgs{}
	params.ServerArgs.ExternalIODir = dir
	tc := testcluster.StartTestCluster(t, 1, params)
	defer tc.Stopper().Stop(ctx)

	tc.Server(0).JobRegistry().(*jobs.Registry).TestingResumerCreationKnobs = map[jobspb.Type]func(raw jobs.Resumer) jobs.Resumer{
		// Arrange for our special job resumer to be returned.
		jobspb.TypeRestore: func(raw jobs.Resumer) jobs.Resumer {
			return &restartAfterPublishDescriptorsResumer{
				t:       t,
				wrapped: raw.(*restoreResumer),
			}
		},
	}

	runner := sqlutils.MakeSQLRunner(tc.ServerConn(0))
	runner.Exec(t, `
CREATE USER user_to_drop;
CREATE TABLE foo (k INT PRIMARY KEY, v BYTES);
GRANT SELECT ON TABLE foo TO user_to_drop;
BACKUP TABLE foo TO 'nodelocal://0/foo';
DROP TABLE foo;
DROP ROLE user_to_drop;
RESTORE TABLE foo FROM 'nodelocal://0/foo';
`)
}

// TestClusterRestoreFailCleanup tests that a failed RESTORE is cleaned up.
func TestClusterRestoreFailCleanup(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderRace(t, "takes >1 min under race")

	ctx := context.Background()

	blockCh := make(chan struct{})
	defer close(blockCh)
	params := base.TestClusterArgs{}
	params.ServerArgs.Knobs.GCJob = &sql.GCJobTestingKnobs{
		// Disable GC job so that the final check of crdb_internal.tables is
		// guaranteed to not be cleaned up. Although this was never observed by a
		// stress test, it is here for safety.
		RunBeforeResume: func(_ int64) error { <-blockCh; return nil },
	}

	dir, dirCleanupFn := testutils.TempDir(t)
	defer dirCleanupFn()

	params.ServerArgs.ExternalIODir = dir
	tc := testcluster.StartTestCluster(t, 1, params)
	defer tc.Stopper().Stop(ctx)

	sqlDB := sqlutils.MakeSQLRunner(tc.ServerConn(0))
	// Setup the system systemTablesToVerify to ensure that they are copied to the new cluster.
	// Populate system.users.
	for i := 0; i < 1000; i++ {
		sqlDB.Exec(t, fmt.Sprintf("CREATE USER maxroach%d", i))
	}
	sqlDB.Exec(t, `
CREATE DATABASE data;
CREATE TABLE data.bank AS SELECT * FROM generate_series(1,1000);
	`)
	sqlDB.Exec(t, `BACKUP TO 'nodelocal://1/missing-ssts'`)
	// Bugger the backup by removing the SST files. (Note this messes up all of
	// the backups, but there is only one at this point.)
	if err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			t.Fatal(err)
		}
		if info.Name() == BackupManifestName || !strings.HasSuffix(path, ".sst") {
			return nil
		}
		return os.Remove(path)
	}); err != nil {
		t.Fatal(err)
	}

	localFoo := "nodelocal://1/foo"

	// Create a non-corrupted backup.
	// Populate system.jobs.
	// Note: this is not the backup under test, this just serves as a job which
	// should appear in the restore.
	// This job will eventually fail since it will run from a new cluster.
	sqlDB.Exec(t, `BACKUP data.bank TO 'nodelocal://0/throwawayjob'`)
	sqlDB.Exec(t, `BACKUP TO $1`, localFoo)

	t.Run("during restoration of data", func(t *testing.T) {
		params := base.TestClusterArgs{}
		params.ServerArgs.ExternalIODir = dir
		tcRestore := testcluster.StartTestCluster(t, 1, params)
		defer tcRestore.Stopper().Stop(ctx)
		sqlDBRestore := sqlutils.MakeSQLRunner(tcRestore.ServerConn(0))

		sqlDBRestore.ExpectErr(t, "sst: no such file", `RESTORE FROM 'nodelocal://1/missing-ssts'`)
		// Verify the failed RESTORE added some DROP tables.
		// Note that the system tables here correspond to the temporary tables
		// imported, not the system tables themselves.
		sqlDBRestore.CheckQueryResults(t,
			`SELECT name FROM crdb_internal.tables WHERE state = 'DROP' ORDER BY name`,
			[][]string{
				{"bank"},
				{"comments"},
				{"jobs"},
				{"locations"},
				{"role_members"},
				{"role_options"},
				{"settings"},
				{"ui"},
				{"users"},
				{"zones"},
			},
		)
	})

	// This test retries the job (by injected a retry error) after restoring a
	// every system table that has a custom restore function. This tried to tease
	// out any errors that may occur if some of the system table restoration
	// functions are not idempotent (e.g. jobs table), but are retried by the
	// restore anyway.
	t.Run("retry-during-custom-system-table-restore", func(t *testing.T) {
		defer func(oldInterval time.Duration) {
			jobs.DefaultAdoptInterval = oldInterval
		}(jobs.DefaultAdoptInterval)
		jobs.DefaultAdoptInterval = 100 * time.Millisecond

		customRestoreSystemTables := []string{sqlbase.JobsTable.Name, sqlbase.SettingsTable.Name}
		for _, customRestoreSystemTable := range customRestoreSystemTables {
			t.Run(customRestoreSystemTable, func(t *testing.T) {
				params := base.TestClusterArgs{}
				params.ServerArgs.ExternalIODir = dir
				tcRestore := testcluster.StartTestCluster(t, 1, params)
				defer tcRestore.Stopper().Stop(ctx)
				sqlDBRestore := sqlutils.MakeSQLRunner(tcRestore.ServerConn(0))

				alreadyErrored := false
				// Inject a retry error
				for _, server := range tcRestore.Servers {
					registry := server.JobRegistry().(*jobs.Registry)
					registry.TestingResumerCreationKnobs = map[jobspb.Type]func(raw jobs.Resumer) jobs.Resumer{
						jobspb.TypeRestore: func(raw jobs.Resumer) jobs.Resumer {
							r := raw.(*restoreResumer)
							r.testingKnobs.duringSystemTableRestoration = func(systemTableName string) error {
								if !alreadyErrored && systemTableName == customRestoreSystemTable {
									alreadyErrored = true
									return jobs.NewRetryJobError("injected error")
								}
								return nil
							}
							return r
						},
					}
				}

				// The initial restore will fail, and restart.
				sqlDBRestore.ExpectErr(t, `injected error: restarting in background`, `RESTORE FROM $1`, localFoo)
				// Expect the job to succeed. If the job fails, it's likely due to
				// attempting to restore the same system table data twice.
				sqlDBRestore.CheckQueryResultsRetry(t,
					`SELECT count(*) FROM [SHOW JOBS] WHERE job_type = 'RESTORE' AND status = 'succeeded'`,
					[][]string{{"1"}})
			})
		}
	})

	t.Run("during system table restoration", func(t *testing.T) {
		params := base.TestClusterArgs{}
		params.ServerArgs.ExternalIODir = dir
		tcRestore := testcluster.StartTestCluster(t, 1, params)
		defer tcRestore.Stopper().Stop(ctx)
		sqlDBRestore := sqlutils.MakeSQLRunner(tcRestore.ServerConn(0))

		// Bugger the backup by injecting a failure while restoring the system data.
		for _, server := range tcRestore.Servers {
			registry := server.JobRegistry().(*jobs.Registry)
			registry.TestingResumerCreationKnobs = map[jobspb.Type]func(raw jobs.Resumer) jobs.Resumer{
				jobspb.TypeRestore: func(raw jobs.Resumer) jobs.Resumer {
					r := raw.(*restoreResumer)
					r.testingKnobs.duringSystemTableRestoration = func(_ string) error {
						return errors.New("injected error")
					}
					return r
				},
			}
		}
		sqlDBRestore.ExpectErr(t, "injected error", `RESTORE FROM $1`, localFoo)
		// Verify the failed RESTORE added some DROP tables.
		// Note that the system tables here correspond to the temporary tables
		// imported, not the system tables themselves.
		sqlDBRestore.CheckQueryResults(t,
			`SELECT name FROM crdb_internal.tables WHERE state = 'DROP' ORDER BY name`,
			[][]string{
				{"bank"},
				{"comments"},
				{"jobs"},
				{"locations"},
				{"role_members"},
				{"role_options"},
				{"settings"},
				{"ui"},
				{"users"},
				{"zones"},
			},
		)
	})
	t.Run("after offline tables", func(t *testing.T) {
		params := base.TestClusterArgs{}
		params.ServerArgs.ExternalIODir = dir
		tcRestore := testcluster.StartTestCluster(t, 1, params)
		defer tcRestore.Stopper().Stop(ctx)
		sqlDBRestore := sqlutils.MakeSQLRunner(tcRestore.ServerConn(0))

		// Bugger the backup by injecting a failure while restoring the system data.
		for _, server := range tcRestore.Servers {
			registry := server.JobRegistry().(*jobs.Registry)
			registry.TestingResumerCreationKnobs = map[jobspb.Type]func(raw jobs.Resumer) jobs.Resumer{
				jobspb.TypeRestore: func(raw jobs.Resumer) jobs.Resumer {
					r := raw.(*restoreResumer)
					r.testingKnobs.afterOfflineTableCreation = func() error {
						return errors.New("injected error")
					}
					return r
				},
			}
		}
		sqlDBRestore.ExpectErr(t, "injected error", `RESTORE FROM $1`, localFoo)
	})
}
