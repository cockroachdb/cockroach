// Copyright 2016 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package backupccl_test

import (
	"bytes"
	"context"
	gosql "database/sql"
	"fmt"
	"hash/crc32"
	"io"
	"io/ioutil"
	"math/rand"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach-go/crdb"
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/backupccl"
	_ "github.com/cockroachdb/cockroach/pkg/ccl/partitionccl"
	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl/sampledataccl"
	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/stats"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/jobutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/workload/bank"
	"github.com/cockroachdb/cockroach/pkg/workload/workloadsql"
	"github.com/gogo/protobuf/proto"
	"github.com/kr/pretty"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
)

const (
	singleNode                  = 1
	multiNode                   = 3
	backupRestoreDefaultRanges  = 10
	backupRestoreRowPayloadSize = 100
	localFoo                    = "nodelocal:///foo"
)

func backupRestoreTestSetupWithParams(
	t testing.TB,
	clusterSize int,
	numAccounts int,
	init func(tc *testcluster.TestCluster),
	params base.TestClusterArgs,
) (
	ctx context.Context,
	tc *testcluster.TestCluster,
	sqlDB *sqlutils.SQLRunner,
	tempDir string,
	cleanup func(),
) {
	ctx = context.Background()

	dir, dirCleanupFn := testutils.TempDir(t)
	params.ServerArgs.ExternalIODir = dir
	params.ServerArgs.UseDatabase = "data"
	tc = testcluster.StartTestCluster(t, clusterSize, params)
	init(tc)

	const payloadSize = 100
	splits := 10
	if numAccounts == 0 {
		splits = 0
	}
	bankData := bank.FromConfig(numAccounts, payloadSize, splits)

	sqlDB = sqlutils.MakeSQLRunner(tc.Conns[0])
	sqlDB.Exec(t, `CREATE DATABASE data`)
	l := workloadsql.InsertsDataLoader{BatchSize: 1000, Concurrency: 4}
	if _, err := workloadsql.Setup(ctx, sqlDB.DB.(*gosql.DB), bankData, l); err != nil {
		t.Fatalf("%+v", err)
	}
	if err := workloadsql.Split(ctx, sqlDB.DB.(*gosql.DB), bankData.Tables()[0], 1 /* concurrency */); err != nil {
		// This occasionally flakes, so ignore errors.
		t.Logf("failed to split: %+v", err)
	}

	if err := tc.WaitForFullReplication(); err != nil {
		t.Fatal(err)
	}

	cleanupFn := func() {
		tempDirs := []string{dir}
		for _, s := range tc.Servers {
			for _, e := range s.Engines() {
				tempDirs = append(tempDirs, e.GetAuxiliaryDir())
			}
		}
		tc.Stopper().Stop(context.TODO()) // cleans up in memory storage's auxiliary dirs
		dirCleanupFn()                    // cleans up dir, which is the nodelocal:// storage

		for _, temp := range tempDirs {
			testutils.SucceedsSoon(t, func() error {
				items, err := ioutil.ReadDir(temp)
				if err != nil && !os.IsNotExist(err) {
					t.Fatal(err)
				}
				for _, leftover := range items {
					return errors.Errorf("found %q remaining in %s", leftover.Name(), temp)
				}
				return nil
			})
		}
	}

	return ctx, tc, sqlDB, dir, cleanupFn
}

func backupRestoreTestSetup(
	t testing.TB, clusterSize int, numAccounts int, init func(*testcluster.TestCluster),
) (
	ctx context.Context,
	tc *testcluster.TestCluster,
	sqlDB *sqlutils.SQLRunner,
	tempDir string,
	cleanup func(),
) {
	return backupRestoreTestSetupWithParams(t, clusterSize, numAccounts, init, base.TestClusterArgs{})
}

func verifyBackupRestoreStatementResult(
	t *testing.T, sqlDB *sqlutils.SQLRunner, query string, args ...interface{},
) error {
	t.Helper()
	rows := sqlDB.Query(t, query, args...)

	columns, err := rows.Columns()
	if err != nil {
		return err
	}
	if e, a := columns, []string{
		"job_id", "status", "fraction_completed", "rows", "index_entries", "system_records", "bytes",
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
		return errors.New("zero rows in result")
	}
	if err := rows.Scan(
		&actualJob.id, &actualJob.status, &actualJob.fractionCompleted, &unused, &unused, &unused, &unused,
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

	if e, a := expectedJob, actualJob; !reflect.DeepEqual(e, a) {
		return errors.Errorf("result does not match system.jobs:\n%s",
			strings.Join(pretty.Diff(e, a), "\n"))
	}

	return nil
}

func TestBackupRestoreStatementResult(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const numAccounts = 1
	_, _, sqlDB, _, cleanupFn := backupRestoreTestSetup(t, singleNode, numAccounts, initNone)
	defer cleanupFn()

	if err := verifyBackupRestoreStatementResult(
		t, sqlDB, "BACKUP DATABASE data TO $1", localFoo,
	); err != nil {
		t.Fatal(err)
	}

	sqlDB.Exec(t, "CREATE DATABASE data2")

	if err := verifyBackupRestoreStatementResult(
		t, sqlDB, "RESTORE data.* FROM $1 WITH OPTIONS ('into_db'='data2')", localFoo,
	); err != nil {
		t.Fatal(err)
	}
}

func TestBackupRestoreLocal(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const numAccounts = 1000
	ctx, tc, _, _, cleanupFn := backupRestoreTestSetup(t, singleNode, numAccounts, initNone)
	defer cleanupFn()

	backupAndRestore(ctx, t, tc, localFoo, numAccounts)
}

func TestBackupRestoreEmpty(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const numAccounts = 0
	ctx, tc, _, _, cleanupFn := backupRestoreTestSetup(t, singleNode, numAccounts, initNone)
	defer cleanupFn()

	backupAndRestore(ctx, t, tc, localFoo, numAccounts)
}

// Regression test for #16008. In short, the way RESTORE constructed split keys
// for tables with negative primary key data caused AdminSplit to fail.
func TestBackupRestoreNegativePrimaryKey(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const numAccounts = 1000

	ctx, tc, sqlDB, _, cleanupFn := backupRestoreTestSetup(t, multiNode, numAccounts, initNone)
	defer cleanupFn()

	// Give half the accounts negative primary keys.
	sqlDB.Exec(t, `UPDATE data.bank SET id = $1 - id WHERE id > $1`, numAccounts/2)

	// Resplit that half of the table space.
	sqlDB.Exec(t,
		`ALTER TABLE data.bank SPLIT AT SELECT generate_series($1, 0, $2)`,
		-numAccounts/2, numAccounts/backupRestoreDefaultRanges/2,
	)

	backupAndRestore(ctx, t, tc, localFoo, numAccounts)
}

func backupAndRestore(
	ctx context.Context, t *testing.T, tc *testcluster.TestCluster, dest string, numAccounts int,
) {
	conn := tc.Conns[0]
	sqlDB := sqlutils.MakeSQLRunner(conn)
	{
		sqlDB.Exec(t, `CREATE INDEX balance_idx ON data.bank (balance)`)
		testutils.SucceedsSoon(t, func() error {
			var unused string
			var createTable string
			sqlDB.QueryRow(t, `SHOW CREATE TABLE data.bank`).Scan(&unused, &createTable)
			if !strings.Contains(createTable, "balance_idx") {
				return errors.New("expected a balance_idx index")
			}
			return nil
		})

		var unused string
		var exported struct {
			rows, idx, sys, bytes int64
		}
		sqlDB.QueryRow(t, `BACKUP DATABASE data TO $1`, dest).Scan(
			&unused, &unused, &unused, &exported.rows, &exported.idx, &exported.sys, &exported.bytes,
		)
		// When numAccounts == 0, our approxBytes formula breaks down because
		// backups of no data still contain the system.users and system.descriptor
		// tables. Just skip the check in this case.
		if numAccounts > 0 {
			approxBytes := int64(backupRestoreRowPayloadSize * numAccounts)
			if max := approxBytes * 3; exported.bytes < approxBytes || exported.bytes > max {
				t.Errorf("expected data size in [%d,%d] but was %d", approxBytes, max, exported.bytes)
			}
		}
		if expected := int64(numAccounts * 1); exported.rows != expected {
			t.Fatalf("expected %d rows for %d accounts, got %d", expected, numAccounts, exported.rows)
		}

		sqlDB.ExpectErr(t, "already contains a BACKUP file", `BACKUP DATABASE data TO $1`, dest)
	}

	// Start a new cluster to restore into.
	{
		args := base.TestServerArgs{ExternalIODir: tc.Servers[0].ClusterSettings().ExternalIODir}
		tcRestore := testcluster.StartTestCluster(t, singleNode, base.TestClusterArgs{ServerArgs: args})
		defer tcRestore.Stopper().Stop(ctx)
		sqlDBRestore := sqlutils.MakeSQLRunner(tcRestore.Conns[0])

		// Create some other descriptors to change up IDs
		sqlDBRestore.Exec(t, `CREATE DATABASE other`)
		// Force the ID of the restored bank table to be different.
		sqlDBRestore.Exec(t, `CREATE TABLE other.empty (a INT PRIMARY KEY)`)

		var unused string
		var restored struct {
			rows, idx, sys, bytes int64
		}

		sqlDBRestore.QueryRow(t, `RESTORE DATABASE DATA FROM $1`, dest).Scan(
			&unused, &unused, &unused, &restored.rows, &restored.idx, &restored.sys, &restored.bytes,
		)
		approxBytes := int64(backupRestoreRowPayloadSize * numAccounts)
		if max := approxBytes * 3; restored.bytes < approxBytes || restored.bytes > max {
			t.Errorf("expected data size in [%d,%d] but was %d", approxBytes, max, restored.bytes)
		}
		if expected := int64(numAccounts); restored.rows != expected {
			t.Fatalf("expected %d rows for %d accounts, got %d", expected, numAccounts, restored.rows)
		}
		if expected := int64(numAccounts); restored.idx != expected {
			t.Fatalf("expected %d idx rows for %d accounts, got %d", expected, numAccounts, restored.idx)
		}

		var rowCount int64
		sqlDBRestore.QueryRow(t, `SELECT count(*) FROM data.bank`).Scan(&rowCount)
		if rowCount != int64(numAccounts) {
			t.Fatalf("expected %d rows but found %d", numAccounts, rowCount)
		}

		sqlDBRestore.QueryRow(t, `SELECT count(*) FROM data.bank@balance_idx`).Scan(&rowCount)
		if rowCount != int64(numAccounts) {
			t.Fatalf("expected %d rows but found %d", numAccounts, rowCount)
		}

		// Verify there's no /Table/51 - /Table/51/1 empty span.
		{
			var count int
			sqlDBRestore.QueryRow(t, `
			SELECT count(*) FROM crdb_internal.ranges
			WHERE start_pretty = (
				('/Table/' ||
				(SELECT table_id FROM crdb_internal.tables
					WHERE database_name = $1 AND name = $2
				)::STRING) ||
				'/1'
			)
		`, "data", "bank").Scan(&count)
			if count != 0 {
				t.Fatal("unexpected span start at primary index")
			}
		}
	}
}

func TestBackupRestoreSystemTables(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const numAccounts = 0
	ctx, _, sqlDB, _, cleanupFn := backupRestoreTestSetup(t, multiNode, numAccounts, initNone)
	conn := sqlDB.DB.(*gosql.DB)
	defer cleanupFn()

	// At the time this test was written, these were the only system tables that
	// were reasonable for a user to backup and restore into another cluster.
	tables := []string{"locations", "role_members", "users", "zones"}
	tableSpec := "system." + strings.Join(tables, ", system.")

	// Take a consistent fingerprint of the original tables.
	var backupAsOf string
	expectedFingerprints := map[string][][]string{}
	err := crdb.ExecuteTx(ctx, conn, nil /* txopts */, func(tx *gosql.Tx) error {
		for _, table := range tables {
			rows, err := conn.Query("SHOW EXPERIMENTAL_FINGERPRINTS FROM TABLE system." + table)
			if err != nil {
				return err
			}
			defer rows.Close()
			expectedFingerprints[table], err = sqlutils.RowsToStrMatrix(rows)
			if err != nil {
				return err
			}
		}
		// Record the transaction's timestamp so we can take a backup at the
		// same time.
		return conn.QueryRow("SELECT cluster_logical_timestamp()").Scan(&backupAsOf)
	})
	if err != nil {
		t.Fatal(err)
	}

	// Backup and restore the tables into a new database.
	sqlDB.Exec(t, `CREATE DATABASE system_new`)
	sqlDB.Exec(t, fmt.Sprintf(`BACKUP %s TO '%s' AS OF SYSTEM TIME %s`, tableSpec, localFoo, backupAsOf))
	sqlDB.Exec(t, fmt.Sprintf(`RESTORE %s FROM '%s' WITH into_db='system_new'`, tableSpec, localFoo))

	// Verify the fingerprints match.
	for _, table := range tables {
		a := sqlDB.QueryStr(t, "SHOW EXPERIMENTAL_FINGERPRINTS FROM TABLE system_new."+table)
		if e := expectedFingerprints[table]; !reflect.DeepEqual(e, a) {
			t.Fatalf("fingerprints between system.%[1]s and system_new.%[1]s did not match:%s\n",
				table, strings.Join(pretty.Diff(e, a), "\n"))
		}
	}

	// Verify we can't shoot ourselves in the foot by accidentally restoring
	// directly over the existing system tables.
	sqlDB.ExpectErr(
		t, `relation ".+" already exists`,
		fmt.Sprintf(`RESTORE %s FROM '%s'`, tableSpec, localFoo),
	)
}

func TestBackupRestoreSystemJobs(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const numAccounts = 0
	_, _, sqlDB, _, cleanupFn := backupRestoreTestSetup(t, multiNode, numAccounts, initNone)
	conn := sqlDB.DB.(*gosql.DB)
	defer cleanupFn()

	sanitizedIncDir := localFoo + "/inc"
	incDir := sanitizedIncDir + "?secretCredentialsHere"

	sanitizedFullDir := localFoo + "/full"
	fullDir := sanitizedFullDir + "?moarSecretsHere"

	backupDatabaseID := sqlutils.QueryDatabaseID(t, conn, "data")
	backupTableID := sqlutils.QueryTableID(t, conn, "data", "bank")

	sqlDB.Exec(t, `CREATE DATABASE restoredb`)
	restoreDatabaseID := sqlutils.QueryDatabaseID(t, conn, "restoredb")

	// We create a full backup so that, below, we can test that incremental
	// backups sanitize credentials in "INCREMENTAL FROM" URLs.
	//
	// NB: We don't bother making assertions about this full backup since there
	// are no meaningful differences in how full and incremental backups report
	// status to the system.jobs table. Since the incremental BACKUP syntax is a
	// superset of the full BACKUP syntax, we'll cover everything by verifying the
	// incremental backup below.
	sqlDB.Exec(t, `BACKUP DATABASE data TO $1`, fullDir)
	sqlDB.Exec(t, `SET DATABASE = data`)

	sqlDB.Exec(t, `BACKUP TABLE bank TO $1 INCREMENTAL FROM $2`, incDir, fullDir)
	if err := jobutils.VerifySystemJob(t, sqlDB, 1, jobspb.TypeBackup, jobs.StatusSucceeded, jobs.Record{
		Username: security.RootUser,
		Description: fmt.Sprintf(
			`BACKUP TABLE bank TO '%s' INCREMENTAL FROM '%s'`,
			sanitizedIncDir, sanitizedFullDir,
		),
		DescriptorIDs: sqlbase.IDs{
			sqlbase.ID(backupDatabaseID),
			sqlbase.ID(backupTableID),
		},
	}); err != nil {
		t.Fatal(err)
	}

	sqlDB.Exec(t, `RESTORE TABLE bank FROM $1, $2 WITH OPTIONS ('into_db'='restoredb')`, fullDir, incDir)
	if err := jobutils.VerifySystemJob(t, sqlDB, 0, jobspb.TypeRestore, jobs.StatusSucceeded, jobs.Record{
		Username: security.RootUser,
		Description: fmt.Sprintf(
			`RESTORE TABLE bank FROM '%s', '%s' WITH into_db = 'restoredb'`,
			sanitizedFullDir, sanitizedIncDir,
		),
		DescriptorIDs: sqlbase.IDs{
			sqlbase.ID(restoreDatabaseID + 1),
		},
	}); err != nil {
		t.Fatal(err)
	}
}

type inProgressChecker func(context context.Context, ip inProgressState) error

// inProgressState holds state about an in-progress backup or restore
// for use in inProgressCheckers.
type inProgressState struct {
	*gosql.DB
	backupTableID uint32
	dir, name     string
}

func (ip inProgressState) latestJobID() (int64, error) {
	var id int64
	if err := ip.QueryRow(
		`SELECT job_id FROM crdb_internal.jobs ORDER BY created DESC LIMIT 1`,
	).Scan(&id); err != nil {
		return 0, err
	}
	return id, nil
}

// checkInProgressBackupRestore will run a backup and restore, pausing each
// approximately halfway through to run either `checkBackup` or `checkRestore`.
func checkInProgressBackupRestore(
	t testing.TB, checkBackup inProgressChecker, checkRestore inProgressChecker,
) {
	// To test incremental progress updates, we install a store response filter,
	// which runs immediately before a KV command returns its response, in our
	// test cluster. Whenever we see an Export or Import response, we do a
	// blocking read on the allowResponse channel to give the test a chance to
	// assert the progress of the job.
	var allowResponse chan struct{}
	params := base.TestClusterArgs{}
	params.ServerArgs.Knobs.Store = &storage.StoreTestingKnobs{
		TestingResponseFilter: func(ba roachpb.BatchRequest, br *roachpb.BatchResponse) *roachpb.Error {
			for _, ru := range br.Responses {
				switch ru.GetInner().(type) {
				case *roachpb.ExportResponse, *roachpb.ImportResponse:
					<-allowResponse
				}
			}
			return nil
		},
	}

	const numAccounts = 1000
	const totalExpectedResponses = backupRestoreDefaultRanges

	ctx, _, sqlDB, dir, cleanup := backupRestoreTestSetupWithParams(t, multiNode, numAccounts, initNone, params)
	conn := sqlDB.DB.(*gosql.DB)
	defer cleanup()

	sqlDB.Exec(t, `CREATE DATABASE restoredb`)

	backupTableID := sqlutils.QueryTableID(t, conn, "data", "bank")

	do := func(query string, check inProgressChecker) {
		jobDone := make(chan error)
		allowResponse = make(chan struct{}, totalExpectedResponses)

		go func() {
			_, err := conn.Exec(query, localFoo)
			jobDone <- err
		}()

		// Allow half the total expected responses to proceed.
		for i := 0; i < totalExpectedResponses/2; i++ {
			allowResponse <- struct{}{}
		}

		err := retry.ForDuration(testutils.DefaultSucceedsSoonDuration, func() error {
			return check(ctx, inProgressState{
				DB:            conn,
				backupTableID: backupTableID,
				dir:           dir,
				name:          "foo",
			})
		})

		// Close the channel to allow all remaining responses to proceed. We do this
		// even if the above retry.ForDuration failed, otherwise the test will hang
		// forever.
		close(allowResponse)

		if err := <-jobDone; err != nil {
			t.Fatalf("%q: %+v", query, err)
		}

		if err != nil {
			t.Fatal(err)
		}
	}

	do(`BACKUP DATABASE data TO $1`, checkBackup)
	do(`RESTORE data.* FROM $1 WITH OPTIONS ('into_db'='restoredb')`, checkRestore)
}

func TestBackupRestoreSystemJobsProgress(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer jobs.TestingSetProgressThresholds()()

	checkFraction := func(ctx context.Context, ip inProgressState) error {
		jobID, err := ip.latestJobID()
		if err != nil {
			return err
		}
		var fractionCompleted float32
		if err := ip.QueryRow(
			`SELECT fraction_completed FROM crdb_internal.jobs WHERE job_id = $1`,
			jobID,
		).Scan(&fractionCompleted); err != nil {
			return err
		}
		if fractionCompleted < 0.25 || fractionCompleted > 0.75 {
			return errors.Errorf(
				"expected progress to be in range [0.25, 0.75] but got %f",
				fractionCompleted,
			)
		}
		return nil
	}

	checkInProgressBackupRestore(t, checkFraction, checkFraction)
}

func TestBackupRestoreCheckpointing(t *testing.T) {
	defer leaktest.AfterTest(t)()

	t.Skip("https://github.com/cockroachdb/cockroach/issues/33357")

	defer func(oldInterval time.Duration) {
		backupccl.BackupCheckpointInterval = oldInterval
	}(backupccl.BackupCheckpointInterval)
	backupccl.BackupCheckpointInterval = 0

	var checkpointPath string

	checkBackup := func(ctx context.Context, ip inProgressState) error {
		checkpointPath = filepath.Join(ip.dir, ip.name, backupccl.BackupDescriptorCheckpointName)
		checkpointDescBytes, err := ioutil.ReadFile(checkpointPath)
		if err != nil {
			return errors.Errorf("%+v", err)
		}
		var checkpointDesc backupccl.BackupDescriptor
		if err := protoutil.Unmarshal(checkpointDescBytes, &checkpointDesc); err != nil {
			return errors.Errorf("%+v", err)
		}
		if len(checkpointDesc.Files) == 0 {
			return errors.Errorf("empty backup checkpoint descriptor")
		}
		return nil
	}

	checkRestore := func(ctx context.Context, ip inProgressState) error {
		jobID, err := ip.latestJobID()
		if err != nil {
			return err
		}
		highWaterMark, err := getHighWaterMark(jobID, ip.DB)
		if err != nil {
			return err
		}
		low := keys.MakeTablePrefix(ip.backupTableID)
		high := keys.MakeTablePrefix(ip.backupTableID + 1)
		if bytes.Compare(highWaterMark, low) <= 0 || bytes.Compare(highWaterMark, high) >= 0 {
			return errors.Errorf("expected high-water mark %v to be between %v and %v",
				highWaterMark, roachpb.Key(low), roachpb.Key(high))
		}
		return nil
	}

	checkInProgressBackupRestore(t, checkBackup, checkRestore)

	if _, err := os.Stat(checkpointPath); err == nil {
		t.Fatalf("backup checkpoint descriptor at %s not cleaned up", checkpointPath)
	} else if !os.IsNotExist(err) {
		t.Fatal(err)
	}
}

func createAndWaitForJob(
	t *testing.T,
	db *sqlutils.SQLRunner,
	descriptorIDs []sqlbase.ID,
	details jobspb.Details,
	progress jobspb.ProgressDetails,
) {
	t.Helper()
	now := timeutil.ToUnixMicros(timeutil.Now())
	payload, err := protoutil.Marshal(&jobspb.Payload{
		Username:      security.RootUser,
		DescriptorIDs: descriptorIDs,
		StartedMicros: now,
		Details:       jobspb.WrapPayloadDetails(details),
		Lease:         &jobspb.Lease{NodeID: 1},
	})
	if err != nil {
		t.Fatal(err)
	}

	progressBytes, err := protoutil.Marshal(&jobspb.Progress{
		ModifiedMicros: now,
		Details:        jobspb.WrapProgressDetails(progress),
	})
	if err != nil {
		t.Fatal(err)
	}

	var jobID int64
	db.QueryRow(
		t, `INSERT INTO system.jobs (created, status, payload, progress) VALUES ($1, $2, $3, $4) RETURNING id`,
		timeutil.FromUnixMicros(now), jobs.StatusRunning, payload, progressBytes,
	).Scan(&jobID)
	jobutils.WaitForJob(t, db, jobID)
}

// TestBackupRestoreResume tests whether backup and restore jobs are properly
// resumed after a coordinator failure. It synthesizes a partially-complete
// backup job and a partially-complete restore job, both with expired leases, by
// writing checkpoints directly to system.jobs, then verifies they are resumed
// and successfully completed within a few seconds. The test additionally
// verifies that backup and restore do not re-perform work the checkpoint claims
// to have completed.
func TestBackupRestoreResume(t *testing.T) {
	defer leaktest.AfterTest(t)()

	defer func(oldInterval time.Duration) {
		jobs.DefaultAdoptInterval = oldInterval
	}(jobs.DefaultAdoptInterval)
	jobs.DefaultAdoptInterval = 100 * time.Millisecond

	ctx := context.Background()

	const numAccounts = 1000
	_, tc, outerDB, dir, cleanupFn := backupRestoreTestSetup(t, multiNode, numAccounts, initNone)
	defer cleanupFn()

	backupTableDesc := sqlbase.GetTableDescriptor(tc.Servers[0].DB(), "data", "bank")

	t.Run("backup", func(t *testing.T) {
		sqlDB := sqlutils.MakeSQLRunner(outerDB.DB)
		backupStartKey := backupTableDesc.PrimaryIndexSpan().Key
		backupEndKey, err := sqlbase.TestingMakePrimaryIndexKey(backupTableDesc, numAccounts/2)
		if err != nil {
			t.Fatal(err)
		}
		backupCompletedSpan := roachpb.Span{Key: backupStartKey, EndKey: backupEndKey}
		backupDesc, err := protoutil.Marshal(&backupccl.BackupDescriptor{
			ClusterID: tc.Servers[0].ClusterID(),
			Files: []backupccl.BackupDescriptor_File{
				{Path: "garbage-checkpoint", Span: backupCompletedSpan},
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		backupDir := filepath.Join(dir, "backup")
		if err := os.MkdirAll(backupDir, 0755); err != nil {
			t.Fatal(err)
		}
		checkpointFile := filepath.Join(backupDir, backupccl.BackupDescriptorCheckpointName)
		if err := ioutil.WriteFile(checkpointFile, backupDesc, 0644); err != nil {
			t.Fatal(err)
		}
		createAndWaitForJob(
			t, sqlDB, []sqlbase.ID{backupTableDesc.ID},
			jobspb.BackupDetails{
				EndTime:          tc.Servers[0].Clock().Now(),
				URI:              "nodelocal:///backup",
				BackupDescriptor: backupDesc,
			},
			jobspb.BackupProgress{},
		)

		// If the backup properly took the (incorrect) checkpoint into account, it
		// won't have tried to re-export any keys within backupCompletedSpan.
		backupDescriptorFile := filepath.Join(backupDir, backupccl.BackupDescriptorName)
		backupDescriptorBytes, err := ioutil.ReadFile(backupDescriptorFile)
		if err != nil {
			t.Fatal(err)
		}
		var backupDescriptor backupccl.BackupDescriptor
		if err := protoutil.Unmarshal(backupDescriptorBytes, &backupDescriptor); err != nil {
			t.Fatal(err)
		}
		for _, file := range backupDescriptor.Files {
			if file.Span.Overlaps(backupCompletedSpan) && file.Path != "garbage-checkpoint" {
				t.Fatalf("backup re-exported checkpointed span %s", file.Span)
			}
		}
	})

	t.Run("restore", func(t *testing.T) {
		sqlDB := sqlutils.MakeSQLRunner(outerDB.DB)
		restoreDir := "nodelocal:///restore"
		sqlDB.Exec(t, `BACKUP DATABASE DATA TO $1`, restoreDir)
		sqlDB.Exec(t, `CREATE DATABASE restoredb`)
		restoreDatabaseID := sqlutils.QueryDatabaseID(t, sqlDB.DB, "restoredb")
		restoreTableID, err := sql.GenerateUniqueDescID(ctx, tc.Servers[0].DB())
		if err != nil {
			t.Fatal(err)
		}
		restoreHighWaterMark, err := sqlbase.TestingMakePrimaryIndexKey(backupTableDesc, numAccounts/2)
		if err != nil {
			t.Fatal(err)
		}
		createAndWaitForJob(
			t, sqlDB, []sqlbase.ID{restoreTableID},
			jobspb.RestoreDetails{
				TableRewrites: map[sqlbase.ID]*jobspb.RestoreDetails_TableRewrite{
					backupTableDesc.ID: {
						ParentID: sqlbase.ID(restoreDatabaseID),
						TableID:  restoreTableID,
					},
				},
				URIs: []string{restoreDir},
			},
			jobspb.RestoreProgress{
				HighWater: restoreHighWaterMark,
			},
		)
		// If the restore properly took the (incorrect) low-water mark into account,
		// the first half of the table will be missing.
		var restoredCount int64
		sqlDB.QueryRow(t, `SELECT count(*) FROM restoredb.bank`).Scan(&restoredCount)
		if e, a := int64(numAccounts)/2, restoredCount; e != a {
			t.Fatalf("expected %d restored rows, but got %d\n", e, a)
		}
		sqlDB.Exec(t, `DELETE FROM data.bank WHERE id < $1`, numAccounts/2)
		sqlDB.CheckQueryResults(t,
			`SHOW EXPERIMENTAL_FINGERPRINTS FROM TABLE restoredb.bank`,
			sqlDB.QueryStr(t, `SHOW EXPERIMENTAL_FINGERPRINTS FROM TABLE data.bank`),
		)
	})
}

func getHighWaterMark(jobID int64, sqlDB *gosql.DB) (roachpb.Key, error) {
	var progressBytes []byte
	if err := sqlDB.QueryRow(
		`SELECT progress FROM system.jobs WHERE id = $1`, jobID,
	).Scan(&progressBytes); err != nil {
		return nil, err
	}
	var payload jobspb.Progress
	if err := protoutil.Unmarshal(progressBytes, &payload); err != nil {
		return nil, err
	}
	switch d := payload.Details.(type) {
	case *jobspb.Progress_Restore:
		return d.Restore.HighWater, nil
	default:
		return nil, errors.Errorf("unexpected job details type %T", d)
	}
}

// TestBackupRestoreControlJob tests that PAUSE JOB, RESUME JOB, and CANCEL JOB
// work as intended on backup and restore jobs.
func TestBackupRestoreControlJob(t *testing.T) {
	defer leaktest.AfterTest(t)()
	t.Skip("#24136")

	// force every call to update
	defer jobs.TestingSetProgressThresholds()()

	defer func(oldInterval time.Duration) {
		jobs.DefaultAdoptInterval = oldInterval
	}(jobs.DefaultAdoptInterval)
	jobs.DefaultAdoptInterval = 100 * time.Millisecond

	serverArgs := base.TestServerArgs{}
	// Disable external processing of mutations so that the final check of
	// crdb_internal.tables is guaranteed to not be cleaned up. Although this
	// was never observed by a stress test, it is here for safety.
	serverArgs.Knobs.SQLSchemaChanger = &sql.SchemaChangerTestingKnobs{
		AsyncExecNotification: func() error {
			return errors.New("async schema changer disabled")
		},
	}

	// PAUSE JOB and CANCEL JOB are racy in that it's hard to guarantee that the
	// job is still running when executing a PAUSE or CANCEL--or that the job has
	// even started running. To synchronize, we install a store response filter
	// which does a blocking receive whenever it encounters an export or import
	// response. Below, when we want to guarantee the job is in progress, we do
	// exactly one blocking send. When this send completes, we know the job has
	// started, as we've seen one export or import response. We also know the job
	// has not finished, because we're blocking all future export and import
	// responses until we close the channel, and our backup or restore is large
	// enough that it will generate more than one export or import response.
	var allowResponse chan struct{}
	params := base.TestClusterArgs{ServerArgs: serverArgs}
	params.ServerArgs.Knobs.Store = &storage.StoreTestingKnobs{
		TestingResponseFilter: jobutils.BulkOpResponseFilter(&allowResponse),
	}

	// We need lots of ranges to see what happens when they get chunked. Rather
	// than make a huge table, dial down the zone config for the bank table.
	init := func(tc *testcluster.TestCluster) {
		config.TestingSetupZoneConfigHook(tc.Stopper())
		v, err := tc.Servers[0].DB().Get(context.TODO(), keys.DescIDGenerator)
		if err != nil {
			t.Fatal(err)
		}
		last := uint32(v.ValueInt())
		zoneConfig := config.DefaultZoneConfig()
		zoneConfig.RangeMaxBytes = proto.Int64(5000)
		config.TestingSetZoneConfig(last+1, zoneConfig)
	}
	const numAccounts = 1000
	_, _, outerDB, _, cleanup := backupRestoreTestSetupWithParams(t, multiNode, numAccounts, init, params)
	defer cleanup()

	sqlDB := sqlutils.MakeSQLRunner(outerDB.DB)

	t.Run("foreign", func(t *testing.T) {
		foreignDir := "nodelocal:///foreign"
		sqlDB.Exec(t, `CREATE DATABASE orig_fkdb`)
		sqlDB.Exec(t, `CREATE DATABASE restore_fkdb`)
		sqlDB.Exec(t, `CREATE TABLE orig_fkdb.fk (i INT REFERENCES data.bank)`)
		// Generate some FK data with splits so backup/restore block correctly.
		for i := 0; i < 10; i++ {
			sqlDB.Exec(t, `INSERT INTO orig_fkdb.fk (i) VALUES ($1)`, i)
			sqlDB.Exec(t, `ALTER TABLE orig_fkdb.fk SPLIT AT VALUES ($1)`, i)
		}

		for i, query := range []string{
			`BACKUP TABLE orig_fkdb.fk TO $1`,
			`RESTORE TABLE orig_fkdb.fk FROM $1 WITH OPTIONS ('skip_missing_foreign_keys', 'into_db'='restore_fkdb')`,
		} {
			jobID, err := jobutils.RunJob(t, sqlDB, &allowResponse, []string{"PAUSE"}, query, foreignDir)
			if !testutils.IsError(err, "job paused") {
				t.Fatalf("%d: expected 'job paused' error, but got %+v", i, err)
			}
			sqlDB.Exec(t, fmt.Sprintf(`RESUME JOB %d`, jobID))
			jobutils.WaitForJob(t, sqlDB, jobID)
		}

		sqlDB.CheckQueryResults(t,
			`SHOW EXPERIMENTAL_FINGERPRINTS FROM TABLE orig_fkdb.fk`,
			sqlDB.QueryStr(t, `SHOW EXPERIMENTAL_FINGERPRINTS FROM TABLE restore_fkdb.fk`),
		)
	})

	t.Run("pause", func(t *testing.T) {
		pauseDir := "nodelocal:///pause"
		sqlDB.Exec(t, `CREATE DATABASE pause`)

		for i, query := range []string{
			`BACKUP DATABASE data TO $1`,
			`RESTORE TABLE data.* FROM $1 WITH OPTIONS ('into_db'='pause')`,
		} {
			ops := []string{"PAUSE", "RESUME", "PAUSE"}
			jobID, err := jobutils.RunJob(t, sqlDB, &allowResponse, ops, query, pauseDir)
			if !testutils.IsError(err, "job paused") {
				t.Fatalf("%d: expected 'job paused' error, but got %+v", i, err)
			}
			sqlDB.Exec(t, fmt.Sprintf(`RESUME JOB %d`, jobID))
			jobutils.WaitForJob(t, sqlDB, jobID)
		}
		sqlDB.CheckQueryResults(t,
			`SELECT count(*) FROM pause.bank`,
			sqlDB.QueryStr(t, `SELECT count(*) FROM data.bank`),
		)

		sqlDB.CheckQueryResults(t,
			`SHOW EXPERIMENTAL_FINGERPRINTS FROM TABLE pause.bank`,
			sqlDB.QueryStr(t, `SHOW EXPERIMENTAL_FINGERPRINTS FROM TABLE data.bank`),
		)
	})

	t.Run("cancel", func(t *testing.T) {
		cancelDir := "nodelocal:///cancel"
		sqlDB.Exec(t, `CREATE DATABASE cancel`)

		for i, query := range []string{
			`BACKUP DATABASE data TO $1`,
			`RESTORE TABLE data.* FROM $1 WITH OPTIONS ('into_db'='cancel')`,
		} {
			if _, err := jobutils.RunJob(
				t, sqlDB, &allowResponse, []string{"cancel"}, query, cancelDir,
			); !testutils.IsError(err, "job canceled") {
				t.Fatalf("%d: expected 'job canceled' error, but got %+v", i, err)
			}
			// Check that executing the same backup or restore succeeds. This won't
			// work if the first backup or restore was not successfully canceled.
			sqlDB.Exec(t, query, cancelDir)
		}
		// Verify the canceled RESTORE added some DROP tables.
		sqlDB.CheckQueryResults(t,
			`SELECT name FROM crdb_internal.tables WHERE database_name = 'cancel' AND state = 'DROP'`,
			[][]string{{"bank"}},
		)
	})
}

// TestRestoreFailCleanup tests that a failed RESTORE is cleaned up.
func TestRestoreFailCleanup(t *testing.T) {
	defer leaktest.AfterTest(t)()

	params := base.TestServerArgs{}
	// Disable external processing of mutations so that the final check of
	// crdb_internal.tables is guaranteed to not be cleaned up. Although this
	// was never observed by a stress test, it is here for safety.
	params.Knobs.SQLSchemaChanger = &sql.SchemaChangerTestingKnobs{
		AsyncExecNotification: func() error {
			return errors.New("async schema changer disabled")
		},
	}

	const numAccounts = 1000
	_, _, sqlDB, dir, cleanup := backupRestoreTestSetupWithParams(t, singleNode, numAccounts,
		initNone, base.TestClusterArgs{ServerArgs: params})
	defer cleanup()

	dir = filepath.Join(dir, "foo")

	sqlDB.Exec(t, `CREATE DATABASE restore`)
	sqlDB.Exec(t, `BACKUP DATABASE data TO $1`, localFoo)
	// Bugger the backup by removing the SST files.
	if err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			t.Fatal(err)
		}
		if info.Name() == backupccl.BackupDescriptorName || !strings.HasSuffix(path, ".sst") {
			return nil
		}
		return os.Remove(path)
	}); err != nil {
		t.Fatal(err)
	}
	sqlDB.ExpectErr(
		t, "sst: no such file",
		`RESTORE data.* FROM $1 WITH OPTIONS ('into_db'='restore')`, localFoo,
	)
	// Verify the failed RESTORE added some DROP tables.
	sqlDB.CheckQueryResults(t,
		`SELECT name FROM crdb_internal.tables WHERE database_name = 'restore' AND state = 'DROP'`,
		[][]string{{"bank"}},
	)
}

func TestBackupRestoreInterleaved(t *testing.T) {
	defer leaktest.AfterTest(t)()
	const numAccounts = 20

	_, _, sqlDB, dir, cleanupFn := backupRestoreTestSetup(t, singleNode, numAccounts, initNone)
	defer cleanupFn()
	args := base.TestServerArgs{ExternalIODir: dir}

	_ = sqlDB.Exec(t, `SET CLUSTER SETTING kv.range_merge.queue_enabled = false`)

	// TODO(dan): The INTERLEAVE IN PARENT clause currently doesn't allow the
	// `db.table` syntax. Fix that and use it here instead of `SET DATABASE`.
	_ = sqlDB.Exec(t, `SET DATABASE = data`)

	_ = sqlDB.Exec(t, `CREATE TABLE strpk (id string, v int, primary key (id, v)) PARTITION BY LIST (id) ( PARTITION ab VALUES IN (('a'), ('b')), PARTITION xy VALUES IN (('x'), ('y')) );`)
	_ = sqlDB.Exec(t, `ALTER PARTITION ab OF TABLE strpk CONFIGURE ZONE USING gc.ttlseconds = 60`)
	_ = sqlDB.Exec(t, `INSERT INTO strpk VALUES ('a', 1), ('a', 2), ('x', 100), ('y', 101)`)
	const numStrPK = 4
	_ = sqlDB.Exec(t, `CREATE TABLE strpkchild (a string, b int, c int, primary key (a, b, c)) INTERLEAVE IN PARENT strpk (a, b)`)

	// i0 interleaves in parent with a, and has a multi-col PK of its own b, c
	_ = sqlDB.Exec(t, `CREATE TABLE i0 (a INT, b INT, c INT, PRIMARY KEY (a, b, c)) INTERLEAVE IN PARENT bank (a)`)
	// Split at at a _strict prefix_ of the cols in i_0's PK
	_ = sqlDB.Exec(t, `ALTER TABLE i0 SPLIT AT VALUES (1, 1)`)

	// i0_0 interleaves into i0.
	_ = sqlDB.Exec(t, `CREATE TABLE i0_0 (a INT, b INT, c INT, d INT, PRIMARY KEY (a, b, c, d)) INTERLEAVE IN PARENT i0 (a, b, c)`)
	_ = sqlDB.Exec(t, `CREATE TABLE i1 (a INT, b CHAR, PRIMARY KEY (a, b)) INTERLEAVE IN PARENT bank (a)`)
	_ = sqlDB.Exec(t, `CREATE TABLE i2 (a INT, b CHAR, PRIMARY KEY (a, b)) INTERLEAVE IN PARENT bank (a)`)

	// The bank table has numAccounts accounts, put 2x that in i0, 3x in i0_0,
	// and 4x in i1.
	totalRows := numAccounts + numStrPK
	for i := 0; i < numAccounts; i++ {
		_ = sqlDB.Exec(t, `INSERT INTO i0 VALUES ($1, 1, 1), ($1, 2, 2)`, i)
		totalRows += 2
		_ = sqlDB.Exec(t, `INSERT INTO i0_0 VALUES ($1, 1, 1, 1), ($1, 2, 2, 2), ($1, 3, 3, 3)`, i)
		totalRows += 3
		_ = sqlDB.Exec(t, `INSERT INTO i1 VALUES ($1, 'a'), ($1, 'b'), ($1, 'c'), ($1, 'd')`, i)
		totalRows += 4
		_ = sqlDB.Exec(t, `INSERT INTO i2 VALUES ($1, 'e'), ($1, 'f'), ($1, 'g'), ($1, 'h')`, i)
		totalRows += 4
	}
	// Split some rows to attempt to exercise edge conditions in the key rewriter.
	_ = sqlDB.Exec(t, `ALTER TABLE i0 SPLIT AT SELECT * from i0 where a % 2 = 0 LIMIT $1`, numAccounts)
	_ = sqlDB.Exec(t, `ALTER TABLE i0_0 SPLIT AT SELECT * from i0_0 LIMIT $1`, numAccounts)
	_ = sqlDB.Exec(t, `ALTER TABLE i1 SPLIT AT SELECT * from i1 WHERE a % 3 = 0`)
	_ = sqlDB.Exec(t, `ALTER TABLE i2 SPLIT AT SELECT * from i2 WHERE a % 5 = 0`)

	// Truncate will allocate a new ID for i1. At that point the splits we created
	// above will still exist, but will contain the old table ID. Since the table
	// does not exist anymore, it will not be in the backup, so the rewriting will
	// not have a configured rewrite for that part of those splits, but we still
	// expect RESTORE to succeed.
	_ = sqlDB.Exec(t, `TRUNCATE i1`)
	for i := 0; i < numAccounts; i++ {
		_ = sqlDB.Exec(t, `INSERT INTO i1 VALUES ($1, 'a'), ($1, 'b'), ($1, 'c'), ($1, 'd')`, i)
	}

	var unused string
	var exportedRows int
	sqlDB.QueryRow(t, `BACKUP DATABASE data TO $1`, localFoo).Scan(
		&unused, &unused, &unused, &exportedRows, &unused, &unused, &unused,
	)
	if exportedRows != totalRows {
		// TODO(dt): fix row-count including interleaved garbarge
		t.Logf("expected %d rows in BACKUP, got %d", totalRows, exportedRows)
	}

	t.Run("all tables in interleave hierarchy", func(t *testing.T) {
		tcRestore := testcluster.StartTestCluster(t, singleNode, base.TestClusterArgs{ServerArgs: args})
		defer tcRestore.Stopper().Stop(context.TODO())
		sqlDBRestore := sqlutils.MakeSQLRunner(tcRestore.Conns[0])
		// Create a dummy database to verify rekeying is correctly performed.
		sqlDBRestore.Exec(t, `CREATE DATABASE ignored`)
		sqlDBRestore.Exec(t, `CREATE DATABASE data`)

		var importedRows int
		sqlDBRestore.QueryRow(t, `RESTORE data.* FROM $1`, localFoo).Scan(
			&unused, &unused, &unused, &importedRows, &unused, &unused, &unused,
		)

		if importedRows != totalRows {
			t.Fatalf("expected %d rows, got %d", totalRows, importedRows)
		}

		var rowCount int64
		sqlDBRestore.QueryRow(t, `SELECT count(*) FROM data.bank`).Scan(&rowCount)
		if rowCount != numAccounts {
			t.Errorf("expected %d rows but found %d", numAccounts, rowCount)
		}
		sqlDBRestore.QueryRow(t, `SELECT count(*) FROM data.i0`).Scan(&rowCount)
		if rowCount != 2*numAccounts {
			t.Errorf("expected %d rows but found %d", 2*numAccounts, rowCount)
		}
		sqlDBRestore.QueryRow(t, `SELECT count(*) FROM data.i0_0`).Scan(&rowCount)
		if rowCount != 3*numAccounts {
			t.Errorf("expected %d rows but found %d", 3*numAccounts, rowCount)
		}
		sqlDBRestore.QueryRow(t, `SELECT count(*) FROM data.i1`).Scan(&rowCount)
		if rowCount != 4*numAccounts {
			t.Errorf("expected %d rows but found %d", 4*numAccounts, rowCount)
		}
	})

	t.Run("interleaved table without parent", func(t *testing.T) {
		sqlDB.ExpectErr(t, "without interleave parent", `BACKUP data.i0 TO $1`, localFoo)

		tcRestore := testcluster.StartTestCluster(t, singleNode, base.TestClusterArgs{ServerArgs: args})
		defer tcRestore.Stopper().Stop(context.TODO())
		sqlDBRestore := sqlutils.MakeSQLRunner(tcRestore.Conns[0])
		sqlDBRestore.Exec(t, `CREATE DATABASE data`)
		sqlDBRestore.ExpectErr(
			t, "without interleave parent",
			`RESTORE TABLE data.i0 FROM $1`, localFoo,
		)
	})

	t.Run("interleaved table without child", func(t *testing.T) {
		sqlDB.ExpectErr(t, "without interleave child", `BACKUP data.bank TO $1`, localFoo)

		tcRestore := testcluster.StartTestCluster(t, singleNode, base.TestClusterArgs{ServerArgs: args})
		defer tcRestore.Stopper().Stop(context.TODO())
		sqlDBRestore := sqlutils.MakeSQLRunner(tcRestore.Conns[0])
		sqlDBRestore.Exec(t, `CREATE DATABASE data`)
		sqlDBRestore.ExpectErr(t, "without interleave child", `RESTORE TABLE data.bank FROM $1`, localFoo)
	})
}

func TestBackupRestoreCrossTableReferences(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const numAccounts = 30
	const createStore = "CREATE DATABASE store"
	const createStoreStats = "CREATE DATABASE storestats"

	_, _, origDB, dir, cleanupFn := backupRestoreTestSetup(t, singleNode, numAccounts, initNone)
	defer cleanupFn()
	args := base.TestServerArgs{ExternalIODir: dir}

	// Generate some testdata and back it up.
	{
		origDB.Exec(t, createStore)
		origDB.Exec(t, createStoreStats)

		// customers has multiple inbound FKs, to different indexes.
		origDB.Exec(t, `CREATE TABLE store.customers (
			id INT PRIMARY KEY,
			email STRING UNIQUE
		)`)

		// orders has both in and outbound FKs (receipts and customers).
		// the index on placed makes indexIDs non-contiguous.
		origDB.Exec(t, `CREATE TABLE store.orders (
			id INT PRIMARY KEY,
			placed TIMESTAMP,
			INDEX (placed DESC),
			customerid INT REFERENCES store.customers
		)`)

		// unused makes our table IDs non-contiguous.
		origDB.Exec(t, `CREATE TABLE data.unused (id INT PRIMARY KEY)`)

		// receipts is has a self-referential FK.
		origDB.Exec(t, `CREATE TABLE store.receipts (
			id INT PRIMARY KEY,
			reissue INT REFERENCES store.receipts(id),
			dest STRING REFERENCES store.customers(email),
			orderid INT REFERENCES store.orders
		)`)

		// and a few views for good measure.
		origDB.Exec(t, `CREATE VIEW store.early_customers AS SELECT id, email from store.customers WHERE id < 5`)
		origDB.Exec(t, `CREATE VIEW storestats.ordercounts AS
			SELECT c.id, c.email, count(o.id)
			FROM store.customers AS c
			LEFT OUTER JOIN store.orders AS o ON o.customerid = c.id
			GROUP BY c.id, c.email
			ORDER BY c.id, c.email
		`)
		origDB.Exec(t, `CREATE VIEW store.unused_view AS SELECT id from store.customers WHERE FALSE`)
		origDB.Exec(t, `CREATE VIEW store.referencing_early_customers AS SELECT id, email FROM store.early_customers`)

		for i := 0; i < numAccounts; i++ {
			origDB.Exec(t, `INSERT INTO store.customers VALUES ($1, $1::string)`, i)
		}
		// Each even customerID gets 3 orders, with predictable order and receipt IDs.
		for cID := 0; cID < numAccounts; cID += 2 {
			for i := 0; i < 3; i++ {
				oID := cID*100 + i
				rID := oID * 10
				origDB.Exec(t, `INSERT INTO store.orders VALUES ($1, now(), $2)`, oID, cID)
				origDB.Exec(t, `INSERT INTO store.receipts VALUES ($1, NULL, $2, $3)`, rID, cID, oID)
				if i > 1 {
					origDB.Exec(t, `INSERT INTO store.receipts VALUES ($1, $2, $3, $4)`, rID+1, rID, cID, oID)
				}
			}
		}
		_ = origDB.Exec(t, `BACKUP DATABASE store, storestats TO $1`, localFoo)
	}

	origCustomers := origDB.QueryStr(t, `SHOW CONSTRAINTS FROM store.customers`)
	origOrders := origDB.QueryStr(t, `SHOW CONSTRAINTS FROM store.orders`)
	origReceipts := origDB.QueryStr(t, `SHOW CONSTRAINTS FROM store.receipts`)

	origEarlyCustomers := origDB.QueryStr(t, `SELECT * from store.early_customers`)
	origOrderCounts := origDB.QueryStr(t, `SELECT * from storestats.ordercounts ORDER BY id`)

	t.Run("restore everything to new cluster", func(t *testing.T) {
		tc := testcluster.StartTestCluster(t, singleNode, base.TestClusterArgs{ServerArgs: args})
		defer tc.Stopper().Stop(context.TODO())
		db := sqlutils.MakeSQLRunner(tc.Conns[0])

		db.Exec(t, createStore)
		db.Exec(t, `RESTORE store.* FROM $1`, localFoo)
		// Restore's Validate checks all the tables point to each other correctly.

		db.CheckQueryResults(t, `SHOW CONSTRAINTS FROM store.customers`, origCustomers)
		db.CheckQueryResults(t, `SHOW CONSTRAINTS FROM store.orders`, origOrders)
		db.CheckQueryResults(t, `SHOW CONSTRAINTS FROM store.receipts`, origReceipts)

		// FK validation on customers from receipts is preserved.
		db.ExpectErr(
			t, "foreign key violation.* referenced in table \"receipts\"",
			`UPDATE store.customers SET email = concat(id::string, 'nope')`,
		)

		// FK validation on customers from orders is preserved.
		db.ExpectErr(
			t, "foreign key violation.* referenced in table \"orders\"",
			`UPDATE store.customers SET id = id * 1000`,
		)

		// FK validation of customer id is preserved.
		db.ExpectErr(
			t, "foreign key violation.* in customers@primary",
			`INSERT INTO store.orders VALUES (999, NULL, 999)`,
		)

		// FK validation of self-FK is preserved.
		db.ExpectErr(
			t, "foreign key violation: value .999. not found in receipts@primary",
			`INSERT INTO store.receipts VALUES (1, 999, NULL, NULL)`,
		)
	})

	t.Run("restore customers to new cluster", func(t *testing.T) {
		tc := testcluster.StartTestCluster(t, singleNode, base.TestClusterArgs{ServerArgs: args})
		defer tc.Stopper().Stop(context.TODO())
		db := sqlutils.MakeSQLRunner(tc.Conns[0])
		db.Exec(t, createStore)
		db.Exec(t, `RESTORE store.customers, store.orders FROM $1`, localFoo)
		// Restore's Validate checks all the tables point to each other correctly.

		// FK validation on customers from orders is preserved.
		db.ExpectErr(
			t, "foreign key violation.* referenced in table \"orders\"",
			`UPDATE store.customers SET id = id*100`,
		)

		// FK validation on customers from receipts is gone.
		db.Exec(t, `UPDATE store.customers SET email = id::string`)
	})

	t.Run("restore orders to new cluster", func(t *testing.T) {
		tc := testcluster.StartTestCluster(t, singleNode, base.TestClusterArgs{ServerArgs: args})
		defer tc.Stopper().Stop(context.TODO())
		db := sqlutils.MakeSQLRunner(tc.Conns[0])
		db.Exec(t, createStore)

		// FK validation of self-FK is preserved.
		db.ExpectErr(
			t, "cannot restore table \"orders\" without referenced table .* \\(or \"skip_missing_foreign_keys\" option\\)",
			`RESTORE store.orders FROM $1`, localFoo,
		)

		db.Exec(t, `RESTORE store.orders FROM $1 WITH OPTIONS ('skip_missing_foreign_keys')`, localFoo)
		// Restore's Validate checks all the tables point to each other correctly.

		// FK validation is gone.
		db.Exec(t, `INSERT INTO store.orders VALUES (999, NULL, 999)`)
		db.Exec(t, `DELETE FROM store.orders`)
	})

	t.Run("restore receipts to new cluster", func(t *testing.T) {
		tc := testcluster.StartTestCluster(t, singleNode, base.TestClusterArgs{ServerArgs: args})
		defer tc.Stopper().Stop(context.TODO())
		db := sqlutils.MakeSQLRunner(tc.Conns[0])
		db.Exec(t, createStore)
		db.Exec(t, `RESTORE store.receipts FROM $1 WITH OPTIONS ('skip_missing_foreign_keys')`, localFoo)
		// Restore's Validate checks all the tables point to each other correctly.

		// FK validation of orders and customer is gone.
		db.Exec(t, `INSERT INTO store.receipts VALUES (1, NULL, '987', 999)`)

		// FK validation of self-FK is preserved.
		db.ExpectErr(
			t, "foreign key violation: value .999. not found in receipts@primary",
			`INSERT INTO store.receipts VALUES (1, 999, NULL, NULL)`,
		)
	})

	t.Run("restore receipts and customers to new cluster", func(t *testing.T) {
		tc := testcluster.StartTestCluster(t, singleNode, base.TestClusterArgs{ServerArgs: args})
		defer tc.Stopper().Stop(context.TODO())
		db := sqlutils.MakeSQLRunner(tc.Conns[0])
		db.Exec(t, createStore)
		db.Exec(t, `RESTORE store.receipts, store.customers FROM $1 WITH OPTIONS ('skip_missing_foreign_keys')`, localFoo)
		// Restore's Validate checks all the tables point to each other correctly.

		// FK validation of orders is gone.
		db.Exec(t, `INSERT INTO store.receipts VALUES (1, NULL, '0', 999)`)

		// FK validation of customer email is preserved.
		db.ExpectErr(
			t, "foreign key violation.* in customers@customers_email_key",
			`INSERT INTO store.receipts VALUES (1, NULL, '999', 999)`,
		)

		// FK validation on customers from receipts is preserved.
		db.ExpectErr(
			t, "foreign key violation.* referenced in table \"receipts\"",
			`DELETE FROM store.customers`,
		)

		// FK validation of self-FK is preserved.
		db.ExpectErr(
			t, "foreign key violation: value .999. not found in receipts@primary",
			`INSERT INTO store.receipts VALUES (1, 999, NULL, NULL)`,
		)
	})

	t.Run("restore simple view", func(t *testing.T) {
		tc := testcluster.StartTestCluster(t, singleNode, base.TestClusterArgs{ServerArgs: args})
		defer tc.Stopper().Stop(context.TODO())
		db := sqlutils.MakeSQLRunner(tc.Conns[0])
		db.Exec(t, createStore)
		db.ExpectErr(
			t, `cannot restore view "early_customers" without restoring referenced table`,
			`RESTORE store.early_customers FROM $1`, localFoo,
		)
		db.Exec(t, `RESTORE store.early_customers, store.customers, store.orders FROM $1`, localFoo)
		db.CheckQueryResults(t, `SELECT * FROM store.early_customers`, origEarlyCustomers)

		// nothing depends on orders so it can be dropped.
		db.Exec(t, `DROP TABLE store.orders`)

		// customers is aware of the view that depends on it.
		db.ExpectErr(
			t, `cannot drop relation "customers" because view "early_customers" depends on it`,
			`DROP TABLE store.customers`,
		)

		// We want to be able to drop columns not used by the view,
		// however the detection thereof is currently broken - #17269.
		//
		// // columns not depended on by the view are unaffected.
		// db.Exec(`ALTER TABLE store.customers DROP COLUMN email`)
		// db.CheckQueryResults(t, `SELECT * FROM store.early_customers`, origEarlyCustomers)

		db.Exec(t, `DROP TABLE store.customers CASCADE`)
	})

	t.Run("restore multi-table view", func(t *testing.T) {
		tc := testcluster.StartTestCluster(t, singleNode, base.TestClusterArgs{ServerArgs: args})
		defer tc.Stopper().Stop(context.TODO())
		db := sqlutils.MakeSQLRunner(tc.Conns[0])

		db.ExpectErr(
			t, `cannot restore view "ordercounts" without restoring referenced table`,
			`RESTORE DATABASE storestats FROM $1`, localFoo,
		)

		db.Exec(t, createStore)
		db.Exec(t, createStoreStats)

		db.ExpectErr(
			t, `cannot restore view "ordercounts" without restoring referenced table`,
			`RESTORE storestats.ordercounts, store.customers FROM $1`, localFoo,
		)

		db.Exec(t, `RESTORE store.customers, storestats.ordercounts, store.orders FROM $1`, localFoo)

		// we want to observe just the view-related errors, not fk errors below.
		db.Exec(t, `ALTER TABLE store.orders DROP CONSTRAINT fk_customerid_ref_customers`)

		// customers is aware of the view that depends on it.
		db.ExpectErr(
			t, `cannot drop relation "customers" because view "storestats.public.ordercounts" depends on it`,
			`DROP TABLE store.customers`,
		)
		db.ExpectErr(
			t, `cannot drop column "email" because view "storestats.public.ordercounts" depends on it`,
			`ALTER TABLE store.customers DROP COLUMN email`,
		)

		// orders is aware of the view that depends on it.
		db.ExpectErr(
			t, `cannot drop relation "orders" because view "storestats.public.ordercounts" depends on it`,
			`DROP TABLE store.orders`,
		)

		db.CheckQueryResults(t, `SELECT * FROM storestats.ordercounts ORDER BY id`, origOrderCounts)

		db.Exec(t, `CREATE DATABASE otherstore`)
		db.Exec(t, `RESTORE store.* FROM $1 WITH into_db = 'otherstore'`, localFoo)
		// we want to observe just the view-related errors, not fk errors below.
		db.Exec(t, `ALTER TABLE otherstore.orders DROP CONSTRAINT fk_customerid_ref_customers`)
		db.Exec(t, `DROP TABLE otherstore.receipts`)

		db.ExpectErr(
			t, `cannot drop relation "customers" because view "early_customers" depends on it`,
			`DROP TABLE otherstore.customers`,
		)

		db.ExpectErr(t, `cannot drop column "email" because view "early_customers" depends on it`,
			`ALTER TABLE otherstore.customers DROP COLUMN email`,
		)
		db.Exec(t, `DROP DATABASE store CASCADE`)
		db.CheckQueryResults(t, `SELECT * FROM otherstore.early_customers ORDER BY id`, origEarlyCustomers)

	})

	t.Run("restore and skip missing views", func(t *testing.T) {
		tc := testcluster.StartTestCluster(t, singleNode, base.TestClusterArgs{ServerArgs: args})
		defer tc.Stopper().Stop(context.TODO())
		db := sqlutils.MakeSQLRunner(tc.Conns[0])

		// Test cases where, after filtering out views that can't be restored, there are no other tables to restore

		db.ExpectErr(t, `no tables to restore: DATABASE storestats`,
			`RESTORE DATABASE storestats from $1 WITH OPTIONS ('skip_missing_views')`, localFoo)

		db.ExpectErr(t, `no tables to restore: TABLE storestats.ordercounts`,
			`RESTORE storestats.ordercounts from $1 WITH OPTIONS ('skip_missing_views')`, localFoo)

		// referencing_early_customers depends only on early_customers, which can't be restored
		db.ExpectErr(t, `no tables to restore: TABLE store.early_customers, store.referencing_early_customers`,
			`RESTORE store.early_customers, store.referencing_early_customers from $1 WITH OPTIONS ('skip_missing_views')`, localFoo)

		// Test that views with valid dependencies are restored

		db.Exec(t, `RESTORE DATABASE store from $1 WITH OPTIONS ('skip_missing_views')`, localFoo)
		db.CheckQueryResults(t, `SELECT * FROM store.early_customers`, origEarlyCustomers)
		db.CheckQueryResults(t, `SELECT * FROM store.referencing_early_customers`, origEarlyCustomers)
		db.Exec(t, `DROP DATABASE store CASCADE`)

		// Test when some tables (views) are skipped and others are restored

		db.Exec(t, createStore)
		// storestats.ordercounts depends also on store.orders, so it can't be restored
		db.Exec(t, `RESTORE storestats.ordercounts, store.customers from $1 WITH OPTIONS ('skip_missing_views')`, localFoo)
		db.CheckQueryResults(t, `SHOW CONSTRAINTS FROM store.customers`, origCustomers)
		db.ExpectErr(t, `relation "storestats.ordercounts" does not exist`, `SELECT * FROM storestats.ordercounts`)
	})
}

func checksumBankPayload(t *testing.T, sqlDB *sqlutils.SQLRunner) uint32 {
	crc := crc32.New(crc32.MakeTable(crc32.Castagnoli))
	rows := sqlDB.Query(t, `SELECT id, balance, payload FROM data.bank`)
	defer rows.Close()
	var id, balance int
	var payload []byte
	for rows.Next() {
		if err := rows.Scan(&id, &balance, &payload); err != nil {
			t.Fatal(err)
		}
		if _, err := crc.Write(payload); err != nil {
			t.Fatal(err)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return crc.Sum32()
}

func TestBackupRestoreIncremental(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const numAccounts = 10
	const numBackups = 4
	windowSize := int(numAccounts / 3)

	_, tc, sqlDB, dir, cleanupFn := backupRestoreTestSetup(t, singleNode, 0, initNone)
	defer cleanupFn()
	args := base.TestServerArgs{ExternalIODir: dir}
	rng, _ := randutil.NewPseudoRand()

	var backupDirs []string
	var checksums []uint32
	{
		for backupNum := 0; backupNum < numBackups; backupNum++ {
			// In the following, windowSize is `w` and offset is `o`. The first
			// mutation creates accounts with id [w,3w). Every mutation after
			// that deletes everything less than o, leaves [o, o+w) unchanged,
			// mutates [o+w,o+2w), and inserts [o+2w,o+3w).
			offset := windowSize * backupNum
			var buf bytes.Buffer
			fmt.Fprintf(&buf, `DELETE FROM data.bank WHERE id < %d; `, offset)
			buf.WriteString(`UPSERT INTO data.bank VALUES `)
			for j := 0; j < windowSize*2; j++ {
				if j != 0 {
					buf.WriteRune(',')
				}
				id := offset + windowSize + j
				payload := randutil.RandBytes(rng, backupRestoreRowPayloadSize)
				fmt.Fprintf(&buf, `(%d, %d, '%s')`, id, backupNum, payload)
			}
			sqlDB.Exec(t, buf.String())

			checksums = append(checksums, checksumBankPayload(t, sqlDB))

			backupDir := fmt.Sprintf("nodelocal:///%d", backupNum)
			var from string
			if backupNum > 0 {
				from = fmt.Sprintf(` INCREMENTAL FROM %s`, strings.Join(backupDirs, `,`))
			}
			sqlDB.Exec(t, fmt.Sprintf(`BACKUP TABLE data.bank TO '%s' %s`, backupDir, from))

			backupDirs = append(backupDirs, fmt.Sprintf(`'%s'`, backupDir))
		}

		// Test a regression in RESTORE where the batch end key was not
		// being set correctly in Import: make an incremental backup such that
		// the greatest key in the diff is less than the previous backups.
		sqlDB.Exec(t, `INSERT INTO data.bank VALUES (0, -1, 'final')`)
		checksums = append(checksums, checksumBankPayload(t, sqlDB))
		sqlDB.Exec(t, fmt.Sprintf(`BACKUP TABLE data.bank TO '%s' %s`,
			"nodelocal:///final", fmt.Sprintf(` INCREMENTAL FROM %s`, strings.Join(backupDirs, `,`)),
		))
		backupDirs = append(backupDirs, `'nodelocal:///final'`)
	}

	// Start a new cluster to restore into.
	{
		restoreTC := testcluster.StartTestCluster(t, singleNode, base.TestClusterArgs{ServerArgs: args})
		defer restoreTC.Stopper().Stop(context.TODO())
		sqlDBRestore := sqlutils.MakeSQLRunner(restoreTC.Conns[0])

		sqlDBRestore.Exec(t, `CREATE DATABASE data`)
		sqlDBRestore.Exec(t, `CREATE TABLE data.bank (id INT PRIMARY KEY)`)
		// This "data.bank" table isn't actually the same table as the backup at all
		// so we should not allow using a backup of the other in incremental. We
		// usually compare IDs, but those are only meaningful in the context of a
		// single cluster, so we also need to ensure the previous backup was indeed
		// generated by the same cluster.

		sqlDBRestore.ExpectErr(
			t, fmt.Sprintf("belongs to cluster %s", tc.Servers[0].ClusterID()),
			`BACKUP TABLE data.bank TO $1 INCREMENTAL FROM $2`,
			"nodelocal:///some-other-table", "nodelocal:///0",
		)

		for i := len(backupDirs); i > 0; i-- {
			sqlDBRestore.Exec(t, `DROP TABLE IF EXISTS data.bank`)
			from := strings.Join(backupDirs[:i], `,`)
			sqlDBRestore.Exec(t, fmt.Sprintf(`RESTORE data.bank FROM %s`, from))

			testutils.SucceedsSoon(t, func() error {
				checksum := checksumBankPayload(t, sqlDBRestore)
				if checksum != checksums[i-1] {
					return errors.Errorf("checksum mismatch at index %d: got %d expected %d",
						i-1, checksum, checksums[i])
				}
				return nil
			})

		}
	}
}

// a bg worker is intended to write to the bank table concurrent with other
// operations (writes, backups, restores), mutating the payload on rows-maxID.
// it notified the `wake` channel (to allow ensuring bg activity has occurred)
// and can be informed when errors are allowable (e.g. when the bank table is
// unavailable between a drop and restore) via the atomic "bool" allowErrors.
func startBackgroundWrites(
	stopper *stop.Stopper, sqlDB *gosql.DB, maxID int, wake chan<- struct{}, allowErrors *int32,
) error {
	rng, _ := randutil.NewPseudoRand()

	for {
		select {
		case <-stopper.ShouldQuiesce():
			return nil // All done.
		default:
			// Keep going.
		}

		id := rand.Intn(maxID)
		payload := randutil.RandBytes(rng, backupRestoreRowPayloadSize)

		updateFn := func() error {
			select {
			case <-stopper.ShouldQuiesce():
				return nil // All done.
			default:
				// Keep going.
			}
			_, err := sqlDB.Exec(`UPDATE data.bank SET payload = $1 WHERE id = $2`, payload, id)
			if atomic.LoadInt32(allowErrors) == 1 {
				return nil
			}
			return err
		}
		if err := retry.ForDuration(testutils.DefaultSucceedsSoonDuration, updateFn); err != nil {
			return err
		}
		select {
		case wake <- struct{}{}:
		default:
		}
	}
}

func TestBackupRestoreWithConcurrentWrites(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const rows = 10
	const numBackgroundTasks = multiNode

	_, tc, sqlDB, _, cleanupFn := backupRestoreTestSetup(t, multiNode, rows, initNone)
	defer cleanupFn()

	bgActivity := make(chan struct{})
	// allowErrors is used as an atomic bool to tell bg workers when to allow
	// errors, between dropping and restoring the table they are using.
	var allowErrors int32
	for task := 0; task < numBackgroundTasks; task++ {
		taskNum := task
		tc.Stopper().RunWorker(context.TODO(), func(context.Context) {
			conn := tc.Conns[taskNum%len(tc.Conns)]
			// Use different sql gateways to make sure leasing is right.
			if err := startBackgroundWrites(tc.Stopper(), conn, rows, bgActivity, &allowErrors); err != nil {
				t.Error(err)
			}
		})
	}

	// Use the data.bank table as a key (id), value (balance) table with a
	// payload.The background tasks are mutating the table concurrently while we
	// backup and restore.
	<-bgActivity

	// Set, break, then reset the id=balance invariant -- while doing concurrent
	// writes -- to get multiple MVCC revisions as well as txn conflicts.
	sqlDB.Exec(t, `UPDATE data.bank SET balance = id`)
	<-bgActivity
	sqlDB.Exec(t, `UPDATE data.bank SET balance = -1`)
	<-bgActivity
	sqlDB.Exec(t, `UPDATE data.bank SET balance = id`)
	<-bgActivity

	// Backup DB while concurrent writes continue.
	sqlDB.Exec(t, `BACKUP DATABASE data TO $1`, localFoo)

	// Drop the table and restore from backup and check our invariant.
	atomic.StoreInt32(&allowErrors, 1)
	sqlDB.Exec(t, `DROP TABLE data.bank`)
	sqlDB.Exec(t, `RESTORE data.* FROM $1`, localFoo)
	atomic.StoreInt32(&allowErrors, 0)

	bad := sqlDB.QueryStr(t, `SELECT id, balance, payload FROM data.bank WHERE id != balance`)
	for _, r := range bad {
		t.Errorf("bad row ID %s = bal %s (payload: %q)", r[0], r[1], r[2])
	}
}

func TestConcurrentBackupRestores(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const numAccounts = 10
	const concurrency, numIterations = 2, 3
	ctx, _, sqlDB, _, cleanupFn := backupRestoreTestSetup(t, multiNode, numAccounts, initNone)
	defer cleanupFn()

	g, gCtx := errgroup.WithContext(ctx)
	for i := 0; i < concurrency; i++ {
		table := fmt.Sprintf("bank_%d", i)
		sqlDB.Exec(t, fmt.Sprintf(
			`CREATE TABLE data.%s AS (SELECT * FROM data.bank WHERE id > %d ORDER BY id)`,
			table, i,
		))
		g.Go(func() error {
			for j := 0; j < numIterations; j++ {
				dbName := fmt.Sprintf("%s_%d", table, j)
				backupDir := fmt.Sprintf("nodelocal:///%s", dbName)
				backupQ := fmt.Sprintf(`BACKUP data.%s TO $1`, table)
				if _, err := sqlDB.DB.ExecContext(gCtx, backupQ, backupDir); err != nil {
					return err
				}
				if _, err := sqlDB.DB.ExecContext(gCtx, fmt.Sprintf(`CREATE DATABASE %s`, dbName)); err != nil {
					return err
				}
				restoreQ := fmt.Sprintf(`RESTORE data.%s FROM $1 WITH OPTIONS ('into_db'='%s')`, table, dbName)
				if _, err := sqlDB.DB.ExecContext(gCtx, restoreQ, backupDir); err != nil {
					return err
				}
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		t.Fatalf("%+v", err)
	}

	for i := 0; i < concurrency; i++ {
		orig := sqlDB.QueryStr(t, `SELECT * FROM data.bank WHERE id > $1 ORDER BY id`, i)
		for j := 0; j < numIterations; j++ {
			selectQ := fmt.Sprintf(`SELECT * FROM bank_%d_%d.bank_%d ORDER BY id`, i, j, i)
			sqlDB.CheckQueryResults(t, selectQ, orig)
		}
	}
}

func TestBackupAsOfSystemTime(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const numAccounts = 1000

	ctx, _, sqlDB, _, cleanupFn := backupRestoreTestSetup(t, singleNode, numAccounts, initNone)
	defer cleanupFn()

	var beforeTs, equalTs string
	var rowCount int

	sqlDB.QueryRow(t, `SELECT cluster_logical_timestamp()`).Scan(&beforeTs)

	err := crdb.ExecuteTx(ctx, sqlDB.DB.(*gosql.DB), nil /* txopts */, func(tx *gosql.Tx) error {
		_, err := tx.Exec(`DELETE FROM data.bank WHERE id % 4 = 1`)
		if err != nil {
			return err
		}
		return tx.QueryRow(`SELECT cluster_logical_timestamp()`).Scan(&equalTs)
	})
	if err != nil {
		t.Fatal(err)
	}

	sqlDB.QueryRow(t, `SELECT count(*) FROM data.bank`).Scan(&rowCount)
	if expected := numAccounts * 3 / 4; rowCount != expected {
		t.Fatalf("expected %d rows but found %d", expected, rowCount)
	}

	beforeDir := filepath.Join(localFoo, `beforeTs`)
	sqlDB.Exec(t, fmt.Sprintf(`BACKUP DATABASE data TO '%s' AS OF SYSTEM TIME %s`, beforeDir, beforeTs))
	equalDir := filepath.Join(localFoo, `equalTs`)
	sqlDB.Exec(t, fmt.Sprintf(`BACKUP DATABASE data TO '%s' AS OF SYSTEM TIME %s`, equalDir, equalTs))

	sqlDB.Exec(t, `DROP TABLE data.bank`)
	sqlDB.Exec(t, `RESTORE data.* FROM $1`, beforeDir)
	sqlDB.QueryRow(t, `SELECT count(*) FROM data.bank`).Scan(&rowCount)
	if expected := numAccounts; rowCount != expected {
		t.Fatalf("expected %d rows but found %d", expected, rowCount)
	}

	sqlDB.Exec(t, `DROP TABLE data.bank`)
	sqlDB.Exec(t, `RESTORE data.* FROM $1`, equalDir)
	sqlDB.QueryRow(t, `SELECT count(*) FROM data.bank`).Scan(&rowCount)
	if expected := numAccounts * 3 / 4; rowCount != expected {
		t.Fatalf("expected %d rows but found %d", expected, rowCount)
	}
}

func TestRestoreAsOfSystemTime(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const numAccounts = 10
	ctx, _, sqlDB, _, cleanupFn := backupRestoreTestSetup(t, singleNode, numAccounts, initNone)
	defer cleanupFn()
	const dir = "nodelocal:///"

	ts := make([]string, 9)

	sqlDB.QueryRow(t, `SELECT cluster_logical_timestamp()`).Scan(&ts[0])

	sqlDB.Exec(t, `UPDATE data.bank SET balance = 1`)
	sqlDB.QueryRow(t, `SELECT cluster_logical_timestamp()`).Scan(&ts[1])

	// Change the data in the tabe.
	sqlDB.Exec(t, `CREATE TABLE data.teller (id INT PRIMARY KEY, name STRING)`)
	sqlDB.Exec(t, `INSERT INTO data.teller VALUES (1, 'alice'), (7, 'bob'), (3, 'eve')`)

	err := crdb.ExecuteTx(ctx, sqlDB.DB.(*gosql.DB), nil /* txopts */, func(tx *gosql.Tx) error {
		_, err := tx.Exec(`UPDATE data.bank SET balance = 2`)
		if err != nil {
			return err
		}
		return tx.QueryRow(`SELECT cluster_logical_timestamp()`).Scan(&ts[2])
	})
	if err != nil {
		t.Fatal(err)
	}

	fullBackup, latestBackup := filepath.Join(dir, "full"), filepath.Join(dir, "latest")
	incBackup, incLatestBackup := filepath.Join(dir, "inc"), filepath.Join(dir, "inc-latest")
	inc2Backup, inc2LatestBackup := incBackup+".2", incLatestBackup+".2"

	sqlDB.Exec(t,
		fmt.Sprintf(`BACKUP DATABASE data TO $1 AS OF SYSTEM TIME %s WITH revision_history`, ts[2]),
		fullBackup,
	)
	sqlDB.Exec(t,
		fmt.Sprintf(`BACKUP DATABASE data TO $1 AS OF SYSTEM TIME %s`, ts[2]),
		latestBackup,
	)

	fullTableBackup := filepath.Join(dir, "tbl")
	sqlDB.Exec(t,
		fmt.Sprintf(`BACKUP data.bank TO $1 AS OF SYSTEM TIME %s WITH revision_history`, ts[2]),
		fullTableBackup,
	)

	sqlDB.Exec(t, `UPDATE data.bank SET balance = 3`)

	// Create a table in some other DB -- this won't be in this backup (yet).
	sqlDB.Exec(t, `CREATE DATABASE other`)
	sqlDB.Exec(t, `CREATE TABLE other.sometable (id INT PRIMARY KEY, somevalue INT)`)
	sqlDB.Exec(t, `INSERT INTO other.sometable VALUES (1, 2), (7, 5), (3, 3)`)

	sqlDB.QueryRow(t, `SELECT cluster_logical_timestamp()`).Scan(&ts[3])

	sqlDB.Exec(t, `DELETE FROM data.bank WHERE id >= $1 / 2`, numAccounts)
	sqlDB.Exec(t, `ALTER TABLE other.sometable RENAME TO data.sometable`)
	sqlDB.QueryRow(t, `SELECT cluster_logical_timestamp()`).Scan(&ts[4])

	sqlDB.Exec(t, `INSERT INTO data.sometable VALUES (2, 2), (4, 5), (6, 3)`)
	sqlDB.Exec(t, `ALTER TABLE data.bank ADD COLUMN points_balance INT DEFAULT 50`)
	sqlDB.QueryRow(t, `SELECT cluster_logical_timestamp()`).Scan(&ts[5])

	sqlDB.Exec(t, `TRUNCATE TABLE data.bank`)
	sqlDB.Exec(t, `TRUNCATE TABLE data.bank`)
	sqlDB.Exec(t, `TRUNCATE TABLE data.bank`)
	sqlDB.Exec(t, `ALTER TABLE data.sometable RENAME TO other.sometable`)
	sqlDB.Exec(t, `CREATE INDEX ON data.teller (name)`)
	sqlDB.Exec(t, `INSERT INTO data.bank VALUES (2, 2), (4, 4)`)
	sqlDB.Exec(t, `INSERT INTO data.teller VALUES (2, 'craig')`)
	sqlDB.QueryRow(t, `SELECT cluster_logical_timestamp()`).Scan(&ts[6])

	sqlDB.Exec(t, `TRUNCATE TABLE data.bank`)
	sqlDB.Exec(t, `INSERT INTO data.bank VALUES (2, 2), (4, 4)`)
	sqlDB.Exec(t, `DROP TABLE other.sometable`)
	sqlDB.QueryRow(t, `SELECT cluster_logical_timestamp()`).Scan(&ts[7])

	sqlDB.Exec(t, `UPSERT INTO data.bank (id, balance)
	           SELECT i, 4 FROM generate_series(0, $1 - 1) AS g(i)`, numAccounts)
	sqlDB.QueryRow(t, `SELECT cluster_logical_timestamp()`).Scan(&ts[8])

	sqlDB.Exec(t,
		fmt.Sprintf(`BACKUP DATABASE data TO $1 AS OF SYSTEM TIME %s INCREMENTAL FROM $2 WITH revision_history`, ts[5]),
		incBackup, fullBackup,
	)
	sqlDB.Exec(t,
		`BACKUP DATABASE data TO $1 INCREMENTAL FROM $2, $3 WITH revision_history`,
		inc2Backup, fullBackup, incBackup,
	)

	sqlDB.Exec(t,
		fmt.Sprintf(`BACKUP DATABASE data TO $1	AS OF SYSTEM TIME %s INCREMENTAL FROM $2`, ts[5]),
		incLatestBackup, latestBackup,
	)
	sqlDB.Exec(t,
		`BACKUP DATABASE data TO $1 INCREMENTAL FROM $2, $3`,
		inc2LatestBackup, latestBackup, incLatestBackup,
	)

	incTableBackup := filepath.Join(dir, "inctbl")
	sqlDB.Exec(t,
		`BACKUP data.bank TO $1 INCREMENTAL FROM $2 WITH revision_history`,
		incTableBackup, fullTableBackup,
	)

	var after string
	sqlDB.QueryRow(t, `SELECT cluster_logical_timestamp()`).Scan(&after)

	for i, timestamp := range ts {
		name := fmt.Sprintf("ts%d", i)
		t.Run(name, func(t *testing.T) {
			sqlDB = sqlutils.MakeSQLRunner(sqlDB.DB)
			// Create new DBs into which we'll restore our copies without conflicting
			// with the existing, original table.
			sqlDB.Exec(t, fmt.Sprintf(`CREATE DATABASE %s`, name))
			sqlDB.Exec(t, fmt.Sprintf(`CREATE DATABASE %stbl`, name))
			// Restore the bank table from the full DB MVCC backup to time x, into a
			// separate DB so that we can later compare it to the original table via
			// time-travel.
			sqlDB.Exec(t,
				fmt.Sprintf(
					`RESTORE data.* FROM $1, $2, $3 AS OF SYSTEM TIME %s WITH into_db='%s'`,
					timestamp, name,
				),
				fullBackup, incBackup, inc2Backup,
			)
			// Similarly restore the since-table backup -- since full DB and single table
			// backups sometimes behave differently.
			sqlDB.Exec(t,
				fmt.Sprintf(
					`RESTORE data.bank FROM $1, $2 AS OF SYSTEM TIME %s WITH into_db='%stbl'`,
					timestamp, name,
				),
				fullTableBackup, incTableBackup,
			)

			// Use time-travel on the existing bank table to determine what RESTORE
			// with AS OF should have produced.
			expected := sqlDB.QueryStr(
				t, fmt.Sprintf(`SELECT * FROM data.bank AS OF SYSTEM TIME %s ORDER BY id`, timestamp),
			)
			// Confirm reading (with no as-of) from the as-of restored table matches.
			sqlDB.CheckQueryResults(t, fmt.Sprintf(`SELECT * FROM %s.bank ORDER BY id`, name), expected)
			sqlDB.CheckQueryResults(t, fmt.Sprintf(`SELECT * FROM %stbl.bank ORDER BY id`, name), expected)

			// `sometable` moved in to data between after ts 3 and removed before 5.
			if i == 4 || i == 5 {
				sqlDB.CheckQueryResults(t,
					fmt.Sprintf(`SELECT * FROM %s.sometable ORDER BY id`, name),
					sqlDB.QueryStr(t, fmt.Sprintf(`SELECT * FROM data.sometable AS OF SYSTEM TIME %s ORDER BY id`, timestamp)),
				)
			}
			// teller was created after ts 2.
			if i > 2 {
				sqlDB.CheckQueryResults(t,
					fmt.Sprintf(`SELECT * FROM %s.teller ORDER BY id`, name),
					sqlDB.QueryStr(t, fmt.Sprintf(`SELECT * FROM data.teller AS OF SYSTEM TIME %s ORDER BY id`, timestamp)),
				)
			}
		})
	}

	t.Run("latest", func(t *testing.T) {
		sqlDB = sqlutils.MakeSQLRunner(sqlDB.DB)
		// The "latest" backup didn't specify ALL mvcc values, so we can't restore
		// to times in the middle.
		sqlDB.Exec(t, `CREATE DATABASE err`)

		// fullBackup covers up to ts[2], inc to ts[5], inc2 to > ts[8].
		sqlDB.ExpectErr(
			t, "incompatible RESTORE timestamp",
			fmt.Sprintf(`RESTORE data.* FROM $1 AS OF SYSTEM TIME %s WITH into_db='err'`, ts[3]),
			fullBackup,
		)

		for _, i := range ts {
			sqlDB.ExpectErr(
				t, "incompatible RESTORE timestamp",
				fmt.Sprintf(`RESTORE data.* FROM $1 AS OF SYSTEM TIME %s WITH into_db='err'`, i),
				latestBackup,
			)

			sqlDB.ExpectErr(
				t, "incompatible RESTORE timestamp",
				fmt.Sprintf(`RESTORE data.* FROM $1, $2, $3 AS OF SYSTEM TIME %s WITH into_db='err'`, i),
				latestBackup, incLatestBackup, inc2LatestBackup,
			)
		}

		sqlDB.ExpectErr(
			t, "incompatible RESTORE timestamp",
			fmt.Sprintf(`RESTORE data.* FROM $1 AS OF SYSTEM TIME %s WITH into_db='err'`, after),
			latestBackup,
		)
	})
}

func TestRestoreAsOfSystemTimeGCBounds(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const numAccounts = 10
	ctx, tc, sqlDB, _, cleanupFn := backupRestoreTestSetup(t, singleNode, numAccounts, initNone)
	defer cleanupFn()
	const dir = "nodelocal:///"
	preGC := tree.TimestampToDecimal(tc.Server(0).Clock().Now()).String()

	gcr := roachpb.GCRequest{
		// Bogus span to make it a valid request.
		RequestHeader: roachpb.RequestHeader{
			Key:    keys.MakeTablePrefix(keys.MinUserDescID),
			EndKey: keys.MaxKey,
		},
		Threshold: tc.Server(0).Clock().Now(),
	}
	if _, err := client.SendWrapped(ctx, tc.Server(0).DistSender(), &gcr); err != nil {
		t.Fatal(err)
	}

	postGC := tree.TimestampToDecimal(tc.Server(0).Clock().Now()).String()

	lateFullTableBackup := filepath.Join(dir, "tbl-after-gc")
	sqlDB.Exec(t, `BACKUP data.bank TO $1 WITH revision_history`, lateFullTableBackup)
	sqlDB.Exec(t, `DROP TABLE data.bank`)
	sqlDB.ExpectErr(
		t, `BACKUP for requested time only has revision history from`,
		fmt.Sprintf(`RESTORE data.bank FROM $1 AS OF SYSTEM TIME %s`, preGC),
		lateFullTableBackup,
	)
	sqlDB.Exec(
		t, fmt.Sprintf(`RESTORE data.bank FROM $1 AS OF SYSTEM TIME %s`, postGC), lateFullTableBackup,
	)
}

func TestAsOfSystemTimeOnRestoredData(t *testing.T) {
	defer leaktest.AfterTest(t)()

	_, _, sqlDB, dir, cleanupFn := backupRestoreTestSetup(t, singleNode, 0, initNone)
	defer cleanupFn()

	sqlDB.Exec(t, `DROP TABLE data.bank`)

	const numAccounts = 10
	bankData := bank.FromRows(numAccounts).Tables()[0]
	if _, err := sampledataccl.ToBackup(t, bankData, filepath.Join(dir, "foo")); err != nil {
		t.Fatalf("%+v", err)
	}

	var beforeTs string
	sqlDB.QueryRow(t, `SELECT cluster_logical_timestamp()`).Scan(&beforeTs)
	sqlDB.Exec(t, `RESTORE data.* FROM $1`, localFoo)
	var afterTs string
	sqlDB.QueryRow(t, `SELECT cluster_logical_timestamp()`).Scan(&afterTs)

	var rowCount int
	const q = `SELECT count(*) FROM data.bank AS OF SYSTEM TIME '%s'`
	// Before the RESTORE, the table doesn't exist, so an AS OF query should fail.
	sqlDB.ExpectErr(
		t, `relation "data.bank" does not exist`,
		fmt.Sprintf(q, beforeTs),
	)
	// After the RESTORE, an AS OF query should work.
	sqlDB.QueryRow(t, fmt.Sprintf(q, afterTs)).Scan(&rowCount)
	if expected := numAccounts; rowCount != expected {
		t.Fatalf("expected %d rows but found %d", expected, rowCount)
	}
}

func TestBackupRestoreChecksum(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const numAccounts = 1000
	_, _, sqlDB, dir, cleanupFn := backupRestoreTestSetup(t, singleNode, numAccounts, initNone)
	defer cleanupFn()
	dir = filepath.Join(dir, "foo")

	sqlDB.Exec(t, `BACKUP DATABASE data TO $1`, localFoo)

	var backupDesc backupccl.BackupDescriptor
	{
		backupDescBytes, err := ioutil.ReadFile(filepath.Join(dir, backupccl.BackupDescriptorName))
		if err != nil {
			t.Fatalf("%+v", err)
		}
		if err := protoutil.Unmarshal(backupDescBytes, &backupDesc); err != nil {
			t.Fatalf("%+v", err)
		}
	}

	// Corrupt one of the files in the backup.
	f, err := os.OpenFile(filepath.Join(dir, backupDesc.Files[1].Path), os.O_WRONLY, 0)
	if err != nil {
		t.Fatalf("%+v", err)
	}
	defer f.Close()
	// The last eight bytes of an SST file store a nonzero magic number. We can
	// blindly null out those bytes and guarantee that the checksum will change.
	if _, err := f.Seek(-8, io.SeekEnd); err != nil {
		t.Fatalf("%+v", err)
	}
	if _, err := f.Write(make([]byte, 8)); err != nil {
		t.Fatalf("%+v", err)
	}
	if err := f.Sync(); err != nil {
		t.Fatalf("%+v", err)
	}

	sqlDB.Exec(t, `DROP TABLE data.bank`)
	sqlDB.ExpectErr(t, "checksum mismatch", `RESTORE data.* FROM $1`, localFoo)
}

func TestTimestampMismatch(t *testing.T) {
	defer leaktest.AfterTest(t)()
	const numAccounts = 1

	_, _, sqlDB, _, cleanupFn := backupRestoreTestSetup(t, singleNode, numAccounts, initNone)
	defer cleanupFn()

	sqlDB.Exec(t, `CREATE TABLE data.t2 (a INT PRIMARY KEY)`)
	sqlDB.Exec(t, `INSERT INTO data.t2 VALUES (1)`)

	fullBackup := filepath.Join(localFoo, "0")
	incrementalT1FromFull := filepath.Join(localFoo, "1")
	incrementalT2FromT1 := filepath.Join(localFoo, "2")
	incrementalT3FromT1OneTable := filepath.Join(localFoo, "3")

	sqlDB.Exec(t, `BACKUP DATABASE data TO $1`,
		fullBackup)
	sqlDB.Exec(t, `BACKUP DATABASE data TO $1 INCREMENTAL FROM $2`,
		incrementalT1FromFull, fullBackup)
	sqlDB.Exec(t, `BACKUP TABLE data.bank TO $1 INCREMENTAL FROM $2`,
		incrementalT3FromT1OneTable, fullBackup)
	sqlDB.Exec(t, `BACKUP DATABASE data TO $1 INCREMENTAL FROM $2, $3`,
		incrementalT2FromT1, fullBackup, incrementalT1FromFull)

	t.Run("Backup", func(t *testing.T) {
		// Missing the initial full backup.
		sqlDB.ExpectErr(
			t, "backups listed out of order",
			`BACKUP DATABASE data TO $1 INCREMENTAL FROM $2`,
			localFoo, incrementalT1FromFull,
		)

		// Missing an intermediate incremental backup.
		sqlDB.ExpectErr(
			t, "backups listed out of order",
			`BACKUP DATABASE data TO $1 INCREMENTAL FROM $2, $3`,
			localFoo, fullBackup, incrementalT2FromT1,
		)

		// Backups specified out of order.
		sqlDB.ExpectErr(
			t, "out of order",
			`BACKUP DATABASE data TO $1 INCREMENTAL FROM $2, $3`,
			localFoo, incrementalT1FromFull, fullBackup,
		)

		// Missing data for one table in the most recent backup.
		sqlDB.ExpectErr(
			t, "previous backup does not contain table",
			`BACKUP DATABASE data TO $1 INCREMENTAL FROM $2, $3`,
			localFoo, fullBackup, incrementalT3FromT1OneTable,
		)
	})

	sqlDB.Exec(t, `DROP TABLE data.bank`)
	sqlDB.Exec(t, `DROP TABLE data.t2`)
	t.Run("Restore", func(t *testing.T) {
		// Missing the initial full backup.
		sqlDB.ExpectErr(t, "no backup covers time", `RESTORE data.* FROM $1`, incrementalT1FromFull)

		// Missing an intermediate incremental backup.
		sqlDB.ExpectErr(
			t, "no backup covers time",
			`RESTORE data.* FROM $1, $2`, fullBackup, incrementalT2FromT1,
		)

		// Backups specified out of order.
		sqlDB.ExpectErr(
			t, "out of order",
			`RESTORE data.* FROM $1, $2`, incrementalT1FromFull, fullBackup,
		)

		// Missing data for one table in the most recent backup.
		sqlDB.ExpectErr(
			t, "table \"data.t2\" does not exist",
			`RESTORE data.bank, data.t2 FROM $1, $2`, fullBackup, incrementalT3FromT1OneTable,
		)
	})
}

func TestBackupLevelDB(t *testing.T) {
	defer leaktest.AfterTest(t)()

	_, _, sqlDB, rawDir, cleanupFn := backupRestoreTestSetup(t, singleNode, 1, initNone)
	defer cleanupFn()

	_ = sqlDB.Exec(t, `BACKUP DATABASE data TO $1`, localFoo)
	// Verify that the sstables are in LevelDB format by checking the trailer
	// magic.
	var magic = []byte("\x57\xfb\x80\x8b\x24\x75\x47\xdb")
	foundSSTs := 0
	if err := filepath.Walk(rawDir, func(path string, info os.FileInfo, err error) error {
		if filepath.Ext(path) == ".sst" {
			foundSSTs++
			data, err := ioutil.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.HasSuffix(data, magic) {
				t.Fatalf("trailer magic is not LevelDB sstable: %s", path)
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("%+v", err)
	}
	if foundSSTs == 0 {
		t.Fatal("found no sstables")
	}
}

func TestRestoredPrivileges(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const numAccounts = 1
	_, _, sqlDB, dir, cleanupFn := backupRestoreTestSetup(t, singleNode, numAccounts, initNone)
	defer cleanupFn()
	args := base.TestServerArgs{ExternalIODir: dir}

	rootOnly := sqlDB.QueryStr(t, `SHOW GRANTS ON data.bank`)

	sqlDB.Exec(t, `CREATE USER someone`)
	sqlDB.Exec(t, `GRANT SELECT, INSERT, UPDATE, DELETE ON data.bank TO someone`)

	withGrants := sqlDB.QueryStr(t, `SHOW GRANTS ON data.bank`)

	sqlDB.Exec(t, `BACKUP DATABASE data TO $1`, localFoo)
	sqlDB.Exec(t, `DROP TABLE data.bank`)

	t.Run("into fresh db", func(t *testing.T) {
		tc := testcluster.StartTestCluster(t, singleNode, base.TestClusterArgs{ServerArgs: args})
		defer tc.Stopper().Stop(context.TODO())
		sqlDBRestore := sqlutils.MakeSQLRunner(tc.Conns[0])
		sqlDBRestore.Exec(t, `CREATE DATABASE data`)
		sqlDBRestore.Exec(t, `RESTORE data.bank FROM $1`, localFoo)
		sqlDBRestore.CheckQueryResults(t, `SHOW GRANTS ON data.bank`, rootOnly)
	})

	t.Run("into db with added grants", func(t *testing.T) {
		tc := testcluster.StartTestCluster(t, singleNode, base.TestClusterArgs{ServerArgs: args})
		defer tc.Stopper().Stop(context.TODO())
		sqlDBRestore := sqlutils.MakeSQLRunner(tc.Conns[0])
		sqlDBRestore.Exec(t, `CREATE DATABASE data`)
		sqlDBRestore.Exec(t, `CREATE USER someone`)
		sqlDBRestore.Exec(t, `GRANT SELECT, INSERT, UPDATE, DELETE ON DATABASE data TO someone`)
		sqlDBRestore.Exec(t, `RESTORE data.bank FROM $1`, localFoo)
		sqlDBRestore.CheckQueryResults(t, `SHOW GRANTS ON data.bank`, withGrants)
	})
}

func TestRestoreInto(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const numAccounts = 1
	_, _, sqlDB, _, cleanupFn := backupRestoreTestSetup(t, singleNode, numAccounts, initNone)
	defer cleanupFn()

	sqlDB.Exec(t, `BACKUP DATABASE data TO $1`, localFoo)

	restoreStmt := fmt.Sprintf(`RESTORE data.bank FROM '%s' WITH into_db = 'data 2'`, localFoo)

	sqlDB.ExpectErr(t, "a database named \"data 2\" needs to exist", restoreStmt)

	sqlDB.Exec(t, `CREATE DATABASE "data 2"`)
	sqlDB.Exec(t, restoreStmt)

	expected := sqlDB.QueryStr(t, `SELECT * FROM data.bank`)
	sqlDB.CheckQueryResults(t, `SELECT * FROM "data 2".bank`, expected)
}

func TestBackupRestorePermissions(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const numAccounts = 1
	_, tc, sqlDB, _, cleanupFn := backupRestoreTestSetup(t, singleNode, numAccounts, initNone)
	defer cleanupFn()

	sqlDB.Exec(t, `CREATE USER testuser`)
	pgURL, cleanupFunc := sqlutils.PGUrl(
		t, tc.Server(0).ServingAddr(), "TestBackupRestorePermissions-testuser", url.User("testuser"),
	)
	defer cleanupFunc()
	testuser, err := gosql.Open("postgres", pgURL.String())
	if err != nil {
		t.Fatal(err)
	}
	defer testuser.Close()

	backupStmt := fmt.Sprintf(`BACKUP DATABASE data TO '%s'`, localFoo)

	t.Run("root-only", func(t *testing.T) {
		if _, err := testuser.Exec(backupStmt); !testutils.IsError(
			err, "only superusers are allowed to BACKUP",
		) {
			t.Fatal(err)
		}
		if _, err := testuser.Exec(`RESTORE blah FROM 'blah'`); !testutils.IsError(
			err, "only superusers are allowed to RESTORE",
		) {
			t.Fatal(err)
		}
	})

	t.Run("privs-required", func(t *testing.T) {
		sqlDB.Exec(t, backupStmt)
		// Root doesn't have CREATE on `system` DB, so that should fail. Still need
		// a valid `dir` though, since descriptors are always loaded first.
		sqlDB.ExpectErr(
			t, "user root does not have CREATE privilege",
			`RESTORE data.bank FROM $1 WITH OPTIONS ('into_db'='system')`, localFoo,
		)
	})
}

func TestRestoreDatabaseVersusTable(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const numAccounts = 1
	_, tc, origDB, _, cleanupFn := backupRestoreTestSetup(t, singleNode, numAccounts, initNone)
	defer cleanupFn()
	args := base.TestServerArgs{ExternalIODir: tc.Servers[0].ClusterSettings().ExternalIODir}

	for _, q := range []string{
		`CREATE DATABASE d2`,
		`CREATE DATABASE d3`,
		`CREATE TABLE d3.foo (a INT)`,
		`CREATE DATABASE d4`,
		`CREATE TABLE d4.foo (a INT)`,
		`CREATE TABLE d4.bar (a INT)`,
	} {
		origDB.Exec(t, q)
	}

	d4foo := "nodelocal:///d4foo"
	d4foobar := "nodelocal:///d4foobar"
	d4star := "nodelocal:///d4star"

	origDB.Exec(t, `BACKUP DATABASE data, d2, d3, d4 TO $1`, localFoo)
	origDB.Exec(t, `BACKUP d4.foo TO $1`, d4foo)
	origDB.Exec(t, `BACKUP d4.foo, d4.bar TO $1`, d4foobar)
	origDB.Exec(t, `BACKUP d4.* TO $1`, d4star)

	t.Run("incomplete-db", func(t *testing.T) {
		tcRestore := testcluster.StartTestCluster(t, singleNode, base.TestClusterArgs{ServerArgs: args})
		defer tcRestore.Stopper().Stop(context.TODO())
		sqlDB := sqlutils.MakeSQLRunner(tcRestore.Conns[0])

		sqlDB.Exec(t, `create database d5`)

		sqlDB.ExpectErr(
			t, "cannot RESTORE DATABASE from a backup of individual tables",
			`RESTORE database d4 FROM $1`, d4foo,
		)

		sqlDB.ExpectErr(
			t, "cannot RESTORE <database>.* from a backup of individual tables",
			`RESTORE d4.* FROM $1 WITH into_db = 'd5'`, d4foo,
		)

		sqlDB.ExpectErr(
			t, "cannot RESTORE DATABASE from a backup of individual tables",
			`RESTORE database d4 FROM $1`, d4foobar,
		)

		sqlDB.ExpectErr(
			t, "cannot RESTORE <database>.* from a backup of individual tables",
			`RESTORE d4.* FROM $1 WITH into_db = 'd5'`, d4foobar,
		)

		sqlDB.ExpectErr(
			t, "cannot RESTORE DATABASE from a backup of individual tables",
			`RESTORE database d4 FROM $1`, d4foo,
		)

		sqlDB.Exec(t, `RESTORE database d4 FROM $1`, d4star)

	})

	t.Run("db", func(t *testing.T) {
		tcRestore := testcluster.StartTestCluster(t, singleNode, base.TestClusterArgs{ServerArgs: args})
		defer tcRestore.Stopper().Stop(context.TODO())
		sqlDB := sqlutils.MakeSQLRunner(tcRestore.Conns[0])
		sqlDB.Exec(t, `RESTORE DATABASE data, d2, d3 FROM $1`, localFoo)
	})

	t.Run("db-exists", func(t *testing.T) {
		tcRestore := testcluster.StartTestCluster(t, singleNode, base.TestClusterArgs{ServerArgs: args})
		defer tcRestore.Stopper().Stop(context.TODO())
		sqlDB := sqlutils.MakeSQLRunner(tcRestore.Conns[0])

		sqlDB.Exec(t, `CREATE DATABASE data`)
		sqlDB.ExpectErr(t, "already exists", `RESTORE DATABASE data FROM $1`, localFoo)
	})

	t.Run("tables", func(t *testing.T) {
		tcRestore := testcluster.StartTestCluster(t, singleNode, base.TestClusterArgs{ServerArgs: args})
		defer tcRestore.Stopper().Stop(context.TODO())
		sqlDB := sqlutils.MakeSQLRunner(tcRestore.Conns[0])

		sqlDB.Exec(t, `CREATE DATABASE data`)
		sqlDB.Exec(t, `RESTORE data.* FROM $1`, localFoo)
	})

	t.Run("tables-needs-db", func(t *testing.T) {
		tcRestore := testcluster.StartTestCluster(t, singleNode, base.TestClusterArgs{ServerArgs: args})
		defer tcRestore.Stopper().Stop(context.TODO())
		sqlDB := sqlutils.MakeSQLRunner(tcRestore.Conns[0])

		sqlDB.ExpectErr(t, "needs to exist", `RESTORE data.*, d4.* FROM $1`, localFoo)
	})

	t.Run("into_db", func(t *testing.T) {
		tcRestore := testcluster.StartTestCluster(t, singleNode, base.TestClusterArgs{ServerArgs: args})
		defer tcRestore.Stopper().Stop(context.TODO())
		sqlDB := sqlutils.MakeSQLRunner(tcRestore.Conns[0])

		sqlDB.ExpectErr(
			t, `cannot use "into_db"`,
			`RESTORE DATABASE data FROM $1 WITH into_db = 'other'`, localFoo,
		)
	})
}

func TestBackupAzureAccountName(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const numAccounts = 1
	_, _, sqlDB, _, cleanupFn := backupRestoreTestSetup(t, singleNode, numAccounts, initNone)
	defer cleanupFn()

	values := url.Values{}
	values.Set("AZURE_ACCOUNT_KEY", "password")
	values.Set("AZURE_ACCOUNT_NAME", "\n")

	url := &url.URL{
		Scheme:   "azure",
		Host:     "host",
		Path:     "/backup",
		RawQuery: values.Encode(),
	}

	// Verify newlines in the account name cause an error.
	sqlDB.ExpectErr(t, "azure: account name is not valid", `backup database d to $1`, url.String())
}

// If an operator issues a bad query or if a deploy contains a bug that corrupts
// data, it should be possible to return to a previous point in time before the
// badness. For cases when the last good timestamp is within the gc threshold,
// see the subtests for two ways this can work.
func TestPointInTimeRecovery(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const numAccounts = 1000
	_, _, sqlDB, _, cleanupFn := backupRestoreTestSetup(t, singleNode, numAccounts, initNone)
	defer cleanupFn()

	fullBackupDir := filepath.Join(localFoo, "full")
	sqlDB.Exec(t, `BACKUP data.* TO $1`, fullBackupDir)

	sqlDB.Exec(t, `UPDATE data.bank SET balance = 2`)

	incBackupDir := filepath.Join(localFoo, "inc")
	sqlDB.Exec(t, `BACKUP data.* TO $1 INCREMENTAL FROM $2`, incBackupDir, fullBackupDir)

	var beforeBadThingTs string
	sqlDB.Exec(t, `UPDATE data.bank SET balance = 3`)
	sqlDB.QueryRow(t, `SELECT cluster_logical_timestamp()`).Scan(&beforeBadThingTs)

	// Something bad happens.
	sqlDB.Exec(t, `UPDATE data.bank SET balance = 4`)

	beforeBadThingData := sqlDB.QueryStr(t,
		fmt.Sprintf(`SELECT * FROM data.bank AS OF SYSTEM TIME '%s' ORDER BY id`, beforeBadThingTs),
	)

	// If no previous BACKUPs have been taken, a new one can be taken using `AS
	// OF SYSTEM TIME` with a timestamp before the badness started. This can
	// then be RESTORE'd into a temporary database. The operator can manually
	// reconcile the current data with the restored data before finally
	// RENAME-ing the table into the final location.
	t.Run("recovery=new-backup", func(t *testing.T) {
		sqlDB = sqlutils.MakeSQLRunner(sqlDB.DB)
		recoveryDir := filepath.Join(localFoo, "new-backup")
		sqlDB.Exec(t,
			fmt.Sprintf(`BACKUP data.* TO $1 AS OF SYSTEM TIME '%s'`, beforeBadThingTs),
			recoveryDir,
		)
		sqlDB.Exec(t, `CREATE DATABASE newbackup`)
		sqlDB.Exec(t, `RESTORE data.* FROM $1 WITH into_db=newbackup`, recoveryDir)

		// Some manual reconciliation of the data in data.bank and
		// newbackup.bank could be done here by the operator.

		sqlDB.Exec(t, `DROP TABLE data.bank`)
		sqlDB.Exec(t, `ALTER TABLE newbackup.bank RENAME TO data.bank`)
		sqlDB.Exec(t, `DROP DATABASE newbackup`)
		sqlDB.CheckQueryResults(t, `SELECT * FROM data.bank ORDER BY id`, beforeBadThingData)
	})

	// If there is a recent BACKUP (either full or incremental), then it will
	// likely be faster to make a BACKUP that is incremental from it and RESTORE
	// using that. Everything else works the same as above.
	t.Run("recovery=inc-backup", func(t *testing.T) {
		sqlDB = sqlutils.MakeSQLRunner(sqlDB.DB)
		recoveryDir := filepath.Join(localFoo, "inc-backup")
		sqlDB.Exec(t,
			fmt.Sprintf(`BACKUP data.* TO $1 AS OF SYSTEM TIME '%s' INCREMENTAL FROM $2, $3`, beforeBadThingTs),
			recoveryDir, fullBackupDir, incBackupDir,
		)
		sqlDB.Exec(t, `CREATE DATABASE incbackup`)
		sqlDB.Exec(t,
			`RESTORE data.* FROM $1, $2, $3 WITH into_db=incbackup`,
			fullBackupDir, incBackupDir, recoveryDir,
		)

		// Some manual reconciliation of the data in data.bank and
		// incbackup.bank could be done here by the operator.

		sqlDB.Exec(t, `DROP TABLE data.bank`)
		sqlDB.Exec(t, `ALTER TABLE incbackup.bank RENAME TO data.bank`)
		sqlDB.Exec(t, `DROP DATABASE incbackup`)
		sqlDB.CheckQueryResults(t, `SELECT * FROM data.bank ORDER BY id`, beforeBadThingData)
	})
}

func TestBackupRestoreDropDB(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const numAccounts = 1
	_, _, sqlDB, _, cleanupFn := backupRestoreTestSetup(t, singleNode, numAccounts, initNone)
	defer cleanupFn()

	sqlDB.Exec(t, `DROP DATABASE data`)
	sqlDB.Exec(t, `CREATE DATABASE data`)
	sqlDB.Exec(t, `CREATE TABLE data.bank (i int)`)
	sqlDB.Exec(t, `INSERT INTO data.bank VALUES (1)`)

	sqlDB.Exec(t, "BACKUP DATABASE data TO $1", localFoo)
	sqlDB.Exec(t, "CREATE DATABASE data2")
	sqlDB.Exec(t, "RESTORE data.* FROM $1 WITH OPTIONS ('into_db'='data2')", localFoo)

	expected := sqlDB.QueryStr(t, `SELECT * FROM data.bank`)
	sqlDB.CheckQueryResults(t, `SELECT * FROM data2.bank`, expected)
}

func TestBackupRestoreDropTable(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const numAccounts = 1
	_, _, sqlDB, _, cleanupFn := backupRestoreTestSetup(t, singleNode, numAccounts, initNone)
	defer cleanupFn()

	sqlDB.Exec(t, `DROP TABLE data.bank`)
	sqlDB.Exec(t, `
		CREATE TABLE data.bank (i int);
		INSERT INTO data.bank VALUES (1);
	`)

	sqlDB.Exec(t, "BACKUP DATABASE data TO $1", localFoo)
	sqlDB.Exec(t, "CREATE DATABASE data2")
	sqlDB.Exec(t, "RESTORE data.* FROM $1 WITH OPTIONS ('into_db'='data2')", localFoo)

	expected := sqlDB.QueryStr(t, `SELECT * FROM data.bank`)
	sqlDB.CheckQueryResults(t, `SELECT * FROM data2.bank`, expected)
}

func TestBackupRestoreIncrementalAddTable(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const numAccounts = 1
	_, _, sqlDB, _, cleanupFn := backupRestoreTestSetup(t, singleNode, numAccounts, initNone)
	defer cleanupFn()
	sqlDB.Exec(t, `CREATE DATABASE data2`)
	sqlDB.Exec(t, `CREATE TABLE data.t (s string PRIMARY KEY)`)
	full, inc := filepath.Join(localFoo, "full"), filepath.Join(localFoo, "inc")

	sqlDB.Exec(t, `INSERT INTO data.t VALUES ('before')`)
	sqlDB.Exec(t, `BACKUP data.*, data2.* TO $1`, full)
	sqlDB.Exec(t, `UPDATE data.t SET s = 'after'`)

	sqlDB.Exec(t, `CREATE TABLE data2.t2 (i int)`)
	sqlDB.Exec(t, "BACKUP data.*, data2.* TO $1 INCREMENTAL FROM $2", inc, full)
}

func TestBackupRestoreIncrementalAddTableMissing(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const numAccounts = 1
	_, _, sqlDB, _, cleanupFn := backupRestoreTestSetup(t, singleNode, numAccounts, initNone)
	defer cleanupFn()
	sqlDB.Exec(t, `CREATE DATABASE data2`)
	sqlDB.Exec(t, `CREATE TABLE data.t (s string PRIMARY KEY)`)
	full, inc := filepath.Join(localFoo, "full"), filepath.Join(localFoo, "inc")

	sqlDB.Exec(t, `INSERT INTO data.t VALUES ('before')`)
	sqlDB.Exec(t, `BACKUP data.* TO $1`, full)
	sqlDB.Exec(t, `UPDATE data.t SET s = 'after'`)

	sqlDB.Exec(t, `CREATE TABLE data2.t2 (i int)`)
	sqlDB.ExpectErr(
		t, "previous backup does not contain table",
		"BACKUP data.*, data2.* TO $1 INCREMENTAL FROM $2", inc, full,
	)
}

func TestBackupRestoreIncrementalTrucateTable(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const numAccounts = 1
	_, _, sqlDB, _, cleanupFn := backupRestoreTestSetup(t, singleNode, numAccounts, initNone)
	defer cleanupFn()
	sqlDB.Exec(t, `CREATE TABLE data.t (s string PRIMARY KEY)`)
	full, inc := filepath.Join(localFoo, "full"), filepath.Join(localFoo, "inc")

	sqlDB.Exec(t, `INSERT INTO data.t VALUES ('before')`)
	sqlDB.Exec(t, `BACKUP DATABASE data TO $1`, full)
	sqlDB.Exec(t, `UPDATE data.t SET s = 'after'`)
	sqlDB.Exec(t, `TRUNCATE data.t`)

	sqlDB.Exec(t, "BACKUP DATABASE data TO $1 INCREMENTAL FROM $2", inc, full)
}

func TestBackupRestoreIncrementalDropTable(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const numAccounts = 1
	_, _, sqlDB, _, cleanupFn := backupRestoreTestSetup(t, singleNode, numAccounts, initNone)
	defer cleanupFn()
	sqlDB.Exec(t, `CREATE TABLE data.t (s string PRIMARY KEY)`)
	full, inc := filepath.Join(localFoo, "full"), filepath.Join(localFoo, "inc")

	sqlDB.Exec(t, `INSERT INTO data.t VALUES ('before')`)
	sqlDB.Exec(t, `BACKUP DATABASE data TO $1`, full)
	sqlDB.Exec(t, `UPDATE data.t SET s = 'after'`)
	sqlDB.Exec(t, `DROP TABLE data.t`)

	sqlDB.Exec(t, "BACKUP DATABASE data TO $1 INCREMENTAL FROM $2", inc, full)
	sqlDB.Exec(t, `DROP DATABASE data`)

	// Restoring to backup before DROP restores t.
	sqlDB.Exec(t, `RESTORE DATABASE data FROM $1`, full)
	sqlDB.Exec(t, `SELECT 1 FROM data.t LIMIT 0`)
	sqlDB.Exec(t, `DROP DATABASE data`)

	// Restoring to backup after DROP does not restore t.
	sqlDB.Exec(t, `RESTORE DATABASE data FROM $1, $2`, full, inc)
	sqlDB.ExpectErr(t, "relation \"data.t\" does not exist", `SELECT 1 FROM data.t LIMIT 0`)
}

func TestFileIOLimits(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const numAccounts = 11
	_, _, sqlDB, _, cleanupFn := backupRestoreTestSetup(t, singleNode, numAccounts, initNone)
	defer cleanupFn()

	elsewhere := "nodelocal:///../../blah"

	sqlDB.Exec(t, `BACKUP data.bank TO $1`, localFoo)
	sqlDB.ExpectErr(
		t, "local file access to paths outside of external-io-dir is not allowed",
		`BACKUP data.bank TO $1`, elsewhere,
	)

	sqlDB.Exec(t, `DROP TABLE data.bank`)

	sqlDB.Exec(t, `RESTORE data.bank FROM $1`, localFoo)
	sqlDB.ExpectErr(
		t, "local file access to paths outside of external-io-dir is not allowed",
		`RESTORE data.bank FROM $1`, elsewhere,
	)
}

func TestBackupRestoreNotInTxn(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const numAccounts = 1
	_, _, sqlDB, _, cleanupFn := backupRestoreTestSetup(t, singleNode, numAccounts, initNone)
	defer cleanupFn()

	db := sqlDB.DB.(*gosql.DB)
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`BACKUP DATABASE data TO 'blah'`); !testutils.IsError(err, "cannot be used inside a transaction") {
		t.Fatal(err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}

	sqlDB.Exec(t, `BACKUP DATABASE data TO $1`, localFoo)
	sqlDB.Exec(t, `DROP DATABASE data`)
	sqlDB.Exec(t, `RESTORE DATABASE data FROM $1`, localFoo)

	tx, err = db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`RESTORE DATABASE data FROM 'blah'`); !testutils.IsError(err, "cannot be used inside a transaction") {
		t.Fatal(err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}

	// TODO(dt): move to importccl.
	tx, err = db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`IMPORT TABLE t (id INT PRIMARY KEY) CSV DATA ('blah')`); !testutils.IsError(err, "cannot be used inside a transaction") {
		t.Fatal(err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
}

func TestBackupRestoreSequence(t *testing.T) {
	defer leaktest.AfterTest(t)()
	const numAccounts = 1
	_, _, origDB, dir, cleanupFn := backupRestoreTestSetup(t, singleNode, numAccounts, initNone)
	defer cleanupFn()
	args := base.TestServerArgs{ExternalIODir: dir}

	backupLoc := localFoo

	origDB.Exec(t, `CREATE SEQUENCE data.t_id_seq`)
	origDB.Exec(t, `CREATE TABLE data.t (id INT PRIMARY KEY DEFAULT nextval('data.t_id_seq'), v text)`)
	origDB.Exec(t, `INSERT INTO data.t (v) VALUES ('foo'), ('bar'), ('baz')`)

	origDB.Exec(t, `BACKUP DATABASE data TO $1`, backupLoc)

	t.Run("restore both table & sequence to a new cluster", func(t *testing.T) {
		tc := testcluster.StartTestCluster(t, singleNode, base.TestClusterArgs{ServerArgs: args})
		defer tc.Stopper().Stop(context.TODO())
		newDB := sqlutils.MakeSQLRunner(tc.Conns[0])

		newDB.Exec(t, `RESTORE DATABASE data FROM $1`, backupLoc)
		newDB.Exec(t, `USE data`)

		// Verify that the db was restored correctly.
		newDB.CheckQueryResults(t, `SELECT * FROM t`, [][]string{
			{"1", "foo"},
			{"2", "bar"},
			{"3", "baz"},
		})
		newDB.CheckQueryResults(t, `SELECT last_value FROM t_id_seq`, [][]string{
			{"3"},
		})

		// Verify that we can kkeep inserting into the table, without violating a uniqueness constraint.
		newDB.Exec(t, `INSERT INTO data.t (v) VALUES ('bar')`)

		// Verify that sequence <=> table dependencies are still in place.
		newDB.ExpectErr(
			t, "pq: cannot drop sequence t_id_seq because other objects depend on it",
			`DROP SEQUENCE t_id_seq`,
		)
	})

	t.Run("restore just the table to a new cluster", func(t *testing.T) {
		tc := testcluster.StartTestCluster(t, singleNode, base.TestClusterArgs{ServerArgs: args})
		defer tc.Stopper().Stop(context.TODO())
		newDB := sqlutils.MakeSQLRunner(tc.Conns[0])

		newDB.Exec(t, `CREATE DATABASE data`)
		newDB.Exec(t, `USE data`)

		newDB.ExpectErr(
			t, "pq: cannot restore table \"t\" without referenced sequence 54 \\(or \"skip_missing_sequences\" option\\)",
			`RESTORE TABLE t FROM $1`, localFoo,
		)

		newDB.Exec(t, `RESTORE TABLE t FROM $1 WITH OPTIONS ('skip_missing_sequences')`, localFoo)

		// Verify that the table was restored correctly.
		newDB.CheckQueryResults(t, `SELECT * FROM data.t`, [][]string{
			{"1", "foo"},
			{"2", "bar"},
			{"3", "baz"},
		})

		// Test that insertion without specifying the id column doesn't work, since
		// the DEFAULT expression has been removed.
		newDB.ExpectErr(
			t, `pq: missing \"id\" primary key column`,
			`INSERT INTO t (v) VALUES ('bloop')`,
		)

		// Test that inserting with a value specified works.
		newDB.Exec(t, `INSERT INTO t (id, v) VALUES (4, 'bloop')`)
	})

	t.Run("restore just the sequence to a new cluster", func(t *testing.T) {
		tc := testcluster.StartTestCluster(t, singleNode, base.TestClusterArgs{ServerArgs: args})
		defer tc.Stopper().Stop(context.TODO())
		newDB := sqlutils.MakeSQLRunner(tc.Conns[0])

		newDB.Exec(t, `CREATE DATABASE data`)
		newDB.Exec(t, `USE data`)
		// TODO(vilterp): create `RESTORE SEQUENCE` instead of `RESTORE TABLE`, and force
		// people to use that?
		newDB.Exec(t, `RESTORE TABLE t_id_seq FROM $1`, backupLoc)

		// Verify that the sequence value was restored.
		newDB.CheckQueryResults(t, `SELECT last_value FROM data.t_id_seq`, [][]string{
			{"3"},
		})

		// Verify that the reference to the table that used it was removed, and
		// it can be dropped.
		newDB.Exec(t, `DROP SEQUENCE t_id_seq`)
	})
}

func TestBackupRestoreShowJob(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const numAccounts = 1
	_, _, sqlDB, _, cleanupFn := backupRestoreTestSetup(t, singleNode, numAccounts, initNone)
	defer cleanupFn()

	sqlDB.Exec(t, `BACKUP DATABASE data TO $1 WITH revision_history`, localFoo)
	sqlDB.Exec(t, `CREATE DATABASE "data 2"`)

	sqlDB.Exec(t, `RESTORE data.bank FROM $1 WITH skip_missing_foreign_keys, into_db = $2`, localFoo, "data 2")
	sqlDB.CheckQueryResults(t, "SELECT description FROM [SHOW JOBS] ORDER BY description", [][]string{
		{"BACKUP DATABASE data TO 'nodelocal:///foo' WITH revision_history"},
		{"RESTORE TABLE data.bank FROM 'nodelocal:///foo' WITH into_db = 'data 2', skip_missing_foreign_keys"},
	})
}

func TestCreateStatsAfterRestore(t *testing.T) {
	defer leaktest.AfterTest(t)()

	defer func(oldRefreshInterval, oldAsOf time.Duration) {
		stats.DefaultRefreshInterval = oldRefreshInterval
		stats.DefaultAsOfTime = oldAsOf
	}(stats.DefaultRefreshInterval, stats.DefaultAsOfTime)
	stats.DefaultRefreshInterval = time.Millisecond
	stats.DefaultAsOfTime = time.Microsecond

	const numAccounts = 1
	_, _, sqlDB, _, cleanupFn := backupRestoreTestSetup(t, singleNode, numAccounts, initNone)
	defer cleanupFn()

	sqlDB.Exec(t, `SET CLUSTER SETTING sql.stats.automatic_collection.enabled=true`)

	sqlDB.Exec(t, `BACKUP DATABASE data TO $1 WITH revision_history`, localFoo)
	sqlDB.Exec(t, `CREATE DATABASE "data 2"`)
	sqlDB.Exec(t, `RESTORE data.bank FROM $1 WITH skip_missing_foreign_keys, into_db = $2`,
		localFoo, "data 2")

	// Verify that statistics have been created.
	sqlDB.CheckQueryResultsRetry(t,
		`SELECT statistics_name, column_names, row_count, distinct_count, null_count
	  FROM [SHOW STATISTICS FOR TABLE "data 2".bank]`,
		[][]string{
			{"__auto__", "{id}", "1", "1", "0"},
			{"__auto__", "{balance}", "1", "1", "0"},
			{"__auto__", "{payload}", "1", "1", "0"},
		})
}
