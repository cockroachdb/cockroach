// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package operations

import (
	"context"
	gosql "database/sql"
	"fmt"
	"reflect"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/operation"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestflags"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/tests"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/fingerprintutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

type backupRestoreCleanup struct {
	db string
}

func (cl *backupRestoreCleanup) Cleanup(
	ctx context.Context, o operation.Operation, c cluster.Cluster,
) {
	conn := c.Conn(ctx, o.L(), 1, option.VirtualClusterName(roachtestflags.VirtualCluster))
	defer conn.Close()

	o.Status(fmt.Sprintf("dropping newly created db %s", cl.db))
	_, err := conn.ExecContext(ctx, fmt.Sprintf("DROP DATABASE IF EXISTS %s CASCADE", cl.db))
	if err != nil {
		o.Fatal(err)
	}
}

// createBackupChain creates a backup schedule for the given database, waits for
// a full backup to complete, then triggers and waits for 24 incremental
// backups. It returns the HLC timestamp of the latest backup layer (for
// fingerprint validation).
func createBackupChain(
	ctx context.Context, o operation.Operation, conn *gosql.DB, dbName string, bucket string,
) hlc.Timestamp {
	scheduleName := fmt.Sprintf("backup-restore-op-%d", timeutil.Now().UnixNano())
	o.Status(fmt.Sprintf("creating backup schedule %s for db %s", scheduleName, dbName))
	rows, err := conn.QueryContext(ctx, fmt.Sprintf(
		`CREATE SCHEDULE IF NOT EXISTS '%s'
		FOR BACKUP DATABASE %s INTO '%s'
		WITH revision_history
		RECURRING '@daily'
		FULL BACKUP '@weekly'
		WITH SCHEDULE OPTIONS first_run = 'now'`,
		scheduleName, dbName, bucket))
	if err != nil {
		o.Fatal(err)
	}
	defer rows.Close()
	// The first row is always the incremental schedule and the second is the
	// full schedule. CREATE SCHEDULE returns columns: schedule_id, label,
	// status, first_run, schedule, backup_stmt.
	var incScheduleID, fullScheduleID int64
	var discard interface{}
	if !rows.Next() {
		o.Fatalf("expected incremental schedule row from CREATE SCHEDULE")
	}
	if err := rows.Scan(&incScheduleID, &discard, &discard, &discard, &discard, &discard); err != nil {
		o.Fatal(err)
	}
	if !rows.Next() {
		o.Fatalf("expected full schedule row from CREATE SCHEDULE")
	}
	if err := rows.Scan(&fullScheduleID, &discard, &discard, &discard, &discard, &discard); err != nil {
		o.Fatal(err)
	}

	// Dropping the full schedule also drops the dependent incremental schedule.
	defer func() {
		o.Status("dropping backup schedule")
		_, _ = conn.ExecContext(ctx, fmt.Sprintf("DROP SCHEDULE %d", fullScheduleID))
	}()

	retryOpts := retry.Options{
		InitialBackoff: 2 * time.Second,
		MaxBackoff:     10 * time.Second,
		MaxRetries:     30,
	}

	// Wait for the full backup job to appear.
	o.Status(fmt.Sprintf("waiting for full backup job (schedule %d)", fullScheduleID))
	var fullJobID catpb.JobID
	for r := retry.StartWithCtx(ctx, retryOpts); r.Next(); {
		err = conn.QueryRowContext(ctx,
			"SELECT id FROM system.jobs WHERE created_by_id = $1", fullScheduleID,
		).Scan(&fullJobID)
		if err == nil {
			break
		}
	}
	if err != nil {
		o.Fatalf("full backup job not found for schedule %d: %v", fullScheduleID, err)
	}

	// Wait for the full backup to succeed.
	o.Status(fmt.Sprintf("waiting for full backup job %d to complete", fullJobID))
	if err := tests.WaitForSucceeded(ctx, conn, fullJobID, 8760*time.Hour /* 1 year - operation specs define their own timeouts */); err != nil {
		o.Fatal(err)
	}

	// Run 24 incremental backups via the schedule.
	for i := range 24 {
		o.Status(fmt.Sprintf("triggering incremental backup %d of 24", i+1))
		ts := timeutil.Now()
		_, err = conn.ExecContext(ctx, fmt.Sprintf(
			"ALTER BACKUP SCHEDULE %d EXECUTE IMMEDIATELY", fullScheduleID))
		if err != nil {
			o.Fatal(err)
		}

		// Wait for the incremental backup job to appear.
		var incJobID catpb.JobID
		for r := retry.StartWithCtx(ctx, retryOpts); r.Next(); {
			err = conn.QueryRowContext(ctx,
				"SELECT id FROM system.jobs WHERE created_by_id = $1 AND created > $2",
				incScheduleID, ts,
			).Scan(&incJobID)
			if err == nil {
				break
			}
		}
		if err != nil {
			o.Fatalf("incremental backup job not found for schedule %d: %v", incScheduleID, err)
		}

		o.Status(fmt.Sprintf("waiting for incremental backup job %d to complete", incJobID))
		if err := tests.WaitForSucceeded(ctx, conn, incJobID, 8760*time.Hour /* 1 year - operation specs define their own timeouts */); err != nil {
			o.Fatal(err)
		}
	}

	// Get the end time of the latest backup for fingerprint validation.
	var backupEndTime time.Time
	err = conn.QueryRowContext(ctx, fmt.Sprintf(
		"SELECT max(end_time) FROM [SHOW BACKUP FROM LATEST IN '%s']", bucket,
	)).Scan(&backupEndTime)
	if err != nil {
		o.Fatal(err)
	}
	return hlc.Timestamp{WallTime: backupEndTime.UnixNano()}
}

func runBackupRestore(
	ctx context.Context, o operation.Operation, c cluster.Cluster, online bool, validate bool,
) (cleanup registry.OperationCleanup) {
	defer func() {
		if r := recover(); r != nil {
			o.Errorf("error during backup restore: %v", r)
		}
	}()
	// This operation looks for the district table in a database named cct_tpcc or tpcc.
	rng, _ := randutil.NewPseudoRand()
	dbWhitelist := []string{"cct_tpcc", "tpcc"}
	conn := c.Conn(ctx, o.L(), 1, option.VirtualClusterName(roachtestflags.VirtualCluster))
	defer conn.Close()
	dbs, err := conn.QueryContext(ctx, "SELECT database_name FROM [SHOW DATABASES]")
	if err != nil {
		o.Fatal(err)
	}
	var dbName string
outer:
	for dbs.Next() {
		var dbStr string
		if err := dbs.Scan(&dbStr); err != nil {
			o.Fatal(err)
		}
		for i := range dbWhitelist {
			if dbWhitelist[i] == dbStr {
				// We found a db in the whitelist.
				dbName = dbStr
				break outer
			}
		}
	}
	if dbName == "" {
		o.Status(fmt.Sprintf("did not find a db in the whitelist %v", dbWhitelist))
		return nil
	}

	var scheme string
	switch c.Cloud() {
	case spec.AWS:
		scheme = "s3"
	case spec.Azure:
		scheme = "azure"
	case spec.GCE:
		scheme = "gs"
	default:
		scheme = ""
	}
	bucket := fmt.Sprintf("%s://%s/operation-backup-restore/%d/?AUTH=implicit",
		scheme, testutils.BackupTestingBucket(), timeutil.Now().UnixNano())

	backupTS := createBackupChain(ctx, o, conn, dbName, bucket)

	// Assign cleanup handler early, before RESTORE creates the database.
	// The cleanup uses DROP DATABASE IF EXISTS, so it's safe even if the
	// RESTORE fails and the database was never created.
	restoreDBName := fmt.Sprintf("backup_restore_op_%d", rng.Int63())
	cleanup = &backupRestoreCleanup{db: restoreDBName}

	onlineStr := "online"
	if !online {
		onlineStr = "offline"
	}
	o.Status(fmt.Sprintf("restoring %s into db %s", onlineStr, restoreDBName))

	startTime := timeutil.Now()
	if !online {
		o.Status("beginning offline restore")
		_, err = conn.ExecContext(ctx, fmt.Sprintf(
			"RESTORE DATABASE %s FROM LATEST IN '%s' WITH OPTIONS (new_db_name = '%s')",
			dbName, bucket, restoreDBName))
		if err != nil {
			o.Fatal(err)
		}
	} else {
		var id, tables, approxRows, approxBytes int64
		var downloadJobId catpb.JobID
		o.Status("beginning online restore")

		type scanResult struct {
			id, tables, approxRows, approxBytes int64
			downloadJobId                       catpb.JobID
			err                                 error
		}
		resCh := make(chan scanResult, 1)
		go func() {
			var r scanResult
			row := conn.QueryRowContext(ctx, fmt.Sprintf(
				"RESTORE DATABASE %s FROM LATEST IN '%s' WITH OPTIONS (new_db_name = '%s', EXPERIMENTAL DEFERRED COPY)",
				dbName, bucket, restoreDBName))
			r.err = row.Scan(&r.id, &r.tables, &r.approxRows, &r.approxBytes, &r.downloadJobId)
			resCh <- r
		}()

		var scanErr error
		select {
		case r := <-resCh:
			id, tables, approxRows, approxBytes, downloadJobId = r.id, r.tables, r.approxRows, r.approxBytes, r.downloadJobId
			scanErr = r.err
			if scanErr != nil {
				o.Fatal(scanErr)
			}
		case <-ctx.Done():
			o.Fatal(ctx.Err())
		}

		// Suppress unused variable warnings for id, tables, approxRows, approxBytes.
		_, _, _, _ = id, tables, approxRows, approxBytes

		if scanErr == nil {
			err = tests.WaitForSucceeded(ctx, conn, downloadJobId, 8760*time.Hour /* 1 year - operation specs define their own timeouts */)
			if err != nil {
				o.Fatal(err)
			}
		}
	}
	o.Status(fmt.Sprintf("completed restore in %v", timeutil.Since(startTime)))

	if validate {
		o.Status(fmt.Sprintf("verifying db %s matches %s", dbName, restoreDBName))
		sourceFingerprints, err := fingerprintutils.FingerprintDatabase(ctx, conn, dbName, fingerprintutils.AOST(backupTS), fingerprintutils.Stripped())
		if err != nil {
			o.Fatal(err)
		}

		// No AOST here; the timestamps are rewritten on restore. But nobody else is touching this database, so that's fine.
		destFingerprints, err := fingerprintutils.FingerprintDatabase(ctx, conn, restoreDBName, fingerprintutils.Stripped())
		if err != nil {
			o.Fatal(err)
		}

		if !reflect.DeepEqual(sourceFingerprints, destFingerprints) {
			o.Fatalf("backup and restore fingerprints do not match: %v != %v", sourceFingerprints, destFingerprints)
		}
	}

	return cleanup
}

func runBackupRestoreFn(
	online bool, validate bool,
) func(context.Context, operation.Operation, cluster.Cluster) registry.OperationCleanup {
	return func(ctx context.Context, o operation.Operation, c cluster.Cluster) registry.OperationCleanup {
		return runBackupRestore(ctx, o, c, online, validate)
	}
}

func registerBackupRestore(r registry.Registry) {
	r.AddOperation(registry.OperationSpec{
		Name:               "backup-restore/tpcc/online=false/fingerprint=true",
		Owner:              registry.OwnerDisasterRecovery,
		Timeout:            96 * time.Hour,
		CompatibleClouds:   registry.AllClouds,
		CanRunConcurrently: registry.OperationCanRunConcurrently,
		Dependencies:       []registry.OperationDependency{registry.OperationRequiresPopulatedDatabase},
		Run:                runBackupRestoreFn(false, true),
	})

	r.AddOperation(registry.OperationSpec{
		Name:               "backup-restore/tpcc/online=false/fingerprint=false",
		Owner:              registry.OwnerDisasterRecovery,
		Timeout:            24 * time.Hour,
		CompatibleClouds:   registry.AllClouds,
		CanRunConcurrently: registry.OperationCanRunConcurrently,
		Dependencies:       []registry.OperationDependency{registry.OperationRequiresPopulatedDatabase},
		Run:                runBackupRestoreFn(false, false),
	})

	r.AddOperation(registry.OperationSpec{
		Name:               "backup-restore/tpcc/online=true/fingerprint=true",
		Owner:              registry.OwnerDisasterRecovery,
		Timeout:            96 * time.Hour,
		CompatibleClouds:   registry.AllClouds,
		CanRunConcurrently: registry.OperationCanRunConcurrently,
		Dependencies:       []registry.OperationDependency{registry.OperationRequiresPopulatedDatabase},
		Run:                runBackupRestoreFn(true, true),
	})

	r.AddOperation(registry.OperationSpec{
		Name:               "backup-restore/tpcc/online=true/fingerprint=false",
		Owner:              registry.OwnerDisasterRecovery,
		Timeout:            24 * time.Hour,
		CompatibleClouds:   registry.AllClouds,
		CanRunConcurrently: registry.OperationCanRunConcurrently,
		Dependencies:       []registry.OperationDependency{registry.OperationRequiresPopulatedDatabase},
		Run:                runBackupRestoreFn(true, false),
	})
}
