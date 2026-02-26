// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package operations

import (
	"context"
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

	o.Status(fmt.Sprintf("backing db %s (full)", dbName))
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
	bucket := fmt.Sprintf("%s://%s/operation-backup-restore/%d/?AUTH=implicit", scheme, testutils.BackupTestingBucket(), timeutil.Now().UnixNano())

	backupTS := hlc.Timestamp{WallTime: timeutil.Now().Add(-10 * time.Second).UTC().UnixNano()}

	if !online {
		_, err = conn.ExecContext(ctx, fmt.Sprintf("BACKUP DATABASE %s INTO '%s' AS OF SYSTEM TIME '%s' WITH revision_history", dbName, bucket, backupTS.AsOfSystemTime()))
		if err != nil {
			o.Fatal(err)
		}
		for i := range 24 {
			o.Status(fmt.Sprintf("backing up db %s (incremental layer %d)", dbName, i))
			// Update backupTS to match the latest layer.
			backupTS = hlc.Timestamp{WallTime: timeutil.Now().Add(-10 * time.Second).UTC().UnixNano()}
			_, err = conn.ExecContext(ctx, fmt.Sprintf("BACKUP DATABASE %s INTO LATEST IN '%s' AS OF SYSTEM TIME '%s' WITH revision_history", dbName, bucket, backupTS.AsOfSystemTime()))
			if err != nil {
				o.Fatal(err)
			}
		}
	} else {
		// Revision history doesn't work with online restore.
		_, err = conn.ExecContext(ctx, fmt.Sprintf("BACKUP DATABASE %s INTO '%s' AS OF SYSTEM TIME '%s'", dbName, bucket, backupTS.AsOfSystemTime()))
		if err != nil {
			o.Fatal(err)
		}
	}

	restoreDBName := fmt.Sprintf("backup_restore_op_%d", rng.Int63())

	// Assign cleanup handler early, before RESTORE creates the database.
	// The cleanup uses DROP DATABASE IF EXISTS, so it's safe even if the
	// RESTORE fails and the database was never created.
	cleanup = &backupRestoreCleanup{db: restoreDBName}

	onlineStr := "online"
	if !online {
		onlineStr = "offline"
	}
	o.Status(fmt.Sprintf("restoring %s into db %s", onlineStr, restoreDBName))

	startTime := timeutil.Now()
	if !online {
		o.Status("beginning offline restore")
		_, err = conn.ExecContext(ctx, fmt.Sprintf("RESTORE DATABASE %s FROM LATEST IN '%s' WITH OPTIONS (new_db_name = '%s')", dbName, bucket, restoreDBName))
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
			row := conn.QueryRowContext(ctx, fmt.Sprintf("RESTORE DATABASE %s FROM LATEST IN '%s' WITH OPTIONS (new_db_name = '%s', EXPERIMENTAL DEFERRED COPY)", dbName, bucket, restoreDBName))
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
			err = tests.WaitForSucceeded(ctx, conn, downloadJobId, 8760*time.Hour /* 1 year - test specs define their own timeouts */)
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
