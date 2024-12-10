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
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/tests"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/fingerprintutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

type dropDb struct {
	db string
}

func (cl *dropDb) Cleanup(ctx context.Context, o operation.Operation, c cluster.Cluster) {
	conn := c.Conn(ctx, o.L(), 1, option.VirtualClusterName(roachtestflags.VirtualCluster))
	defer conn.Close()

	o.Status(fmt.Sprintf("dropping newly created db %s", cl.db))
	_, err := conn.ExecContext(ctx, fmt.Sprintf("DROP DATABASE %s CASCADE", cl.db))
	if err != nil {
		o.Fatal(err)
	}
}

type backupRestoreOperation struct {
	cluster   cluster.Cluster
	operation operation.Operation
	conn      *gosql.DB
	restoreDB string
}

type backup struct {
	bucket string
	dbName string
	time   hlc.Timestamp
}

func newBackupRestoreOperation(
	doBackupRestore func(ctx context.Context, b *backupRestoreOperation),
) func(context.Context, operation.Operation, cluster.Cluster) registry.OperationCleanup {
	return func(ctx context.Context, o operation.Operation, c cluster.Cluster) registry.OperationCleanup {
		conn := c.Conn(ctx, o.L(), 1, option.VirtualClusterName(roachtestflags.VirtualCluster))
		defer conn.Close()

		rng, _ := randutil.NewPseudoRand()
		brOp := backupRestoreOperation{
			cluster:   c,
			operation: o,
			conn:      conn,
			restoreDB: fmt.Sprintf("backup_restore_op_%d", rng.Int63()),
		}

		doBackupRestore(ctx, &brOp)

		return &dropDb{db: brOp.restoreDB}
	}
}

func (b *backupRestoreOperation) pickDB(ctx context.Context) string {
	dbWhitelist := map[string]bool{"cct_tpcc": true, "tpcc": true}
	dbs, err := b.conn.QueryContext(ctx, "SELECT database_name FROM [SHOW DATABASES]")
	if err != nil {
		b.operation.Fatal(err)
	}
	for dbs.Next() {
		var dbName string
		if err := dbs.Scan(&dbName); err != nil {
			b.operation.Fatal(err)
		}
		if dbWhitelist[dbName] {
			return dbName
		}
	}
	b.operation.Fatal(fmt.Errorf("did not find db in the whitelist %v", dbWhitelist))
	return "" // unreachable
}

func (b *backupRestoreOperation) backup(
	ctx context.Context, dbName string, incrementalDepth int,
) backup {
	b.operation.Status(fmt.Sprintf("backing db %s (full)", dbName))
	bucket := fmt.Sprintf("gs://%s/operation-backup-restore/%d/?AUTH=implicit", testutils.BackupTestingBucket(), timeutil.Now().UnixNano())

	backupTS := hlc.Timestamp{WallTime: timeutil.Now().Add(-10 * time.Second).UTC().UnixNano()}
	_, err := b.conn.ExecContext(ctx, fmt.Sprintf("BACKUP DATABASE %s INTO '%s' AS OF SYSTEM TIME '%s'", dbName, bucket, backupTS.AsOfSystemTime()))
	if err != nil {
		b.operation.Fatal(err)
	}
	for i := range incrementalDepth {
		b.operation.Status(fmt.Sprintf("backing up db %s (incremental layer %d)", dbName, i))
		// Update backupTS to match the latest layer.
		backupTS = hlc.Timestamp{WallTime: timeutil.Now().Add(-10 * time.Second).UTC().UnixNano()}
		_, err = b.conn.ExecContext(ctx, fmt.Sprintf("BACKUP DATABASE %s INTO LATEST IN '%s' AS OF SYSTEM TIME '%s'", dbName, bucket, backupTS.AsOfSystemTime()))
		if err != nil {
			b.operation.Fatal(err)
		}
	}
	return backup{
		bucket: bucket,
		time:   backupTS,
		dbName: dbName,
	}
}

func (b *backupRestoreOperation) restoreOffline(
	ctx context.Context, backup backup, targetDB string,
) {
	b.operation.Status(fmt.Sprintf("beginning offline restore into: %s", targetDB))
	_, err := b.conn.ExecContext(ctx, fmt.Sprintf("RESTORE DATABASE %s FROM LATEST IN '%s' WITH OPTIONS (new_db_name = '%s')", backup.dbName, backup.bucket, targetDB))
	if err != nil {
		b.operation.Fatal(err)
	}
}

func (b *backupRestoreOperation) restoreOnline(
	ctx context.Context, backup backup, targetDB string,
) catpb.JobID {
	var id, tables, approxRows, approxBytes int64
	var downloadJobId catpb.JobID

	b.operation.Status(fmt.Sprintf("beginning online restore into: %s", targetDB))
	res := b.conn.QueryRowContext(ctx, fmt.Sprintf("RESTORE DATABASE %s FROM LATEST IN '%s' WITH OPTIONS (new_db_name = '%s', EXPERIMENTAL DEFERRED COPY)", backup.dbName, backup.bucket, targetDB))

	err := res.Scan(&id, &tables, &approxRows, &approxBytes, &downloadJobId)
	if err != nil {
		b.operation.Fatal(err)
	}

	return downloadJobId
}

func (b *backupRestoreOperation) validateRestore(
	ctx context.Context, backup backup, restoreDBName string,
) {
	b.operation.Status(fmt.Sprintf("verifying db %s matches %s", backup.dbName, restoreDBName))
	sourceFingerprints, err := fingerprintutils.FingerprintDatabase(ctx, b.conn, backup.dbName, fingerprintutils.AOST(backup.time), fingerprintutils.Stripped())
	if err != nil {
		b.operation.Fatal(err)
	}

	// No AOST here; the timestamps are rewritten on restore. But nobody else is touching this database, so that's fine.
	destFingerprints, err := fingerprintutils.FingerprintDatabase(ctx, b.conn, restoreDBName, fingerprintutils.Stripped())
	if err != nil {
		b.operation.Fatal(err)
	}

	if !reflect.DeepEqual(sourceFingerprints, destFingerprints) {
		b.operation.Fatalf("backup and restore fingerprints do not match: %v != %v", sourceFingerprints, destFingerprints)
	}
}

func (b *backupRestoreOperation) scatter(ctx context.Context, dbName string) {
	var tableNames []string

	b.operation.Status(fmt.Sprintf("scattering database '%s'", dbName))

	rows, err := b.conn.QueryContext(ctx, fmt.Sprintf("SELECT table_name FROM [SHOW TABLES FROM %s]", dbName))
	if err != nil {
		b.operation.Fatal(err)
	}
	defer rows.Close()

	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			b.operation.Fatal(err)
		}
		tableNames = append(tableNames, tableName)
	}
	if rows.Err() != nil {
		b.operation.Fatal(err)
	}

	for _, table := range tableNames {
		b.operation.Status(fmt.Sprintf("scattering table '%s.%s'", dbName, table))
		_, err := b.conn.Exec(fmt.Sprintf("ALTER TABLE %s.%s SCATTER", dbName, table))
		if err != nil {
			b.operation.Fatal(err)
		}
	}
}

func (b *backupRestoreOperation) RunOfflineRestore(ctx context.Context, skipValidating bool) {
	dbName := b.pickDB(ctx)
	backup := b.backup(ctx, dbName, 24)
	b.restoreOffline(ctx, backup, b.restoreDB)
	if !skipValidating {
		b.validateRestore(ctx, backup, b.restoreDB)
	}
}

func (b *backupRestoreOperation) RunOnlineRestore(ctx context.Context, skipValidating bool) {
	dbName := b.pickDB(ctx)
	backup := b.backup(ctx, dbName, 0)

	downloadJob := b.restoreOnline(ctx, backup, b.restoreDB)
	if err := tests.WaitForSucceeded(ctx, b.conn, downloadJob, 8760*time.Hour /* 1 year - test specs define their own timeouts */); err != nil {
		b.operation.Fatal(err)
	}

	if !skipValidating {
		b.validateRestore(ctx, backup, b.restoreDB)
	}
}

func (b *backupRestoreOperation) RunPartialRestore(ctx context.Context) {
	const (
		// Introduce waits to spend more time in intermediate states. The restore
		// operations are intend to run in parallel with workloads and chaos
		// operations, so waiting in weird states may uncover bugs. These times are
		// relatively arbitrariy. The operation would be correct with the times set
		// to zero or increased to multiple hours.
		scatterWait  = 15 * time.Minute
		downloadWait = 15 * time.Minute
	)

	dbName := b.pickDB(ctx)

	_, err := b.conn.Exec("SET CLUSTER SETTING backup.restore.test_skip_download = 0.5")
	if err != nil {
		b.operation.Fatal(err)
	}

	backup := b.backup(ctx, dbName, 0)

	downloadJob := b.restoreOnline(ctx, backup, b.restoreDB)

	b.operation.Status("waiting to re-scatter")
	time.Sleep(scatterWait)
	b.scatter(ctx, b.restoreDB)

	b.operation.Status("waiting to for download progress and scatter")
	time.Sleep(downloadWait)

	b.validateRestore(ctx, backup, b.restoreDB)

	b.operation.Status("reseting 'backup.restore.test_skip_download' so download can complete")
	_, err = b.conn.Exec("RESET CLUSTER SETTING backup.restore.test_skip_download")
	if err != nil {
		b.operation.Fatal(err)
	}

	if err := tests.WaitForSucceeded(ctx, b.conn, downloadJob, 8760*time.Hour /* 1 year - test specs define their own timeouts */); err != nil {
		b.operation.Fatal(err)
	}

	b.validateRestore(ctx, backup, b.restoreDB)
}

func registerBackupRestore(r registry.Registry) {
	r.AddOperation(registry.OperationSpec{
		Name:               "backup-restore/tpcc/online=false/fingerprint=true",
		Owner:              registry.OwnerDisasterRecovery,
		Timeout:            96 * time.Hour,
		CompatibleClouds:   registry.AllClouds,
		CanRunConcurrently: registry.OperationCanRunConcurrently,
		Dependencies:       []registry.OperationDependency{registry.OperationRequiresPopulatedDatabase},
		Run: newBackupRestoreOperation(func(ctx context.Context, b *backupRestoreOperation) {
			b.RunOfflineRestore(ctx, true)
		}),
	})

	r.AddOperation(registry.OperationSpec{
		Name:               "backup-restore/tpcc/online=false/fingerprint=false",
		Owner:              registry.OwnerDisasterRecovery,
		Timeout:            24 * time.Hour,
		CompatibleClouds:   registry.AllClouds,
		CanRunConcurrently: registry.OperationCanRunConcurrently,
		Dependencies:       []registry.OperationDependency{registry.OperationRequiresPopulatedDatabase},
		Run: newBackupRestoreOperation(func(ctx context.Context, b *backupRestoreOperation) {
			b.RunOfflineRestore(ctx, false)
		}),
	})

	r.AddOperation(registry.OperationSpec{
		Name:               "backup-restore/tpcc/online=true/fingerprint=true",
		Owner:              registry.OwnerDisasterRecovery,
		Timeout:            96 * time.Hour,
		CompatibleClouds:   registry.AllClouds,
		CanRunConcurrently: registry.OperationCanRunConcurrently,
		Dependencies:       []registry.OperationDependency{registry.OperationRequiresPopulatedDatabase},
		Run: newBackupRestoreOperation(func(ctx context.Context, b *backupRestoreOperation) {
			b.RunOnlineRestore(ctx, true)
		}),
	})

	r.AddOperation(registry.OperationSpec{
		Name:               "backup-restore/tpcc/online=true/fingerprint=false",
		Owner:              registry.OwnerDisasterRecovery,
		Timeout:            24 * time.Hour,
		CompatibleClouds:   registry.AllClouds,
		CanRunConcurrently: registry.OperationCanRunConcurrently,
		Dependencies:       []registry.OperationDependency{registry.OperationRequiresPopulatedDatabase},
		Run: newBackupRestoreOperation(func(ctx context.Context, b *backupRestoreOperation) {
			b.RunOnlineRestore(ctx, false)
		}),
	})

	r.AddOperation(registry.OperationSpec{
		Name:               "backup-restore/tpcc/partial-download",
		Owner:              registry.OwnerDisasterRecovery,
		Timeout:            24 * time.Hour,
		CompatibleClouds:   registry.AllClouds,
		CanRunConcurrently: registry.OperationCanRunConcurrently,
		Dependencies:       []registry.OperationDependency{registry.OperationRequiresPopulatedDatabase},
		Run: newBackupRestoreOperation(func(ctx context.Context, b *backupRestoreOperation) {
			b.RunPartialRestore(ctx)
		}),
	})
}
