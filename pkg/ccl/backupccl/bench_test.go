// Copyright 2016 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package backupccl_test

import (
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/ccl/backupccl"
	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl/sampledataccl"
	"github.com/cockroachdb/cockroach/pkg/workload/bank"
)

func BenchmarkDatabaseBackup(b *testing.B) {
	// NB: This benchmark takes liberties in how b.N is used compared to the go
	// documentation's description. We're getting useful information out of it,
	// but this is not a pattern to cargo-cult.

	_, _, sqlDB, dir, cleanupFn := backupccl.BackupRestoreTestSetup(b, backupccl.MultiNode,
		0 /* numAccounts */, backupccl.InitManualReplication)
	defer cleanupFn()
	sqlDB.Exec(b, `DROP TABLE data.bank`)

	bankData := bank.FromRows(b.N).Tables()[0]
	loadURI := "nodelocal://0/load"
	if _, err := sampledataccl.ToBackup(b, bankData, dir, "load"); err != nil {
		b.Fatalf("%+v", err)
	}
	sqlDB.Exec(b, fmt.Sprintf(`RESTORE data.* FROM '%s'`, loadURI))

	// TODO(dan): Ideally, this would split and rebalance the ranges in a more
	// controlled way. A previous version of this code did it manually with
	// `SPLIT AT` and TestCluster's TransferRangeLease, but it seemed to still
	// be doing work after returning, which threw off the timing and the results
	// of the benchmark. DistSQL is working on improving this infrastructure, so
	// use what they build.

	b.ResetTimer()
	var unused string
	var dataSize int64
	sqlDB.QueryRow(b, fmt.Sprintf(`BACKUP DATABASE data TO '%s'`, backupccl.LocalFoo)).Scan(
		&unused, &unused, &unused, &unused, &unused, &dataSize,
	)
	b.StopTimer()
	b.SetBytes(dataSize / int64(b.N))
}

func BenchmarkDatabaseRestore(b *testing.B) {
	// NB: This benchmark takes liberties in how b.N is used compared to the go
	// documentation's description. We're getting useful information out of it,
	// but this is not a pattern to cargo-cult.

	_, _, sqlDB, dir, cleanup := backupccl.BackupRestoreTestSetup(b, backupccl.MultiNode,
		0 /* numAccounts*/, backupccl.InitManualReplication)
	defer cleanup()
	sqlDB.Exec(b, `DROP TABLE data.bank`)

	bankData := bank.FromRows(b.N).Tables()[0]
	if _, err := sampledataccl.ToBackup(b, bankData, dir, "foo"); err != nil {
		b.Fatalf("%+v", err)
	}

	b.ResetTimer()
	sqlDB.Exec(b, `RESTORE data.* FROM 'nodelocal://0/foo'`)
	b.StopTimer()
}

func BenchmarkEmptyIncrementalBackup(b *testing.B) {
	const numStatements = 100000

	_, _, sqlDB, dir, cleanupFn := backupccl.BackupRestoreTestSetup(b, backupccl.MultiNode,
		0 /* numAccounts */, backupccl.InitManualReplication)
	defer cleanupFn()

	restoreURI := backupccl.LocalFoo + "/restore"
	fullURI := backupccl.LocalFoo + "/full"

	bankData := bank.FromRows(numStatements).Tables()[0]
	_, err := sampledataccl.ToBackup(b, bankData, dir, "foo/restore")
	if err != nil {
		b.Fatalf("%+v", err)
	}
	sqlDB.Exec(b, `DROP TABLE data.bank`)
	sqlDB.Exec(b, `RESTORE data.* FROM $1`, restoreURI)

	var unused string
	var dataSize int64
	sqlDB.QueryRow(b, `BACKUP DATABASE data TO $1`, fullURI).Scan(
		&unused, &unused, &unused, &unused, &unused, &dataSize,
	)

	// We intentionally don't write anything to the database between the full and
	// incremental backup.

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		incrementalDir := backupccl.LocalFoo + fmt.Sprintf("/incremental%d", i)
		sqlDB.Exec(b, `BACKUP DATABASE data TO $1 INCREMENTAL FROM $2`, incrementalDir, fullURI)
	}
	b.StopTimer()

	// We report the number of bytes that incremental backup was able to
	// *skip*--i.e., the number of bytes in the full backup.
	b.SetBytes(int64(b.N) * dataSize)
}

func BenchmarkDatabaseFullBackup(b *testing.B) {
	const numStatements = 100000

	_, _, sqlDB, dir, cleanupFn := backupccl.BackupRestoreTestSetup(b, backupccl.MultiNode,
		0 /* numAccounts */, backupccl.InitManualReplication)
	defer cleanupFn()

	restoreURI := backupccl.LocalFoo + "/restore"
	fullURI := backupccl.LocalFoo + "/full"

	bankData := bank.FromRows(numStatements).Tables()[0]
	_, err := sampledataccl.ToBackup(b, bankData, dir, "foo/restore")
	if err != nil {
		b.Fatalf("%+v", err)
	}
	sqlDB.Exec(b, `DROP TABLE data.bank`)
	sqlDB.Exec(b, `RESTORE data.* FROM $1`, restoreURI)

	var unused string
	var dataSize int64
	sqlDB.QueryRow(b, `BACKUP DATABASE data TO $1`, fullURI).Scan(
		&unused, &unused, &unused, &unused, &unused, &dataSize,
	)

	// We intentionally don't write anything to the database between the full and
	// incremental backup.

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		backupDir := backupccl.LocalFoo + fmt.Sprintf("/backup%d", i)
		sqlDB.Exec(b, `BACKUP DATABASE data TO $1`, backupDir)
	}
	b.StopTimer()

	// We report the number of bytes that incremental backup was able to
	// *skip*--i.e., the number of bytes in the full backup.
	b.SetBytes(int64(b.N) * dataSize)
}
