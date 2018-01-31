// Copyright 2016 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package sqlccl_test

import (
	"bytes"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/ccl/sqlccl"
	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl/sampledataccl"
	"github.com/cockroachdb/cockroach/pkg/testutils/workload"
	"github.com/cockroachdb/cockroach/pkg/testutils/workload/bank"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

func bankBuf(numAccounts int) *bytes.Buffer {
	bankData := bank.FromRows(numAccounts).Tables()[0]
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "CREATE TABLE %s %s;\n", bankData.Name, bankData.Schema)
	for rowIdx := 0; rowIdx < bankData.InitialRowCount; rowIdx++ {
		row := workload.StringTuple(bankData.InitialRowFn(rowIdx))
		fmt.Fprintf(&buf, "INSERT INTO %s VALUES (%s);\n", bankData.Name, strings.Join(row, `,`))
	}
	return &buf
}

func BenchmarkClusterBackup(b *testing.B) {
	// NB: This benchmark takes liberties in how b.N is used compared to the go
	// documentation's description. We're getting useful information out of it,
	// but this is not a pattern to cargo-cult.

	_, _, sqlDB, dir, cleanupFn := backupRestoreTestSetup(b, multiNode, 0, initNone)
	defer cleanupFn()
	sqlDB.Exec(b, `DROP TABLE data.bank`)

	bankData := bank.FromRows(b.N).Tables()[0]
	loadDir := filepath.Join(dir, "load")
	if _, err := sampledataccl.ToBackup(b, bankData, loadDir); err != nil {
		b.Fatalf("%+v", err)
	}
	sqlDB.Exec(b, fmt.Sprintf(`RESTORE data.* FROM '%s'`, loadDir))

	// TODO(dan): Ideally, this would split and rebalance the ranges in a more
	// controlled way. A previous version of this code did it manually with
	// `SPLIT AT` and TestCluster's TransferRangeLease, but it seemed to still
	// be doing work after returning, which threw off the timing and the results
	// of the benchmark. DistSQL is working on improving this infrastructure, so
	// use what they build.

	b.ResetTimer()
	var unused string
	var dataSize int64
	sqlDB.QueryRow(b, fmt.Sprintf(`BACKUP DATABASE data TO '%s'`, dir)).Scan(
		&unused, &unused, &unused, &unused, &unused, &unused, &dataSize,
	)
	b.StopTimer()
	b.SetBytes(dataSize / int64(b.N))
}

func BenchmarkClusterRestore(b *testing.B) {
	// NB: This benchmark takes liberties in how b.N is used compared to the go
	// documentation's description. We're getting useful information out of it,
	// but this is not a pattern to cargo-cult.

	_, _, sqlDB, dir, cleanup := backupRestoreTestSetup(b, multiNode, 0, initNone)
	defer cleanup()
	sqlDB.Exec(b, `DROP TABLE data.bank`)

	bankData := bank.FromRows(b.N).Tables()[0]
	backup, err := sampledataccl.ToBackup(b, bankData, filepath.Join(dir, "foo"))
	if err != nil {
		b.Fatalf("%+v", err)
	}
	b.SetBytes(backup.Desc.EntryCounts.DataSize / int64(b.N))

	b.ResetTimer()
	sqlDB.Exec(b, `RESTORE data.* FROM 'nodelocal:///foo'`)
	b.StopTimer()
}

func BenchmarkLoadRestore(b *testing.B) {
	// NB: This benchmark takes liberties in how b.N is used compared to the go
	// documentation's description. We're getting useful information out of it,
	// but this is not a pattern to cargo-cult.

	ctx, _, sqlDB, dir, cleanup := backupRestoreTestSetup(b, multiNode, 0, initNone)
	defer cleanup()
	sqlDB.Exec(b, `DROP TABLE data.bank`)

	buf := bankBuf(b.N)
	b.SetBytes(int64(buf.Len() / b.N))
	ts := hlc.Timestamp{WallTime: hlc.UnixNano()}
	b.ResetTimer()
	if _, err := sqlccl.Load(ctx, sqlDB.DB, buf, "data", dir, ts, 0, dir); err != nil {
		b.Fatalf("%+v", err)
	}
	sqlDB.Exec(b, fmt.Sprintf(`RESTORE data.* FROM '%s'`, dir))
	b.StopTimer()
}

func BenchmarkLoadSQL(b *testing.B) {
	// NB: This benchmark takes liberties in how b.N is used compared to the go
	// documentation's description. We're getting useful information out of it,
	// but this is not a pattern to cargo-cult.
	_, _, sqlDB, _, cleanup := backupRestoreTestSetup(b, multiNode, 0, initNone)
	defer cleanup()
	sqlDB.Exec(b, `DROP TABLE data.bank`)

	buf := bankBuf(b.N)
	b.SetBytes(int64(buf.Len() / b.N))
	lines := make([]string, 0, b.N)
	for {
		line, err := buf.ReadString(';')
		if err == io.EOF {
			break
		} else if err != nil {
			b.Fatalf("%+v", err)
		}
		lines = append(lines, line)
	}

	b.ResetTimer()
	for _, line := range lines {
		sqlDB.Exec(b, line)
	}
	b.StopTimer()
}

func BenchmarkClusterEmptyIncrementalBackup(b *testing.B) {
	const numStatements = 100000

	_, _, sqlDB, _, cleanupFn := backupRestoreTestSetup(b, multiNode, 0, initNone)
	defer cleanupFn()

	restoreDir := filepath.Join(localFoo, "restore")
	fullDir := filepath.Join(localFoo, "full")

	bankData := bank.FromRows(numStatements).Tables()[0]
	_, err := sampledataccl.ToBackup(b, bankData, restoreDir)
	if err != nil {
		b.Fatalf("%+v", err)
	}
	sqlDB.Exec(b, `DROP TABLE data.bank`)
	sqlDB.Exec(b, `RESTORE data.* FROM $1`, restoreDir)

	var unused string
	var dataSize int64
	sqlDB.QueryRow(b, `BACKUP DATABASE data TO $1`, fullDir).Scan(
		&unused, &unused, &unused, &unused, &unused, &unused, &dataSize,
	)

	// We intentionally don't write anything to the database between the full and
	// incremental backup.

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		incrementalDir := filepath.Join(localFoo, fmt.Sprintf("incremental%d", i))
		sqlDB.Exec(b, `BACKUP DATABASE data TO $1 INCREMENTAL FROM $2`, incrementalDir, fullDir)
	}
	b.StopTimer()

	// We report the number of bytes that incremental backup was able to
	// *skip*--i.e., the number of bytes in the full backup.
	b.SetBytes(int64(b.N) * dataSize)
}
