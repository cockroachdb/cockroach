// Copyright 2016 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/LICENSE

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
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

func bankBuf(numAccounts int) *bytes.Buffer {
	bankData := sampledataccl.BankRows(numAccounts)
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "CREATE TABLE %s %s;\n", bankData.Name(), bankData.Schema())
	for {
		row, ok := bankData.NextRow()
		if !ok {
			break
		}
		fmt.Fprintf(&buf, "INSERT INTO %s VALUES (%s);\n", bankData.Name(), strings.Join(row, `,`))
	}
	return &buf
}

func BenchmarkClusterBackup(b *testing.B) {
	// NB: This benchmark takes liberties in how b.N is used compared to the go
	// documentation's description. We're getting useful information out of it,
	// but this is not a pattern to cargo-cult.

	_, dir, _, sqlDB, cleanupFn := backupRestoreTestSetup(b, multiNode, 0, initNone)
	defer cleanupFn()
	sqlDB.Exec(`DROP TABLE data.bank`)

	bankData := sampledataccl.BankRows(b.N)
	loadDir := filepath.Join(dir, "load")
	if _, err := sampledataccl.ToBackup(b, bankData, loadDir); err != nil {
		b.Fatalf("%+v", err)
	}
	sqlDB.Exec(fmt.Sprintf(`RESTORE data.* FROM '%s'`, loadDir))

	// TODO(dan): Ideally, this would split and rebalance the ranges in a more
	// controlled way. A previous version of this code did it manually with
	// `SPLIT AT` and TestCluster's TransferRangeLease, but it seemed to still
	// be doing work after returning, which threw off the timing and the results
	// of the benchmark. DistSQL is working on improving this infrastructure, so
	// use what they build.

	b.ResetTimer()
	var unused string
	var dataSize int64
	sqlDB.QueryRow(fmt.Sprintf(`BACKUP DATABASE data TO '%s'`, dir)).Scan(
		&unused, &unused, &unused, &unused, &unused, &unused, &dataSize,
	)
	b.StopTimer()
	b.SetBytes(dataSize / int64(b.N))
}

func BenchmarkClusterRestore(b *testing.B) {
	b.Run("AddSSTable", func(b *testing.B) {
		runBenchmarkClusterRestore(b, enableAddSSTable)
	})
	b.Run("WriteBatch", func(b *testing.B) {
		runBenchmarkClusterRestore(b, disableAddSSTable)
	})
}

func runBenchmarkClusterRestore(b *testing.B, init func(*cluster.Settings)) {
	// NB: This benchmark takes liberties in how b.N is used compared to the go
	// documentation's description. We're getting useful information out of it,
	// but this is not a pattern to cargo-cult.

	_, dir, _, sqlDB, cleanup := backupRestoreTestSetup(b, multiNode, 0, init)
	defer cleanup()
	sqlDB.Exec(`DROP TABLE data.bank`)

	bankData := sampledataccl.BankRows(b.N)
	backup, err := sampledataccl.ToBackup(b, bankData, dir)
	if err != nil {
		b.Fatalf("%+v", err)
	}
	b.SetBytes(backup.Desc.EntryCounts.DataSize / int64(b.N))

	b.ResetTimer()
	sqlDB.Exec(fmt.Sprintf(`RESTORE data.* FROM '%s'`, dir))
	b.StopTimer()
}

func BenchmarkLoadRestore(b *testing.B) {
	// NB: This benchmark takes liberties in how b.N is used compared to the go
	// documentation's description. We're getting useful information out of it,
	// but this is not a pattern to cargo-cult.

	ctx, dir, _, sqlDB, cleanup := backupRestoreTestSetup(b, multiNode, 0, initNone)
	defer cleanup()
	sqlDB.Exec(`DROP TABLE data.bank`)

	buf := bankBuf(b.N)
	b.SetBytes(int64(buf.Len() / b.N))
	ts := hlc.Timestamp{WallTime: hlc.UnixNano()}
	b.ResetTimer()
	if _, err := sqlccl.Load(ctx, sqlDB.DB, buf, "data", dir, ts, 0, dir); err != nil {
		b.Fatalf("%+v", err)
	}
	sqlDB.Exec(fmt.Sprintf(`RESTORE data.* FROM '%s'`, dir))
	b.StopTimer()
}

func BenchmarkLoadSQL(b *testing.B) {
	// NB: This benchmark takes liberties in how b.N is used compared to the go
	// documentation's description. We're getting useful information out of it,
	// but this is not a pattern to cargo-cult.
	_, _, _, sqlDB, cleanup := backupRestoreTestSetup(b, multiNode, 0, initNone)
	defer cleanup()
	sqlDB.Exec(`DROP TABLE data.bank`)

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
		sqlDB.Exec(line)
	}
	b.StopTimer()
}

func BenchmarkClusterEmptyIncrementalBackup(b *testing.B) {
	const numStatements = 100000

	_, dir, _, sqlDB, cleanupFn := backupRestoreTestSetup(b, multiNode, 0, initNone)
	defer cleanupFn()

	restoreDir := filepath.Join(dir, "restore")
	fullDir := filepath.Join(dir, "full")

	bankData := sampledataccl.BankRows(numStatements)
	_, err := sampledataccl.ToBackup(b, bankData, restoreDir)
	if err != nil {
		b.Fatalf("%+v", err)
	}
	sqlDB.Exec(`DROP TABLE data.bank`)
	sqlDB.Exec(`RESTORE data.* FROM $1`, restoreDir)

	var unused string
	var dataSize int64
	sqlDB.QueryRow(`BACKUP DATABASE data TO $1`, fullDir).Scan(
		&unused, &unused, &unused, &unused, &unused, &unused, &dataSize,
	)

	// We intentionally don't write anything to the database between the full and
	// incremental backup.

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		incrementalDir := filepath.Join(dir, fmt.Sprintf("incremental%d", i))
		sqlDB.Exec(`BACKUP DATABASE data TO $1 INCREMENTAL FROM $2`, incrementalDir, fullDir)
	}
	b.StopTimer()

	// We report the number of bytes that incremental backup was able to
	// *skip*--i.e., the number of bytes in the full backup.
	b.SetBytes(int64(b.N) * dataSize)
}
