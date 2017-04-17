// Copyright 2016 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/pkg/ccl/LICENSE

package sqlccl

import (
	"bytes"
	"fmt"
	"io"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/pkg/errors"
)

func bankStatementBuf(numAccounts int) *bytes.Buffer {
	var buf bytes.Buffer
	buf.WriteString(bankCreateTable)
	buf.WriteString(";\n")
	stmts := bankDataInsertStmts(numAccounts)
	for _, s := range stmts {
		buf.WriteString(s)
		buf.WriteString(";\n")
	}
	return &buf
}

func BenchmarkClusterBackup(b *testing.B) {
	// NB: This benchmark takes liberties in how b.N is used compared to the go
	// documentation's description. We're getting useful information out of it,
	// but this is not a pattern to cargo-cult.
	defer tracing.Disable()()

	ctx, dir, _, sqlDB, cleanupFn := backupRestoreTestSetup(b, multiNode, 0)
	defer cleanupFn()

	ts := hlc.Timestamp{WallTime: hlc.UnixNano()}
	if _, err := Load(ctx, sqlDB.DB, bankStatementBuf(b.N), "bench", dir, ts, 0, dir); err != nil {
		b.Fatalf("%+v", err)
	}
	sqlDB.Exec(fmt.Sprintf(`RESTORE bench.* FROM '%s'`, dir))

	// TODO(dan): Ideally, this would split and rebalance the ranges in a more
	// controlled way. A previous version of this code did it manually with
	// `SPLIT AT` and TestCluster's TransferRangeLease, but it seemed to still
	// be doing work after returning, which threw off the timing and the results
	// of the benchmark. DistSQL is working on improving this infrastructure, so
	// use what they build.

	b.ResetTimer()
	var unused string
	var dataSize int64
	sqlDB.QueryRow(fmt.Sprintf(`BACKUP DATABASE bench TO '%s'`, dir)).Scan(
		&unused, &unused, &unused, &dataSize,
	)
	b.StopTimer()
	b.SetBytes(dataSize / int64(b.N))
}

func BenchmarkClusterRestore(b *testing.B) {
	// NB: This benchmark takes liberties in how b.N is used compared to the go
	// documentation's description. We're getting useful information out of it,
	// but this is not a pattern to cargo-cult.
	defer tracing.Disable()()

	ctx, dir, _, sqlDB, cleanup := backupRestoreTestSetup(b, multiNode, 0)
	defer cleanup()

	ts := hlc.Timestamp{WallTime: hlc.UnixNano()}
	backup, err := Load(ctx, sqlDB.DB, bankStatementBuf(b.N), "bench", dir, ts, 0, dir)
	if err != nil {
		b.Fatalf("%+v", err)
	}
	b.SetBytes(backup.DataSize / int64(b.N))
	b.ResetTimer()
	sqlDB.Exec(fmt.Sprintf(`RESTORE bench.* FROM '%s'`, dir))
	b.StopTimer()
}

func BenchmarkLoadRestore(b *testing.B) {
	// NB: This benchmark takes liberties in how b.N is used compared to the go
	// documentation's description. We're getting useful information out of it,
	// but this is not a pattern to cargo-cult.
	defer tracing.Disable()()

	ctx, dir, _, sqlDB, cleanup := backupRestoreTestSetup(b, multiNode, 0)
	defer cleanup()

	buf := bankStatementBuf(b.N)
	b.SetBytes(int64(buf.Len() / b.N))
	ts := hlc.Timestamp{WallTime: hlc.UnixNano()}
	b.ResetTimer()
	if _, err := Load(ctx, sqlDB.DB, buf, "bench", dir, ts, 0, dir); err != nil {
		b.Fatalf("%+v", err)
	}
	sqlDB.Exec(fmt.Sprintf(`RESTORE bench.* FROM '%s'`, dir))
	b.StopTimer()
}

func BenchmarkLoadSQL(b *testing.B) {
	// NB: This benchmark takes liberties in how b.N is used compared to the go
	// documentation's description. We're getting useful information out of it,
	// but this is not a pattern to cargo-cult.
	_, _, _, sqlDB, cleanup := backupRestoreTestSetup(b, multiNode, 0)
	defer cleanup()

	buf := bankStatementBuf(b.N)
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

const withTimeBoundIterators = true
const withoutTimeBoundIterators = false

func setTimeBoundIterators(tb testing.TB, tc *testcluster.TestCluster, timeBoundIterators bool) {
	runner := sqlutils.MakeSQLRunner(tb, tc.Conns[0])
	runner.Exec(
		fmt.Sprintf("SET CLUSTER SETTING engineccl.time.bound.iterators.enabled = %t", timeBoundIterators),
	)

	show := "SHOW CLUSTER SETTING engineccl.time.bound.iterators.enabled"
	for _, conn := range tc.Conns {
		testutils.SucceedsSoon(tb, func() error {
			var set string
			sqlutils.MakeSQLRunner(tb, conn).QueryRow(show).Scan(&set)
			if set != fmt.Sprintf("%t", timeBoundIterators) {
				return errors.Errorf("engineccl.time.bound.iterators.enabled is not %t", timeBoundIterators)
			}
			return nil
		})
	}
}

func runEmptyIncrementalBackup(b *testing.B, timeBoundIterators bool) {
	defer tracing.Disable()()

	const numStatements = 100000

	ctx, dir, tc, sqlDB, cleanupFn := backupRestoreTestSetup(b, multiNode, 0)
	defer cleanupFn()

	setTimeBoundIterators(b, tc, timeBoundIterators)

	restoreDir := filepath.Join(dir, "restore")
	fullDir := filepath.Join(dir, "full")

	ts := hlc.Timestamp{WallTime: hlc.UnixNano()}
	if _, err := Load(
		ctx, sqlDB.DB, bankStatementBuf(numStatements), "bench", restoreDir, ts, 0, restoreDir,
	); err != nil {
		b.Fatalf("%+v", err)
	}
	sqlDB.Exec(`RESTORE bench.* FROM $1`, restoreDir)

	var unused string
	var dataSize int64
	sqlDB.QueryRow(`BACKUP DATABASE bench TO $1`, fullDir).Scan(
		&unused, &unused, &unused, &dataSize,
	)

	// We intentionally don't write anything to the database between the full and
	// incremental backup.

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		incrementalDir := filepath.Join(dir, fmt.Sprintf("incremental%d", i))
		sqlDB.Exec(`BACKUP DATABASE bench TO $1 INCREMENTAL FROM $2`, incrementalDir, fullDir)
	}
	b.StopTimer()

	// We report the number of bytes that incremental backup was able to
	// *skip*--i.e., the number of bytes in the full backup.
	b.SetBytes(int64(b.N) * dataSize)
}

func BenchmarkClusterEmptyIncrementalBackup(b *testing.B) {
	b.Run("Normal", func(b *testing.B) {
		runEmptyIncrementalBackup(b, withoutTimeBoundIterators)
	})

	b.Run("TimeBound", func(b *testing.B) {
		runEmptyIncrementalBackup(b, withTimeBoundIterators)
	})
}
