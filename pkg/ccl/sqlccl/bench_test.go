// Copyright 2016 The Cockroach Authors.
//
// Licensed under the Cockroach Community Licence (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/pkg/ccl/LICENSE

package sqlccl

import (
	"bytes"
	"fmt"
	"io"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
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
	defer tracing.Disable()()

	ctx, dir, _, sqlDB, cleanupFn := backupRestoreTestSetup(b, multiNode, 0)
	defer cleanupFn()

	ts := hlc.Timestamp{WallTime: hlc.UnixNano()}
	if _, err := Load(ctx, sqlDB.DB, bankStatementBuf(b.N), "bench", dir, ts, 0); err != nil {
		b.Fatalf("%+v", err)
	}
	sqlDB.Exec(fmt.Sprintf(`RESTORE DATABASE bench FROM '%s'`, dir))

	// TODO(dan): Occasionally, this returns but seems to still be doing work,
	// which wildly throws off the timing of below and so the results of the
	// benchmark. When DistSQL improves this infrastructure, use what they've
	// built.
	//
	// for _, split := range bankSplitStmts(b.N, backupRestoreDefaultRanges) {
	// 	sqlDB.Exec(split)
	// }
	// rebalanceLeases(b, tc)

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
	defer tracing.Disable()()
	ctx, dir, _, sqlDB, cleanup := backupRestoreTestSetup(b, multiNode, 0)
	defer cleanup()

	ts := hlc.Timestamp{WallTime: hlc.UnixNano()}
	backup, err := Load(ctx, sqlDB.DB, bankStatementBuf(b.N), "bench", dir, ts, 0)
	if err != nil {
		b.Fatalf("%+v", err)
	}
	b.SetBytes(backup.DataSize / int64(b.N))
	b.ResetTimer()
	sqlDB.Exec(fmt.Sprintf(`RESTORE DATABASE bench FROM '%s'`, dir))
}

func BenchmarkImportRestore(b *testing.B) {
	defer tracing.Disable()()
	ctx, dir, _, sqlDB, cleanup := backupRestoreTestSetup(b, multiNode, 0)
	defer cleanup()

	buf := bankStatementBuf(b.N)
	b.SetBytes(int64(buf.Len() / b.N))
	ts := hlc.Timestamp{WallTime: hlc.UnixNano()}
	b.ResetTimer()
	if _, err := Load(ctx, sqlDB.DB, buf, "bench", dir, ts, 0); err != nil {
		b.Fatalf("%+v", err)
	}
	sqlDB.Exec(fmt.Sprintf(`RESTORE DATABASE bench FROM '%s'`, dir))
}

func BenchmarkImportSQL(b *testing.B) {
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
}
