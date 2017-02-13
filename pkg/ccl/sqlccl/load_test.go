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

	"github.com/cockroachdb/cockroach/pkg/testutils"
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

func TestImportChunking(t *testing.T) {
	// Generate at least 2 chunks.
	const chunkSize = 1024 * 500
	numAccounts := int(chunkSize / backupRestoreRowPayloadSize * 2)

	ctx, dir, _, _, sqlDB, cleanupFn := backupRestoreTestSetup(t, singleNode, 0)
	defer cleanupFn()

	ts := hlc.Timestamp{WallTime: hlc.UnixNano()}
	desc, err := Load(ctx, sqlDB.DB, bankStatementBuf(numAccounts), "bench", dir, ts, chunkSize)
	if err != nil {
		t.Fatalf("%+v", err)
	}
	if len(desc.Files) < 2 {
		t.Errorf("expected at least 2 ranges")
	}
}

func TestImportOutOfOrder(t *testing.T) {
	ctx, dir, _, _, sqlDB, cleanupFn := backupRestoreTestSetup(t, singleNode, 0)
	defer cleanupFn()

	var buf bytes.Buffer
	buf.WriteString(bankCreateTable + ";\n")
	stmts := bankDataInsertStmts(2 * bankDataInsertRows)
	buf.WriteString(stmts[1] + ";\n")
	buf.WriteString(stmts[0] + ";\n")

	ts := hlc.Timestamp{WallTime: hlc.UnixNano()}
	_, err := Load(ctx, sqlDB.DB, &buf, "bench", dir, ts, 0)
	if !testutils.IsError(err, "out of order row") {
		t.Fatalf("expected out of order row, got: %+v", err)
	}
}

func BenchmarkImport(b *testing.B) {
	defer tracing.Disable()()
	ctx, dir, _, _, sqlDB, cleanup := backupRestoreTestSetup(b, multiNode, 0)
	defer cleanup()

	buf := bankStatementBuf(b.N)
	ts := hlc.Timestamp{WallTime: hlc.UnixNano()}
	b.SetBytes(int64(buf.Len() / b.N))
	b.ResetTimer()
	if _, err := Load(ctx, sqlDB.DB, buf, "bench", dir, ts, 0); err != nil {
		b.Fatalf("%+v", err)
	}
}

func BenchmarkRestore(b *testing.B) {
	defer tracing.Disable()()
	ctx, dir, _, _, sqlDB, cleanup := backupRestoreTestSetup(b, multiNode, 0)
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
	ctx, dir, _, _, sqlDB, cleanup := backupRestoreTestSetup(b, multiNode, 0)
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
	_, _, _, _, sqlDB, cleanup := backupRestoreTestSetup(b, multiNode, 0)
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
