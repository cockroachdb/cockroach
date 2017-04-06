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
	"testing"

	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
)

func TestImportChunking(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Generate at least 2 chunks.
	const chunkSize = 1024 * 500
	numAccounts := int(chunkSize / backupRestoreRowPayloadSize * 2)

	ctx, dir, _, sqlDB, cleanupFn := backupRestoreTestSetup(t, singleNode, 0)
	defer cleanupFn()

	ts := hlc.Timestamp{WallTime: hlc.UnixNano()}
	desc, err := Load(ctx, sqlDB.DB, bankStatementBuf(numAccounts), "bench", dir, ts, chunkSize, dir)
	if err != nil {
		t.Fatalf("%+v", err)
	}
	if len(desc.Files) < 2 {
		t.Errorf("expected at least 2 ranges")
	}
}

func TestImportOutOfOrder(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx, dir, _, sqlDB, cleanupFn := backupRestoreTestSetup(t, singleNode, 0)
	defer cleanupFn()

	var buf bytes.Buffer
	buf.WriteString(bankCreateTable + ";\n")
	stmts := bankDataInsertStmts(2 * bankDataInsertRows)
	buf.WriteString(stmts[1] + ";\n")
	buf.WriteString(stmts[0] + ";\n")

	ts := hlc.Timestamp{WallTime: hlc.UnixNano()}
	_, err := Load(ctx, sqlDB.DB, &buf, "bench", dir, ts, 0, dir)
	if !testutils.IsError(err, "out of order row") {
		t.Fatalf("expected out of order row, got: %+v", err)
	}
}

func BenchmarkImport(b *testing.B) {
	// NB: This benchmark takes liberties in how b.N is used compared to the go
	// documentation's description. We're getting useful information out of it,
	// but this is not a pattern to cargo-cult.
	defer tracing.Disable()()
	ctx, dir, _, sqlDB, cleanup := backupRestoreTestSetup(b, multiNode, 0)
	defer cleanup()

	buf := bankStatementBuf(b.N)
	ts := hlc.Timestamp{WallTime: hlc.UnixNano()}
	b.SetBytes(int64(buf.Len() / b.N))
	b.ResetTimer()
	if _, err := Load(ctx, sqlDB.DB, buf, "bench", dir, ts, 0, dir); err != nil {
		b.Fatalf("%+v", err)
	}
}
