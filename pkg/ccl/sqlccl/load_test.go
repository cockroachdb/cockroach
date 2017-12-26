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
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/ccl/sqlccl"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/workload/bank"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestImportChunking(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Generate at least 2 chunks.
	const chunkSize = 1024 * 500
	numAccounts := int(chunkSize / backupRestoreRowPayloadSize * 2)

	ctx, _, sqlDB, dir, cleanupFn := backupRestoreTestSetup(t, singleNode, 0, initNone)
	defer cleanupFn()

	ts := hlc.Timestamp{WallTime: hlc.UnixNano()}
	desc, err := sqlccl.Load(ctx, sqlDB.DB, bankBuf(numAccounts), "data", "nodelocal://"+dir, ts, chunkSize, dir)
	if err != nil {
		t.Fatalf("%+v", err)
	}
	if len(desc.Files) < 2 {
		t.Errorf("expected at least 2 ranges")
	}
}

func TestImportOutOfOrder(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx, _, sqlDB, dir, cleanupFn := backupRestoreTestSetup(t, singleNode, 0, initNone)
	defer cleanupFn()

	bankData := bank.FromRows(2).Tables()[0]
	row1 := bankData.InitialRowFn(0)
	row2 := bankData.InitialRowFn(1)

	var buf bytes.Buffer
	fmt.Fprintf(&buf, "CREATE TABLE %s %s;\n", bankData.Name, bankData.Schema)
	// Intentionally write the rows out of order.
	fmt.Fprintf(&buf, "INSERT INTO %s VALUES (%s);\n", bankData.Name, strings.Join(row2, `,`))
	fmt.Fprintf(&buf, "INSERT INTO %s VALUES (%s);\n", bankData.Name, strings.Join(row1, `,`))

	ts := hlc.Timestamp{WallTime: hlc.UnixNano()}
	_, err := sqlccl.Load(ctx, sqlDB.DB, &buf, "data", localFoo, ts, 0, dir)
	if !testutils.IsError(err, "out of order row") {
		t.Fatalf("expected out of order row, got: %+v", err)
	}
}

func BenchmarkLoad(b *testing.B) {
	// NB: This benchmark takes liberties in how b.N is used compared to the go
	// documentation's description. We're getting useful information out of it,
	// but this is not a pattern to cargo-cult.
	ctx, _, sqlDB, dir, cleanup := backupRestoreTestSetup(b, multiNode, 0, initNone)
	defer cleanup()

	ts := hlc.Timestamp{WallTime: hlc.UnixNano()}
	buf := bankBuf(b.N)
	b.SetBytes(int64(buf.Len() / b.N))
	b.ResetTimer()
	if _, err := sqlccl.Load(ctx, sqlDB.DB, buf, "data", dir, ts, 0, dir); err != nil {
		b.Fatalf("%+v", err)
	}
}
