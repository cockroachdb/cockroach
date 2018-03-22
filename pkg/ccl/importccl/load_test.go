// Copyright 2016 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package importccl_test

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/importccl"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/workload"
	"github.com/cockroachdb/cockroach/pkg/workload/bank"
)

func bankBuf(numAccounts int) *bytes.Buffer {
	bankData := bank.FromRows(numAccounts).Tables()[0]
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "CREATE TABLE %s %s;\n", bankData.Name, bankData.Schema)
	for rowIdx := 0; rowIdx < bankData.InitialRows.NumBatches; rowIdx++ {
		for _, row := range bankData.InitialRows.Batch(rowIdx) {
			rowTuple := strings.Join(workload.StringTuple(row), `,`)
			fmt.Fprintf(&buf, "INSERT INTO %s VALUES (%s);\n", bankData.Name, rowTuple)
		}
	}
	return &buf
}
func TestImportChunking(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Generate at least 2 chunks.
	const chunkSize = 1024 * 500
	numAccounts := int(chunkSize / 100 * 2)

	ctx := context.Background()
	dir, cleanup := testutils.TempDir(t)
	defer cleanup()

	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{ServerArgs: base.TestServerArgs{ExternalIODir: dir}})
	defer tc.Stopper().Stop(ctx)

	if _, err := tc.Conns[0].Exec("CREATE DATABASE data"); err != nil {
		t.Fatal(err)
	}

	ts := hlc.Timestamp{WallTime: hlc.UnixNano()}
	desc, err := importccl.Load(ctx, tc.Conns[0], bankBuf(numAccounts), "data", "nodelocal://"+dir, ts, chunkSize, dir)
	if err != nil {
		t.Fatalf("%+v", err)
	}
	if len(desc.Files) < 2 {
		t.Errorf("expected at least 2 ranges")
	}
}

func TestImportOutOfOrder(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	dir, cleanup := testutils.TempDir(t)
	defer cleanup()

	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{ServerArgs: base.TestServerArgs{ExternalIODir: dir}})
	defer tc.Stopper().Stop(ctx)

	if _, err := tc.Conns[0].Exec("CREATE DATABASE data"); err != nil {
		t.Fatal(err)
	}
	bankData := bank.FromRows(2).Tables()[0]
	row1 := workload.StringTuple(bankData.InitialRows.Batch(0)[0])
	row2 := workload.StringTuple(bankData.InitialRows.Batch(1)[0])

	var buf bytes.Buffer
	fmt.Fprintf(&buf, "CREATE TABLE %s %s;\n", bankData.Name, bankData.Schema)
	// Intentionally write the rows out of order.
	fmt.Fprintf(&buf, "INSERT INTO %s VALUES (%s);\n", bankData.Name, strings.Join(row2, `,`))
	fmt.Fprintf(&buf, "INSERT INTO %s VALUES (%s);\n", bankData.Name, strings.Join(row1, `,`))

	ts := hlc.Timestamp{WallTime: hlc.UnixNano()}
	_, err := importccl.Load(ctx, tc.Conns[0], &buf, "data", "nodelocal:///foo", ts, 0, dir)
	if !testutils.IsError(err, "out of order row") {
		t.Fatalf("expected out of order row, got: %+v", err)
	}
}

func BenchmarkLoad(b *testing.B) {
	// NB: This benchmark takes liberties in how b.N is used compared to the go
	// documentation's description. We're getting useful information out of it,
	// but this is not a pattern to cargo-cult.
	ctx := context.Background()
	dir, cleanup := testutils.TempDir(b)
	defer cleanup()

	tc := testcluster.StartTestCluster(b, 1, base.TestClusterArgs{ServerArgs: base.TestServerArgs{ExternalIODir: dir}})
	defer tc.Stopper().Stop(ctx)
	if _, err := tc.Conns[0].Exec("CREATE DATABASE data"); err != nil {
		b.Fatal(err)
	}

	ts := hlc.Timestamp{WallTime: hlc.UnixNano()}
	buf := bankBuf(b.N)
	b.SetBytes(int64(buf.Len() / b.N))
	b.ResetTimer()
	if _, err := importccl.Load(ctx, tc.Conns[0], buf, "data", dir, ts, 0, dir); err != nil {
		b.Fatalf("%+v", err)
	}
}
