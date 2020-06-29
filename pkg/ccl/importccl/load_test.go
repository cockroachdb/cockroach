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
	gosql "database/sql"
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/importccl"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/tests"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/workload/bank"
	"github.com/cockroachdb/cockroach/pkg/workload/workloadsql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func bankBuf(numAccounts int) *bytes.Buffer {
	bankData := bank.FromRows(numAccounts).Tables()[0]
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "CREATE TABLE %s %s;\n", bankData.Name, bankData.Schema)
	for rowIdx := 0; rowIdx < bankData.InitialRows.NumBatches; rowIdx++ {
		for _, row := range bankData.InitialRows.BatchRows(rowIdx) {
			rowTuple := strings.Join(workloadsql.StringTuple(row), `,`)
			fmt.Fprintf(&buf, "INSERT INTO %s VALUES (%s);\n", bankData.Name, rowTuple)
		}
	}
	return &buf
}

func TestGetDescriptorFromDB(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	params, _ := tests.CreateTestServerParams()
	s, sqlDB, kvDB := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	aliceDesc := sqlbase.NewInitialDatabaseDescriptor(10000, "alice")
	bobDesc := sqlbase.NewInitialDatabaseDescriptor(9999, "bob")

	err := kvDB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		if err := txn.SetSystemConfigTrigger(); err != nil {
			return err
		}
		batch := txn.NewBatch()
		batch.Put(sqlbase.NewDatabaseKey("bob").Key(keys.SystemSQLCodec), bobDesc.GetID())
		batch.Put(sqlbase.NewDeprecatedDatabaseKey("alice").Key(keys.SystemSQLCodec), aliceDesc.GetID())

		batch.Put(sqlbase.MakeDescMetadataKey(keys.SystemSQLCodec, bobDesc.GetID()), bobDesc.DescriptorProto())
		batch.Put(sqlbase.MakeDescMetadataKey(keys.SystemSQLCodec, aliceDesc.GetID()), aliceDesc.DescriptorProto())
		return txn.CommitInBatch(ctx, batch)
	})
	require.NoError(t, err)

	for _, tc := range []struct {
		dbName string

		expected    *sqlbase.DatabaseDescriptor
		expectedErr error
	}{
		{"bob", bobDesc.DatabaseDesc(), nil},
		{"alice", aliceDesc.DatabaseDesc(), nil},
		{"not_found", nil, gosql.ErrNoRows},
	} {
		t.Run(tc.dbName, func(t *testing.T) {
			ret, err := importccl.TestingGetDescriptorFromDB(ctx, sqlDB, tc.dbName)
			if tc.expectedErr != nil {
				assert.Error(t, err)
				assert.Equal(t, tc.expectedErr, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.expected, ret.DatabaseDesc())
			}
		})
	}
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
	desc, err := importccl.Load(ctx, tc.Conns[0], bankBuf(numAccounts), "data",
		"nodelocal://0"+dir, ts, chunkSize, dir, dir, security.RootUser)
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
	row1 := workloadsql.StringTuple(bankData.InitialRows.BatchRows(0)[0])
	row2 := workloadsql.StringTuple(bankData.InitialRows.BatchRows(1)[0])

	var buf bytes.Buffer
	fmt.Fprintf(&buf, "CREATE TABLE %s %s;\n", bankData.Name, bankData.Schema)
	// Intentionally write the rows out of order.
	fmt.Fprintf(&buf, "INSERT INTO %s VALUES (%s);\n", bankData.Name, strings.Join(row2, `,`))
	fmt.Fprintf(&buf, "INSERT INTO %s VALUES (%s);\n", bankData.Name, strings.Join(row1, `,`))

	ts := hlc.Timestamp{WallTime: hlc.UnixNano()}
	_, err := importccl.Load(ctx, tc.Conns[0], &buf, "data", "nodelocal://0/foo", ts,
		0, dir, dir, security.RootUser)
	if !testutils.IsError(err, "out of order row") {
		t.Fatalf("expected out of order row, got: %+v", err)
	}
}

func BenchmarkLoad(b *testing.B) {
	if testing.Short() {
		b.Skip("TODO: fix benchmark")
	}
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
	if _, err := importccl.Load(ctx, tc.Conns[0], buf, "data", dir, ts,
		0, dir, dir, security.RootUser); err != nil {
		b.Fatalf("%+v", err)
	}
}
