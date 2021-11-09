// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/lease"
	"github.com/cockroachdb/cockroach/pkg/sql/tests"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestMergeProcess(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	defer lease.TestingDisableTableLeases()()

	params, _ := tests.CreateTestServerParams()

	server, tdb, kvDB := serverutils.StartServer(t, params)
	defer server.Stopper().Stop(context.Background())

	// Run the initial setupSQL.
	if _, err := tdb.Exec(`CREATE DATABASE d`); err != nil {
		t.Fatal(err)
	}
	if _, err := tdb.Exec(`CREATE TABLE d.t (k INT PRIMARY KEY, a INT, b INT,
		INDEX idx (b), 
		INDEX idx_temp (b)
);
`); err != nil {
		t.Fatal(err)
	}

	// Fetch the descriptor ID for the relevant table.
	var tableID descpb.ID

	// Run the testCase's setupDesc function to prepare an index backfill
	// mutation. Also, create an associated job and set it up to be blocked.
	codec := keys.SystemSQLCodec
	lm := server.LeaseManager().(*lease.Manager)
	settings := server.ClusterSettings()
	execCfg := server.ExecutorConfig().(sql.ExecutorConfig)
	jr := server.JobRegistry().(*jobs.Registry)

	changer := sql.NewSchemaChangerForTesting(
		tableID, 1, execCfg.NodeID.SQLInstanceID(), kvDB, lm, jr, &execCfg, settings)

	tableDesc := catalogkv.TestingGetMutableExistingTableDescriptor(kvDB, codec, "d", "t")

	// Here want to have different entries for the two indices, so we manipulate
	// the index to DELETE_ONLY when we don't want to write to it, and
	// DELETE_AND_WRITE_ONLY when we write to it.
	//
	// Populate idx with some 1000/1, 2000/2 entries Populate idx_temp with
	// 3000/3, 4000/4 entries, and a deleted for the 2000/2 entry.
	mTest := makeMutationTest(t, kvDB, tdb, tableDesc)
	mTest.writeIndexMutation(ctx, "idx", descpb.DescriptorMutation{State: descpb.DescriptorMutation_DELETE_AND_WRITE_ONLY})
	mTest.updateIndexMutation(ctx, "idx_temp", descpb.DescriptorMutation{State: descpb.DescriptorMutation_DELETE_ONLY}, true)

	tdb.Exec(`INSERT INTO d.t (k, a, b) VALUES (1, 100, 1000), (2, 200, 2000)`)

	mTest.makeMutationsActive(ctx)
	mTest.writeIndexMutation(ctx, "idx", descpb.DescriptorMutation{State: descpb.DescriptorMutation_DELETE_ONLY})
	mTest.writeIndexMutation(ctx, "idx_temp", descpb.DescriptorMutation{State: descpb.DescriptorMutation_DELETE_AND_WRITE_ONLY})

	tdb.Exec(`INSERT INTO d.t (k, a, b) VALUES (3, 300, 3000), (4, 400, 4000)`)
	tdb.Exec(`DELETE FROM d.t WHERE k = 2`)

	mTest.makeMutationsActive(ctx)
	mTest.writeIndexMutation(ctx, "idx", descpb.DescriptorMutation{State: descpb.DescriptorMutation_DELETE_AND_WRITE_ONLY})
	mTest.writeIndexMutation(ctx, "idx_temp", descpb.DescriptorMutation{State: descpb.DescriptorMutation_DELETE_ONLY})

	// Insert another row for the 2000/2 entry just so that there's a primary
	// index row for the index to return when we read it.
	tdb.Exec(`INSERT INTO d.t (k, a, b) VALUES (2, 201, 2000)`)

	mTest.makeMutationsActive(ctx)
	tableDesc = catalogkv.TestingGetMutableExistingTableDescriptor(kvDB, codec, "d", "t")

	idx, err := tableDesc.FindIndexWithName("idx")
	if err != nil {
		t.Fatal(err)
	}

	idxTemp, err := tableDesc.FindIndexWithName("idx_temp")
	if err != nil {
		t.Fatal(err)
	}
	sqlDB := sqlutils.MakeSQLRunner(tdb)

	fmt.Println("table = ", sqlDB.QueryStr(t, "SELECT * FROM d.t ORDER BY k ASC"))
	fmt.Println("idx = ", sqlDB.QueryStr(t, "SELECT * FROM d.t@idx ORDER BY k ASC"))
	fmt.Println("tmp = ", sqlDB.QueryStr(t, "SELECT * FROM d.t@idx_temp ORDER BY k ASC"))

	// Before merge idx has its 1000/1, 2000/2 entries
	require.Equal(t, [][]string{{"1", "100", "1000"}, {"2", "201", "2000"}},
		sqlDB.QueryStr(t, "SELECT * FROM d.t@idx ORDER BY k ASC"))

	if err := changer.Merge(context.Background(),
		codec,
		tableDesc,
		idxTemp,
		idx); err != nil {
		t.Fatal(err)
	}

	fmt.Println("changer = ", changer)
	fmt.Println("tdb = ", tdb)

	fmt.Println("table = ", sqlDB.QueryStr(t, "SELECT * FROM d.t ORDER BY k ASC"))
	fmt.Println("idx = ", sqlDB.QueryStr(t, "SELECT * FROM d.t@idx ORDER BY k ASC"))
	fmt.Println("tmp = ", sqlDB.QueryStr(t, "SELECT * FROM d.t@idx_temp ORDER BY k ASC"))

	// After merge idx has 1000/1, 3000/3, 4000/4 entries. Its 2000/2 entry was
	// deleted by the idx_temp delete.
	require.Equal(t, [][]string{{"1", "100", "1000"}, {"3", "300", "3000"}, {"4", "400", "4000"}},
		sqlDB.QueryStr(t, "SELECT * FROM d.t@idx ORDER BY k ASC"))

}

func (mt mutationTest) updateIndexMutation(
	ctx context.Context, index string, m descpb.DescriptorMutation, preserveDeletes bool,
) {
	tableDesc := mt.tableDesc
	idx, err := tableDesc.FindIndexWithName(index)
	if err != nil {
		mt.Fatal(err)
	}
	// The rewrite below potentially invalidates the original object with an overwrite.
	// Clarify what's going on.
	idxCopy := *idx.IndexDesc()
	idxCopy.UseDeletePreservingEncoding = preserveDeletes
	tableDesc.RemovePublicNonPrimaryIndex(idx.Ordinal())
	m.Descriptor_ = &descpb.DescriptorMutation_Index{Index: &idxCopy}
	mt.writeMutation(ctx, m)
}
