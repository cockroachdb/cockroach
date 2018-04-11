// Copyright 2018 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package importccl_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/workload"
	"github.com/cockroachdb/cockroach/pkg/workload/tpcc"
)

func TestExportStmt(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const (
		nodes       = 3
		rowsPerFile = 100
	)
	ctx := context.Background()
	dir, cleanup := testutils.TempDir(t)
	defer cleanup()

	tc := testcluster.StartTestCluster(t, nodes,
		base.TestClusterArgs{ServerArgs: base.TestServerArgs{ExternalIODir: dir, UseDatabase: "test"}},
	)
	defer tc.Stopper().Stop(ctx)
	conn := tc.Conns[0]
	db := sqlutils.MakeSQLRunner(conn)

	db.Exec(t, "CREATE DATABASE test")

	tbl := tpcc.FromWarehouses(100).Tables()[0]
	t.Log(fmt.Sprintf("CREATE TABLE %s %s ", tbl.Name, tbl.Schema))
	db.Exec(t, fmt.Sprintf("CREATE TABLE %s %s ", tbl.Name, tbl.Schema))

	if _, err := workload.FillTable(ctx, db.DB, tbl, 100, 3); err != nil {
		t.Fatal(err)
	}
	db.Exec(t, fmt.Sprintf(`EXPORT INTO CSV 'nodelocal:///t' FROM TABLE %s`, tbl.Name))
}
