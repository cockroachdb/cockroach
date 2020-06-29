// Copyright 2020 The Cockroach Authors.
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
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

// TestTruncateDoesNotSetDropJobOnNewTable ensures that the new version of a
// table after a TRUNCATE does not contain a DropJobID. This is a regression
// test for #50587.
func TestTruncateDoesNotSetDropJobOnNewTable(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)

	tdb := sqlutils.MakeSQLRunner(tc.ServerConn(0))

	getTableDescID := func(t *testing.T, tableName string) (id sqlbase.ID) {
		tdb.QueryRow(t, `SELECT '`+tableName+`'::regclass::int`).Scan(&id)
		return id
	}

	tdb.Exec(t, `CREATE TABLE foo (i INT PRIMARY KEY)`)
	tdb.Exec(t, `INSERT INTO foo VALUES (1)`)
	oldID := getTableDescID(t, "foo")
	tdb.Exec(t, `TRUNCATE foo`)
	newID := getTableDescID(t, "foo")
	require.NotEqual(t, oldID, newID)
	var td *sqlbase.TableDescriptor
	require.NoError(t, tc.Server(0).DB().Txn(ctx, func(
		ctx context.Context, txn *client.Txn,
	) (err error) {
		td, err = sqlbase.GetTableDescFromID(ctx, txn, newID)
		return err
	}))
	require.Zero(t, td.DropJobID)
}
