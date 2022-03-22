// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package randgen

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/stretchr/testify/require"
)

// TestPopulateTableWithRandData generates some random tables and passes if it
// at least one of those tables will be successfully populated.
func TestPopulateTableWithRandData(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	s, dbConn, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	rng, _ := randutil.NewTestRand()

	sqlDB := sqlutils.MakeSQLRunner(dbConn)
	sqlDB.Exec(t, "CREATE DATABASE rand")

	// Turn off auto stats collection to prevent out of memory errors on stress tests
	sqlDB.Exec(t, "SET CLUSTER SETTING sql.stats.automatic_collection.enabled = false")

	tablePrefix := "table"
	numTables := 10

	stmts := RandCreateTables(rng, tablePrefix, numTables,
		PartialIndexMutator,
		ForeignKeyMutator,
	)

	var sb strings.Builder
	for _, stmt := range stmts {
		sb.WriteString(tree.SerializeForDisplay(stmt))
		sb.WriteString(";\n")
	}
	sqlDB.Exec(t, sb.String())

	// To prevent the test from being flaky, pass the test if PopulateTableWithRandomData
	// inserts at least one row in at least one table.
	success := false
	for i := 1; i <= numTables; i++ {
		tableName := tablePrefix + fmt.Sprint(i)
		numRows := 30
		numRowsInserted, err := PopulateTableWithRandData(rng, dbConn, tableName, numRows)
		require.NoError(t, err)
		res := sqlDB.QueryStr(t, fmt.Sprintf("SELECT count(*) FROM %s", tableName))
		require.Equal(t, fmt.Sprint(numRowsInserted), res[0][0])
		if numRowsInserted > 0 {
			success = true
			break
		}
	}
	require.Equal(t, true, success)
}
