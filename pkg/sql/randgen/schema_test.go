// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package randgen_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl"
	"github.com/cockroachdb/cockroach/pkg/sql/randgen"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/stretchr/testify/require"
)

// TestPopulateTableWithRandData generates some random tables and passes if it
// at least one of those tables will be successfully populated.
func TestPopulateTableWithRandData(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, dbConn, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	rng, _ := randutil.NewTestRand()
	defer ccl.TestingEnableEnterprise()() // allow usage of partitions

	sqlDB := sqlutils.MakeSQLRunner(dbConn)
	sqlDB.Exec(t, "CREATE DATABASE rand")

	// Turn off auto stats collection to prevent out of memory errors on stress tests
	sqlDB.Exec(t, "SET CLUSTER SETTING sql.stats.automatic_collection.enabled = false")

	tablePrefix := "table"
	numTables := 10

	stmts := randgen.RandCreateTables(
		ctx, rng, tablePrefix, numTables, randgen.TableOptNone,
		randgen.PartialIndexMutator, randgen.ForeignKeyMutator,
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
	for i := 0; i < numTables; i++ {
		tableName := string(stmts[i].(*tree.CreateTable).Table.ObjectName)
		numRows := 30
		numRowsInserted, err := randgen.PopulateTableWithRandData(rng, dbConn, tableName, numRows, nil)
		require.NoError(t, err)
		res := sqlDB.QueryStr(t, fmt.Sprintf("SELECT count(*) FROM %s", tree.NameString(tableName)))
		require.Equal(t, fmt.Sprint(numRowsInserted), res[0][0])
		if numRowsInserted > 0 {
			success = true
			break
		}
	}
	require.Equal(t, true, success)
}
