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
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/stretchr/testify/require"
)

func PopulateRandTableTest(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	skip.UnderRace(t, "takes >1 min under race")
	rng, _ := randutil.NewPseudoRand()
	dir, dirCleanupFn := testutils.TempDir(t)
	defer dirCleanupFn()
	params := base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			UseDatabase:   "rand",
			ExternalIODir: dir,
		},
	}
	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 1, params)
	defer tc.Stopper().Stop(ctx)
	sqlDB := sqlutils.MakeSQLRunner(tc.Conns[0])
	sqlDB.Exec(t, "CREATE DATABASE rand")

	stmts := RandCreateTables(rng, "table", 5,
		PartialIndexMutator,
		ForeignKeyMutator,
	)

	var sb strings.Builder
	for _, stmt := range stmts {
		sb.WriteString(tree.SerializeForDisplay(stmt))
		sb.WriteString(";\n")
	}
	sqlDB.Exec(t, sb.String())

	tables := sqlDB.Query(t, `SELECT name FROM crdb_internal.
tables WHERE database_name = 'rand' AND schema_name = 'public'`)

	for tables.Next() {
		numRows := rng.Intn(100)
		var tableName string
		if err := tables.Scan(&tableName); err != nil {
			t.Fatal(err)
		}
		err := PopulateRandTable(rng, tc.Conns[0], tableName, numRows)
		if err != nil {
			t.Fatal(err)
		}
		res := sqlDB.QueryStr(t, fmt.Sprintf("SELECT count(*) FROM %s", tableName))
		require.Equal(t, fmt.Sprint(numRows), res[0][0])
	}
}
