// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rand

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/security/securityassets"
	"github.com/cockroachdb/cockroach/pkg/security/securitytest"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/sql/randgen"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/lib/pq/oid"
	"github.com/stretchr/testify/require"
)

func TestMain(m *testing.M) {
	randutil.SeedForTests()
	securityassets.SetLoader(securitytest.EmbeddedAssets)
	serverutils.InitTestServerFactory(server.TestServerFactory)
	os.Exit(m.Run())
}

// TestRandRun tests that for every type in SeedTypes (i.e., those
// that can be used when creating columns for the rand workload), we
// are able to generate a random value for that column type *and*
// insert it into a column
func TestRandRun(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderDuress(t, "random test, can time out under duress")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dbName := "rand_test"
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{UseDatabase: dbName})
	defer s.Stopper().Stop(ctx)
	sqlDB := sqlutils.MakeSQLRunner(db)
	sqlDB.Exec(t, "CREATE DATABASE "+tree.NameString(dbName))

	rng, seed := randutil.NewTestRand()
	t.Logf("rand workload random seed: %v", seed)
	RandomSeed.Set(seed)
	tblName := "datum_to_go_sql_test"
	colName := "c1"

	for _, typeT := range types.OidToType {
		// The BIT[] type is broken, so skip it.
		// TODO(mgartner): BIT[] type is broken, primarily because the BIT type
		// is broken. It should have a non-zero width. In should probably be
		// exported from the types package, and there should be a datum type for
		// BIT that is distinct from VARBIT.
		if typeT.Family() == types.ArrayFamily && typeT.ArrayContents().Oid() == oid.T_bit {
			continue
		}
		if !randgen.IsLegalColumnType(typeT) {
			continue
		}

		// generate a datum of the given type just for the purposes of
		// printing the Go type in the test description passed to t.Run()
		sqlName := typeT.SQLStandardName()
		datum := randgen.RandDatum(rng, typeT, false)
		unwrapped := tree.UnwrapDOidWrapper(datum)

		t.Run(fmt.Sprintf("%s-%T", sqlName, unwrapped), func(t *testing.T) {
			sqlDB.Exec(t, fmt.Sprintf("DROP TABLE IF EXISTS %s.%s", tree.NameString(dbName), tree.NameString(tblName)))
			createTableStmt := fmt.Sprintf("CREATE TABLE %s.%s (%s %s)",
				tree.NameString(dbName), tree.NameString(tblName), tree.NameString(colName), sqlName)
			sqlDB.Exec(t, createTableStmt)

			stmt := fmt.Sprintf("INSERT INTO %s.%s (%s) VALUES ($1)",
				tree.NameString(dbName), tree.NameString(tblName), tree.NameString(colName))
			writeStmt, err := db.Prepare(stmt)
			require.NoError(t, err)

			dataType, err := typeForOid(db, typeT.InternalType.Oid, tblName, colName)
			require.NoError(t, err)
			cols := []col{{name: colName, dataType: dataType}}
			op := randOp{
				config:    &random{batchSize: 1},
				db:        db,
				cols:      cols,
				rng:       rng,
				writeStmt: writeStmt,
			}
			require.NoError(t, op.run(ctx))
		})
	}
}
