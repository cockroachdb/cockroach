// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package logical

import (
	"context"
	gosql "database/sql"
	"fmt"
	"math/rand"
	"slices"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/desctestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/parser/statements"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/datadriven"
	"github.com/stretchr/testify/require"
)

func TestReplicationStatements(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{
		Locality: roachpb.Locality{
			Tiers: []roachpb.Tier{
				{Key: "region", Value: "us-east1"},
			},
		},
	})
	defer s.Stopper().Stop(ctx)

	getTableDesc := func(tableName string) catalog.TableDescriptor {
		return desctestutils.TestingGetTableDescriptor(
			s.DB(),
			s.Codec(),
			"defaultdb",
			"public",
			tableName,
		)
	}

	prepareStatement := func(t *testing.T, db *gosql.DB, columnTypes []*types.T, statement statements.Statement[tree.Statement]) {
		t.Helper()

		var types []tree.ResolvableTypeReference
		for _, typ := range columnTypes {
			types = append(types, typ)
		}

		p := tree.Prepare{
			Name:      tree.Name(fmt.Sprintf("stmt_%d", rand.Int())),
			Types:     types,
			Statement: statement.AST,
		}

		asSql := tree.Serialize(&p)
		_, err := db.Exec(asSql)
		require.NoError(t, err)
	}

	getTypes := func(desc catalog.TableDescriptor) []*types.T {
		columns := getColumnSchema(desc)
		types := make([]*types.T, len(columns))
		for i, col := range columns {
			types[i] = col.column.GetType()
		}
		return types
	}

	datadriven.Walk(t, datapathutils.TestDataPath(t), func(t *testing.T, path string) {
		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			switch d.Cmd {
			case "exec":
				_, err := sqlDB.Exec(d.Input)
				if err != nil {
					return err.Error()
				}
				return "ok"
			case "show-insert":
				var tableName string
				d.ScanArgs(t, "table", &tableName)

				desc := getTableDesc(tableName)

				insertStmt, err := newInsertStatement(desc)
				require.NoError(t, err)

				prepareStatement(t, sqlDB, getTypes(desc), insertStmt)

				return insertStmt.SQL
			case "show-update":
				var tableName string
				d.ScanArgs(t, "table", &tableName)

				desc := getTableDesc(tableName)

				updateStmt, err := newUpdateStatement(desc)
				require.NoError(t, err)

				// update expects previous and current values to be passed as
				// parameters.
				types := slices.Concat(getTypes(desc), getTypes(desc))
				prepareStatement(t, sqlDB, types, updateStmt)

				return updateStmt.SQL
			case "show-delete":
				var tableName string
				d.ScanArgs(t, "table", &tableName)

				desc := getTableDesc(tableName)

				// delete expects previous and current values to be passed as
				// parameters.
				deleteStmt, err := newDeleteStatement(desc)
				require.NoError(t, err)

				types := slices.Concat(getTypes(desc), getTypes(desc))
				prepareStatement(t, sqlDB, types, deleteStmt)

				return deleteStmt.SQL
			case "show-select":
				var tableName string
				d.ScanArgs(t, "table", &tableName)

				desc := getTableDesc(tableName)

				stmt, err := newBulkSelectStatement(desc)
				require.NoError(t, err)

				allColumns := getColumnSchema(desc)
				var primaryKeyTypes []*types.T
				for _, col := range allColumns {
					if col.isPrimaryKey {
						primaryKeyTypes = append(primaryKeyTypes, types.MakeArray(col.column.GetType()))
					}
				}

				prepareStatement(t, sqlDB, primaryKeyTypes, stmt)

				return stmt.SQL
			default:
				return "unknown command: " + d.Cmd
			}
		})
	})
}
