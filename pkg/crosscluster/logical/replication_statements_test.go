// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package logical

import (
	"context"
	"fmt"
	"math/rand"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/desctestutils"
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

				// Test preparing the statement to ensure it is valid SQL.
				_, err = sqlDB.Exec(fmt.Sprintf("PREPARE stmt_%d AS %s", rand.Int(), insertStmt.SQL))
				require.NoError(t, err)

				return insertStmt.SQL
			case "show-update":
				var tableName string
				d.ScanArgs(t, "table", &tableName)

				desc := getTableDesc(tableName)

				updateStmt, err := newUpdateStatement(desc)
				require.NoError(t, err)

				// Test preparing the statement to ensure it is valid SQL.
				_, err = sqlDB.Exec(fmt.Sprintf("PREPARE stmt_%d AS %s", rand.Int(), updateStmt.SQL))
				require.NoError(t, err)

				return updateStmt.SQL
			case "show-delete":
				var tableName string
				d.ScanArgs(t, "table", &tableName)

				desc := getTableDesc(tableName)

				deleteStmt, err := newDeleteStatement(desc)
				require.NoError(t, err)

				// Test preparing the statement to ensure it is valid SQL.
				_, err = sqlDB.Exec(fmt.Sprintf("PREPARE stmt_%d AS %s", rand.Int(), deleteStmt.SQL))
				require.NoError(t, err)

				return deleteStmt.SQL
			case "show-select":
				var tableName string
				d.ScanArgs(t, "table", &tableName)

				desc := getTableDesc(tableName)

				stmt, err := newBulkSelectStatement(desc)
				require.NoError(t, err)

				// Test preparing the statement to ensure it is valid SQL.
				_, err = sqlDB.Exec(fmt.Sprintf("PREPARE stmt_%d AS %s", rand.Int(), stmt.SQL))
				require.NoError(t, err)

				return stmt.SQL
			default:
				return "unknown command: " + d.Cmd
			}
		})
	})
}
