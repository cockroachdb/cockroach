// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

// TestCreateAsVTable verifies that all vtables can be used as the source of
// CREATE TABLE AS and CREATE MATERIALIZED VIEW AS.
func TestCreateAsVTable(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// These are the vtables that need to be fixed.
	// The map should be empty if all vtables are supported.
	brokenTables := map[string]struct{}{
		// TODO(sql-foundations): Fix nil pointer dereference.
		//  See https://github.com/cockroachdb/cockroach/issues/106167.
		`pg_catalog.pg_cursors`: {},
		// TODO(sql-foundations): Fix nil pointer dereference.
		//  See https://github.com/cockroachdb/cockroach/issues/106168.
		`"".crdb_internal.create_statements`: {},
	}

	ctx := context.Background()
	testCluster := serverutils.StartNewTestCluster(t, 1, base.TestClusterArgs{})
	defer testCluster.Stopper().Stop(ctx)
	sqlRunner := sqlutils.MakeSQLRunner(testCluster.ServerConn(0))
	var p parser.Parser

	i := 0
	for _, vSchema := range virtualSchemas {
		for _, vSchemaDef := range vSchema.tableDefs {
			if vSchemaDef.isUnimplemented() {
				continue
			}

			var name tree.TableName
			var ctasColumns []string
			schema := vSchemaDef.getSchema()
			statements, err := p.Parse(schema)
			require.NoErrorf(t, err, schema)
			require.Lenf(t, statements, 1, schema)
			switch stmt := statements[0].AST.(type) {
			case *tree.CreateTable:
				name = stmt.Table
				for _, def := range stmt.Defs {
					if colDef, ok := def.(*tree.ColumnTableDef); ok {
						if colDef.Hidden {
							continue
						}
						// Filter out vector columns to prevent error in CTAS:
						// "VECTOR column types are unsupported".
						if colDef.Type == types.Int2Vector || colDef.Type == types.OidVector {
							continue
						}
						ctasColumns = append(ctasColumns, colDef.Name.String())
					}
				}
			case *tree.CreateView:
				name = stmt.Name
				ctasColumns = []string{"*"}
			default:
				require.Failf(t, "missing case", "unexpected type %T for schema %s", stmt, schema)
			}

			fqName := name.FQString()
			if _, ok := brokenTables[fqName]; ok {
				continue
			}

			// Filter by trace_id to prevent error when selecting from
			// crdb_internal.cluster_inflight_traces:
			// "pq: a trace_id value needs to be specified".
			var where string
			if fqName == `"".crdb_internal.cluster_inflight_traces` {
				where = " WHERE trace_id = 1"
			}

			createTableStmt := fmt.Sprintf(
				"CREATE TABLE test_table_%d AS SELECT %s FROM %s%s",
				i, strings.Join(ctasColumns, ", "), fqName, where,
			)
			sqlRunner.Exec(t, createTableStmt)
			// Skip `CREATE MATERIALIZED VIEW` in 22.2 to avoid
			// "views do not currently support * expressions" error.
			i++
		}
	}

	waitForJobsSuccess(t, sqlRunner)
}

func waitForJobsSuccess(t *testing.T, sqlRunner *sqlutils.SQLRunner) {
	query := `SELECT job_id, status, error, description 
FROM [SHOW JOBS] 
WHERE job_type IN ('SCHEMA CHANGE', 'NEW SCHEMA CHANGE')
AND status != 'succeeded'`
	sqlRunner.CheckQueryResultsRetry(t, query, [][]string{})
}
