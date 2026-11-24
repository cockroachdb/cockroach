// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// VirtualSchemaTest validates virtualSchema which undefinedTables
// only has tables that are not defined as virtualSchemaTable.
// There is a -rewrite-tables flag, when used, if it is a fixable schema
// This will remove the tables from undefinedTables if there is a
// tableDef in that schema.
//
// Test Usage (in pkg/sql directory):
//   go test -run TestVirtualSchemas
//
// To Fix undefinedTables values (in pkg/sql directory):
//   go test -run TestVirtualSchemas -rewrite-tables

package sql

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemadesc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

var rewriteTables = flag.Bool(
	"rewrite-tables",
	false,
	"rewrite undefinedTables by removing defined tables",
)

var fixableSchemas = map[string]string{
	"pg_catalog":         "pg_catalog.go",
	"information_schema": "information_schema.go",
}

func TestVirtualSchemas(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	for schemaID, schema := range virtualSchemas {
		_, isSchemaFixable := fixableSchemas[schema.name]
		if len(schema.undefinedTables) == 0 || !isSchemaFixable {
			continue
		}

		if *rewriteTables {
			validateUndefinedTablesField(t)
			unimplementedTables, err := getUndefinedTablesList(PGMetadataTables{}, schema)
			if err != nil {
				t.Fatal(err)
			}
			rewriteSchema(schema.name, unimplementedTables)
		} else {
			t.Run(fmt.Sprintf("VirtualSchemaTest/%s", schema.name), func(t *testing.T) {
				for tableID, virtualTable := range schema.tableDefs {
					tableName, err := getTableNameFromCreateTable(virtualTable.getSchema())
					if err != nil {
						t.Fatal(err)
					}
					if _, ok := schema.undefinedTables[tableName]; ok {
						t.Errorf(
							"Table %s.%s is defined and not expected to be part of undefinedTables",
							schema.name,
							tableName,
						)
					}

					// Sanity check indexes are all defined.
					sc, ok := schemadesc.GetVirtualSchemaByID(schemaID)
					require.True(t, ok)
					d, err := virtualTable.initVirtualTableDesc(
						ctx,
						cluster.MakeClusterSettings(),
						sc,
						tableID,
					)
					require.NoError(t, err)
					switch virtualTable := virtualTable.(type) {
					case *virtualSchemaTable:
						require.Equalf(
							t,
							len(d.GetIndexes()),
							len(virtualTable.indexes),
							"number of indexes in description must match number of indexes defined for table %s",
							d.GetName(),
						)
					}
				}
			})
		}
	}
}

func rewriteSchema(schemaName string, tableNames []string) {
	unimplementedTablesText := formatUndefinedTablesText(tableNames)
	rewriteFile(fixableSchemas[schemaName], func(input *os.File, output outputFile) {
		reader := bufio.NewScanner(input)
		for reader.Scan() {
			line := reader.Text()
			output.appendString(line)
			output.appendString("\n")

			if strings.TrimSpace(line) == undefinedTablesDeclaration {
				printBeforeTerminalString(reader, output, undefinedTablesTerminal, unimplementedTablesText)
			}
		}
	})
}

// TestVirtualTablesIgnoreDisallowFullTableScans checks that we ignore
// disallow_full_table_scans for virtual tables, even when the virtual table is
// populated using an internal executor query that itself does a full scan.
func TestVirtualTablesIgnoreDisallowFullTableScans(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Inject a virtual table into crdb_internal which is populated using an
	// internal executor query that does a full scan.
	crdbInternalFooTable := virtualSchemaTable{
		schema: `
CREATE TABLE crdb_internal.foo (
  status STRING NOT NULL,
  created TIMESTAMP NOT NULL
)
`,
		comment: "foo",
		indexes: []virtualIndex{},
		populate: func(ctx context.Context, p *planner, db catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) (retErr error) {
			it, err := p.InternalSQLTxn().QueryIteratorEx(
				ctx, "foo", p.Txn(), sessiondata.NodeUserSessionDataOverride,
				// Use a full-table scan query with an index hint to hit the
				// disallow_full_table_scans check in execbuilder.
				"SELECT status, created FROM system.jobs@jobs_status_created_idx",
			)
			if err != nil {
				return err
			}
			defer func() {
				if err := it.Close(); err != nil {
					retErr = errors.CombineErrors(retErr, err)
				}
			}()
			for {
				hasNext, err := it.Next(ctx)
				if !hasNext || err != nil {
					return err
				}
				currentRow := it.Cur()
				if err := addRow(currentRow...); err != nil {
					return err
				}
			}
		},
	}
	crdbInternal.tableDefs[catconstants.CrdbInternalTestID] = crdbInternalFooTable

	ctx := context.Background()

	srv, conn, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)

	sqlDB := sqlutils.MakeSQLRunner(conn)
	sqlDB.Exec(t, "SET disallow_full_table_scans = on")
	sqlDB.CheckQueryResults(t, "SELECT * FROM crdb_internal.foo WHERE status = 'banana'", [][]string{})
}
