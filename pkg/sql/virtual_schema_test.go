// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
	"flag"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
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

	for _, schema := range virtualSchemas {
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
				for _, virtualTable := range schema.tableDefs {
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
