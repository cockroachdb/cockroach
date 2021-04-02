// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// VirtualSchemaTest validates virtualSchema that unimplementedTableNames
// only have tables that do not have a virtualSchemaTable defined.
// There is a -rewrite-tables flag, when used, if it is a fixable schema
// This will remove the tables from unimplementedTableNames if there is a
// tableDef in that schema.
//
// Test Usage (in pkg/sql directory):
//   go test -run TestVirtualSchemas
//
// To Fix unimplementedTableNames values (in pkg/sql directory):
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
	"rewrite unimplementedTableNames by removing defined tables",
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
		if len(schema.unimplementedTableNames) == 0 || !isSchemaFixable {
			continue
		}

		if *rewriteTables {
			unimplementedTables, err := getUnimplementedTableNamesList(PGMetadataTables{}, schema)
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
					if _, ok := schema.unimplementedTableNames[tableName]; ok {
						t.Errorf(
							"Table %s.%s is defined and not expected to be part of unimplementedTableNames",
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
	unimplementedTablesText := formatUnimplementedTableNamesText(tableNames)
	rewriteFile(fixableSchemas[schemaName], func(input *os.File, output outputFile) {
		reader := bufio.NewScanner(input)
		for reader.Scan() {
			line := reader.Text()
			output.appendString(line)
			output.appendString("\n")

			if strings.TrimSpace(line) == unimplementedTableNamesDeclaration {
				printBeforeTerminalString(reader, output, unimplementedTableNamesTerminal, unimplementedTablesText)
			}
		}
	})
}
