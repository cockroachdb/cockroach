// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sqltestutils

import (
	"fmt"
	"math"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
)

// GenerateViewBasedGraphSchema generates a large nested schema with a number
// of nested references.
func GenerateViewBasedGraphSchema(
	t *testing.T,
	sqlDB *sqlutils.SQLRunner,
	schemaName string,
	numTablesPerDepth int,
	numColumnsPerTable int,
	graphDepth int,
) {
	sqlDB.Exec(t, "BEGIN;")
	// Create the base tables for our complex database.
	const BaseTableName = "table%d"
	tblColumnCache := make(map[string]string)
	for i := 0; i < numTablesPerDepth; i++ {
		tableName := fmt.Sprintf(BaseTableName, i)
		columnRefStmt := fmt.Sprintf("%s_%%d", tableName)
		createStatement := strings.Builder{}
		columns := strings.Builder{}
		createStatement.WriteString("CREATE TABLE ")
		createStatement.WriteString(schemaName)
		createStatement.WriteString(".")
		createStatement.WriteString(tableName)
		createStatement.WriteString("(")
		for col := 0; col < numColumnsPerTable; col++ {
			columnRef := fmt.Sprintf(columnRefStmt, col)
			if col != 0 {
				createStatement.WriteString(", ")
				columns.WriteString(",")
			}
			createStatement.WriteString(columnRef)
			createStatement.WriteString(" int")
			columns.WriteString(columnRef)
			columns.WriteString("")
		}
		tblColumnCache[tableName] = columns.String()
		createStatement.WriteString(")")
		sqlDB.Exec(t, "use largedb;")
		sqlDB.Exec(t, createStatement.String())
	}
	// Next create the views at each nesting level, where each view at the current
	// level will refer to base tables or the previous level.
	const BaseViewName = "view%d_%d"
	for nest := 0; nest < graphDepth; nest++ {
		for viewIdx := 0; viewIdx < numTablesPerDepth; viewIdx++ {
			viewName := fmt.Sprintf(BaseViewName, nest, viewIdx)
			// Refer to the last set of tables or views.
			createStatement := strings.Builder{}
			createStatement.WriteString("CREATE VIEW ")
			createStatement.WriteString(schemaName)
			createStatement.WriteString(".")
			createStatement.WriteString(viewName)
			createStatement.WriteString("( ")
			viewDefColumn := fmt.Sprintf("%s_%%d", viewName)
			viewDef := strings.Builder{}
			selectColumns := strings.Builder{}
			fromTables := strings.Builder{}
			// Next setup the column references.
			refBase := fmt.Sprintf("view%d_%%d", nest-1)
			colIdx := 0
			if nest == 0 {
				refBase = BaseTableName
			}
			// Loop over the views / tables in the previous nesting level.
			for baseIdx := 0; baseIdx < numTablesPerDepth; baseIdx++ {
				baseTblName := fmt.Sprintf(refBase, baseIdx)
				// Next add the columns into the definition.
				if colIdx != 0 {
					selectColumns.WriteString(", ")
				}
				for col := 0; col < int(math.Pow(float64(numTablesPerDepth), float64(nest))*float64(numColumnsPerTable)); col++ {
					// First add this column into the column definitions.
					if colIdx != 0 {
						viewDef.WriteString(", ")
					}
					viewDef.WriteString(fmt.Sprintf(viewDefColumn, colIdx))
					colIdx++
				}
				selectColumns.WriteString(tblColumnCache[baseTblName])
				if baseIdx != 0 {
					fromTables.WriteString(", ")
				}
				fromTables.WriteString(baseTblName)
			}
			tblColumnCache[viewName] = viewDef.String()
			createStatement.WriteString(viewDef.String())
			createStatement.WriteString(") AS SELECT ")
			createStatement.WriteString(selectColumns.String())
			createStatement.WriteString(" FROM ")
			createStatement.WriteString(fromTables.String())
			sqlDB.Exec(t, createStatement.String())
		}
	}
	sqlDB.Exec(t,
		`COMMIT;`)
}
