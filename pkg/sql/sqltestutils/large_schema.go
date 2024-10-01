// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sqltestutils

import (
	"fmt"
	"math"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/parser/statements"
)

// GenerateViewBasedGraphSchemaParams input parameters for
// GenerateViewBasedGraphSchema.
type GenerateViewBasedGraphSchemaParams struct {
	// SchemaName schema under which the views/tables should be created.
	SchemaName string
	// NumTablesPerDepth number of tables generated at each depth.
	NumTablesPerDepth int
	// NumColumnsPerTable number of columns at the initial depth.
	NumColumnsPerTable int
	// GraphDepth depth of the table/view graph that will be generated.
	GraphDepth int
}

// GenerateViewBasedGraphSchema generates a complex nested schema that takes
// the following form:
//
//  1. Tables generated at depth 0 will have NumColumnsPerTable, where
//     NumTablesPerDepth tables will be created with the name format:
//     table{tableIndex}.
//     Columns will have the name format: {tableName}_{columnIndex}.
//
//  2. All greater than zero depths we will generate views, where NumTablesPerDepth
//     views will be generated. The views at a given depth will select from all
//     the columns from the views/tables from the previous depth. This means
//     a view at depth N will have (NumTablesPerDepth^(depth)) * NumColumnsPerTable
//     Each generate view will have the name format: view{depth}_{tableIndex}.
//     Columns will have the name format: {viewName}_{columnIndex}.
//
//     This setup will generate a large number of column references between views,
//     as the depth increases.
func GenerateViewBasedGraphSchema(
	params GenerateViewBasedGraphSchemaParams,
) (statements.Statements, error) {
	statements := make(statements.Statements, 0, int(math.Pow(float64(params.NumTablesPerDepth), float64(params.GraphDepth))))
	stmt, err := parser.ParseOne("BEGIN;")
	if err != nil {
		return nil, err
	}
	statements = append(statements, stmt)
	// Create the base tables for our complex database.
	const BaseTableName = "table%d"
	tblColumnCache := make(map[string]string)
	for i := 0; i < params.NumTablesPerDepth; i++ {
		tableName := fmt.Sprintf(BaseTableName, i)
		columnRefStmt := fmt.Sprintf("%s_%%d", tableName)
		createStatement := strings.Builder{}
		columns := strings.Builder{}
		createStatement.WriteString("CREATE TABLE ")
		createStatement.WriteString(params.SchemaName)
		createStatement.WriteString(".")
		createStatement.WriteString(tableName)
		createStatement.WriteString("(")
		for col := 0; col < params.NumColumnsPerTable; col++ {
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
		stmt, err = parser.ParseOne(createStatement.String())
		if err != nil {
			return nil, err
		}
		statements = append(statements, stmt)
	}
	// Next create the views at each nesting level, where each view at the current
	// level will refer to base tables or the previous level.
	const BaseViewName = "view%d_%d"
	for nest := 0; nest < params.GraphDepth; nest++ {
		for viewIdx := 0; viewIdx < params.NumTablesPerDepth; viewIdx++ {
			viewName := fmt.Sprintf(BaseViewName, nest, viewIdx)
			// Refer to the last set of tables or views.
			createStatement := strings.Builder{}
			createStatement.WriteString("CREATE VIEW ")
			createStatement.WriteString(params.SchemaName)
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
			for baseIdx := 0; baseIdx < params.NumTablesPerDepth; baseIdx++ {
				baseTblName := fmt.Sprintf(refBase, baseIdx)
				// Next add the columns into the definition.
				if colIdx != 0 {
					selectColumns.WriteString(", ")
				}
				columnsForTable := int(math.Pow(float64(params.NumTablesPerDepth), float64(nest)) *
					float64(params.NumColumnsPerTable))
				for col := 0; col < columnsForTable; col++ {
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
				fromTables.WriteString(params.SchemaName)
				fromTables.WriteString(".")
				fromTables.WriteString(baseTblName)
			}
			tblColumnCache[viewName] = viewDef.String()
			createStatement.WriteString(viewDef.String())
			createStatement.WriteString(") AS SELECT ")
			createStatement.WriteString(selectColumns.String())
			createStatement.WriteString(" FROM ")
			createStatement.WriteString(fromTables.String())
			stmt, err = parser.ParseOne(createStatement.String())
			if err != nil {
				return nil, err
			}
			statements = append(statements, stmt)
		}
	}
	stmt, err = parser.ParseOne("COMMIT;")
	if err != nil {
		return nil, err
	}
	statements = append(statements, stmt)
	return statements, nil
}
