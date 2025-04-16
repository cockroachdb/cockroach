// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package logical

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/parser/statements"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree/treecmp"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

type columnSchema struct {
	column       catalog.Column
	isPrimaryKey bool
}

// getPhysicalColumns returns the list of columns that are part of the table's
// primary key and value.
func getPhysicalColumns(table catalog.TableDescriptor) []catalog.Column {
	columns := table.AllColumns()
	result := make([]catalog.Column, 0, len(columns))
	for _, col := range columns {
		if !col.IsComputed() && !col.IsVirtual() && !col.IsSystemColumn() {
			result = append(result, col)
		}
	}
	return result
}

func getPhysicalColumnsSchema(table catalog.TableDescriptor) []columnSchema {
	columns := getPhysicalColumns(table)
	primaryIdx := table.GetPrimaryIndex()

	// Create a map of column ID to column for fast lookup
	isPrimaryKey := make(map[catid.ColumnID]bool)
	for _, col := range primaryIdx.IndexDesc().KeyColumnIDs {
		isPrimaryKey[col] = true
	}

	cols := make([]columnSchema, 0, len(columns))
	for _, col := range columns {
		cols = append(cols, columnSchema{
			column:       col,
			isPrimaryKey: isPrimaryKey[col.GetID()],
		})
	}

	return cols
}

// newTypedPlaceholder creates a placeholder with the appropriate type for a column.
func newTypedPlaceholder(idx int, col catalog.Column) (*tree.CastExpr, error) {
	placeholder, err := tree.NewPlaceholder(fmt.Sprintf("%d", idx))
	if err != nil {
		return nil, err
	}
	return &tree.CastExpr{
		Expr:       placeholder,
		Type:       col.GetType(),
		SyntaxMode: tree.CastShort,
	}, nil
}

// newInsertStatement returns a statement that can be used to insert a row into
// the table.
//
// The statement will have `n` parameters, where `n` is the number of columns
// in the table. Parameters are ordered by column ID.
func newInsertStatement(
	table catalog.TableDescriptor,
) (statements.Statement[tree.Statement], error) {
	columns := getPhysicalColumns(table)

	columnNames := make(tree.NameList, len(columns))
	for i, col := range columns {
		columnNames[i] = tree.Name(col.GetName())
	}

	parameters := make(tree.Exprs, len(columnNames))
	for i, col := range columns {
		var err error
		parameters[i], err = newTypedPlaceholder(i+1, col)
		if err != nil {
			return statements.Statement[tree.Statement]{}, err
		}
	}

	parameterValues := &tree.ValuesClause{
		Rows: []tree.Exprs{
			parameters,
		},
	}

	rows := &tree.Select{
		Select: parameterValues,
	}

	insert := &tree.Insert{
		Table: &tree.TableRef{
			TableID: int64(table.GetID()),
			As:      tree.AliasClause{Alias: "replication_target"},
		},
		Rows:      rows,
		Columns:   columnNames,
		Returning: tree.AbsentReturningClause,
	}

	return toParsedStatement(insert)
}

// newMatchesLastRow creates a WHERE clause for matching all columns of a row.
// It returns a tree.Expr that compares each column to a placeholder parameter.
// Parameters are ordered by column ID, starting from startParamIdx.
func newMatchesLastRow(columns []catalog.Column, startParamIdx int) (tree.Expr, error) {
	var whereClause tree.Expr
	for i, col := range columns {
		placeholder, err := newTypedPlaceholder(startParamIdx+i, col)
		if err != nil {
			return nil, err
		}
		colExpr := &tree.ComparisonExpr{
			Operator: treecmp.MakeComparisonOperator(treecmp.IsNotDistinctFrom),
			Left:     &tree.ColumnItem{ColumnName: tree.Name(col.GetName())},
			Right:    placeholder,
		}

		if whereClause == nil {
			whereClause = colExpr
		} else {
			whereClause = &tree.AndExpr{
				Left:  whereClause,
				Right: colExpr,
			}
		}
	}
	return whereClause, nil
}

// newUpdateStatement returns a statement that can be used to update a row in
// the table. If a table has `n` columns, the statement will have `2n`
// parameters, where the first `n` parameters are the previous values of the row
// and the last `n` parameters are the new values of the row.
//
// Parameters are ordered by column ID.
func newUpdateStatement(
	table catalog.TableDescriptor,
) (statements.Statement[tree.Statement], error) {
	columns := getPhysicalColumns(table)

	// Create WHERE clause for matching the previous row values
	whereClause, err := newMatchesLastRow(columns, 1)
	if err != nil {
		return statements.Statement[tree.Statement]{}, err
	}

	exprs := make(tree.UpdateExprs, len(columns))
	for i, col := range columns {
		nameNode := tree.Name(col.GetName())
		names := tree.NameList{nameNode}

		// Create a placeholder for the new value (len(columns)+i+1) since we
		// use 1-indexed placeholders and the first len(columns) placeholders
		// are for the where clause.
		placeholder, err := newTypedPlaceholder(len(columns)+i+1, col)
		if err != nil {
			return statements.Statement[tree.Statement]{}, err
		}

		exprs[i] = &tree.UpdateExpr{
			Names: names,
			Expr:  placeholder,
		}
	}

	// Create the final update statement
	update := &tree.Update{
		Table: &tree.TableRef{
			TableID: int64(table.GetID()),
			As:      tree.AliasClause{Alias: "replication_target"},
		},
		Exprs:     exprs,
		Where:     &tree.Where{Type: tree.AstWhere, Expr: whereClause},
		Returning: tree.AbsentReturningClause,
	}

	return toParsedStatement(update)
}

// newDeleteStatement returns a statement that can be used to delete a row from
// the table. The statement will have `n` parameters, where `n` is the number of
// columns in the table. Parameters are used in the WHERE clause to precisely
// identify the row to delete.
//
// Parameters are ordered by column ID.
func newDeleteStatement(
	table catalog.TableDescriptor,
) (statements.Statement[tree.Statement], error) {
	columns := getPhysicalColumns(table)

	// Create WHERE clause for matching the row to delete
	whereClause, err := newMatchesLastRow(columns, 1)
	if err != nil {
		return statements.Statement[tree.Statement]{}, err
	}

	// Create the final delete statement
	delete := &tree.Delete{
		Table: &tree.TableRef{
			TableID: int64(table.GetID()),
			As:      tree.AliasClause{Alias: "replication_target"},
		},
		Where:     &tree.Where{Type: tree.AstWhere, Expr: whereClause},
		Returning: tree.AbsentReturningClause,
	}

	return toParsedStatement(delete)
}

// newBulkSelectStatement returns a statement that can be used to query
// multiple rows by primary key in a single operation. It uses ROWS FROM clause
// with UNNEST to handle a variable number of primary keys provided as array
// parameters.
//
// The statement will have one parameter for each primary key column, where
// each parameter is an array of values for that column. The columns are
// expected in column ID order, not primary key order.
//
// For example, given a table with primary key columns (id, secondary_id) and
// additional columns (value1, value2), the generated statement would be
// equivalent to:
//
//	SELECT
//		key_list.index,
//		replication_target.crdb_internal_origin_timestamp,
//		replication_target.crdb_internal_mvcc_timestamp,
//		replication_target.id, replication_target.secondary_id,
//		replication_target.value1, replication_target.value2
//	FROM ROWS FROM unnest($1::INT[], $2::INT[]) WITH ORDINALITY AS key_list(key1, key2, ordinality)
//	INNER JOIN LOOKUP [table_id AS replication_target]
//		ON replication_target.id = key_list.key1
//			AND replication_target.secondary_id = key_list.key2
func newBulkSelectStatement(
	table catalog.TableDescriptor,
) (statements.Statement[tree.Statement], error) {
	cols := getPhysicalColumnsSchema(table)
	primaryKeyColumns := make([]catalog.Column, 0, len(cols))
	for _, col := range cols {
		if col.isPrimaryKey {
			primaryKeyColumns = append(primaryKeyColumns, col.column)
		}
	}

	// keyListName is the name of the CTE that contains the primary keys supplied
	// via array parameters.
	keyListName, err := tree.NewUnresolvedObjectName(1, [3]string{"key_list"}, tree.NoAnnotation)
	if err != nil {
		return statements.Statement[tree.Statement]{}, err
	}

	// targetName is used to name the user's table.
	targetName, err := tree.NewUnresolvedObjectName(1, [3]string{"replication_target"}, tree.NoAnnotation)
	if err != nil {
		return statements.Statement[tree.Statement]{}, err
	}

	// Create the `SELECT unnest($1::[]INT, $2::[]INT) WITH ORDINALITY AS key_list(key1, key2, index)` table expression.
	primaryKeyExprs := make(tree.Exprs, 0, len(primaryKeyColumns))
	primaryKeyNames := make(tree.ColumnDefList, 0, len(primaryKeyColumns)+1)
	for i, pkCol := range primaryKeyColumns {
		primaryKeyNames = append(primaryKeyNames, tree.ColumnDef{
			Name: tree.Name(fmt.Sprintf("key%d", i+1)),
		})
		primaryKeyExprs = append(primaryKeyExprs, &tree.CastExpr{
			Expr:       &tree.Placeholder{Idx: tree.PlaceholderIdx(i)},
			Type:       types.MakeArray(pkCol.GetType()),
			SyntaxMode: tree.CastShort,
		})
	}
	primaryKeyNames = append(primaryKeyNames, tree.ColumnDef{
		Name: tree.Name("index"),
	})
	keyList := &tree.AliasedTableExpr{
		Expr: &tree.RowsFromExpr{
			Items: tree.Exprs{
				&tree.FuncExpr{
					Func: tree.ResolvableFunctionReference{FunctionReference: &tree.UnresolvedName{
						NumParts: 1,
						Parts:    [4]string{"unnest"},
					}},
					Exprs: primaryKeyExprs,
				},
			},
		},
		As: tree.AliasClause{
			Alias: "key_list",
			Cols:  primaryKeyNames,
		},
		Ordinality: true,
	}

	// Build the select statement for the final query.
	selectColumns := make(tree.SelectExprs, 0, 1+len(primaryKeyColumns))
	selectColumns = append(selectColumns, tree.SelectExpr{
		Expr: &tree.ColumnItem{
			ColumnName: "index",
			TableName:  keyListName,
		},
	})
	selectColumns = append(selectColumns, tree.SelectExpr{
		Expr: &tree.ColumnItem{
			ColumnName: "crdb_internal_origin_timestamp",
			TableName:  targetName,
		},
	})
	selectColumns = append(selectColumns, tree.SelectExpr{
		Expr: &tree.ColumnItem{
			ColumnName: "crdb_internal_mvcc_timestamp",
			TableName:  targetName,
		},
	})

	for _, col := range cols {
		selectColumns = append(selectColumns, tree.SelectExpr{
			Expr: &tree.ColumnItem{
				ColumnName: tree.Name(col.column.GetName()),
				TableName:  targetName,
			},
		})
	}

	// Construct the JOIN clause for the final query.
	var joinCond tree.Expr
	for i, pkCol := range primaryKeyColumns {
		colName := tree.Name(pkCol.GetName())
		keyColName := fmt.Sprintf("key%d", i+1)

		eqExpr := &tree.ComparisonExpr{
			// Use EQ operator to compare primary key columns because primary key
			// columns are guaranteed to be non-NULL. For some reason using IS NOT
			// DISTINCT FROM causes the query to be unable to use a lookup join.
			Operator: treecmp.MakeComparisonOperator(treecmp.EQ),
			Left: &tree.ColumnItem{
				TableName:  targetName,
				ColumnName: colName,
			},
			Right: &tree.ColumnItem{
				TableName:  keyListName,
				ColumnName: tree.Name(keyColName),
			},
		}

		if i == 0 {
			joinCond = eqExpr
		} else {
			joinCond = &tree.AndExpr{
				Left:  joinCond,
				Right: eqExpr,
			}
		}
	}

	// Construct the SELECT statement that is the root of the AST.
	selectStmt := &tree.Select{
		Select: &tree.SelectClause{
			Exprs: selectColumns,
			From: tree.From{
				Tables: tree.TableExprs{
					&tree.JoinTableExpr{
						JoinType: tree.AstInner,
						Left:     keyList,
						Right: &tree.TableRef{
							TableID: int64(table.GetID()),
							As:      tree.AliasClause{Alias: "replication_target"},
						},
						Cond: &tree.OnJoinCond{
							Expr: joinCond,
						},
						Hint: tree.AstLookup,
					},
				},
			},
		},
	}

	return toParsedStatement(selectStmt)
}

func toParsedStatement(stmt tree.Statement) (statements.Statement[tree.Statement], error) {
	// User Serialize instead of String to ensure the type casts use fully
	// qualified names.
	return parser.ParseOne(tree.Serialize(stmt))
}
