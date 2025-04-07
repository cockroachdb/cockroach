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

func getReplicatedColumns(table catalog.TableDescriptor) []columnSchema {
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

// newBulkSelectStatement returns a statement that can be used to query multiple rows
// by primary key in a single operation. It uses a CTE with UNNEST to handle a variable
// number of primary keys provided as array parameters.
//
// The statement will have one parameter that is the index of each input row and
// one parameter for each primary key column, where each parameter is an array
// of values for that column. The columns are expected in column ID order, not
// primary key order.
//
// For example, given a table with primary key columns (id, secondary_id) and additional
// columns (value1, value2), the generated statement would be equivalent to:
//
//	WITH key_list AS (
//	  SELECT unnest($1::INT[]) as index, unnest($2::INT[]) as key1, unnest($3::INT[]) as key2
//	)
//	SELECT key_list.index,
//	       replication_target.id, replication_target.secondary_id,
//	       replication_target.value1, replication_target.value2
//	FROM table_id AS replication_target
//	INNER JOIN key_list ON replication_target.id = key_list.key1
//	                    AND replication_target.secondary_id = key_list.key2
func newBulkSelectStatement(
	table catalog.TableDescriptor,
) (statements.Statement[tree.Statement], error) {
	cols := getReplicatedColumns(table)
	primaryKeyColumns := make([]catalog.Column, 0, len(cols))
	for _, col := range cols {
		if col.isPrimaryKey {
			primaryKeyColumns = append(primaryKeyColumns, col.column)
		}
	}

	targetName, err := tree.NewUnresolvedObjectName(1, [3]string{"replication_target"}, tree.NoAnnotation)
	if err != nil {
		return statements.Statement[tree.Statement]{}, err
	}
	keyListName, err := tree.NewUnresolvedObjectName(1, [3]string{"key_list"}, tree.NoAnnotation)
	if err != nil {
		return statements.Statement[tree.Statement]{}, err
	}

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

	var joinCond tree.Expr
	for i, pkCol := range primaryKeyColumns {
		colName := tree.Name(pkCol.GetName())
		keyColName := fmt.Sprintf("key%d", i+1)

		// Create equality comparison for the join
		eqExpr := &tree.ComparisonExpr{
			Operator: treecmp.MakeComparisonOperator(treecmp.IsNotDistinctFrom),
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

	// Create the table reference
	tableRef := &tree.TableRef{
		TableID: int64(table.GetID()),
		As:      tree.AliasClause{Alias: "replication_target"},
	}

	// Create the CTE
	unnestExpr := &tree.UnresolvedName{
		NumParts: 1,
		Parts:    [4]string{"unnest"},
	}

	// Create the unnest function call with named columns
	cteSelectExprs := make(tree.SelectExprs, 0, 1+len(primaryKeyColumns))

	// Add index column
	cteSelectExprs = append(cteSelectExprs, tree.SelectExpr{
		Expr: &tree.FuncExpr{
			Func: tree.ResolvableFunctionReference{FunctionReference: unnestExpr},
			Exprs: tree.Exprs{&tree.CastExpr{
				Expr:       &tree.Placeholder{Idx: 0},
				Type:       types.MakeArray(types.Int),
				SyntaxMode: tree.CastShort,
			}},
		},
		As: "index",
	})

	// Add key columns
	for i, pkCol := range primaryKeyColumns {
		cteSelectExprs = append(cteSelectExprs, tree.SelectExpr{
			Expr: &tree.FuncExpr{
				Func: tree.ResolvableFunctionReference{FunctionReference: unnestExpr},
				Exprs: tree.Exprs{&tree.CastExpr{
					Expr:       &tree.Placeholder{Idx: tree.PlaceholderIdx(i + 1)},
					Type:       types.MakeArray(pkCol.GetType()),
					SyntaxMode: tree.CastShort,
				}},
			},
			As: tree.UnrestrictedName(fmt.Sprintf("key%d", i+1)),
		})
	}

	// Create the main SELECT statement with the CTE
	selectStmt := &tree.Select{
		Select: &tree.SelectClause{
			Exprs: selectColumns,
			From: tree.From{
				Tables: tree.TableExprs{
					&tree.JoinTableExpr{
						JoinType: tree.AstInner,
						Left:     tableRef,
						Right: &tree.AliasedTableExpr{
							Expr: tree.NewUnqualifiedTableName(tree.Name("key_list")),
						},
						Cond: &tree.OnJoinCond{
							Expr: joinCond,
						},
					},
				},
			},
		},
		With: &tree.With{
			CTEList: []*tree.CTE{
				{
					Name: tree.AliasClause{Alias: "key_list"},
					Stmt: &tree.Select{
						Select: &tree.SelectClause{
							Exprs: cteSelectExprs,
						},
					},
				},
			},
		},
	}

	return toParsedStatement(selectStmt)
}

func toParsedStatement(stmt tree.Statement) (statements.Statement[tree.Statement], error) {
	// TODO(jeffswenson): do I have to round trip through the string or can I
	// safely construct the statement directly?
	return parser.ParseOne(stmt.String())
}
