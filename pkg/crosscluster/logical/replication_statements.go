// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package logical

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree/treecmp"
)

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

// newInsertStatement returns a statement that can be used to insert a row into
// the table.
//
// The statement will have `n` parameters, where `n` is the number of columns
// in the table. Parameters are ordered by column ID.
func newInsertStatement(table catalog.TableDescriptor) (tree.Statement, error) {
	columns := getPhysicalColumns(table)

	columnNames := make(tree.NameList, len(columns))
	for i, col := range columns {
		columnNames[i] = tree.Name(col.GetName())
	}

	parameters := make(tree.Exprs, len(columnNames))
	for i := range columnNames {
		var err error
		parameters[i], err = tree.NewPlaceholder(fmt.Sprintf("%d", i+1))
		if err != nil {
			return nil, err
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

	return insert, nil
}

// newMatchesLastRow creates a WHERE clause for matching all columns of a row.
// It returns a tree.Expr that compares each column to a placeholder parameter.
// Parameters are ordered by column ID, starting from startParamIdx.
func newMatchesLastRow(columns []catalog.Column, startParamIdx int) (tree.Expr, error) {
	var whereClause tree.Expr
	for i, col := range columns {
		placeholder, err := tree.NewPlaceholder(fmt.Sprintf("%d", startParamIdx+i))
		if err != nil {
			return nil, err
		}
		colExpr := &tree.ComparisonExpr{
			Operator: treecmp.MakeComparisonOperator(treecmp.EQ),
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
func newUpdateStatement(table catalog.TableDescriptor) (tree.Statement, error) {
	columns := getPhysicalColumns(table)

	// Create WHERE clause for matching the previous row values
	whereClause, err := newMatchesLastRow(columns, 1)
	if err != nil {
		return nil, err
	}

	exprs := make(tree.UpdateExprs, len(columns))
	for i, col := range columns {
		nameNode := tree.Name(col.GetName())
		names := tree.NameList{nameNode}

		// Create a placeholder for the new value (len(columns)+i+1) since we
		// use 1-indexed placeholders and the first len(columns) placeholders
		// are for the where clause.
		placeholder, err := tree.NewPlaceholder(fmt.Sprintf("%d", len(columns)+i+1))
		if err != nil {
			return nil, err
		}

		exprs[i] = &tree.UpdateExpr{
			Names: names,
			Expr: &tree.CastExpr{
				Expr:       placeholder,
				Type:       col.GetType(),
				SyntaxMode: tree.CastPrepend,
			},
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

	return update, nil
}

// newDeleteStatement returns a statement that can be used to delete a row from
// the table. The statement will have `n` parameters, where `n` is the number of
// columns in the table. Parameters are used in the WHERE clause to precisely
// identify the row to delete.
//
// Parameters are ordered by column ID.
func newDeleteStatement(table catalog.TableDescriptor) (tree.Statement, error) {
	columns := getPhysicalColumns(table)

	// Create WHERE clause for matching the row to delete
	whereClause, err := newMatchesLastRow(columns, 1)
	if err != nil {
		return nil, err
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

	return delete, nil
}
