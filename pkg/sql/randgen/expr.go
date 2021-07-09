// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package randgen

import (
	"math/rand"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

// randPartialIndexPredicateFromCols creates a partial index expression with a
// random subset of the given columns. There is a possibility that a partial
// index expression cannot be created for the given columns, in which case nil
// is returned. This happens when none of the columns have types that are
// supported for creating partial index predicate expressions. See
// isAllowedPartialIndexColType for details on which types are supported.
func randPartialIndexPredicateFromCols(
	rng *rand.Rand, columnTableDefs []*tree.ColumnTableDef, tableName *tree.TableName,
) tree.Expr {
	// Shuffle the columns.
	cpy := make([]*tree.ColumnTableDef, len(columnTableDefs))
	copy(cpy, columnTableDefs)
	rng.Shuffle(len(cpy), func(i, j int) { cpy[i], cpy[j] = cpy[j], cpy[i] })

	// Select a random number of columns (at least 1). Loop through the columns
	// to find columns with types that are currently supported for generating
	// partial index expressions.
	nCols := rng.Intn(len(cpy)) + 1
	cols := make([]*tree.ColumnTableDef, 0, nCols)
	for _, col := range cpy {
		if isAllowedPartialIndexColType(col) {
			cols = append(cols, col)
		}
		if len(cols) == nCols {
			break
		}
	}

	// Build a boolean expression tree with containing a reference to each
	// column.
	var e tree.Expr
	for _, columnTableDef := range cols {
		expr := randBoolColumnExpr(rng, columnTableDef, tableName)
		// If an expression has already been built, combine the previous and
		// current expression with an AndExpr or OrExpr.
		if e != nil {
			expr = randAndOrExpr(rng, e, expr)
		}
		e = expr
	}
	return e
}

// isAllowedPartialIndexColType returns true if the column type is supported and
// the column can be included in generating random partial index predicate
// expressions. Currently, the following types are supported:
//
//   - Booleans
//   - Types that are valid in comparison operations (=, !=, <, <=, >, >=).
//
// This function must be kept in sync with the implementation of
// randBoolColumnExpr.
func isAllowedPartialIndexColType(columnTableDef *tree.ColumnTableDef) bool {
	switch fam := columnTableDef.Type.(*types.T).Family(); fam {
	case types.BoolFamily, types.IntFamily, types.FloatFamily, types.DecimalFamily,
		types.StringFamily, types.DateFamily, types.TimeFamily, types.TimeTZFamily,
		types.TimestampFamily, types.TimestampTZFamily, types.BytesFamily:
		return true
	default:
		return false
	}
}

var cmpOps = []tree.ComparisonOperatorSymbol{tree.EQ, tree.NE, tree.LT, tree.LE, tree.GE, tree.GT}

// randBoolColumnExpr returns a random boolean expression with the given column.
func randBoolColumnExpr(
	rng *rand.Rand, columnTableDef *tree.ColumnTableDef, tableName *tree.TableName,
) tree.Expr {
	varExpr := tree.NewColumnItem(tableName, columnTableDef.Name)
	t := columnTableDef.Type.(*types.T)

	// If the column is a boolean, then return it or NOT it as an expression.
	if t.Family() == types.BoolFamily {
		if rng.Intn(2) == 0 {
			return &tree.NotExpr{Expr: varExpr}
		}
		return varExpr
	}

	// Otherwise, return a comparison expression with a random comparison
	// operator, the column as the left side, and an interesting datum as the
	// right side.
	op := tree.MakeComparisonOperator(cmpOps[rng.Intn(len(cmpOps))])
	datum := randInterestingDatum(rng, t)
	return &tree.ComparisonExpr{Operator: op, Left: varExpr, Right: datum}
}

// randAndOrExpr combines the left and right expressions with either an OrExpr
// or an AndExpr.
func randAndOrExpr(rng *rand.Rand, left, right tree.Expr) tree.Expr {
	if rng.Intn(2) == 0 {
		return &tree.OrExpr{
			Left:  left,
			Right: right,
		}
	}

	return &tree.AndExpr{
		Left:  left,
		Right: right,
	}
}
