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
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree/treebin"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree/treecmp"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
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

var cmpOps = []treecmp.ComparisonOperatorSymbol{treecmp.EQ, treecmp.NE, treecmp.LT, treecmp.LE, treecmp.GE, treecmp.GT}

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
	op := treecmp.MakeComparisonOperator(cmpOps[rng.Intn(len(cmpOps))])
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

// randExpr produces a random expression that refers to columns in
// normalColDefs. It can be used to generate random computed columns and
// expression indexes. The return type is the type of the expression. The
// returned nullability is NotNull if all columns referenced in the expression
// have a NotNull nullability.
func randExpr(
	rng *rand.Rand, normalColDefs []*tree.ColumnTableDef, nullOk bool,
) (_ tree.Expr, _ *types.T, _ tree.Nullability, referencedCols map[tree.Name]struct{}) {
	nullability := tree.NotNull
	referencedCols = make(map[tree.Name]struct{})

	if rng.Intn(2) == 0 {
		// Try to find a set of numeric columns with the same type; the computed
		// expression will be of the form "a+b+c".
		var cols []*tree.ColumnTableDef
		var fam types.Family
		for _, idx := range rng.Perm(len(normalColDefs)) {
			x := normalColDefs[idx]
			xFam := x.Type.(*types.T).Family()

			if len(cols) == 0 {
				switch xFam {
				case types.IntFamily, types.FloatFamily, types.DecimalFamily:
					fam = xFam
					cols = append(cols, x)
				}
			} else if fam == xFam {
				cols = append(cols, x)
				if len(cols) > 1 && rng.Intn(2) == 0 {
					break
				}
			}
		}
		if len(cols) > 1 {
			// If any of the columns are nullable, set the computed column to be
			// nullable.
			for _, x := range cols {
				if x.Nullable.Nullability != tree.NotNull {
					nullability = x.Nullable.Nullability
					break
				}
			}

			var expr tree.Expr
			expr = tree.NewUnresolvedName(string(cols[0].Name))
			referencedCols[cols[0].Name] = struct{}{}
			for _, x := range cols[1:] {
				expr = &tree.BinaryExpr{
					Operator: treebin.MakeBinaryOperator(treebin.Plus),
					Left:     expr,
					Right:    tree.NewUnresolvedName(string(x.Name)),
				}
				referencedCols[x.Name] = struct{}{}
			}
			return expr, cols[0].Type.(*types.T), nullability, referencedCols
		}
	}

	// Pick a single column and create a computed column that depends on it.
	// The expression is as follows:
	//  - for numeric types (int, float, decimal), the expression is "x+1";
	//  - for string type, the expression is "lower(x)";
	//  - for types that can be cast to string in computed columns, the expression
	//    is "lower(x::string)";
	//  - otherwise, the expression is `CASE WHEN x IS NULL THEN 'foo' ELSE 'bar'`.
	x := normalColDefs[randutil.RandIntInRange(rng, 0, len(normalColDefs))]
	xTyp := x.Type.(*types.T)
	referencedCols[x.Name] = struct{}{}

	// Match the nullability with the nullability of the reference column.
	nullability = x.Nullable.Nullability
	nullOk = nullOk && nullability != tree.NotNull

	var expr tree.Expr
	var typ *types.T
	switch xTyp.Family() {
	case types.IntFamily, types.FloatFamily, types.DecimalFamily:
		typ = xTyp
		expr = &tree.BinaryExpr{
			Operator: treebin.MakeBinaryOperator(treebin.Plus),
			Left:     tree.NewUnresolvedName(string(x.Name)),
			Right:    RandDatum(rng, xTyp, nullOk),
		}

	case types.StringFamily:
		typ = types.String
		expr = &tree.FuncExpr{
			Func:  tree.WrapFunction("lower"),
			Exprs: tree.Exprs{tree.NewUnresolvedName(string(x.Name))},
		}

	default:
		volatility, ok := tree.LookupCastVolatility(xTyp, types.String, nil /* sessionData */)
		if ok && volatility <= tree.VolatilityImmutable &&
			!typeToStringCastHasIncorrectVolatility(xTyp) {
			// We can cast to string; use lower(x::string)
			typ = types.String
			expr = &tree.FuncExpr{
				Func: tree.WrapFunction("lower"),
				Exprs: tree.Exprs{
					&tree.CastExpr{
						Expr: tree.NewUnresolvedName(string(x.Name)),
						Type: types.String,
					},
				},
			}
		} else {
			// We cannot cast this type to string in a computed column expression.
			// Use CASE WHEN x IS NULL THEN 'foo' ELSE 'bar'.
			typ = types.String
			expr = &tree.CaseExpr{
				Whens: []*tree.When{
					{
						Cond: &tree.IsNullExpr{
							Expr: tree.NewUnresolvedName(string(x.Name)),
						},
						Val: RandDatum(rng, types.String, nullOk),
					},
				},
				Else: RandDatum(rng, types.String, nullOk),
			}
		}
	}

	return expr, typ, nullability, referencedCols
}

// typeToStringCastHasIncorrectVolatility returns true for a given type if the
// cast from it to STRING types has been given an incorrect volatility. For
// example, REGCLASS->STRING casts are immutable when they should be stable (see
// #74286 and #74553 for more details).
//
// Creating computed column expressions with such a cast can cause logical
// correctness bugs and internal errors. The volatilities cannot be fixed
// without causing backward incompatibility, so this function is used to prevent
// sqlsmith and TLP from repetitively finding these known volatility bugs.
func typeToStringCastHasIncorrectVolatility(t *types.T) bool {
	switch t.Family() {
	case types.DateFamily, types.EnumFamily, types.TimestampFamily,
		types.IntervalFamily, types.TupleFamily:
		return true
	case types.OidFamily:
		return t == types.RegClass || t == types.RegNamespace || t == types.RegProc ||
			t == types.RegProcedure || t == types.RegRole || t == types.RegType
	default:
		return false
	}
}
