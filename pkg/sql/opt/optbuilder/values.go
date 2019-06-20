// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL.txt and at www.mariadb.com/bsl11.
//
// Change Date: 2022-10-01
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the Apache License, Version 2.0,
// included in the file licenses/APL.txt and at
// https://www.apache.org/licenses/LICENSE-2.0

package optbuilder

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

// buildValuesClause builds a set of memo groups that represent the given values
// clause.
//
// See Builder.buildStmt for a description of the remaining input and
// return values.
func (b *Builder) buildValuesClause(
	values *tree.ValuesClause, desiredTypes []*types.T, inScope *scope,
) (outScope *scope) {
	numRows := len(values.Rows)
	numCols := 0
	if numRows > 0 {
		numCols = len(values.Rows[0])
	}

	// We need to save and restore the previous value of the field in
	// semaCtx in case we are recursively called within a subquery
	// context.
	defer b.semaCtx.Properties.Restore(b.semaCtx.Properties)

	// Ensure there are no special functions in the clause.
	b.semaCtx.Properties.Require("VALUES", tree.RejectSpecial)
	inScope.context = "VALUES"

	// Typing a VALUES clause is not trivial; consider:
	//   VALUES (NULL), (1)
	// We want to type the entire column as INT. For this, we must find the first
	// expression that resolves to a definite type. Moreover, we want the NULL to
	// be typed correctly; so once we figure out the type, we must go back and
	// add casts as necessary.
	// To implement this, we have to break open resolveType() and do the walk and
	// TypeCheck first (storing the result), then do the ensureNullType() call
	// after figuring out the types. We do this column by column and store the
	// groups in a linearized matrix.

	// Bulk allocate ScalarListExpr slices: we need a matrix of size
	// numRows*numCols and one slice of length numRows for the tuples.
	elems := make(memo.ScalarListExpr, numRows*numCols+numRows)
	tuples, elems := elems[:numRows], elems[numRows:]

	colTypes := make([]types.T, numCols)
	// colExprs stores the typed expressions for a column (we reuse the same
	// slice).
	colExprs := make([]tree.TypedExpr, numRows)
	for colIdx := range colTypes {
		desired := types.Any
		if colIdx < len(desiredTypes) {
			desired = desiredTypes[colIdx]
		}
		colTypes[colIdx] = *types.Unknown

		for rowIdx, tuple := range values.Rows {
			if numCols != len(tuple) {
				reportValuesLenError(numCols, len(tuple))
			}

			expr := inScope.walkExprTree(tuple[colIdx])
			texpr, err := tree.TypeCheck(expr, b.semaCtx, desired)
			if err != nil {
				panic(builderError{err})
			}
			if typ := texpr.ResolvedType(); typ.Family() != types.UnknownFamily {
				if colTypes[colIdx].Family() == types.UnknownFamily {
					colTypes[colIdx] = *typ
				} else if !typ.Equivalent(&colTypes[colIdx]) {
					panic(pgerror.Newf(pgcode.DatatypeMismatch,
						"VALUES types %s and %s cannot be matched", typ, &colTypes[colIdx]))
				}
			}
			colExprs[rowIdx] = texpr
		}

		// If we still don't have a type for the column, set it to the desired type.
		if colTypes[colIdx].Family() == types.UnknownFamily && desired.Family() != types.AnyFamily {
			colTypes[colIdx] = *desired
		}

		for rowIdx := range values.Rows {
			texpr := ensureNullType(colExprs[rowIdx], &colTypes[colIdx])
			elems[rowIdx*numCols+colIdx] = b.buildScalar(texpr, inScope, nil, nil, nil)
		}
	}

	// Finally, build the tuples.
	tupleTyp := types.MakeTuple(colTypes)
	for rowIdx := range tuples {
		rowElems := elems[:numCols]
		elems = elems[numCols:]
		tuples[rowIdx] = b.factory.ConstructTuple(rowElems, tupleTyp)
	}

	outScope = inScope.push()
	for colIdx := 0; colIdx < numCols; colIdx++ {
		// The column names for VALUES are column1, column2, etc.
		alias := fmt.Sprintf("column%d", colIdx+1)
		b.synthesizeColumn(outScope, alias, &colTypes[colIdx], nil, nil /* scalar */)
	}

	colList := colsToColList(outScope.cols)
	outScope.expr = b.factory.ConstructValues(tuples, &memo.ValuesPrivate{
		Cols: colList,
		ID:   b.factory.Metadata().NextValuesID(),
	})
	return outScope
}

func reportValuesLenError(expected, actual int) {
	panic(pgerror.Newf(
		pgcode.Syntax,
		"VALUES lists must all be the same length, expected %d columns, found %d",
		expected, actual))
}
