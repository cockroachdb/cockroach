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
	var numCols int
	if len(values.Rows) > 0 {
		numCols = len(values.Rows[0])
	}

	colTypes := make([]types.T, numCols)
	for i := range colTypes {
		colTypes[i] = *types.Unknown
	}
	rows := make(memo.ScalarListExpr, 0, len(values.Rows))

	// We need to save and restore the previous value of the field in
	// semaCtx in case we are recursively called within a subquery
	// context.
	defer b.semaCtx.Properties.Restore(b.semaCtx.Properties)

	// Ensure there are no special functions in the clause.
	b.semaCtx.Properties.Require("VALUES", tree.RejectSpecial)
	inScope.context = "VALUES"

	for _, tuple := range values.Rows {
		if numCols != len(tuple) {
			reportValuesLenError(numCols, len(tuple))
		}

		elems := make(memo.ScalarListExpr, numCols)
		for i, expr := range tuple {
			desired := types.Any
			if i < len(desiredTypes) {
				desired = desiredTypes[i]
			}

			texpr := inScope.resolveType(expr, desired)
			typ := texpr.ResolvedType()
			elems[i] = b.buildScalar(texpr, inScope, nil, nil, nil)

			// Verify that types of each tuple match one another.
			if colTypes[i].Family() == types.UnknownFamily {
				colTypes[i] = *typ
			} else if typ.Family() != types.UnknownFamily && !typ.Equivalent(&colTypes[i]) {
				panic(pgerror.Newf(pgcode.DatatypeMismatch,
					"VALUES types %s and %s cannot be matched", typ, &colTypes[i]))
			}
		}

		tupleTyp := types.MakeTuple(colTypes)
		rows = append(rows, b.factory.ConstructTuple(elems, tupleTyp))
	}

	outScope = inScope.push()
	for i := 0; i < numCols; i++ {
		// The column names for VALUES are column1, column2, etc.
		alias := fmt.Sprintf("column%d", i+1)
		b.synthesizeColumn(outScope, alias, &colTypes[i], nil, nil /* scalar */)
	}

	colList := colsToColList(outScope.cols)
	outScope.expr = b.factory.ConstructValues(rows, &memo.ValuesPrivate{
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
