// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package optbuilder

import (
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
)

// buildZip builds a set of memo groups which represent a functional zip over
// the given expressions.
//
// Reminder, for context: the functional zip over iterators a,b,c
// returns tuples of values from a,b,c picked "simultaneously". NULLs
// are used when an iterator is "shorter" than another. For example:
//
//    zip([1,2,3], ['a','b']) = [(1,'a'), (2,'b'), (3, null)]
//
func (b *Builder) buildZip(exprs tree.Exprs, inScope *scope) (outScope *scope) {
	outScope = inScope.push()

	// Build each of the provided expressions.
	elems := make([]memo.GroupID, len(exprs))
	for i, expr := range exprs {
		b.assertNoAggregationOrWindowing(expr, "FROM")

		// Output column names should exactly match the original expression, so we
		// have to determine the output column name before we perform type
		// checking.
		_, label, err := tree.ComputeColNameInternal(b.semaCtx.SearchPath, expr)
		if err != nil {
			panic(builderError{err})
		}

		// Set the flag to allow generator functions. It needs to be reset for each
		// expression since it is set to false once an SRF is seen to disallow
		// nested SRFs.
		// TODO(rytaft): This is a temporary solution and will need to change once
		// we support SRFs in the SELECT list.
		inScope.allowGeneratorFunc = true

		texpr := inScope.resolveType(expr, types.Any)
		elems[i] = b.buildScalarHelper(texpr, label, inScope, outScope)
	}

	// Get the output columns of the Zip operation and construct the Zip.
	colList := make(opt.ColList, len(outScope.cols))
	for i := 0; i < len(colList); i++ {
		colList[i] = outScope.cols[i].id
	}
	outScope.group = b.factory.ConstructZip(
		b.factory.InternList(elems), b.factory.InternColList(colList),
	)

	return outScope
}

// finishBuildGeneratorFunction finishes building a set-generating function
// (SRF) such as generate_series() or unnest(). It synthesizes new columns in
// outScope for each of the SRF's output columns.
func (b *Builder) finishBuildGeneratorFunction(
	f *tree.FuncExpr, group memo.GroupID, inScope, outScope *scope,
) (out memo.GroupID) {
	typ := f.ResolvedType().(types.TTuple)

	// Add scope columns. Use the tuple labels in the SRF's return type as column
	// labels.
	for i := range typ.Types {
		b.synthesizeColumn(outScope, typ.Labels[i], typ.Types[i], nil, group)
	}

	return group
}
