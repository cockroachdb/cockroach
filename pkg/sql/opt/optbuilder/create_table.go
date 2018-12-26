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
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util"
)

// buildCreateTable constructs a CreateTable operator based on the CREATE TABLE
// statement.
func (b *Builder) buildCreateTable(ct *tree.CreateTable, inScope *scope) (outScope *scope) {
	sch := b.resolveSchemaForCreate(&ct.Table)
	schID := b.factory.Metadata().AddSchema(sch)

	// HoistConstraints normalizes any column constraints in the CreateTable AST
	// node.
	ct.HoistConstraints()

	var input memo.RelExpr
	var inputCols opt.ColList
	if ct.As() {
		// Build the input query.
		outScope := b.buildSelect(ct.AsSource, nil /* desiredTypes */, inScope)

		numColNames := len(ct.AsColumnNames)
		numColumns := len(outScope.cols)
		if numColNames != 0 && numColNames != numColumns {
			panic(builderError{sqlbase.NewSyntaxError(fmt.Sprintf(
				"CREATE TABLE specifies %d column name%s, but data source has %d column%s",
				numColNames, util.Pluralize(int64(numColNames)),
				numColumns, util.Pluralize(int64(numColumns))))})
		}

		// Synthesize rowid column, and append to end of column list.
		props, overloads := builtins.GetBuiltinProperties("unique_rowid")
		private := &memo.FunctionPrivate{
			Name:       "unique_rowid",
			Typ:        types.Int,
			Properties: props,
			Overload:   &overloads[0],
		}
		fn := b.factory.ConstructFunction(memo.EmptyScalarListExpr, private)
		scopeCol := b.synthesizeColumn(outScope, "rowid", types.Int, nil /* expr */, fn)
		input = b.factory.CustomFuncs().ProjectExtraCol(outScope.expr, fn, scopeCol.id)
		inputCols = colsToColList(outScope.cols)
	} else {
		// Create dummy empty input.
		input = b.factory.ConstructZeroValues()
	}

	expr := b.factory.ConstructCreateTable(
		input,
		&memo.CreateTablePrivate{
			Schema:    schID,
			InputCols: inputCols,
			Syntax:    ct,
		},
	)
	return &scope{builder: b, expr: expr}
}
