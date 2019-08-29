// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package optbuilder

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props/physical"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util"
)

// buildCreateTable constructs a CreateTable operator based on the CREATE TABLE
// statement.
func (b *Builder) buildCreateTable(ct *tree.CreateTable, inScope *scope) (outScope *scope) {
	b.DisableMemoReuse = true
	sch, resName := b.resolveSchemaForCreate(&ct.Table)
	// TODO(radu): we are modifying the AST in-place here. We should be storing
	// the resolved name separately.
	ct.Table.TableNamePrefix = resName
	schID := b.factory.Metadata().AddSchema(sch)

	// HoistConstraints normalizes any column constraints in the CreateTable AST
	// node.
	ct.HoistConstraints()

	var input memo.RelExpr
	var inputCols physical.Presentation
	if ct.As() {
		// The execution code might need to stringify the query to run it
		// asynchronously. For that we need the data sources to be fully qualified.
		// TODO(radu): this interaction is pretty hacky, investigate moving the
		// generation of the string to the optimizer.
		b.qualifyDataSourceNamesInAST = true
		defer func() {
			b.qualifyDataSourceNamesInAST = false
		}()

		// Build the input query.
		outScope := b.buildSelect(ct.AsSource, nil /* desiredTypes */, inScope)

		numColNames := 0
		for i := 0; i < len(ct.Defs); i++ {
			if _, ok := ct.Defs[i].(*tree.ColumnTableDef); ok {
				numColNames++
			}
		}
		numColumns := len(outScope.cols)
		if numColNames != 0 && numColNames != numColumns {
			panic(sqlbase.NewSyntaxError(fmt.Sprintf(
				"CREATE TABLE specifies %d column name%s, but data source has %d column%s",
				numColNames, util.Pluralize(int64(numColNames)),
				numColumns, util.Pluralize(int64(numColumns)))))
		}

		input = outScope.expr
		if !ct.AsHasUserSpecifiedPrimaryKey() {
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
		}
		inputCols = outScope.makePhysicalProps().Presentation
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
