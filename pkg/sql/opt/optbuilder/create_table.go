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
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props/physical"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util"
)

// buildCreateTable constructs a CreateTable operator based on the CREATE TABLE
// statement.
func (b *Builder) buildCreateTable(ct *tree.CreateTable, inScope *scope) (outScope *scope) {
	b.DisableMemoReuse = true
	isTemp := resolveTemporaryStatus(&ct.Table, ct.Persistence)
	if isTemp {
		// Postgres allows using `pg_temp` as an alias for the session specific temp
		// schema. In PG, the following are equivalent:
		// CREATE TEMP TABLE t <=> CREATE TABLE pg_temp.t <=> CREATE TEMP TABLE pg_temp.t
		//
		// The temporary schema is created the first time a session creates
		// a temporary object, so it is possible to use `pg_temp` in a fully
		// qualified name when the temporary schema does not exist. To allow this,
		// we explicitly set the SchemaName to `public` for temporary tables, as
		// the public schema is guaranteed to exist. This ensures the FQN can be
		// resolved correctly.
		// TODO(solon): Once it is possible to drop schemas, it will no longer be
		// safe to set the schema name to `public`, as it may have been dropped.
		ct.Table.ObjectNamePrefix.SchemaName = tree.PublicSchemaName
		ct.Persistence = tree.PersistenceTemporary
	}
	sch, resName := b.resolveSchemaForCreate(&ct.Table)
	ct.Table.ObjectNamePrefix = resName
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
		outScope = b.buildStmtAtRoot(ct.AsSource, nil /* desiredTypes */)

		numColNames := 0
		for i := 0; i < len(ct.Defs); i++ {
			if _, ok := ct.Defs[i].(*tree.ColumnTableDef); ok {
				numColNames++
			}
		}
		numColumns := len(outScope.cols)
		if numColNames != 0 && numColNames != numColumns {
			panic(sqlerrors.NewSyntaxErrorf(
				"CREATE TABLE specifies %d column name%s, but data source has %d column%s",
				numColNames, util.Pluralize(int64(numColNames)),
				numColumns, util.Pluralize(int64(numColumns))))
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
			scopeCol := b.synthesizeColumn(outScope, scopeColName("rowid"), types.Int, nil /* expr */, fn)
			input = b.factory.CustomFuncs().ProjectExtraCol(outScope.expr, fn, scopeCol.id)
		}
		inputCols = outScope.makePhysicalProps().Presentation
	} else {
		// Create dummy empty input.
		input = b.factory.ConstructZeroValues()
	}

	outScope = b.allocScope()
	outScope.expr = b.factory.ConstructCreateTable(
		input,
		&memo.CreateTablePrivate{
			Schema:    schID,
			InputCols: inputCols,
			Syntax:    ct,
		},
	)
	return outScope
}
