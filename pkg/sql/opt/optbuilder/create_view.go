// Copyright 2019 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/intsets"
	"github.com/cockroachdb/errors"
)

func (b *Builder) buildCreateView(cv *tree.CreateView, inScope *scope) (outScope *scope) {
	b.DisableMemoReuse = true
	sch, resName := b.resolveSchemaForCreateTable(&cv.Name)
	schID := b.factory.Metadata().AddSchema(sch)
	viewName := tree.MakeTableNameFromPrefix(resName, tree.Name(cv.Name.Object()))

	preFuncResolver := b.semaCtx.FunctionResolver
	b.semaCtx.FunctionResolver = nil

	// We build the select statement to:
	//  - check the statement semantically,
	//  - get the fully resolved names into the AST, and
	//  - collect the view dependencies in b.schemaDeps.
	// The result is not otherwise used.
	b.insideViewDef = true
	b.trackSchemaDeps = true
	b.qualifyDataSourceNamesInAST = true
	if b.sourceViews == nil {
		b.sourceViews = make(map[string]struct{})
	}
	b.sourceViews[viewName.FQString()] = struct{}{}
	defer func() {
		b.insideViewDef = false
		b.trackSchemaDeps = false
		b.schemaDeps = nil
		b.schemaTypeDeps = intsets.Fast{}
		b.qualifyDataSourceNamesInAST = false
		delete(b.sourceViews, viewName.FQString())

		b.semaCtx.FunctionResolver = preFuncResolver
		switch recErr := recover().(type) {
		case nil:
			// No error.
		case error:
			if errors.Is(recErr, tree.ErrFunctionUndefined) {
				panic(
					errors.WithHint(
						recErr,
						"There is probably a typo in function name. Or the intention was to use a user-defined "+
							"function in the view query, which is currently not supported.",
					),
				)
			}
			panic(recErr)
		default:
			panic(recErr)
		}
	}()

	defScope := b.buildStmtAtRoot(cv.AsSource, nil /* desiredTypes */)

	p := defScope.makePhysicalProps().Presentation
	if len(cv.ColumnNames) != 0 {
		if len(p) != len(cv.ColumnNames) {
			panic(sqlerrors.NewSyntaxErrorf(
				"CREATE VIEW specifies %d column name%s, but data source has %d column%s",
				len(cv.ColumnNames), util.Pluralize(int64(len(cv.ColumnNames))),
				len(p), util.Pluralize(int64(len(p)))),
			)
		}
		// Override the columns.
		for i := range p {
			p[i].Alias = string(cv.ColumnNames[i])
		}
	}

	// If the type of any column that this view references is user
	// defined, add a type dependency between this view and the UDT.
	if b.trackSchemaDeps {
		for _, d := range b.schemaDeps {
			if !d.ColumnOrdinals.Empty() {
				d.ColumnOrdinals.ForEach(func(ord int) {
					ids, err := d.DataSource.CollectTypes(ord)
					if err != nil {
						panic(err)
					}
					for _, id := range ids {
						b.schemaTypeDeps.Add(int(id))
					}
				})
			}
		}
	}

	outScope = b.allocScope()
	outScope.expr = b.factory.ConstructCreateView(
		&memo.CreateViewPrivate{
			Schema:       schID,
			ViewName:     &viewName,
			IfNotExists:  cv.IfNotExists,
			Replace:      cv.Replace,
			Persistence:  cv.Persistence,
			Materialized: cv.Materialized,
			ViewQuery:    tree.AsStringWithFlags(cv.AsSource, tree.FmtParsable),
			Columns:      p,
			Deps:         b.schemaDeps,
			TypeDeps:     b.schemaTypeDeps,
			WithData:     cv.WithData,
		},
	)
	return outScope
}
