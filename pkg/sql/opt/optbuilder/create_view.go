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
)

func (b *Builder) buildCreateView(cv *tree.CreateView, inScope *scope) (outScope *scope) {
	b.DisableMemoReuse = true
	sch, resName := b.resolveSchemaForCreate(&cv.Name)
	schID := b.factory.Metadata().AddSchema(sch)
	viewName := tree.MakeTableNameFromPrefix(resName, tree.Name(cv.Name.Object()))

	// We build the select statement to:
	//  - check the statement semantically,
	//  - get the fully resolved names into the AST, and
	//  - collect the view dependencies in b.viewDeps.
	// The result is not otherwise used.
	b.insideViewDef = true
	b.trackViewDeps = true
	b.qualifyDataSourceNamesInAST = true
	defer func() {
		b.insideViewDef = false
		b.trackViewDeps = false
		b.viewDeps = nil
		b.viewTypeDeps = util.FastIntSet{}
		b.qualifyDataSourceNamesInAST = false
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
	if b.trackViewDeps {
		for _, d := range b.viewDeps {
			if !d.ColumnOrdinals.Empty() {
				d.ColumnOrdinals.ForEach(func(ord int) {
					ids, err := d.DataSource.CollectTypes(ord)
					if err != nil {
						panic(err)
					}
					for _, id := range ids {
						b.viewTypeDeps.Add(int(id))
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
			Deps:         b.viewDeps,
			TypeDeps:     b.viewTypeDeps,
		},
	)
	return outScope
}
