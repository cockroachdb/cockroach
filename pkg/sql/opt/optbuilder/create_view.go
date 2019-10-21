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
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util"
)

func (b *Builder) buildCreateView(cv *tree.CreateView, inScope *scope) (outScope *scope) {
	b.DisableMemoReuse = true
	sch, _ := b.resolveSchemaForCreate(&cv.Name)
	schID := b.factory.Metadata().AddSchema(sch)

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
		b.qualifyDataSourceNamesInAST = false
	}()

	defScope := b.buildSelect(cv.AsSource, nil /* desiredTypes */, inScope)

	p := defScope.makePhysicalProps().Presentation
	if len(cv.ColumnNames) != 0 {
		if len(p) != len(cv.ColumnNames) {
			panic(sqlbase.NewSyntaxError(fmt.Sprintf(
				"CREATE VIEW specifies %d column name%s, but data source has %d column%s",
				len(cv.ColumnNames), util.Pluralize(int64(len(cv.ColumnNames))),
				len(p), util.Pluralize(int64(len(p)))),
			))
		}
		// Override the columns.
		for i := range p {
			p[i].Alias = string(cv.ColumnNames[i])
		}
	}

	expr := b.factory.ConstructCreateView(
		&memo.CreateViewPrivate{
			Schema:    schID,
			ViewName:  cv.Name.Table(),
			Temporary: cv.Temporary,
			ViewQuery: tree.AsStringWithFlags(cv.AsSource, tree.FmtParsable),
			Columns:   p,
			Deps:      b.viewDeps,
		},
	)
	return &scope{builder: b, expr: expr}
}
