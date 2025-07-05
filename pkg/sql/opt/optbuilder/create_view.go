// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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

	isTemp := resolveTemporaryStatus(cv.Name.ObjectNamePrefix, cv.Persistence)
	if isTemp {
		// Postgres allows using `pg_temp` as an alias for the session specific temp
		// schema. In PG, the following are equivalent:
		// CREATE TEMP TABLE t <=> CREATE TABLE pg_temp.t <=> CREATE TEMP TABLE pg_temp.t
		//
		// The temporary schema is created the first time a session creates a
		// temporary object, so it is possible to use `pg_temp` in a fully qualified
		// name when the temporary schema does not exist. To allow the name to be
		// resolved, we unset the explicitly named schema and set the Persistence to
		// temporary.
		cv.Name.ObjectNamePrefix.SchemaName = ""
		cv.Name.ObjectNamePrefix.ExplicitSchema = false
		cv.Persistence = tree.PersistenceTemporary
	}

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

	viewName := &cv.Name
	sch, resName := b.resolveSchemaForCreateTable(viewName)
	viewName.ObjectNamePrefix = resName
	schID := b.factory.Metadata().AddSchema(sch)

	viewFQString := viewName.FQString()
	b.sourceViews[viewFQString] = struct{}{}
	defer func() {
		b.insideViewDef = false
		b.trackSchemaDeps = false
		b.schemaDeps = nil
		b.schemaTypeDeps = intsets.Fast{}
		b.qualifyDataSourceNamesInAST = false
		delete(b.sourceViews, viewFQString)

		switch recErr := recover().(type) {
		case nil:
			// No error.
		case error:
			if errors.Is(recErr, tree.ErrRoutineUndefined) {
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

	// We need the view query to include user-defined types as a 3-part name to
	// properly detect cross-database type access.
	fmtFlags := tree.FmtParsable | tree.FmtAlwaysQualifyUserDefinedTypeNames
	if cv.Materialized {
		// Don't include any AS OF SYSTEM TIME clauses here: our materialized view
		// shouldn't get refreshed as of a particular time.
		fmtFlags = fmtFlags | tree.FmtSkipAsOfSystemTimeClauses
	}
	outScope = b.allocScope()
	outScope.expr = b.factory.ConstructCreateView(
		&memo.CreateViewPrivate{
			Syntax:    cv,
			Schema:    schID,
			ViewQuery: tree.AsStringWithFlags(cv.AsSource, fmtFlags),
			Columns:   p,
			Deps:      b.schemaDeps,
			TypeDeps:  b.schemaTypeDeps,
			FuncDeps:  b.schemaFunctionDeps,
		},
	)
	return outScope
}
