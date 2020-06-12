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
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/delegate"
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/norm"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/optgen/exprgen"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/errors"
)

// Builder holds the context needed for building a memo structure from a SQL
// statement. Builder.Build() is the top-level function to perform this build
// process. As part of the build process, it performs name resolution and
// type checking on the expressions within Builder.stmt.
//
// The memo structure is the primary data structure used for query optimization,
// so building the memo is the first step required to optimize a query. The memo
// is maintained inside Builder.factory, which exposes methods to construct
// expression groups inside the memo. Once the expression tree has been built,
// the builder calls SetRoot on the memo to indicate the root memo group, as
// well as the set of physical properties (e.g., row and column ordering) that
// at least one expression in the root group must satisfy.
//
// A memo is essentially a compact representation of a forest of logically-
// equivalent query trees. Each tree is either a logical or a physical plan
// for executing the SQL query. After the build process is complete, the memo
// forest will contain exactly one tree: the logical query plan corresponding
// to the AST of the original SQL statement with some number of "normalization"
// transformations applied. Normalization transformations include heuristics
// such as predicate push-down that should always be applied. They do not
// include "exploration" transformations whose benefit must be evaluated with
// the optimizer's cost model (e.g., join reordering).
//
// See factory.go and memo.go inside the opt/xform package for more details
// about the memo structure.
type Builder struct {

	// -- Control knobs --
	//
	// These fields can be set before calling Build to control various aspects of
	// the building process.

	// KeepPlaceholders is a control knob: if set, optbuilder will never replace
	// a placeholder operator with its assigned value, even when it is available.
	// This is used when re-preparing invalidated queries.
	KeepPlaceholders bool

	// -- Results --
	//
	// These fields are set during the building process and can be used after
	// Build is called.

	// HadPlaceholders is set to true if we replaced any placeholders with their
	// values.
	HadPlaceholders bool

	// DisableMemoReuse is set to true if we encountered a statement that is not
	// safe to cache the memo for. This is the case for various DDL and SHOW
	// statements.
	DisableMemoReuse bool

	factory *norm.Factory
	stmt    tree.Statement

	ctx        context.Context
	semaCtx    *tree.SemaContext
	evalCtx    *tree.EvalContext
	catalog    cat.Catalog
	scopeAlloc []scope
	cteStack   [][]cteSource

	// If set, the planner will skip checking for the SELECT privilege when
	// resolving data sources (tables, views, etc). This is used when compiling
	// views and the view SELECT privilege has already been checked. This should
	// be used with care.
	skipSelectPrivilegeChecks bool

	// views contains a cache of views that have already been parsed, in case they
	// are referenced multiple times in the same query.
	views map[cat.View]*tree.Select

	// subquery contains a pointer to the subquery which is currently being built
	// (if any).
	subquery *subquery

	// If set, we are processing a view definition; in this case, catalog caches
	// are disabled and certain statements (like mutations) are disallowed.
	insideViewDef bool

	// If set, we are collecting view dependencies in viewDeps. This can only
	// happen inside view definitions.
	//
	// When a view depends on another view, we only want to track the dependency
	// on the inner view itself, and not the transitive dependencies (so
	// trackViewDeps would be false inside that inner view).
	trackViewDeps bool
	viewDeps      opt.ViewDeps

	// If set, the data source names in the AST are rewritten to the fully
	// qualified version (after resolution). Used to construct the strings for
	// CREATE VIEW and CREATE TABLE AS queries.
	// TODO(radu): modifying the AST in-place is hacky; we will need to switch to
	// using AST annotations.
	qualifyDataSourceNamesInAST bool

	// isCorrelated is set to true if we already reported to telemetry that the
	// query contains a correlated subquery.
	isCorrelated bool
}

// New creates a new Builder structure initialized with the given
// parsed SQL statement.
func New(
	ctx context.Context,
	semaCtx *tree.SemaContext,
	evalCtx *tree.EvalContext,
	catalog cat.Catalog,
	factory *norm.Factory,
	stmt tree.Statement,
) *Builder {
	return &Builder{
		factory: factory,
		stmt:    stmt,
		ctx:     ctx,
		semaCtx: semaCtx,
		evalCtx: evalCtx,
		catalog: catalog,
	}
}

// Build is the top-level function to build the memo structure inside
// Builder.factory from the parsed SQL statement in Builder.stmt. See the
// comment above the Builder type declaration for details.
//
// If any subroutines panic with a non-runtime error as part of the build
// process, the panic is caught here and returned as an error.
func (b *Builder) Build() (err error) {
	defer func() {
		if r := recover(); r != nil {
			// This code allows us to propagate errors without adding lots of checks
			// for `if err != nil` throughout the construction code. This is only
			// possible because the code does not update shared state and does not
			// manipulate locks.
			if ok, e := errorutil.ShouldCatch(r); ok {
				err = e
			} else {
				panic(r)
			}
		}
	}()

	// Special case for CannedOptPlan.
	if canned, ok := b.stmt.(*tree.CannedOptPlan); ok {
		b.factory.DisableOptimizations()
		_, err := exprgen.Build(b.catalog, b.factory, canned.Plan)
		return err
	}

	b.pushWithFrame()

	// Build the memo, and call SetRoot on the memo to indicate the root group
	// and physical properties.
	outScope := b.buildStmtAtRoot(b.stmt, nil /* desiredTypes */, b.allocScope())

	b.popWithFrame(outScope)
	if len(b.cteStack) > 0 {
		panic(errors.AssertionFailedf("dangling CTE stack frames"))
	}

	physical := outScope.makePhysicalProps()
	b.factory.Memo().SetRoot(outScope.expr, physical)
	return nil
}

// unimplementedWithIssueDetailf formats according to a format
// specifier and returns a Postgres error with the
// pg code FeatureNotSupported.
func unimplementedWithIssueDetailf(issue int, detail, format string, args ...interface{}) error {
	return unimplemented.NewWithIssueDetailf(issue, detail, format, args...)
}

// buildStmtAtRoot builds a statement, beginning a new conceptual query
// "context".
func (b *Builder) buildStmtAtRoot(
	stmt tree.Statement, desiredTypes []*types.T, inScope *scope,
) (outScope *scope) {
	defer func(prevAtRoot bool) {
		inScope.atRoot = prevAtRoot
	}(inScope.atRoot)
	inScope.atRoot = true

	return b.buildStmt(stmt, desiredTypes, inScope)
}

// buildStmt builds a set of memo groups that represent the given SQL
// statement.
//
// NOTE: The following descriptions of the inScope parameter and outScope
//       return value apply for all buildXXX() functions in this directory.
//       Note that some buildXXX() functions pass outScope as a parameter
//       rather than a return value so its scopeColumns can be built up
//       incrementally across several function calls.
//
// inScope   This parameter contains the name bindings that are visible for this
//           statement/expression (e.g., passed in from an enclosing statement).
//
// outScope  This return value contains the newly bound variables that will be
//           visible to enclosing statements, as well as a pointer to any
//           "parent" scope that is still visible. The top-level memo expression
//           for the built statement/expression is returned in outScope.expr.
func (b *Builder) buildStmt(
	stmt tree.Statement, desiredTypes []*types.T, inScope *scope,
) (outScope *scope) {
	if b.insideViewDef {
		// A blocklist of statements that can't be used from inside a view.
		switch stmt := stmt.(type) {
		case *tree.Delete, *tree.Insert, *tree.Update, *tree.CreateTable, *tree.CreateView,
			*tree.Split, *tree.Unsplit, *tree.Relocate,
			*tree.ControlJobs, *tree.CancelQueries, *tree.CancelSessions:
			panic(pgerror.Newf(
				pgcode.Syntax, "%s cannot be used inside a view definition", stmt.StatementTag(),
			))
		}
	}

	switch stmt := stmt.(type) {
	case *tree.Select:
		return b.buildSelect(stmt, noRowLocking, desiredTypes, inScope)

	case *tree.ParenSelect:
		return b.buildSelect(stmt.Select, noRowLocking, desiredTypes, inScope)

	case *tree.Delete:
		return b.processWiths(stmt.With, inScope, func(inScope *scope) *scope {
			return b.buildDelete(stmt, inScope)
		})

	case *tree.Insert:
		return b.processWiths(stmt.With, inScope, func(inScope *scope) *scope {
			return b.buildInsert(stmt, inScope)
		})

	case *tree.Update:
		return b.processWiths(stmt.With, inScope, func(inScope *scope) *scope {
			return b.buildUpdate(stmt, inScope)
		})

	case *tree.CreateTable:
		return b.buildCreateTable(stmt, inScope)

	case *tree.CreateView:
		return b.buildCreateView(stmt, inScope)

	case *tree.Explain:
		return b.buildExplain(stmt, inScope)

	case *tree.ShowTraceForSession:
		return b.buildShowTrace(stmt, inScope)

	case *tree.Split:
		return b.buildAlterTableSplit(stmt, inScope)

	case *tree.Unsplit:
		return b.buildAlterTableUnsplit(stmt, inScope)

	case *tree.Relocate:
		return b.buildAlterTableRelocate(stmt, inScope)

	case *tree.ControlJobs:
		return b.buildControlJobs(stmt, inScope)

	case *tree.CancelQueries:
		return b.buildCancelQueries(stmt, inScope)

	case *tree.CancelSessions:
		return b.buildCancelSessions(stmt, inScope)

	case *tree.Export:
		return b.buildExport(stmt, inScope)

	case *tree.ExplainAnalyzeDebug:
		// This statement should have been handled by the executor.
		panic(errors.Errorf("%s can only be used as a top-level statement", stmt.StatementTag()))

	default:
		// See if this statement can be rewritten to another statement using the
		// delegate functionality.
		newStmt, err := delegate.TryDelegate(b.ctx, b.catalog, b.evalCtx, stmt)
		if err != nil {
			panic(err)
		}
		if newStmt != nil {
			// Many delegate implementations resolve objects. It would be tedious to
			// register all those dependencies with the metadata (for cache
			// invalidation). We don't care about caching plans for these statements.
			b.DisableMemoReuse = true
			return b.buildStmt(newStmt, desiredTypes, inScope)
		}

		// See if we have an opaque handler registered for this statement type.
		if outScope := b.tryBuildOpaque(stmt, inScope); outScope != nil {
			// The opaque handler may resolve objects; we don't care about caching
			// plans for these statements.
			b.DisableMemoReuse = true
			return outScope
		}
		panic(errors.AssertionFailedf("unexpected statement: %T", stmt))
	}
}

func (b *Builder) allocScope() *scope {
	if len(b.scopeAlloc) == 0 {
		// scope is relatively large (~250 bytes), so only allocate in small
		// chunks.
		b.scopeAlloc = make([]scope, 4)
	}
	r := &b.scopeAlloc[0]
	b.scopeAlloc = b.scopeAlloc[1:]
	r.builder = b
	return r
}

// trackReferencedColumnForViews is used to add a column to the view's
// dependencies. This should be called whenever a column reference is made in a
// view query.
func (b *Builder) trackReferencedColumnForViews(col *scopeColumn) {
	if b.trackViewDeps {
		for i := range b.viewDeps {
			dep := b.viewDeps[i]
			if ord, ok := dep.ColumnIDToOrd[col.id]; ok {
				dep.ColumnOrdinals.Add(ord)
			}
			b.viewDeps[i] = dep
		}
	}
}

func (b *Builder) maybeTrackRegclassDependenciesForViews(texpr tree.TypedExpr) {
	if b.trackViewDeps {
		if texpr.ResolvedType() == types.RegClass {
			// We do not add a dependency if the RegClass Expr contains variables,
			// we cannot resolve the variables in this context. This matches Postgres
			// behavior.
			if !tree.ContainsVars(texpr) {
				regclass, err := texpr.Eval(b.evalCtx)
				if err != nil {
					panic(err)
				}
				tn := tree.MakeUnqualifiedTableName(tree.Name(regclass.String()))
				ds, _ := b.resolveDataSource(&tn, privilege.SELECT)

				b.viewDeps = append(b.viewDeps, opt.ViewDep{
					DataSource: ds,
				})
			}
		}
	}
}
