// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package optbuilder

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/constraint"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/idxconstraint"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/idxtype"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

func (b *Builder) buildControlJobs(n *tree.ControlJobs, inScope *scope) (outScope *scope) {
	// We don't allow the input statement to reference outer columns, so we
	// pass a "blank" scope rather than inScope.
	emptyScope := b.allocScope()
	colTypes := []*types.T{types.Int}
	inputScope := b.buildStmt(n.Jobs, colTypes, emptyScope)

	var reason opt.ScalarExpr
	if n.Reason != nil {
		reasonStr := emptyScope.resolveType(n.Reason, types.String)
		reason = b.buildScalar(
			reasonStr, emptyScope, nil /* outScope */, nil /* outCol */, nil, /* colRefs */
		)
	} else {
		reason = b.factory.ConstructNull(types.String)
	}

	checkInputColumns(
		fmt.Sprintf("%s JOBS", tree.JobCommandToStatement[n.Command]),
		inputScope,
		[]string{"job_id"},
		colTypes,
		1, /* minPrefix */
	)
	outScope = inScope.push()
	outScope.expr = b.factory.ConstructControlJobs(
		inputScope.expr,
		reason,
		&memo.ControlJobsPrivate{
			Props:   inputScope.makePhysicalProps(),
			Command: n.Command,
		},
	)
	return outScope
}

func (b *Builder) buildShowCompletions(n *tree.ShowCompletions, inScope *scope) (outScope *scope) {
	outScope = inScope.push()
	b.synthesizeResultColumns(outScope, colinfo.ShowCompletionsColumns)
	outScope.expr = b.factory.ConstructShowCompletions(
		&memo.ShowCompletionsPrivate{
			Command: n,
			Columns: colsToColList(outScope.cols),
		},
	)
	return outScope
}

func (b *Builder) buildCancelQueries(n *tree.CancelQueries, inScope *scope) (outScope *scope) {
	// We don't allow the input statement to reference outer columns, so we
	// pass a "blank" scope rather than inScope.
	emptyScope := b.allocScope()
	colTypes := []*types.T{types.String}
	inputScope := b.buildStmt(n.Queries, colTypes, emptyScope)

	checkInputColumns(
		"CANCEL QUERIES",
		inputScope,
		[]string{"query_id"},
		colTypes,
		1, /* minPrefix */
	)
	outScope = inScope.push()
	outScope.expr = b.factory.ConstructCancelQueries(
		inputScope.expr,
		&memo.CancelPrivate{
			Props:    inputScope.makePhysicalProps(),
			IfExists: n.IfExists,
		},
	)
	return outScope
}

func (b *Builder) buildCancelSessions(n *tree.CancelSessions, inScope *scope) (outScope *scope) {
	// We don't allow the input statement to reference outer columns, so we
	// pass a "blank" scope rather than inScope.
	emptyScope := b.allocScope()
	colTypes := []*types.T{types.String}
	inputScope := b.buildStmt(n.Sessions, colTypes, emptyScope)

	checkInputColumns(
		"CANCEL SESSIONS",
		inputScope,
		[]string{"session_id"},
		colTypes,
		1, /* minPrefix */
	)
	outScope = inScope.push()
	outScope.expr = b.factory.ConstructCancelSessions(
		inputScope.expr,
		&memo.CancelPrivate{
			Props:    inputScope.makePhysicalProps(),
			IfExists: n.IfExists,
		},
	)
	return outScope
}

func (b *Builder) buildControlSchedules(
	n *tree.ControlSchedules, inScope *scope,
) (outScope *scope) {
	// We don't allow the input statement to reference outer columns, so we
	// pass a "blank" scope rather than inScope.
	emptyScope := b.allocScope()
	colTypes := []*types.T{types.Int}
	inputScope := b.buildStmt(n.Schedules, colTypes, emptyScope)

	checkInputColumns(
		fmt.Sprintf("%s SCHEDULES", n.Command),
		inputScope,
		[]string{"schedule_id"},
		colTypes,
		1, /* minPrefix */
	)

	outScope = inScope.push()
	outScope.expr = b.factory.ConstructControlSchedules(
		inputScope.expr,
		&memo.ControlSchedulesPrivate{
			Props:   inputScope.makePhysicalProps(),
			Command: n.Command,
		},
	)
	return outScope
}

func (b *Builder) buildCreateStatistics(n *tree.CreateStats, inScope *scope) (outScope *scope) {
	outScope = inScope.push()

	// We add AS OF SYSTEM TIME '-1us' to trigger use of inconsistent
	// scans if left unspecified. This prevents GC TTL errors.
	if n.Options.AsOf.Expr == nil {
		n.Options.AsOf.Expr = tree.NewStrVal("-1us")
	}

	var tabID opt.TableID
	var ds cat.DataSource
	switch t := n.Table.(type) {
	case *tree.UnresolvedObjectName:
		tn := t.ToTableName()
		ds, _, _ = b.resolveDataSource(&tn, privilege.SELECT)
	case *tree.TableRef:
		ds, _ = b.resolveDataSourceRef(t, privilege.SELECT)
	default:
		panic(errors.AssertionFailedf("unexpected table type: %T", t))
	}

	if t, ok := ds.(cat.Table); ok {
		tn := tree.MakeUnqualifiedTableName(t.Name())
		tabMeta := b.addTable(t, &tn)
		tabID = tabMeta.MetaID
	} else {
		panic(pgerror.Newf(pgcode.FeatureNotSupported,
			"cannot use %T as table in CREATE STATISTICS", ds))
	}

	indexOrd := cat.PrimaryIndex
	var c *constraint.Constraint

	if n.Options.Where != nil {
		indexOrd, c = b.buildWhereForStatistics(n, tabID)
	}

	outScope.expr = b.factory.ConstructCreateStatistics(&memo.CreateStatisticsPrivate{
		Table:      tabID,
		Index:      indexOrd,
		Syntax:     n,
		Constraint: c,
	})
	return outScope
}

// buildWhereForStatistics builds and validates the WHERE clause for a CREATE
// STATISTICS statement. It returns an index ordinal and a tight constraint that
// can be used to generate spans for the statistics collection.
func (b *Builder) buildWhereForStatistics(
	n *tree.CreateStats, tabID opt.TableID,
) (indexOrd cat.IndexOrdinal, _ *constraint.Constraint) {
	tabMeta := b.factory.Metadata().TableMeta(tabID)
	tab := tabMeta.Table

	s := b.allocScope()
	b.appendOrdinaryColumnsFromTable(s, tabMeta, &tabMeta.Alias)

	if len(n.ColumnNames) != 1 {
		panic(pgerror.New(pgcode.InvalidColumnReference,
			"partial statistics with WHERE must be on a single column"))
	}
	var colID opt.ColumnID
	foundCol := false
	for i := 0; i < tab.ColumnCount(); i++ {
		if tab.Column(i).ColName() == n.ColumnNames[0] {
			colID = tabID.ColumnID(i)
			foundCol = true
			break
		}
	}
	if !foundCol {
		panic(colinfo.NewUndefinedColumnError(string(n.ColumnNames[0])))
	}

	filter := b.resolveAndBuildScalar(
		n.Options.Where.Expr,
		types.Bool,
		exprKindWhere,
		tree.RejectSpecial,
		s,
		nil,
	)

	fi := b.factory.ConstructFiltersItem(filter)
	if fi.ScalarProps().OuterCols.Len() != 1 {
		panic(pgerror.New(pgcode.Syntax,
			"WHERE filter should have exactly one outer col"))
	}
	fe := memo.FiltersExpr{fi}
	filterColID := (fi.ScalarProps().OuterCols).SingleColumn()
	if filterColID != colID {
		panic(pgerror.New(pgcode.InvalidColumnReference,
			"WHERE filter must be on the same column as the one specified in the column list"))
	}

	if fe.IsTrue() {
		panic(pgerror.New(pgcode.Syntax, "filter is always true"))
	}
	if fe.IsFalse() {
		panic(pgerror.New(pgcode.Syntax, "filter cannot be a contradiction"))
	}

	// Find a non-partial forward index with the filter column as the first key
	// column.
	foundIndex := false
	var orderingCol opt.OrderingColumn
	var notNullCols opt.ColSet
	for i := 0; i < tab.IndexCount(); i++ {
		idx := tab.Index(i)
		if _, isPartial := idx.Predicate(); isPartial || idx.Type() != idxtype.FORWARD {
			continue
		}
		if idx.KeyColumnCount() > 0 {
			col := idx.Column(0)
			if tabID.IndexColumnID(idx, 0) == filterColID {
				indexOrd = idx.Ordinal()
				foundIndex = true
				orderingCol = opt.MakeOrderingColumn(filterColID, idx.Column(0).Descending)
				if !col.IsNullable() {
					notNullCols.Add(filterColID)
				}
				break
			}
		}
	}
	if !foundIndex {
		panic(
			pgerror.Newf(pgcode.InvalidColumnReference,
				"table %s does not contain a non-partial forward index with %s as a prefix column",
				tab.Name(), tab.Column(tabID.ColumnOrdinal(filterColID)).ColName(),
			),
		)
	}

	indexCols := []opt.OrderingColumn{orderingCol}
	computedCols := tabMeta.ComputedCols
	colsInComputedColsExpressions := tabMeta.ColsInComputedColsExpressions
	ps := tabMeta.IndexPartitionLocality(indexOrd)

	var ic idxconstraint.Instance
	// TODO(uzair): Pass in optionalFilters here to produce a better constraint.
	ic.Init(
		b.ctx, fe, nil /* optionalFilters */, indexCols, notNullCols,
		computedCols, colsInComputedColsExpressions, true, /* consolidate */
		b.evalCtx, b.factory, ps, nil, /* checkCancellation */
	)
	var cons constraint.Constraint
	ic.Constraint(&cons)

	remaining := ic.RemainingFilters()
	if !remaining.IsTrue() || cons.IsUnconstrained() {
		panic(pgerror.New(pgcode.Syntax,
			"predicate could not become a constrained scan of an index"))
	}
	if cons.IsContradiction() {
		panic(pgerror.New(pgcode.Syntax,
			"predicate is a contradiction"))
	}

	return indexOrd, &cons
}
