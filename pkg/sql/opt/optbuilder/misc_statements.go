// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package optbuilder

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
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

	outScope.expr = b.factory.ConstructCreateStatistics(&memo.CreateStatisticsPrivate{
		Syntax: n,
	})
	return outScope
}
