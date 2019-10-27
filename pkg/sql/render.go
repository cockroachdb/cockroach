// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

// renderNode encapsulates the render logic of a select statement:
// expressing new values using expressions over source values.
type renderNode struct {
	// source describes where the data is coming from.
	// populated initially by initFrom().
	// potentially modified by index selection.
	source planDataSource

	// sourceInfo contains the reference to the DataSourceInfo in the
	// source planDataSource that is needed for name resolution.
	// We keep one instance of multiSourceInfo cached here so as to avoid
	// re-creating it every time analyzeExpr() is called in computeRender().
	sourceInfo sqlbase.MultiSourceInfo

	// Helper for indexed vars. This holds the actual instances of
	// IndexedVars replaced in Exprs. The indexed vars contain indices
	// to the array of source columns.
	ivarHelper tree.IndexedVarHelper

	// Rendering expressions for rows and corresponding output columns.
	// populated by addOrReuseRenders()
	// as invoked initially by initTargets() and initWhere().
	// sortNode peeks into the render array defined by initTargets() as an optimization.
	// sortNode adds extra renderNode renders for sort columns not requested as select targets.
	// groupNode copies/extends the render array defined by initTargets() and
	// will add extra renderNode renders for the aggregation sources.
	// windowNode also adds additional renders for the window functions.
	render []tree.TypedExpr

	// columns is the set of result columns.
	columns sqlbase.ResultColumns

	reqOrdering ReqOrdering

	// This struct must be allocated on the heap and its location stay
	// stable after construction because it implements
	// IndexedVarContainer and the IndexedVar objects in sub-expressions
	// will link to it by reference after checkRenderStar / analyzeExpr.
	// Enforce this using NoCopy.
	_ util.NoCopy
}

// IndexedVarEval implements the tree.IndexedVarContainer interface.
func (r *renderNode) IndexedVarEval(idx int, ctx *tree.EvalContext) (tree.Datum, error) {
	panic("renderNode can't be run in local mode")
}

// IndexedVarResolvedType implements the tree.IndexedVarContainer interface.
func (r *renderNode) IndexedVarResolvedType(idx int) *types.T {
	return r.sourceInfo[0].SourceColumns[idx].Typ
}

// IndexedVarNodeFormatter implements the tree.IndexedVarContainer interface.
func (r *renderNode) IndexedVarNodeFormatter(idx int) tree.NodeFormatter {
	return r.sourceInfo[0].NodeFormatter(idx)
}

func (r *renderNode) startExec(runParams) error {
	panic("renderNode can't be run in local mode")
}

func (r *renderNode) Next(params runParams) (bool, error) {
	panic("renderNode can't be run in local mode")
}

func (r *renderNode) Values() tree.Datums {
	panic("renderNode can't be run in local mode")
}

func (r *renderNode) Close(ctx context.Context) { r.source.plan.Close(ctx) }

// getTimestamp will get the timestamp for an AS OF clause. It will also
// verify the timestamp against the transaction. If AS OF SYSTEM TIME is
// specified in any part of the query, then it must be consistent with
// what is known to the Executor. If the AsOfClause contains a
// timestamp, then true will be returned.
func (p *planner) getTimestamp(asOf tree.AsOfClause) (hlc.Timestamp, bool, error) {
	if asOf.Expr != nil {
		// At this point, the executor only knows how to recognize AS OF
		// SYSTEM TIME at the top level. When it finds it there,
		// p.asOfSystemTime is set. If AS OF SYSTEM TIME wasn't found
		// there, we cannot accept it anywhere else either.
		// TODO(anyone): this restriction might be lifted if we support
		// table readers at arbitrary timestamps, and each FROM clause
		// can have its own timestamp. In that case, the timestamp
		// would not be set globally for the entire txn.
		if p.semaCtx.AsOfTimestamp == nil {
			return hlc.MaxTimestamp, false,
				pgerror.Newf(pgcode.Syntax,
					"AS OF SYSTEM TIME must be provided on a top-level statement")
		}

		// The Executor found an AS OF SYSTEM TIME clause at the top
		// level. We accept AS OF SYSTEM TIME in multiple places (e.g. in
		// subqueries or view queries) but they must all point to the same
		// timestamp.
		ts, err := p.EvalAsOfTimestamp(asOf)
		if err != nil {
			return hlc.MaxTimestamp, false, err
		}
		if ts != *p.semaCtx.AsOfTimestamp {
			return hlc.MaxTimestamp, false,
				unimplemented.NewWithIssue(35712,
					"cannot specify AS OF SYSTEM TIME with different timestamps")
		}
		return ts, true, nil
	}
	return hlc.MaxTimestamp, false, nil
}
