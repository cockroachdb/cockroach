// Copyright 2015 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package sql

import (
	"bytes"
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
)

// distinctNode de-duplicates rows returned by a wrapped planNode.
type distinctNode struct {
	plan planNode
	// All the columns that are part of the Sort. Set to nil if no-sort, or
	// sort used an expression that was not part of the requested column set.
	columnsInOrder []bool

	// distinctOnColIdxs are the column indices of the child planNode and
	// is what defines the distinct key.
	// For a normal DISTINCT (without the ON clause), distinctOnColIdxs
	// contains all the column indices of the child planNode.
	// Otherwise, distinctOnColIdxs is a strict subset of the child
	// planNode's column indices indicating which columns are specified in
	// the DISTINCT ON (<exprs>) clause.
	distinctOnColIdxs util.FastIntSet

	run distinctRun
}

// distinct constructs a distinctNode.
func (p *planner) distinct(
	ctx context.Context, n *tree.SelectClause, r *renderNode,
) (planNode, *distinctNode, error) {
	if !n.Distinct {
		return nil, nil, nil
	}

	d := &distinctNode{}
	plan := planNode(d)

	if n.DistinctOn == nil {
		return plan, d, nil
	}

	// We grab a copy of columns here because we might add new render targets
	// below. This is the set of columns requested by the query.
	// For example, we want to only return 'b' in the query
	//	SELECT DISTINCT ON (a) b FROM bar
	// but we need to addRender 'a' for computing DISTINCT ON.
	origColumns := r.columns
	origRender := r.render
	numOriginalCols := r.numOriginalCols

	for _, expr := range n.DistinctOn {
		// The expressions in DISTINCT ON follow similar rules as
		// the expressions in ORDER BY (see sort.go:sortBy).

		// The logical data source for DISTINCT ON is the list of render
		// expressions for a SELECT, as specified in the input SQL text
		// (or an entire UNION or VALUES clause).
		// There are some special cases:
		//
		// 1) if the expression is the aliased (AS) name of a render
		//    expression in a SELECT clause, then use that
		//    render as distinct on key.
		//    e.g. SELECT DISTINCT ON (b) a AS b, b AS c
		//    this "distinctifies" on the first render.
		//
		// 2) column ordinals. If a simple integer literal is used,
		//    optionally enclosed within parentheses but *not subject to
		//    any arithmetic*, then this refers to one of the columns of
		//    the data source. Then use the render expression at that
		//    ordinal position as distinct on key.
		//
		// 3) otherwise, if the expression is already in the render list,
		//    then use that render as distinct on key.
		//    e.g. SELECT DISTINCT ON (b) b AS c
		//    this distincts on on the first render.
		//    (this is an optimization)
		//
		// 4) if the distinct on key is not dependent on the data
		//    source (no IndexedVar) then simply do not distinct on.
		//    (this is an optimization)
		//
		// 5) otherwise, add a new render with the DISTINCT ON expression
		//    and use that as distinct on key.
		//    e.g. SELECT DISTINCT ON(b) a FROM t
		//    e.g. SELECT DISTINCT ON(a+b) a, b FROM t

		// Unwrap parentheses like "((a))" to "a".
		expr = tree.StripParens(expr)

		// First, deal with render aliases.
		index, err := r.colIdxByRenderAlias(expr, origColumns, "DISTINCT ON")
		if err != nil {
			return nil, nil, err
		}

		// If the expression does not refer to an alias, deal with
		// column ordinals.
		if index == -1 {
			col, err := p.colIndex(numOriginalCols, expr, "DISTINCT ON")
			if err != nil {
				return nil, nil, err
			}
			if col != -1 {
				index = col
			}
		}

		// Finally, if we haven't found anything so far, we really
		// need a new render.
		// TODO(richardwu): currently this is only possible for renderNode.
		// If we are dealing with a UNION or something else we would need
		// to fabricate an intermediate renderNode to add the new render.
		if index == -1 && r != nil {
			cols, exprs, hasStar, err := p.computeRenderAllowingStars(
				ctx, tree.SelectExpr{Expr: expr}, types.Any,
				r.sourceInfo, r.ivarHelper, autoGenerateRenderOutputName)
			if err != nil {
				return nil, nil, err
			}
			p.curPlan.hasStar = p.curPlan.hasStar || hasStar

			if len(cols) == 0 {
				// Nothing was expanded! No distinct on here.
				continue
			}

			// DISTINCT ON (a, b) -> DISTINCT ON a, b
			cols, exprs = flattenTuples(cols, exprs, &r.ivarHelper)

			colIdxs := r.addOrReuseRenders(cols, exprs, true)
			for i := 0; i < len(colIdxs)-1; i++ {
				// If more than 1 column were expanded, turn them into distinct on columns too.
				// Except the last one, which will be added below.
				d.distinctOnColIdxs.Add(colIdxs[i])
			}
			index = colIdxs[len(colIdxs)-1]
		}

		if index == -1 {
			return nil, nil, sqlbase.NewUndefinedColumnError(expr.String())
		}

		d.distinctOnColIdxs.Add(index)
	}

	// We add a post renderNode if DISTINCT ON introduced additional render
	// expressions.
	if len(origRender) < len(r.render) {
		src := planDataSource{
			info: sqlbase.NewSourceInfoForSingleTable(sqlbase.AnonymousTable, origColumns),
			plan: d,
		}
		postRender := &renderNode{
			source:     src,
			sourceInfo: sqlbase.MakeMultiSourceInfo(src.info),
		}
		postRender.ivarHelper = tree.MakeIndexedVarHelper(postRender, len(src.info.SourceColumns))
		if err := p.initTargets(ctx, postRender, tree.SelectExprs{
			tree.SelectExpr{
				Expr: tree.StarExpr(),
			},
		}, nil /* desiredTypes */); err != nil {
			return nil, nil, err
		}
		plan = postRender
	}

	return plan, d, nil
}

// distinctRun contains the run-time state of distinctNode during local execution.
type distinctRun struct {
	// Encoding of the columnsInOrder columns for the previous row.
	prefixSeen   []byte
	prefixMemAcc mon.BoundAccount

	// Encoding of the non-columnInOrder columns for rows sharing the same
	// prefixSeen value.
	suffixSeen   map[string]struct{}
	suffixMemAcc mon.BoundAccount
}

func (n *distinctNode) startExec(params runParams) error {
	n.run.prefixMemAcc = params.EvalContext().Mon.MakeBoundAccount()
	n.run.suffixMemAcc = params.EvalContext().Mon.MakeBoundAccount()
	n.run.suffixSeen = make(map[string]struct{})
	return nil
}

func (n *distinctNode) Next(params runParams) (bool, error) {
	ctx := params.ctx

	for {
		if err := params.p.cancelChecker.Check(); err != nil {
			return false, err
		}

		next, err := n.plan.Next(params)
		if !next {
			return false, err
		}

		// Detect duplicates
		prefix, suffix, err := n.encodeDistinctOnVals(n.plan.Values())
		if err != nil {
			return false, err
		}

		if !bytes.Equal(prefix, n.run.prefixSeen) {
			// The prefix of the row which is ordered differs from the last row;
			// reset our seen set.
			if len(n.run.suffixSeen) > 0 {
				n.run.suffixMemAcc.Clear(ctx)
				n.run.suffixSeen = make(map[string]struct{})
			}
			if err := n.run.prefixMemAcc.Resize(
				ctx, int64(len(n.run.prefixSeen)), int64(len(prefix))); err != nil {
				return false, err
			}
			n.run.prefixSeen = prefix
			if suffix != nil {
				if err := n.addSuffixSeen(ctx, &n.run.suffixMemAcc, string(suffix)); err != nil {
					return false, err
				}
			}
			return true, nil
		}

		// The prefix of the row is the same as the last row; check
		// to see if the suffix which is not ordered has been seen.
		if suffix != nil {
			sKey := string(suffix)
			if _, ok := n.run.suffixSeen[sKey]; !ok {
				if err := n.addSuffixSeen(ctx, &n.run.suffixMemAcc, sKey); err != nil {
					return false, err
				}
				return true, nil
			}
		}
	}
}

func (n *distinctNode) Values() tree.Datums {
	// We return only the required columns set during planning.
	// These columns are always at the beginning of the child row since
	// we _append_ additional DISTINCT ON columns.
	// See planner.distinct.
	return n.plan.Values()
}

func (n *distinctNode) Close(ctx context.Context) {
	n.plan.Close(ctx)
	n.run.prefixSeen = nil
	n.run.prefixMemAcc.Close(ctx)
	n.run.suffixSeen = nil
	n.run.suffixMemAcc.Close(ctx)
}

func (n *distinctNode) addSuffixSeen(
	ctx context.Context, acc *mon.BoundAccount, sKey string,
) error {
	sz := int64(len(sKey))
	if err := acc.Grow(ctx, sz); err != nil {
		return err
	}
	n.run.suffixSeen[sKey] = struct{}{}
	return nil
}

// TODO(irfansharif): This can be refactored away to use
// sqlbase.EncodeDatums([]byte, tree.Datums)
func (n *distinctNode) encodeDistinctOnVals(values tree.Datums) ([]byte, []byte, error) {
	var prefix, suffix []byte
	var err error
	for i, val := range values {
		// Only encode DISTINCT ON expressions/columns (if applicable).
		if !n.distinctOnColIdxs.Empty() && !n.distinctOnColIdxs.Contains(i) {
			continue
		}

		if n.columnsInOrder != nil && n.columnsInOrder[i] {
			if prefix == nil {
				prefix = make([]byte, 0, 100)
			}
			prefix, err = sqlbase.EncodeDatum(prefix, val)
		} else {
			if suffix == nil {
				suffix = make([]byte, 0, 100)
			}
			suffix, err = sqlbase.EncodeDatum(suffix, val)
		}
		if err != nil {
			break
		}
	}
	return prefix, suffix, err
}

// projectChildPropsToOnExprs takes the physical props (e.g. ordering info,
// weak keys) of its child and projects them onto the columns specified by
// the DISTINCT ON clause ("ON expressions").
func (n *distinctNode) projectChildPropsToOnExprs() physicalProps {
	underlying := planPhysicalProps(n.plan)
	props := underlying.copy()

	if numPlanColumns := len(planColumns(n.plan)); !n.distinctOnColIdxs.Empty() && n.distinctOnColIdxs.Len() < numPlanColumns {
		// The distinctNode has the DISTINCT ON columns defined on a
		// subset of columns
		//   SELECT DISTINCT ON (k) v FROM kv.
		// n.distinctOnCols: k
		// planColumns(n.plan): v, k
		colMap := make([]int, numPlanColumns)
		for i := range colMap {
			if n.distinctOnColIdxs.Contains(i) {
				colMap[i] = i
			} else {
				colMap[i] = -1
			}
		}

		// The DISTINCT ON columns subsume or is a prefix of the any
		// ORDER BY columns: we can therefore eliminate ordering
		// information for all other columns.
		// In addition, the ON columns form a weak key (and potentially
		// a strong key), so any extra columns in the underlying
		// ordering don't help.
		return props.project(colMap)
	}

	return props
}
