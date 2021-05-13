// Copyright 2017 The Cockroach Authors.
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

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil"
	"github.com/cockroachdb/errors"
)

type scatterNode struct {
	optColumnsSlot

	run scatterRun
}

// Scatter moves ranges to random stores
// (`ALTER TABLE/INDEX ... SCATTER ...` statement)
// Privileges: INSERT on table.
func (p *planner) Scatter(ctx context.Context, n *tree.Scatter) (planNode, error) {
	if !p.ExecCfg().Codec.ForSystemTenant() {
		return nil, errorutil.UnsupportedWithMultiTenancy(54255)
	}

	tableDesc, index, err := p.getTableAndIndex(ctx, &n.TableOrIndex, privilege.INSERT)
	if err != nil {
		return nil, err
	}

	var span roachpb.Span
	if n.From == nil {
		// No FROM/TO specified; the span is the entire table/index.
		span = tableDesc.IndexSpan(p.ExecCfg().Codec, index.GetID())
	} else {
		switch {
		case len(n.From) == 0:
			return nil, errors.Errorf("no columns in SCATTER FROM expression")
		case len(n.From) > index.NumKeyColumns():
			return nil, errors.Errorf("too many columns in SCATTER FROM expression")
		case len(n.To) == 0:
			return nil, errors.Errorf("no columns in SCATTER TO expression")
		case len(n.To) > index.NumKeyColumns():
			return nil, errors.Errorf("too many columns in SCATTER TO expression")
		}

		// Calculate the desired types for the select statement:
		//  - column values; it is OK if the select statement returns fewer columns
		//  (the relevant prefix is used).
		desiredTypes := make([]*types.T, index.NumKeyColumns())
		for i := 0; i < index.NumKeyColumns(); i++ {
			colID := index.GetKeyColumnID(i)
			c, err := tableDesc.FindColumnWithID(colID)
			if err != nil {
				return nil, err
			}
			desiredTypes[i] = c.GetType()
		}
		fromVals := make([]tree.Datum, len(n.From))
		for i, expr := range n.From {
			typedExpr, err := p.analyzeExpr(
				ctx, expr, nil, tree.IndexedVarHelper{}, desiredTypes[i], true, "SCATTER",
			)
			if err != nil {
				return nil, err
			}
			fromVals[i], err = typedExpr.Eval(p.EvalContext())
			if err != nil {
				return nil, err
			}
		}
		toVals := make([]tree.Datum, len(n.From))
		for i, expr := range n.To {
			typedExpr, err := p.analyzeExpr(
				ctx, expr, nil, tree.IndexedVarHelper{}, desiredTypes[i], true, "SCATTER",
			)
			if err != nil {
				return nil, err
			}
			toVals[i], err = typedExpr.Eval(p.EvalContext())
			if err != nil {
				return nil, err
			}
		}

		span.Key, err = getRowKey(p.ExecCfg().Codec, tableDesc, index, fromVals)
		if err != nil {
			return nil, err
		}
		span.EndKey, err = getRowKey(p.ExecCfg().Codec, tableDesc, index, toVals)
		if err != nil {
			return nil, err
		}
		// Tolerate reversing FROM and TO; this can be useful for descending
		// indexes.
		if cmp := span.Key.Compare(span.EndKey); cmp > 0 {
			span.Key, span.EndKey = span.EndKey, span.Key
		} else if cmp == 0 {
			// Key==EndKey is invalid, so special-case when the user's FROM and
			// TO are the same tuple.
			span.EndKey = span.EndKey.Next()
		}
	}

	return &scatterNode{
		run: scatterRun{
			span: span,
		},
	}, nil
}

// scatterRun contains the run-time state of scatterNode during local execution.
type scatterRun struct {
	span roachpb.Span

	rangeIdx int
	ranges   []roachpb.Span
}

func (n *scatterNode) startExec(params runParams) error {
	db := params.p.ExecCfg().DB
	req := &roachpb.AdminScatterRequest{
		RequestHeader:   roachpb.RequestHeader{Key: n.run.span.Key, EndKey: n.run.span.EndKey},
		RandomizeLeases: true,
	}
	res, pErr := kv.SendWrapped(params.ctx, db.NonTransactionalSender(), req)
	if pErr != nil {
		return pErr.GoError()
	}
	scatterRes := res.(*roachpb.AdminScatterResponse)
	n.run.rangeIdx = -1
	n.run.ranges = make([]roachpb.Span, len(scatterRes.RangeInfos))
	for i, rangeInfo := range scatterRes.RangeInfos {
		n.run.ranges[i] = roachpb.Span{
			Key:    rangeInfo.Desc.StartKey.AsRawKey(),
			EndKey: rangeInfo.Desc.EndKey.AsRawKey(),
		}
	}
	return nil
}

func (n *scatterNode) Next(params runParams) (bool, error) {
	n.run.rangeIdx++
	hasNext := n.run.rangeIdx < len(n.run.ranges)
	return hasNext, nil
}

func (n *scatterNode) Values() tree.Datums {
	r := n.run.ranges[n.run.rangeIdx]
	return tree.Datums{
		tree.NewDBytes(tree.DBytes(r.Key)),
		tree.NewDString(keys.PrettyPrint(nil /* valDirs */, r.Key)),
	}
}

func (*scatterNode) Close(ctx context.Context) {}
