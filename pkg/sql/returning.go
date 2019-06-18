// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL.txt and at www.mariadb.com/bsl11.
//
// Change Date: 2022-10-01
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the Apache License, Version 2.0,
// included in the file licenses/APL.txt and at
// https://www.apache.org/licenses/LICENSE-2.0

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

// resultsNeeded determines whether a statement that might have a
// RETURNING clause needs to provide values for result rows for a
// downstream plan.
func resultsNeeded(r tree.ReturningClause) bool {
	switch t := r.(type) {
	case *tree.ReturningExprs:
		return true
	case *tree.ReturningNothing, *tree.NoReturningClause:
		return false
	default:
		panic(errors.Errorf("unexpected ReturningClause type: %T", t))
	}
}

// Returning wraps the given source node in a way suitable for the
// given RETURNING specification.
func (p *planner) Returning(
	ctx context.Context,
	source batchedPlanNode,
	r tree.ReturningClause,
	desiredTypes []*types.T,
	tn *tree.TableName,
) (planNode, error) {
	// serialize the data-modifying plan to ensure that no data is
	// observed that hasn't been validated first. See the comments
	// on BatchedNext() in plan_batch.go.

	switch t := r.(type) {
	case *tree.ReturningNothing, *tree.NoReturningClause:
		// We could use serializeNode here, but using rowCountNode is an
		// optimization that saves on calls to Next() by the caller.
		return &rowCountNode{source: source}, nil

	case *tree.ReturningExprs:
		serialized := &serializeNode{source: source}
		info := sqlbase.NewSourceInfoForSingleTable(*tn, planColumns(source))
		r := &renderNode{
			source:     planDataSource{info: info, plan: serialized},
			sourceInfo: sqlbase.MultiSourceInfo{info},
		}

		// We need to save and restore the previous value of the field in
		// semaCtx in case we are recursively called within a subquery
		// context.
		defer p.semaCtx.Properties.Restore(p.semaCtx.Properties)

		// Ensure there are no special functions in the RETURNING clause.
		p.semaCtx.Properties.Require("RETURNING", tree.RejectSpecial)

		err := p.initTargets(ctx, r, tree.SelectExprs(*t), desiredTypes)
		if err != nil {
			return nil, err
		}

		return r, nil

	default:
		return nil, errors.AssertionFailedf("unexpected ReturningClause type: %T", t)
	}
}
