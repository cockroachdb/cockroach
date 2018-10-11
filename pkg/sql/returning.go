// Copyright 2016 The Cockroach Authors.
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
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/pkg/errors"
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
	desiredTypes []types.T,
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

		r.ivarHelper = tree.MakeIndexedVarHelper(r, len(r.source.info.SourceColumns))
		err := p.initTargets(ctx, r, tree.SelectExprs(*t), desiredTypes)
		if err != nil {
			return nil, err
		}

		return r, nil

	default:
		return nil, pgerror.NewAssertionErrorf("unexpected ReturningClause type: %T", t)
	}
}
