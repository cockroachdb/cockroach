// Copyright 2018 The Cockroach Authors.
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

package xform

import (
	"github.com/cockroachdb/cockroach/pkg/sql/opt/opt"
	"github.com/cockroachdb/cockroach/pkg/util"
)

// bestExpr references the lowest cost expression in a memo group for a given
// set of required physical properties. The memo group maintains a map from
// physical property set to the best expression that provides that set.
type bestExpr struct {
	// op is the operator type of the lowest cost expression.
	op opt.Operator

	// fullyOptimized is true if the lowest cost expression has been found for
	// this memo group, with respect to the physical properties which map to
	// this bestExpr. A lower cost expression will never be found, no matter
	// how many additional optimization passes are made.
	fullyOptimized bool

	// fullyOptimizedExprs contains the set of expression ids (exprIDs) that
	// have been fully optimized for the required properties. These never need
	// to be recosted, no matter how many additional optimization passes are
	// made.
	fullyOptimizedExprs util.FastIntSet

	// lowest is the index of the lowest cost expression in the memo group,
	// with respect to the physical properties which map to this bestExpr.
	lowest exprID
}

// ratchetCost overwrites the existing best expression if the new cost is
// lower.
// TODO(andyk): Currently, there is no costing function, so just assume that
// the first expression is the lowest cost.
func (be *bestExpr) ratchetCost(ev ExprView) {
	if be.op == opt.UnknownOp {
		be.op = ev.Operator()
		be.lowest = ev.loc.expr
	}
}

// isFullyOptimized is set to true once the lowest cost expression is
// guaranteed to have been found. No further changes to this bestExpr can
// occur.
func (be *bestExpr) isFullyOptimized() bool {
	return be.fullyOptimized
}

func (be *bestExpr) isExprFullyOptimized(eid exprID) bool {
	return be.fullyOptimizedExprs.Contains(int(eid))
}

func (be *bestExpr) markExprAsFullyOptimized(eid exprID) {
	be.fullyOptimizedExprs.Add(int(eid))
}
