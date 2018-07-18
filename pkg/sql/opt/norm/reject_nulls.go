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

package norm

import (
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
)

// HasNullRejectingFilter returns true if the filter causes some of the columns
// in nullRejectCols to be non-null. For example, if nullRejectCols = (x, z),
// filters such as x < 5, x = y, and z IS NOT NULL all satisfy this property.
func (c *CustomFuncs) HasNullRejectingFilter(filter memo.GroupID, nullRejectCols opt.ColSet) bool {
	filterConstraints := c.LookupLogical(filter).Scalar.Constraints
	if filterConstraints == nil {
		return false
	}

	notNullFilterCols := filterConstraints.ExtractNotNullCols(c.f.evalCtx)
	return notNullFilterCols.Intersects(nullRejectCols)
}

// NullRejectCols returns the set of columns that are candidates for NULL
// rejection filter pushdown. See the Relational.RejectNullCols comment for more
// details.
func (c *CustomFuncs) NullRejectCols(group memo.GroupID) opt.ColSet {
	return c.LookupLogical(group).Relational.Rule.RejectNullCols
}

// NullRejectAggVar scans through the list of aggregate functions and returns
// the Variable input of the first aggregate that is not AnyNotNull. Such an
// aggregate must exist, since this is only called if the NullRejectCols
// property was populated by the rulePropsBuilder in deriveGroupByRejectNullCols
// (see comment for that function for more details on criteria).
func (c *CustomFuncs) NullRejectAggVar(aggs memo.GroupID) memo.GroupID {
	aggsExpr := c.f.mem.NormExpr(aggs).AsAggregations()
	aggsElems := c.f.mem.LookupList(aggsExpr.Aggs())

	for i := len(aggsElems) - 1; i >= 0; i-- {
		agg := c.f.mem.NormExpr(aggsElems[i])
		if agg.Operator() != opt.AnyNotNullOp {
			// Return the input Variable operator.
			return agg.ChildGroup(c.f.mem, 0)
		}
	}
	panic("couldn't find an aggregate that is not AnyNotNull")
}
