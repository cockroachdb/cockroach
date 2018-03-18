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

package optbuilder

import (
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
)

// buildDistinct builds a set of memo groups that represent a DISTINCT
// expression.
//
// in        contains the memo group ID of the input expression.
// distinct  is true if this is a DISTINCT expression. If distinct is false,
//           we just return `in, inScope`.
// byCols    is the set of columns in the DISTINCT expression. Since
//           DISTINCT is equivalent to GROUP BY without any aggregations,
//           byCols are essentially the grouping columns.
//
// See Builder.buildStmt for a description of the remaining input and
// return values.
func (b *Builder) buildDistinct(
	in memo.GroupID, distinct bool, byCols []columnProps, inScope *scope,
) (out memo.GroupID, outScope *scope) {
	if !distinct {
		return in, inScope
	}

	// After DISTINCT, FROM columns are no longer visible. As a side effect,
	// ORDER BY cannot reference columns outside of the SELECT list. This
	// will cause an error for queries like:
	//   SELECT DISTINCT a FROM t ORDER BY b
	// TODO(rytaft): This is not valid syntax in Postgres, but it works in
	// CockroachDB, so we may need to support it eventually.
	outScope = inScope.replace()

	// Distinct is equivalent to group by without any aggregations.
	var groupCols opt.ColSet
	for i := range byCols {
		groupCols.Add(int(byCols[i].index))
		outScope.cols = append(outScope.cols, byCols[i])
	}

	aggList := b.constructList(opt.AggregationsOp, nil, nil)
	return b.factory.ConstructGroupBy(in, aggList, b.factory.InternPrivate(&groupCols)), outScope
}
