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
)

// buildDistinct builds a set of memo groups that represent a DISTINCT
// expression if distinct is true. If distinct is false, we just return
// `inScope`.
//
// See Builder.buildStmt for a description of the remaining input and
// return values.
func (b *Builder) buildDistinct(distinct bool, inScope *scope) (outScope *scope) {
	if !distinct {
		return inScope
	}

	outScope = inScope.replace()
	outScope.physicalProps = inScope.physicalProps

	// Distinct is equivalent to group by without any aggregations.
	var groupCols opt.ColSet
	for i := range inScope.cols {
		if !inScope.cols[i].hidden {
			groupCols.Add(int(inScope.cols[i].id))
			outScope.cols = append(outScope.cols, inScope.cols[i])
		}
	}

	// Check that the ordering can be provided by the projected columns.
	// This will cause an error for queries like:
	//   SELECT DISTINCT a FROM t ORDER BY b
	// TODO(rytaft): This is not valid syntax in Postgres, but it works in
	// CockroachDB, so we may need to support it eventually.
	for _, col := range outScope.physicalProps.Ordering {
		if !outScope.hasColumn(col.ID()) {
			panic(errorf("for SELECT DISTINCT, ORDER BY expressions must appear in select list"))
		}
	}

	aggList := b.constructList(opt.AggregationsOp, nil)
	outScope.group = b.factory.ConstructGroupBy(
		inScope.group, aggList, b.factory.InternColSet(groupCols),
	)
	return outScope
}
