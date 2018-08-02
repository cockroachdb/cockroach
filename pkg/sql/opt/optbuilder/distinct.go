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
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
)

// constructDistinct wraps inScope.group in a DistinctOn operator corresponding
// to a SELECT DISTINCT statement.
func (b *Builder) constructDistinct(inScope *scope) memo.GroupID {
	// We are doing a distinct along all the projected columns.
	var def memo.GroupByDef
	for i := range inScope.cols {
		if !inScope.cols[i].hidden {
			def.GroupingCols.Add(int(inScope.cols[i].id))
		}
	}

	// Check that the ordering only refers to projected columns.
	// This will cause an error for queries like:
	//   SELECT DISTINCT a FROM t ORDER BY b
	// Note: this behavior is consistent with PostgreSQL.
	for _, col := range inScope.physicalProps.Ordering.Columns {
		if !col.Group.Intersects(def.GroupingCols) {
			panic(builderError{pgerror.NewErrorf(
				pgerror.CodeInvalidColumnReferenceError,
				"for SELECT DISTINCT, ORDER BY expressions must appear in select list",
			)})
		}
	}

	// We don't set def.Ordering. Because the ordering can only refer to projected
	// columns, it does not affect the results; it doesn't need to be required of
	// the DistinctOn input.

	return b.factory.ConstructDistinctOn(
		inScope.group,
		b.factory.ConstructAggregations(memo.EmptyList, b.factory.InternColList(nil)),
		b.factory.InternGroupByDef(&def),
	)
}
