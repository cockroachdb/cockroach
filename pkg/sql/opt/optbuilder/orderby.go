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
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
)

// buildOrderBy builds an Ordering physical property from the ORDER BY clause.
// ORDER BY is not a relational expression, but instead a required physical
// property on the output. The Ordering property returned by buildOrderBy is
// set on the scope and later becomes part of the required physical properties
// returned by Build.
// TODO(rytaft): Add support for ORDER BY clause that uses more than simple
// column references.
func (b *Builder) buildOrderBy(orderBy tree.OrderBy, inScope *scope) opt.Ordering {
	if orderBy == nil {
		return nil
	}

	ordering := make(opt.Ordering, 0, len(orderBy))

	for i, order := range orderBy {
		props, ok := inScope.resolveType(order.Expr, types.Any).(*columnProps)
		if !ok {
			panic(errorf("ORDER BY only supports simple column references at this time"))
		}

		index := props.index
		if orderBy[i].Direction == tree.Descending {
			index = -index
		}
		ordering = append(ordering, index)
	}

	return ordering
}
