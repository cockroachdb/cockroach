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

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// max1RowNode wraps another planNode, returning at most 1 row from the wrapped
// node. If the wrapped node produces more than 1 row, this planNode returns an
// error.
//
// This node is useful for constructing subqueries. Some ways of using
// subqueries in SQL, such as using a subquery as an expression, expect that
// the subquery can return at most 1 row - that expectation must be enforced at
// runtime.
type max1RowNode struct {
	plan planNode

	nexted bool
	values tree.Datums
}

func (m *max1RowNode) Next(params runParams) (bool, error) {
	if m.nexted {
		return false, nil
	}
	m.nexted = true

	ok, err := m.plan.Next(params)
	if !ok || err != nil {
		return ok, err
	}
	if ok {
		// We need to eagerly check our parent plan for a new row, to ensure that
		// we return an error as per the contract of this node if the parent plan
		// isn't exhausted after a single row.
		m.values = make(tree.Datums, len(m.plan.Values()))
		copy(m.values, m.plan.Values())
		var secondOk bool
		secondOk, err = m.plan.Next(params)
		if secondOk {
			return false, pgerror.NewErrorf(pgerror.CodeCardinalityViolationError,
				"more than one row returned by a subquery used as an expression")
		}
	}
	return ok, err
}

func (m *max1RowNode) Values() tree.Datums {
	return m.values
}

func (m *max1RowNode) Close(ctx context.Context) {
	m.plan.Close(ctx)
}
