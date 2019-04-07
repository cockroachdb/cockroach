// Copyright 2019 The Cockroach Authors.
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

package sqlsmith

import (
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

// colRef refers to a named result column. If it is from a table, def is
// populated.
type colRef struct {
	typ  *types.T
	item *tree.ColumnItem
}

type colRefs []*colRef

func (t colRefs) extend(refs ...*colRef) colRefs {
	ret := append(make(colRefs, 0, len(t)+len(refs)), t...)
	ret = append(ret, refs...)
	return ret
}

func (t colRefs) stripTableName() {
	for _, c := range t {
		c.item.TableName = nil
	}
}

type scope struct {
	schema *Smither

	// The budget tracks available complexity. It is randomly generated. Each
	// call to canRecurse decreases it such that canRecurse will eventually
	// always return false.
	budget int
}

func (s *Smither) makeScope() *scope {
	return &scope{
		schema: s,
		budget: s.rnd.Intn(100),
	}
}

// canRecurse returns whether the current function should possibly invoke
// a function that calls creates new nodes.
func (s *scope) canRecurse() bool {
	s.budget--
	// Disable recursion randomly so that early expressions don't take all
	// the budget.
	return s.budget > 0 && coin()
}

// Context holds information about what kinds of expressions are legal at
// a particular place in a query.
type Context struct {
	fnClass  tree.FunctionClass
	noWindow bool
}

var (
	emptyCtx   = Context{}
	groupByCtx = Context{fnClass: tree.AggregateClass}
	havingCtx  = Context{
		fnClass:  tree.AggregateClass,
		noWindow: true,
	}
	windowCtx = Context{fnClass: tree.WindowClass}
)
