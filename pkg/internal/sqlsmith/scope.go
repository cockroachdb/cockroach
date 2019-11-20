// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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

func (c *colRef) typedExpr() tree.TypedExpr {
	return makeTypedExpr(c.item, c.typ)
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

// canRecurse returns whether the current function should possibly invoke
// a function that creates new nodes.
func (s *Smither) canRecurse() bool {
	return s.complexity > s.rnd.Float64()
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
