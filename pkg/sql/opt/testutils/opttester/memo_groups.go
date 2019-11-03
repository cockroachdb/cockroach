// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package opttester

import (
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/errors"
)

// groupID identifies a memo group.
type groupID int

// memberOrd identifies an expression within its memo group (0 is the first
// expression). Always 0 for scalar expressions.
type memberOrd int

// memoLoc identifies an expression by its location in the memo.
type memoLoc struct {
	group  groupID
	member memberOrd
}

// memoGroups is used to map expressions to memo locations. AddGroup must be
// called for each memo group.
type memoGroups struct {
	exprMap map[opt.Expr]groupID
	lastID  groupID
}

// AddGroup is called when a new memo group is created.
func (g *memoGroups) AddGroup(firstExpr opt.Expr) {
	g.lastID++
	if g.exprMap == nil {
		g.exprMap = make(map[opt.Expr]groupID)
	}
	g.exprMap[firstExpr] = g.lastID
}

// MemoLoc finds the memo location of the givenexpression.
func (g *memoGroups) MemoLoc(expr opt.Expr) memoLoc {
	var member memberOrd

	for opt.IsEnforcerOp(expr) {
		// Enforcers aren't actually part of the memo group.
		expr = expr.Child(0)
	}

	if rel, ok := expr.(memo.RelExpr); ok {
		for e := rel.FirstExpr(); e != rel; e = e.NextExpr() {
			if e == nil {
				panic(errors.AssertionFailedf("could not reach expr (%s) from FirstExpr", expr.Op()))
			}
			member++
		}
		expr = rel.FirstExpr()
	}
	gID := g.exprMap[expr]
	if gID == 0 {
		panic(errors.AssertionFailedf("could not find group for expr (%s)", expr.Op()))
	}
	return memoLoc{gID, member}
}

// FindPath finds a path from the root memo group to the given target
// expression. The returned path contains the location for each expression on
// this path, including the target expression (in the last entry).
func (g *memoGroups) FindPath(root opt.Expr, target opt.Expr) []memoLoc {
	for opt.IsEnforcerOp(target) {
		// Enforcers aren't actually part of the memo group.
		target = target.Child(0)
	}
	path := g.depthFirstSearch(root, target, nil /* path */)
	if path == nil {
		panic(errors.AssertionFailedf("could not find path to expr (%s)", target.Op()))
	}
	return path
}

func (g *memoGroups) depthFirstSearch(current opt.Expr, target opt.Expr, path []memoLoc) []memoLoc {
	if rel, ok := current.(memo.RelExpr); ok {
		current = rel.FirstExpr()
	}

	// Lists aren't actually part of the memo.
	if opt.IsListOp(current) || opt.IsListItemOp(current) {
		for i, n := 0, current.ChildCount(); i < n; i++ {
			if r := g.depthFirstSearch(current.Child(i), target, path); r != nil {
				return r
			}
		}
		return nil
	}

	// There are various scalar singletons that won't be registered as groups;
	// ignore them.
	if scalar, ok := current.(opt.ScalarExpr); ok && scalar.ChildCount() == 0 {
		if _, found := g.exprMap[scalar]; !found {
			return nil
		}
	}

	group := g.MemoLoc(current).group

	var member memberOrd
	for {
		nextPath := append(path, memoLoc{group: group, member: member})
		if current == target {
			return nextPath
		}
		for i, n := 0, current.ChildCount(); i < n; i++ {
			if r := g.depthFirstSearch(current.Child(i), target, nextPath); r != nil {
				return r
			}
		}

		if rel, ok := current.(memo.RelExpr); ok {
			current = rel.NextExpr()
			if current == nil {
				break
			}
			member++
		} else {
			break
		}
	}
	return nil
}
