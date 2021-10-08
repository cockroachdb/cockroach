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
// expression). It is always 0 for scalar expressions.
type memberOrd int

// memoLoc identifies an expression by its location in the memo.
type memoLoc struct {
	group  groupID
	member memberOrd
}

// memoGroups is used to map expressions to memo locations (see MemoLoc).
// To populate this information, AddGroup must be called with the first
// expression in each memo group.
type memoGroups struct {
	// exprMap maps the first expression in each group to a unique group ID; it
	// includes scalar expressions.
	exprMap map[opt.Expr]groupID
	// lastID is used to generate the next ID when AddGroup is called.
	lastID groupID
}

// AddGroup is called when a new memo group is created.
func (g *memoGroups) AddGroup(firstExpr opt.Expr) {
	g.lastID++
	if g.exprMap == nil {
		g.exprMap = make(map[opt.Expr]groupID)
	}
	g.exprMap[firstExpr] = g.lastID
}

// MemoLoc finds the memo location of the given expression. Panics if the
// expression isn't in the memo. Note that scalar leaf singletons (like True),
// lists and list items are not considered to be part of the memo.
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
	// exprMap contains both scalar and relational groups.
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
	path := g.depthFirstSearch(root, target, make(map[groupID]struct{}), nil /* path */)
	if path == nil {
		panic(errors.AssertionFailedf("could not find path to expr (%s)", target.Op()))
	}
	return path
}

// depthFirstSearch is used to find a path from any expression in the group that
// contains `start` to a `target` expression. If found, returns the path as a
// list of memo locations (starting with the given path and ending with the
// location that corresponds to `target`).
func (g *memoGroups) depthFirstSearch(
	start opt.Expr, target opt.Expr, visited map[groupID]struct{}, path []memoLoc,
) []memoLoc {
	// Lists aren't actually part of the memo; we treat them separately.
	if opt.IsListOp(start) || opt.IsListItemOp(start) {
		for i, n := 0, start.ChildCount(); i < n; i++ {
			if r := g.depthFirstSearch(start.Child(i), target, visited, path); r != nil {
				return r
			}
		}
		return nil
	}

	// There are various scalars that won't be registered as groups (e.g.
	// singletons). Ignore them (rather than panicking in firstInGroup).
	if scalar, ok := start.(opt.ScalarExpr); ok {
		if _, found := g.exprMap[scalar]; !found {
			return nil
		}
	}

	loc, expr := g.firstInGroup(start)
	if _, ok := visited[loc.group]; ok {
		// We already visited this group as part of this DFS.
		return nil
	}
	for ; expr != nil; loc, expr = g.nextInGroup(loc, expr) {
		nextPath := append(path, loc)
		if expr == target {
			return nextPath
		}
		for i, n := 0, expr.ChildCount(); i < n; i++ {
			if r := g.depthFirstSearch(expr.Child(i), target, visited, nextPath); r != nil {
				return r
			}
		}
		// Special hack for scalar expressions that hang off the table meta - we
		// treat these as children of Scan expressions.
		if scan, ok := expr.(*memo.ScanExpr); ok {
			md := scan.Memo().Metadata()
			meta := md.TableMeta(scan.Table)
			if meta.Constraints != nil {
				if r := g.depthFirstSearch(meta.Constraints, target, visited, nextPath); r != nil {
					return r
				}
			}
			for _, expr := range meta.ComputedCols {
				if r := g.depthFirstSearch(expr, target, visited, nextPath); r != nil {
					return r
				}
			}
		}
	}
	return nil
}

// firstInGroup returns the first expression in the given group, along with its
// location. Panics if the expression isn't in the memo.
func (g *memoGroups) firstInGroup(expr opt.Expr) (memoLoc, opt.Expr) {
	if rel, ok := expr.(memo.RelExpr); ok {
		expr = rel.FirstExpr()
	}
	return g.MemoLoc(expr), expr
}

// nextInGroup returns the next memo location and corresponding expression,
// given the current location and expression. If this is the last expression,
// returns a nil expression.
func (g *memoGroups) nextInGroup(loc memoLoc, expr opt.Expr) (memoLoc, opt.Expr) {
	// Only relational groups have more than one expression.
	if rel, ok := expr.(memo.RelExpr); ok {
		if next := rel.NextExpr(); next != nil {
			return memoLoc{group: loc.group, member: loc.member + 1}, next
		}
	}
	return memoLoc{}, nil
}
