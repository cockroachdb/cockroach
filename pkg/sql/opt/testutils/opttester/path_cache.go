// Copyright 2018 The Cockroach Authors.
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
	"bytes"
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
)

// exprPath is the location of an expression in the memo, in the following
// format:
//
//   <child-ord>:<member-ord>.<child-ord>:<member-ord>
//
// Each step in the path consists of the ordinal position of the expression
// within its parent's children (always 1 for the root) followed by the ordinal
// position of the expression within the members of its memo group. If the
// operator is scalar, then the member ordinal part is omitted. An example:
//
//   1:1.2.3:2
//
// This is the 1st member of the root group, followed by the 2nd child of that
// expression, followed by the 2nd member of the 3rd child's group.
//
// Paths are guaranteed to be unique across the expression tree, and stable for
// the lifetime of the expression tree, even if new expressions are added to the
// tree.
type exprPath string

// truncateLastStep returns a path with the last step removed. If the path is
// empty, it returns an empty path as well. Examples:
//
//   1       => <empty>
//   1.2     => 1
//   1:1.2:1 => 1:1
//   1.2:2.3 => 1.2:2
//
func (p exprPath) truncateLastStep() exprPath {
	if len(p) == 0 {
		return p
	}

	var i int
	for i = len(p) - 1; i > 0; i-- {
		if p[i] == '.' {
			break
		}
	}
	return p[:i]
}

// commonAncestor finds the least common ancestor of this path and the other
// path. This is the path containing all parts (child index or member indesx)
// that are common between the two paths. Examples:
//
//   1       1       => 1
//   1       2       => <empty>
//   1.2.3   1.2     => 1.2
//   1:1.2   1:1.3   => 1:1
//   1:1.3:1 1:1.3:2 => 1:1.3
//
func (p exprPath) commonAncestor(other exprPath) exprPath {
	var left, right string
	if len(p) < len(other) {
		left, right = string(p), string(other)
	} else {
		left, right = string(other), string(p)
	}

	// If the left path is a prefix of the right, then it is the common ancestor.
	if strings.HasPrefix(right, left) {
		return exprPath(left)
	}

	var ancestor string
	for i := range left {
		leftChar := left[i]
		rightChar := right[i]
		if leftChar != rightChar {
			break
		}

		if leftChar == '.' || leftChar == ':' {
			ancestor = left[:i]
		}
	}

	return exprPath(ancestor)
}

// isSuppressedBy returns true if the common ancestor is a memo group, but both
// paths descend from different members of that group; in other words, if this
// path would not be in the optimized expression tree because an "uncle"
// expression is instead. Examples:
//
//   <empty>     1:1        => false
//   1:1         1:2        => true
//   1:1.1:1     1:1.2:1    => false
//   1:2.1:1     1:1.1:1    => true
//
func (p exprPath) isSuppressedBy(other exprPath) bool {
	common := p.commonAncestor(other)
	commonLen := len(common)

	if len(p) <= commonLen || len(other) <= commonLen {
		// One of paths is subset of other, so no suppression.
		return false
	}

	if p[commonLen] != ':' {
		// The common ancestor is not a memo group, so no suppression.
		return false
	}

	for i := range p[commonLen+1:] {
		if i >= len(other) || p[i] != other[i] {
			// The two paths pass through different members of the same group.
			return false
		}
		if p[i] == '.' {
			break
		}
	}

	return true
}

// pathCache maintains a mapping from expressions in the tree to their location
// paths in the tree. The mapping is lazily built on lookup of a node.
type pathCache struct {
	buf   bytes.Buffer
	cache map[opt.Expr]exprPath
}

// LookupPath returns the path to the target expression in the tree having the
// the given root.
func (c *pathCache) lookupPath(root, target opt.Expr) exprPath {
	if c.cache != nil {
		if existing, ok := c.cache[target]; ok {
			return existing
		}
	} else {
		c.cache = make(map[opt.Expr]exprPath)
	}

	c.buf.Reset()
	c.buf.WriteString("1")
	c.buildCache(root)

	return c.cache[target]
}

// buildCache recursively builds the expression to path mapping.
func (c *pathCache) buildCache(current opt.Expr) {
	memberMark := c.buf.Len()

	if rel, ok := current.(memo.RelExpr); ok {
		current = rel.FirstExpr()
		c.buf.WriteString(":1")
	}

	memberOrd := 1
	for {
		c.cache[current] = exprPath(c.buf.String())

		childMark := c.buf.Len()
		for i, n := 0, current.ChildCount(); i < n; i++ {
			fmt.Fprintf(&c.buf, ".%d", i+1)
			c.buildCache(current.Child(i))
			c.buf.Truncate(childMark)
		}

		c.buf.Truncate(memberMark)

		if rel, ok := current.(memo.RelExpr); ok {
			current = rel.NextExpr()
			if current == nil {
				break
			}

			memberOrd++
			fmt.Fprintf(&c.buf, ":%d", memberOrd)
		} else {
			break
		}
	}
}
