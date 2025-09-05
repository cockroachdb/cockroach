// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tree

import "github.com/cockroachdb/errors"

// With represents a WITH statement.
type With struct {
	Recursive bool
	CTEList   []*CTE
}

// CTE represents a common table expression inside of a WITH clause.
type CTE struct {
	Name AliasClause
	Mtr  CTEMaterializeClause
	Stmt Statement
}

// InputCount implements the WalkableTreeNode interface.
func (node *CTE) InputCount() int { return 1 }

// Input implements the WalkableTreeNode interface.
func (node *CTE) Input(i int) WalkableTreeNode {
	if i == 0 {
		if walkable, ok := node.Stmt.(WalkableTreeNode); ok {
			return walkable
		}
	}
	return nil
}

// SetChild implements the WalkableTreeNode interface.
func (node *CTE) SetChild(i int, child WalkableTreeNode) error {
	if i == 0 {
		if stmt, ok := child.(Statement); ok {
			node.Stmt = stmt
			return nil
		}
		return errors.Errorf("CTE child 0 must be Statement, got %T", child)
	}
	return errors.Errorf("CTE child index %d out of bounds", i)
}

// CTEMaterializeClause represents either MATERIALIZED, NOT MATERIALIZED, or an
// empty materialization clause.
type CTEMaterializeClause int8

const (
	// CTEMaterializeDefault represents an empty materialization clause.
	CTEMaterializeDefault CTEMaterializeClause = iota
	// CTEMaterializeAlways represents MATERIALIZED.
	CTEMaterializeAlways
	// CTEMaterializeNever represents NOT MATERIALIZED.
	CTEMaterializeNever
)

// Format implements the NodeFormatter interface.
func (node *With) Format(ctx *FmtCtx) {
	if node == nil {
		return
	}
	ctx.WriteString("WITH ")
	if node.Recursive {
		ctx.WriteString("RECURSIVE ")
	}
	for i, cte := range node.CTEList {
		if i != 0 {
			ctx.WriteString(", ")
		}
		ctx.FormatNode(&cte.Name)
		ctx.WriteString(" AS ")
		switch cte.Mtr {
		case CTEMaterializeAlways:
			ctx.WriteString("MATERIALIZED ")
		case CTEMaterializeNever:
			ctx.WriteString("NOT MATERIALIZED ")
		}
		ctx.WriteString("(")
		ctx.FormatNode(cte.Stmt)
		ctx.WriteString(")")
	}
	ctx.WriteByte(' ')
}
