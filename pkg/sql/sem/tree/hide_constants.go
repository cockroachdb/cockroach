// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.
//
//

package tree

import (
	"fmt"
	"strings"
)

// formatNodeOrHideConstants recurses into a node for pretty-printing,
// unless hideConstants is set in the flags and the node is a datum or
// a literal.
func (ctx *FmtCtx) formatNodeOrHideConstants(n NodeFormatter) {
	if ctx.flags.HasFlags(FmtHideConstants) {
		switch v := n.(type) {
		case *ValuesClause:
			v.formatHideConstants(ctx)
			return
		case *Tuple:
			v.formatHideConstants(ctx)
			return
		case *Array:
			v.formatHideConstants(ctx)
			return
		case *Placeholder:
			// Placeholders should be printed as placeholder markers.
			// Deliberately empty so we format as normal.
		case Datum, Constant:
			ctx.WriteByte('_')
			return
		}
	}
	n.Format(ctx)
}

// formatHideConstants shortens multi-valued VALUES clauses to a
// VALUES clause with a single value.
// e.g. VALUES (a,b,c), (d,e,f) -> VALUES (_, _, _), (__more__)
func (node *ValuesClause) formatHideConstants(ctx *FmtCtx) {
	ctx.WriteString("VALUES (")
	node.Rows[0].formatHideConstants(ctx)
	ctx.WriteByte(')')
	if len(node.Rows) > 1 {
		ctx.Printf(", (%s)", arityString(len(node.Rows)-1))
	}
}

// formatHideConstants is used exclusively by ValuesClause above.
// Other AST that contain Exprs do not use this.
func (node *Exprs) formatHideConstants(ctx *FmtCtx) {
	exprs := *node
	if len(exprs) < 2 {
		node.Format(ctx)
		return
	}

	// First, determine if there are only literals/placeholders.
	var i int
	for i = 0; i < len(exprs); i++ {
		switch exprs[i].(type) {
		case Datum, Constant, *Placeholder:
			continue
		}
		break
	}
	// If so, then use the special representation.
	if i == len(exprs) {
		// We copy the node to preserve the "row" boolean flag.
		v2 := append(make(Exprs, 0, 3), exprs[:2]...)
		if len(exprs) > 2 {
			v2 = append(v2, arityIndicator(len(exprs)-2))
		}
		v2.Format(ctx)
		return
	}
	node.Format(ctx)
}

// formatHideConstants formats tuples containing only literals or
// placeholders and longer than 1 element as a tuple of its first
// two elements, scrubbed.
// e.g. (1)               -> (_)
//      (1, 2)            -> (_, _)
//      (1, 2, 3)         -> (_, _, __more3__)
//      ROW()             -> ROW()
//      ROW($1, $2, $3)   -> ROW($1, $2, __more3__)
//      (1+2, 2+3, 3+4)   -> (_ + _, _ + _, _ + _)
//      (1+2, b, c)       -> (_ + _, b, c)
func (node *Tuple) formatHideConstants(ctx *FmtCtx) {
	if len(node.Exprs) < 2 {
		node.Format(ctx)
		return
	}

	// First, determine if there are only literals/placeholders.
	var i int
	for i = 0; i < len(node.Exprs); i++ {
		switch node.Exprs[i].(type) {
		case Datum, Constant, *Placeholder:
			continue
		}
		break
	}
	// If so, then use the special representation.
	if i == len(node.Exprs) {
		// We copy the node to preserve the "row" boolean flag.
		v2 := *node
		v2.Exprs = append(make(Exprs, 0, 3), v2.Exprs[:2]...)
		if len(node.Exprs) > 2 {
			v2.Exprs = append(v2.Exprs, arityIndicator(len(node.Exprs)-2))
		}
		if node.Labels != nil {
			v2.Labels = node.Labels[:2]
		}
		v2.Format(ctx)
		return
	}
	node.Format(ctx)
}

// formatHideConstants formats array expressions containing only
// literals or placeholders and longer than 1 element as an array
// expression of its first two elements, scrubbed.
// e.g. array[1]             -> array[_]
//      array[1, 2]          -> array[_, _]
//      array[1, 2, 3]       -> array[_, _, __more3__]
//      array[1+2, 2+3, 3+4] -> array[_ + _, _ + _, _ + _]
func (node *Array) formatHideConstants(ctx *FmtCtx) {
	if len(node.Exprs) < 2 {
		node.Format(ctx)
		return
	}

	// First, determine if there are only literals/placeholders.
	var i int
	for i = 0; i < len(node.Exprs); i++ {
		switch node.Exprs[i].(type) {
		case Datum, Constant, *Placeholder:
			continue
		}
		break
	}
	// If so, then use the special representation.
	if i == len(node.Exprs) {
		// We copy the node to preserve the "row" boolean flag.
		v2 := *node
		v2.Exprs = append(make(Exprs, 0, 3), v2.Exprs[:2]...)
		if len(node.Exprs) > 2 {
			v2.Exprs = append(v2.Exprs, arityIndicator(len(node.Exprs)-2))
		}
		v2.Format(ctx)
		return
	}
	node.Format(ctx)
}

func arityIndicator(n int) Expr {
	return NewUnresolvedName(arityString(n))
}

func arityString(n int) string {
	var v int
	for v = 1; n >= 10; n /= 10 {
		v = v * 10
	}
	v = v * n
	return fmt.Sprintf("__more%d__", v)
}

func isArityIndicatorString(s string) bool {
	return strings.HasPrefix(s, "__more")
}
