// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tree

// formatNodeForFingerprint recurses into a node to format it for a normalized
// statement fingerprinting.
func (ctx *FmtCtx) formatNodeForFingerprint(n NodeFormatter) {
	if ctx.flags.HasFlags(FmtForFingerprint) {
		switch v := n.(type) {
		case *ValuesClause:
			v.formatForFingerprint(ctx)
			return
		case *Tuple:
			v.formatForFingerprint(ctx)
			return
		case *Array:
			v.formatForFingerprint(ctx)
			return
		case *Placeholder, *StrVal, Datum, Constant:
			// If we have a placeholder, a string literal, a datum or a constant,
			// we want to print the same special character for all of them. This
			// is to avoid creating different fingerprints for queries that are
			// actually equivalent.
			ctx.WriteByte(StmtFingerprintSubChar)
			return
		}
	}
	n.Format(ctx)
}

// It shortens multi-value VALUES clauses to a VALUES clause with a single value.
// Example: VALUES (a,b,c), (d,e,f) -> VALUES (_, _, _), (__more__)
func (node *ValuesClause) formatForFingerprint(ctx *FmtCtx) {
	ctx.WriteString("VALUES (")
	exprs := node.Rows[0].maybeCollapseExprs()
	exprs.Format(ctx)
	ctx.WriteByte(')')
	if len(node.Rows) > 1 {
		ctx.Printf(", (%s)", arityString(len(node.Rows)-1))
	}
}

// onlyContainsLiteralsOrPlaceholders returns true if all the expressions in the list
// are either literals or placeholders or tuples containing only literals or
// placeholders.
func (node *Exprs) onlyContainsLiteralsOrPlaceholders() bool {
	exprs := []Exprs{*node}

	for len(exprs) > 0 {
		exprsCurrent := exprs[len(exprs)-1]
		exprs = exprs[:len(exprs)-1]
		var i int
		for i = 0; i < len(exprsCurrent); i++ {
			expr := exprsCurrent[i]
			switch expr.(type) {
			case Datum, Constant, *Placeholder:
				continue
			case *Tuple:
				exprs = append(exprs, expr.(*Tuple).Exprs)
				continue
			default:
				return false
			}
		}
		if i != len(exprsCurrent) {
			return false
		}
	}
	return true
}

// maybeCollapseExprs is used to condense expressions containing only
// literals or placeholders to a list of expressions to a  special marker.
// Exprs containing elements other than literals or placeholders are returned
// unchanged.
func (node *Exprs) maybeCollapseExprs() Exprs {
	exprs := *node
	if len(exprs) < 2 {
		return exprs
	}

	// First, determine if there are only literals/placeholders.
	var i int
	for i = 0; i < len(exprs); i++ {
		switch expr := exprs[i].(type) {
		case Datum, Constant, *Placeholder:
			continue
		case *Tuple:
			if expr.Exprs.onlyContainsLiteralsOrPlaceholders() {
				continue
			}
		}

		break
	}

	// If so, then use the special representation.
	if i == len(exprs) {
		v2 := append(make(Exprs, 0, 2), exprs[:1]...)
		v2 = append(v2, arityIndicator(len(exprs)-1))
		return v2
	}

	return exprs
}

// formatForFingerprint formats tuples containing only literals and/or
// placeholders and longer than 1 element as a tuple of its first
// element, scrubbed.
// e.g. (1)               -> (_)
//
//	(1, 2)            -> (_, __more1_10__)
//	(1, 2, 3)         -> (_, __more1_10__)
//	ROW()             -> ROW()
//	ROW($1, $2, $3)   -> ROW($1, __more1_10__)
//	(1+2, 2+3, 3+4)   -> (_ + _, _ + _, _ + _)
//	(1+2, b, c)       -> (_ + _, b, c)
//	($1, 1, 3, 'ok')  -> (_, __more1_10__)
func (node *Tuple) formatForFingerprint(ctx *FmtCtx) {
	if len(node.Exprs) < 2 {
		node.Format(ctx)
		return
	}

	// We copy the node to preserve the "row" boolean flag.
	v2 := *node
	v2.Exprs = node.Exprs.maybeCollapseExprs()
	if len(node.Labels) > 1 {
		if len(v2.Exprs) >= 2 {
			v2.Labels = node.Labels
		} else if len(node.Labels) > 0 {
			v2.Labels = node.Labels[:1]
		}
	}
	v2.Format(ctx)
}

// formatForFingerprint formats array expressions containing only
// literals or placeholders and longer than 1 element as an array
// expression of its first two elements, scrubbed.
// e.g. array[1]             -> array[_]
//
//	array[1, 2]          -> array[_, _]
//	array[1, 2, 3]       -> array[_, _, __more1_10__]
//	array[1+2, 2+3, 3+4] -> array[_ + _, _ + _, _ + _]
func (node *Array) formatForFingerprint(ctx *FmtCtx) {
	if len(node.Exprs) < 2 {
		node.Format(ctx)
		return
	}
	// We copy the node to preserve the "row" boolean flag.
	v2 := *node
	v2.Exprs = node.Exprs.maybeCollapseExprs()
	v2.Format(ctx)
}
