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

import "sort"

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
		case *Insert:
			v.formatForFingerprint(ctx)
			return
		case *UpdateExprs:
			// We want to sort name lists to avoid creating different fingerprints
			// for equivalent queries. For example:
			// SELECT a, b FROM t
			// SELECT b, a FROM t
			// TODO if sorting is too slow we'll likely need to hash
			// while walking the tree.
			v.formatForFingerprint(ctx)
			return
		case *Placeholder, *StrVal, Datum, Constant:
			// If we have a placeholder, a string literal, a datum or a constant,
			// we want to print the same special character for all of them. This
			// is to avoid creating different fingerprints for queries that are
			// actually equivalent.
			ctx.WriteString(StmtFingerprintConstPlaceholder)
			return
		}
	}
	n.Format(ctx)
}

func isExprOnlyLiteralsOrPlaceholders(expr Expr) bool {
	switch e := expr.(type) {
	case Datum, Constant, *Placeholder, *CastExpr:
		return true
	case *Tuple:
		return e.Exprs.onlyContainsLiteralsOrPlaceholders()
	default:
		return false
	}
}

func (node *Insert) formatForFingerprint(ctx *FmtCtx) {
	if node.Rows != nil {
		switch node.Rows.Select.(type) {
		case *ValuesClause:
			// Sort col names.
			v2 := *node
			v2.Columns = append(make(NameList, 0, len(node.Columns)), node.Columns...)
			sort.Slice(v2.Columns, func(i, j int) bool {
				return v2.Columns[i] < v2.Columns[j]
			})
			v2.Format(ctx)
			return
		}
	}
	node.Format(ctx)
}

func (node *UpdateExpr) containsOnlyNamesLiteralsOrPlaceholders() bool {
	if node.Tuple {
		return false
	}

	return isExprOnlyLiteralsOrPlaceholders(node.Expr)
}

// formatForFingerprint formats update expressions  that are a simple list of
// assignments for stmt fingerprints. We want to sort them to avoid creating
// different fingerprints for equivalent queries.
// For example:
// UPDATE t SET a = 1, b = 2, c = 3, d = 4
// UPDATE t SET d = 4, c = 3, b = 2, a = 1
// TODO if sorting is too slow we'll likely need to hash while walking the tree.
func (node *UpdateExprs) formatForFingerprint(ctx *FmtCtx) {
	if len(*node) < 2 {
		node.Format(ctx)
		return
	}

	for _, setClause := range *node {
		if !setClause.containsOnlyNamesLiteralsOrPlaceholders() {
			node.Format(ctx)
			return
		}
	}

	v2 := append(make(UpdateExprs, 0, len(*node)), *node...)
	sort.Slice(v2, func(i, j int) bool {
		// We've verified these are not tuple update exprs.
		return v2[i].Names[0] < v2[j].Names[0]
	})
	v2.Format(ctx)

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
	for i := 0; i < len(*node); i++ {
		if !isExprOnlyLiteralsOrPlaceholders((*node)[i]) {
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

	if node.onlyContainsLiteralsOrPlaceholders() {
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
