// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tree

// exprContainsOnlyConstantsAndPlaceholders checks if parts of an expression contains only
// values that would be scrubbed for statement fingerprinting such as literals, placeholders
// or datums. This can be used to determine if an expression can be shortened since it
// does not contain any interesting information.
type exprContainsOnlyConstantsAndPlaceholders struct {
	containsNonConstExpr bool
}

var _ Visitor = &exprContainsOnlyConstantsAndPlaceholders{}

func (v *exprContainsOnlyConstantsAndPlaceholders) VisitPre(
	expr Expr,
) (recurse bool, newExpr Expr) {
	switch expr.(type) {
	case Datum, Constant, *Placeholder:
		return false, expr
	case *Tuple, *Array, *CastExpr, *BinaryExpr, *UnaryExpr, *ParenExpr:
		return !v.containsNonConstExpr, expr
	default:
		v.containsNonConstExpr = true
		return false, expr
	}
}

// onlyLiteralsAndPlaceholders is used to determine if the expression only contains
// literals and/or placeholders. This includes subexpressions such as tuples, arrays
// casts, binary expressions and unary expressions only containing literals
// and/or placeholders.
// See exprContainsOnlyConstantsAndPlaceholders for more details.
// Examples:
//
//	(1+2, 3+4, 5+6)              -> true
//	(1+2, b, c)                  -> false
//	ARRAY[$1, 1, 3, 'ok']        -> true
//	ARRAY[$1, 1, 3, 'ok', b]     -> false
//	(-$1, 2, 'hello')            -> true
//	(-$1, (2+$2)::INT, 'hello')  -> true
func onlyLiteralsAndPlaceholders(exprs Exprs) bool {
	v := exprContainsOnlyConstantsAndPlaceholders{}
	for i := 0; i < len(exprs); i++ {
		WalkExprConst(&v, exprs[i])
		if v.containsNonConstExpr {
			return false
		}
	}

	return true
}

func (v *exprContainsOnlyConstantsAndPlaceholders) VisitPost(expr Expr) Expr { return expr }

// Formatting VALUES for a fingerprint shortens multi-value VALUES clauses to a VALUES
// clause with a single value.
// Subexpressions containing only literals and/or placeholders in the first element
// will also be collapsed.
// Examples:
// VALUES (a,b,c), (d,e,f) -> VALUES (a, b, c), (__more__)
// VALUES ((a, b), (c, d)), ((e, f), (g, h)) -> VALUES ((a, b), (__more__)), (__more__)
func (node *ValuesClause) formatForFingerprint(ctx *FmtCtx) {
	ctx.WriteString("VALUES (")
	ok := canCollapseExprs(node.Rows[0])
	if ok {
		exprs := Exprs{node.Rows[0][0], arityIndicatorNoBounds()}
		exprs.Format(ctx)
	} else {
		node.Rows[0].Format(ctx)
	}

	ctx.WriteByte(')')
	if len(node.Rows) > 1 {
		ctx.Printf(", (%s)", genericArityIndicator)
	}
}

// canCollapseExprs returns true if the list of expressions has more than 1 element and
// each expression contains only literals, placeholders and/or simple subexpressions
// containing only literals/placeholders. This is used to determine if the list can
// be collapsed into a special shortened representation when generating a representative
// query for statement fingerprints.
func canCollapseExprs(exprs Exprs) bool {
	if len(exprs) < 2 {
		return false
	}

	return onlyLiteralsAndPlaceholders(exprs)
}

// formatForFingerprint shortens tuples containing only literals, placeholders
// and/or simple expressions containing only literals and/or placeholders as a
// tuple of its first element, and `__more__` indicating the rest of
// the elements.
// Examples:
//
//	  ()                -> ()
//	  (1)               -> (1)
//		(1, 2)            -> (1, __more__)
//		(1, 2, 3)         -> (1, __more__)
//		ROW()             -> ROW()
//		ROW($1, $2, $3)   -> ROW($1, __more__)
//		(1+2, 2+3, 3+4)   -> (1 + 2, __more__)
//		(1+2, b, c)       -> (1 + 2, b, c)
//		($1, 1, 3, 'ok')  -> ($1, __more__)
func (node *Tuple) formatForFingerprint(ctx *FmtCtx) {
	ok := canCollapseExprs(node.Exprs)
	if !ok {
		node.Format(ctx)
		return
	}

	v2 := *node
	exprs := Exprs{node.Exprs[0], arityIndicatorNoBounds()}
	v2.Exprs = exprs
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
// literals or placeholders that are longer than 1 element as an array
// expression of its first element.
// e.g.
//
//	  array[]              -> array[]
//	  array[1]             -> array[1]
//		array[1, 2]          -> array[1, __more__]
//		array[1, 2, 3]       -> array[1, __more__]
//		array[1+2, 2+3, 3+4] -> array[1 + 2, __more__]
func (node *Array) formatForFingerprint(ctx *FmtCtx) {
	ok := canCollapseExprs(node.Exprs)
	if !ok {
		node.Format(ctx)
		return
	}

	v2 := *node
	v2.Exprs = Exprs{node.Exprs[0], arityIndicatorNoBounds()}
	v2.Format(ctx)
}
