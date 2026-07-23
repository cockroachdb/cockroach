// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.
//
//

package tree

import (
	"fmt"
	"strings"
)

// formatNodeOrAdjustConstants recurses into a node for pretty-printing, unless
// FmtHideConstants or FmtShortenConstants is set in the flags and the node is
// affected by that format.
func (ctx *FmtCtx) formatNodeOrAdjustConstants(n NodeFormatter) {
	if ctx.HasFlags(FmtConstantsAsUnderscores) {
		switch n.(type) {
		case *Placeholder, *StrVal, Datum, Constant:
			// If we have a placeholder, a string literal, a datum or a constant,
			// we want to print the same special character for all of them. This
			// is to avoid creating different fingerprints for queries that are
			// actually equivalent.
			ctx.WriteByte(StmtFingerprintPlaceholder)
			return
		}
	}
	if ctx.flags.HasFlags(FmtCollapseLists) {
		// This is a more aggressive form of collapsing lists than the cases
		// provided by FmtHideConstants and FmtShortenConstants.
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
		}
	}
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
			// Using always '$1' so we limit the amount of different
			// fingerprints created.
			ctx.WriteString("$1")
			return
		case *StrVal:
			ctx.WriteString("'_'")
			return
		case Datum, Constant:
			ctx.WriteByte('_')
			return
		}
	} else if ctx.flags.HasFlags(FmtShortenConstants) {
		switch v := n.(type) {
		case *ValuesClause:
			v.formatShortenConstants(ctx)
			return
		case *Tuple:
			v.formatShortenConstants(ctx)
			return
		case *Array:
			v.formatShortenConstants(ctx)
			return
		case *DTuple:
			v.formatShortenConstants(ctx)
			return
		case *DArray:
			v.formatShortenConstants(ctx)
			return
		}
	}
	n.Format(ctx)
}

// numElementsForShortenedList determines the number of elements included into
// the shortened lists for tuples, arrays, and VALUES. In particular, first
// numElementsForShortenedList-1 and the very last one will be included.
//
// Only used for FmtShortenConstants.
const numElementsForShortenedList = 3

// formatHideConstants shortens multi-valued VALUES clauses to a
// VALUES clause with a single scrubbed value.
// e.g. VALUES (a,b,c), (d,e,f) -> VALUES (_, _, _), (__more__)
func (node *ValuesClause) formatHideConstants(ctx *FmtCtx) {
	ctx.WriteString("VALUES (")
	node.Rows[0].formatHideConstants(ctx)
	ctx.WriteByte(')')
	if len(node.Rows) > 1 {
		ctx.Printf(", (%s)", arityString(len(node.Rows)-1))
	}
}

// formatShortenConstants shortens multi-valued VALUES clauses to a VALUES
// clause with at most 3 values.
// e.g. VALUES (a), (b), (c), (d), (e), ... (z) -> VALUES (a), (b), (__more__), (z)
func (node *ValuesClause) formatShortenConstants(ctx *FmtCtx) {
	if len(node.Rows) <= numElementsForShortenedList {
		node.Format(ctx)
		return
	}
	ctx.WriteString("VALUES (")
	for i := 0; i < numElementsForShortenedList-1; i++ {
		node.Rows[i].Format(ctx)
		ctx.WriteString("), (")
	}
	ctx.Printf("%s), (", arityString(len(node.Rows)-numElementsForShortenedList))
	node.Rows[len(node.Rows)-1].Format(ctx)
	ctx.WriteByte(')')
}

// formatHideConstants is used exclusively by ValuesClause above.
// Other AST that contain Exprs do not use this.
func (node *Exprs) formatHideConstants(ctx *FmtCtx) {
	exprs := *node
	if len(exprs) < 2 {
		node.Format(ctx)
		return
	}

	// Determine if there are only literals/placeholders and use the special
	// representation if so.
	for _, expr := range exprs {
		switch expr.(type) {
		case Datum, Constant, *Placeholder:
			continue
		}
		node.Format(ctx)
		return
	}
	v2 := append(make(Exprs, 0, 3), exprs[:2]...)
	if len(exprs) > 2 {
		v2 = append(v2, arityIndicator(len(exprs)-2))
	}
	v2.Format(ctx)
}

// formatHideConstants formats tuples containing only literals or
// placeholders and longer than 1 element as a tuple of its first
// two elements, scrubbed. For example:
//
//	(1)               -> (_)
//	(1, 2)            -> (_, _)
//	(1, 2, 3)         -> (_, _, __more1_10__)
//	ROW()             -> ROW()
//	ROW($1, $2, $3)   -> ROW($1, $2, __more1_10__)
//	(1+2, 2+3, 3+4)   -> (_ + _, _ + _, _ + _)
//	(1+2, b, c)       -> (_ + _, b, c)
func (node *Tuple) formatHideConstants(ctx *FmtCtx) {
	if len(node.Exprs) < 2 {
		node.Format(ctx)
		return
	}

	// Determine if there are only literals/placeholders and use the special
	// representation if so.
	for _, expr := range node.Exprs {
		switch expr.(type) {
		case Datum, Constant, *Placeholder:
			continue
		}
		node.Format(ctx)
		return
	}
	// We copy the node to preserve the "row" boolean flag.
	v2 := *node
	v2.Exprs = append(make(Exprs, 0, 3), v2.Exprs[:2]...)
	if len(node.Exprs) > 2 {
		v2.Exprs = append(v2.Exprs, arityIndicator(len(node.Exprs)-2))
		if len(node.Labels) > 2 {
			v2.Labels = node.Labels[:2]
		}
	}
	v2.Format(ctx)
}

// formatShortenConstants formats tuples containing only literals or
// placeholders and longer than 3 elements as a tuple of its first two elements
// and the last one. For example:
//
//	(1, 2)             -> (1, 2)
//	(1, 2, 3)          -> (1, 2, 3)
//	(1, 2, 3, 4)       -> (1, 2, __more1_10__, 4)
//	(1, 2, 3, ..., 20) -> (1, 2, __more10_100__, 20)
func (node *Tuple) formatShortenConstants(ctx *FmtCtx) {
	if len(node.Exprs) <= numElementsForShortenedList {
		node.Format(ctx)
		return
	}
	// Determine if there are only literals/placeholders and use the special
	// representation if so.
	for _, expr := range node.Exprs {
		switch expr.(type) {
		case Datum, Constant, *Placeholder:
			continue
		}
		node.Format(ctx)
		return
	}
	// We copy the node to preserve the "row" boolean flag.
	v2 := *node
	v2.Exprs = append(make(Exprs, 0, numElementsForShortenedList+1), v2.Exprs[:numElementsForShortenedList-1]...)
	v2.Exprs = append(v2.Exprs, arityIndicator(len(node.Exprs)-numElementsForShortenedList))
	v2.Exprs = append(v2.Exprs, node.Exprs[len(node.Exprs)-1])
	if len(node.Labels) > numElementsForShortenedList-1 {
		v2.Labels = node.Labels[:numElementsForShortenedList-1]
	}
	v2.Format(ctx)
}

// formatShortenConstants formats the tuple containing more than 3 datums to
// print out only first two and the last one. For example,
//
//	(1, 2)             -> (1, 2)
//	(1, 2, 3)          -> (1, 2, 3)
//	(1, 2, 3, 4)       -> (1, 2, __more1_10__, 4)
//	(1, 2, 3, ..., 20) -> (1, 2, __more10_100__, 20)
func (node *DTuple) formatShortenConstants(ctx *FmtCtx) {
	if len(node.D) <= numElementsForShortenedList {
		node.Format(ctx)
		return
	}
	ctx.WriteByte('(')
	for i := 0; i < numElementsForShortenedList-1; i++ {
		ctx.FormatNode(node.D[i])
		ctx.WriteString(", ")
	}
	ctx.Printf("%s, ", arityIndicator(len(node.D)-numElementsForShortenedList))
	ctx.FormatNode(node.D[len(node.D)-1])
	ctx.WriteByte(')')
}

// formatHideConstants formats array expressions containing only literals or
// placeholders and longer than 1 element as an array expression of its first
// two elements, scrubbed. For example:
//
//	array[1]             -> array[_]
//	array[1, 2]          -> array[_,_]
//	array[1, 2, 3]       -> array[_,_,__more1_10__]
//	array[1+2, 2+3, 3+4] -> array[_ + _,_ + _,_ + _]
func (node *Array) formatHideConstants(ctx *FmtCtx) {
	if len(node.Exprs) < 2 {
		node.Format(ctx)
		return
	}
	// Determine if there are only literals/placeholders and use the special
	// representation if so.
	for _, expr := range node.Exprs {
		switch expr.(type) {
		case Datum, Constant, *Placeholder:
			continue
		}
		node.Format(ctx)
		return
	}
	v2 := *node
	v2.Exprs = append(make(Exprs, 0, 3), v2.Exprs[:2]...)
	if len(node.Exprs) > 2 {
		v2.Exprs = append(v2.Exprs, arityIndicator(len(node.Exprs)-2))
	}
	v2.Format(ctx)
}

// formatShortenConstants formats array expressions containing only literals or
// placeholders and longer than 3 elements as an array of its first two elements
// and the last one. For example:
//
//	array[1]                -> array[1]
//	array[1, 2]             -> array[1,2]
//	array[1, 2, 3]          -> array[1,2,3]
//	array[1, 2, 3, 4]       -> array[1,2,__more1_10__,4]
//	array[1, 2, 3, ..., 20] -> array[1,2,__more10_100__,20]
func (node *Array) formatShortenConstants(ctx *FmtCtx) {
	if len(node.Exprs) <= numElementsForShortenedList {
		node.Format(ctx)
		return
	}
	// Determine if there are only literals/placeholders and use the special
	// representation if so.
	for _, expr := range node.Exprs {
		switch expr.(type) {
		case Datum, Constant, *Placeholder:
			continue
		}
		node.Format(ctx)
		return
	}
	v2 := *node
	v2.Exprs = append(make(Exprs, 0, numElementsForShortenedList+1), v2.Exprs[:numElementsForShortenedList-1]...)
	v2.Exprs = append(v2.Exprs, arityIndicator(len(node.Exprs)-numElementsForShortenedList))
	v2.Exprs = append(v2.Exprs, node.Exprs[len(node.Exprs)-1])
	v2.Format(ctx)
}

// formatShortenConstants formats arrays longer than 3 elements as an array of
// its first two elements and the last one. For example:
//
//	array[1]                -> array[1]
//	array[1, 2]             -> array[1,2]
//	array[1, 2, 3]          -> array[1,2,3]
//	array[1, 2, 3, 4]       -> array[1,2,__more1_10__,4]
//	array[1, 2, 3, ..., 20] -> array[1,2,__more10_100__,20]
func (node *DArray) formatShortenConstants(ctx *FmtCtx) {
	if len(node.Array) <= numElementsForShortenedList {
		node.Format(ctx)
		return
	}
	ctx.WriteString(`ARRAY[`)
	for i := 0; i < numElementsForShortenedList-1; i++ {
		ctx.FormatNode(node.Array[i])
		ctx.WriteByte(',')
	}
	ctx.Printf("%s,", arityIndicator(len(node.Array)-numElementsForShortenedList))
	ctx.FormatNode(node.Array[len(node.Array)-1])
	ctx.WriteByte(']')
}

func arityIndicator(n int) Expr {
	return NewUnresolvedName(arityString(n))
}

// arityIndicatorNoBounds is like arityIndicator, but it does not include the
// bounds in the name. It's used when we don't care about differentiating
// between different bounds, such as in formatting the statement as a
// representative query for fingerprint generation.
func arityIndicatorNoBounds() Expr {
	return NewUnresolvedName(genericArityIndicator)
}

func arityString(n int) string {
	var v int
	if n <= 10 {
		return "__more1_10__"
	}
	if n <= 100 {
		return "__more10_100__"
	}
	if n <= 1000 {
		for v = 1; n >= 10; n /= 10 {
			v = v * 10
		}
		v = v * n
		return fmt.Sprintf("__more%d__", v)
	}
	return "__more1000_plus__"
}

func isArityIndicatorString(s string) bool {
	return strings.HasPrefix(s, "__more")
}
