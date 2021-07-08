// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tree

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/cockroach/pkg/util/pretty"
	"github.com/cockroachdb/errors"
)

// This file contains methods that convert statements to pretty Docs (a tree
// structure that can be pretty printed at a specific line width). Nodes
// implement the docer interface to allow this conversion. In general,
// a node implements doc by copying its Format method and returning a Doc
// structure instead of writing to a buffer. Some guidelines are below.
//
// Nodes should not precede themselves with a space. Instead, the parent
// structure should correctly add spaces when needed.
//
// nestName should be used for most `KEYWORD <expr>` constructs.
//
// Nodes that never need to line break or for which the Format method already
// produces a compact representation should not implement doc, but instead
// rely on the default fallback that uses .Format. Examples include datums
// and constants.

// PrettyCfg holds configuration for pretty printing statements.
type PrettyCfg struct {
	// LineWidth is the desired maximum line width.
	LineWidth int
	// TabWidth is the amount of spaces to use for tabs when UseTabs is
	// false.
	TabWidth int
	// Align, when set to another value than PrettyNoAlign, uses
	// alignment for some constructs as a first choice. If not set or if
	// the line width is insufficient, nesting is used instead.
	Align PrettyAlignMode
	// UseTabs indicates whether to use tab chars to signal indentation.
	UseTabs bool
	// Simplify, when set, removes extraneous parentheses.
	Simplify bool
	// Case, if set, transforms case-insensitive strings (like SQL keywords).
	Case func(string) string
	// JSONFmt, when set, pretty-prints strings that are asserted or cast
	// to JSON.
	JSONFmt bool
}

// DefaultPrettyCfg returns a PrettyCfg with the default
// configuration.
func DefaultPrettyCfg() PrettyCfg {
	return PrettyCfg{
		LineWidth: 60,
		Simplify:  true,
		TabWidth:  4,
		UseTabs:   true,
		Align:     PrettyNoAlign, // TODO(knz): I really want this to be AlignAndDeindent
	}
}

// PrettyAlignMode directs which alignment mode to use.
//
// TODO(knz/mjibson): this variety of options currently exists so as
// to enable comparisons and gauging individual preferences. We should
// aim to remove some or all of these options in the future.
type PrettyAlignMode int

const (
	// PrettyNoAlign disables alignment.
	PrettyNoAlign PrettyAlignMode = 0
	// PrettyAlignOnly aligns sub-clauses only and preserves the
	// hierarchy of logical operators.
	PrettyAlignOnly = 1
	// PrettyAlignAndDeindent does the work of PrettyAlignOnly and also
	// de-indents AND and OR operators.
	PrettyAlignAndDeindent = 2
	// PrettyAlignAndExtraIndent does the work of PrettyAlignOnly and
	// also extra indents the operands of AND and OR operators so
	// that they appear aligned but also indented.
	PrettyAlignAndExtraIndent = 3
)

// keywordWithText returns a pretty.Keyword with left and/or right
// sides concat'd as a pretty.Text.
func (p *PrettyCfg) keywordWithText(left, keyword, right string) pretty.Doc {
	doc := pretty.Keyword(keyword)
	if left != "" {
		doc = pretty.Concat(pretty.Text(left), doc)
	}
	if right != "" {
		doc = pretty.Concat(doc, pretty.Text(right))
	}
	return doc
}

func (p *PrettyCfg) bracket(l string, d pretty.Doc, r string) pretty.Doc {
	return p.bracketDoc(pretty.Text(l), d, pretty.Text(r))
}

func (p *PrettyCfg) bracketDoc(l, d, r pretty.Doc) pretty.Doc {
	return pretty.BracketDoc(l, d, r)
}

func (p *PrettyCfg) bracketKeyword(
	leftKeyword, leftParen string, inner pretty.Doc, rightParen, rightKeyword string,
) pretty.Doc {
	var left, right pretty.Doc
	if leftKeyword != "" {
		left = p.keywordWithText("", leftKeyword, leftParen)
	} else {
		left = pretty.Text(leftParen)
	}
	if rightKeyword != "" {
		right = p.keywordWithText(rightParen, rightKeyword, "")
	} else {
		right = pretty.Text(rightParen)
	}
	return p.bracketDoc(left, inner, right)
}

// Pretty pretty prints stmt with default options.
func Pretty(stmt NodeFormatter) string {
	cfg := DefaultPrettyCfg()
	return cfg.Pretty(stmt)
}

// Pretty pretty prints stmt with specified options.
func (p *PrettyCfg) Pretty(stmt NodeFormatter) string {
	doc := p.Doc(stmt)
	return pretty.Pretty(doc, p.LineWidth, p.UseTabs, p.TabWidth, p.Case)
}

// Doc converts f (generally a Statement) to a pretty.Doc. If f does not have a
// native conversion, its .Format representation is used as a simple Text Doc.
func (p *PrettyCfg) Doc(f NodeFormatter) pretty.Doc {
	if f, ok := f.(docer); ok {
		doc := f.doc(p)
		return doc
	}
	return p.docAsString(f)
}

func (p *PrettyCfg) docAsString(f NodeFormatter) pretty.Doc {
	const prettyFlags = FmtShowPasswords | FmtParsable
	txt := AsStringWithFlags(f, prettyFlags)
	return pretty.Text(strings.TrimSpace(txt))
}

func (p *PrettyCfg) nestUnder(a, b pretty.Doc) pretty.Doc {
	if p.Align != PrettyNoAlign {
		return pretty.AlignUnder(a, b)
	}
	return pretty.NestUnder(a, b)
}

func (p *PrettyCfg) rlTable(rows ...pretty.TableRow) pretty.Doc {
	alignment := pretty.TableNoAlign
	if p.Align != PrettyNoAlign {
		alignment = pretty.TableRightAlignFirstColumn
	}
	return pretty.Table(alignment, pretty.Keyword, rows...)
}

func (p *PrettyCfg) llTable(docFn func(string) pretty.Doc, rows ...pretty.TableRow) pretty.Doc {
	alignment := pretty.TableNoAlign
	if p.Align != PrettyNoAlign {
		alignment = pretty.TableLeftAlignFirstColumn
	}
	return pretty.Table(alignment, docFn, rows...)
}

func (p *PrettyCfg) row(lbl string, d pretty.Doc) pretty.TableRow {
	return pretty.TableRow{Label: lbl, Doc: d}
}

var emptyRow = pretty.TableRow{}

func (p *PrettyCfg) unrow(r pretty.TableRow) pretty.Doc {
	if r.Doc == nil {
		return pretty.Nil
	}
	if r.Label == "" {
		return r.Doc
	}
	return p.nestUnder(pretty.Text(r.Label), r.Doc)
}

func (p *PrettyCfg) commaSeparated(d ...pretty.Doc) pretty.Doc {
	return pretty.Join(",", d...)
}

func (p *PrettyCfg) joinNestedOuter(lbl string, d ...pretty.Doc) pretty.Doc {
	if len(d) == 0 {
		return pretty.Nil
	}
	switch p.Align {
	case PrettyAlignAndDeindent:
		return pretty.JoinNestedOuter(lbl, pretty.Keyword, d...)
	case PrettyAlignAndExtraIndent:
		items := make([]pretty.TableRow, len(d))
		for i, dd := range d {
			if i > 0 {
				items[i].Label = lbl
			}
			items[i].Doc = dd
		}
		return pretty.Table(pretty.TableRightAlignFirstColumn, pretty.Keyword, items...)
	default:
		return pretty.JoinNestedRight(pretty.Keyword(lbl), d...)
	}
}

// docer is implemented by nodes that can convert themselves into
// pretty.Docs. If nodes cannot, node.Format is used instead as a Text Doc.
type docer interface {
	doc(*PrettyCfg) pretty.Doc
}

// tableDocer is implemented by nodes that can convert themselves
// into []pretty.TableRow, i.e. a table.
type tableDocer interface {
	docTable(*PrettyCfg) []pretty.TableRow
}

func (node SelectExprs) doc(p *PrettyCfg) pretty.Doc {
	d := make([]pretty.Doc, len(node))
	for i, e := range node {
		d[i] = e.doc(p)
	}
	return p.commaSeparated(d...)
}

func (node SelectExpr) doc(p *PrettyCfg) pretty.Doc {
	e := node.Expr
	if p.Simplify {
		e = StripParens(e)
	}
	d := p.Doc(e)
	if node.As != "" {
		d = p.nestUnder(
			d,
			pretty.Concat(p.keywordWithText("", "AS", " "), p.Doc(&node.As)),
		)
	}
	return d
}

func (node TableExprs) doc(p *PrettyCfg) pretty.Doc {
	if len(node) == 0 {
		return pretty.Nil
	}
	d := make([]pretty.Doc, len(node))
	for i, e := range node {
		if p.Simplify {
			e = StripTableParens(e)
		}
		d[i] = p.Doc(e)
	}
	return p.commaSeparated(d...)
}

func (node *Where) doc(p *PrettyCfg) pretty.Doc {
	return p.unrow(node.docRow(p))
}

func (node *Where) docRow(p *PrettyCfg) pretty.TableRow {
	if node == nil {
		return emptyRow
	}
	e := node.Expr
	if p.Simplify {
		e = StripParens(e)
	}
	return p.row(node.Type, p.Doc(e))
}

func (node *GroupBy) doc(p *PrettyCfg) pretty.Doc {
	return p.unrow(node.docRow(p))
}

func (node *GroupBy) docRow(p *PrettyCfg) pretty.TableRow {
	if len(*node) == 0 {
		return emptyRow
	}
	d := make([]pretty.Doc, len(*node))
	for i, e := range *node {
		// Beware! The GROUP BY items should never be simplified by
		// stripping parentheses, because parentheses there are
		// semantically important.
		d[i] = p.Doc(e)
	}
	return p.row("GROUP BY", p.commaSeparated(d...))
}

// flattenOp populates a slice with all the leaves operands of an expression
// tree where all the nodes satisfy the given predicate.
func (p *PrettyCfg) flattenOp(
	e Expr,
	pred func(e Expr, recurse func(e Expr)) bool,
	formatOperand func(e Expr) pretty.Doc,
	in []pretty.Doc,
) []pretty.Doc {
	if ok := pred(e, func(sub Expr) {
		in = p.flattenOp(sub, pred, formatOperand, in)
	}); ok {
		return in
	}
	return append(in, formatOperand(e))
}

func (p *PrettyCfg) peelAndOrOperand(e Expr) Expr {
	if !p.Simplify {
		return e
	}
	stripped := StripParens(e)
	switch stripped.(type) {
	case *BinaryExpr, *ComparisonExpr, *RangeCond, *FuncExpr, *IndirectionExpr,
		*UnaryExpr, *AnnotateTypeExpr, *CastExpr, *ColumnItem, *UnresolvedName:
		// All these expressions have higher precedence than binary
		// expressions.
		return stripped
	}
	// Everything else - we don't know. Be conservative and keep the
	// original form.
	return e
}

func (node *AndExpr) doc(p *PrettyCfg) pretty.Doc {
	pred := func(e Expr, recurse func(e Expr)) bool {
		if a, ok := e.(*AndExpr); ok {
			recurse(a.Left)
			recurse(a.Right)
			return true
		}
		return false
	}
	formatOperand := func(e Expr) pretty.Doc {
		return p.Doc(p.peelAndOrOperand(e))
	}
	operands := p.flattenOp(node.Left, pred, formatOperand, nil)
	operands = p.flattenOp(node.Right, pred, formatOperand, operands)
	return p.joinNestedOuter("AND", operands...)
}

func (node *OrExpr) doc(p *PrettyCfg) pretty.Doc {
	pred := func(e Expr, recurse func(e Expr)) bool {
		if a, ok := e.(*OrExpr); ok {
			recurse(a.Left)
			recurse(a.Right)
			return true
		}
		return false
	}
	formatOperand := func(e Expr) pretty.Doc {
		return p.Doc(p.peelAndOrOperand(e))
	}
	operands := p.flattenOp(node.Left, pred, formatOperand, nil)
	operands = p.flattenOp(node.Right, pred, formatOperand, operands)
	return p.joinNestedOuter("OR", operands...)
}

func (node *Exprs) doc(p *PrettyCfg) pretty.Doc {
	if node == nil || len(*node) == 0 {
		return pretty.Nil
	}
	d := make([]pretty.Doc, len(*node))
	for i, e := range *node {
		if p.Simplify {
			e = StripParens(e)
		}
		d[i] = p.Doc(e)
	}
	return p.commaSeparated(d...)
}

// peelBinaryOperand conditionally (p.Simplify) removes the
// parentheses around an expression. The parentheses are always
// removed in the following conditions:
// - if the operand is a unary operator (these are always
//   of higher precedence): "(-a) * b" -> "-a * b"
// - if the operand is a binary operator and its precedence
//   is guaranteed to be higher: "(a * b) + c" -> "a * b + c"
//
// Additionally, iff sameLevel is set, then parentheses are removed
// around any binary operator that has the same precedence level as
// the parent.
// sameLevel can be set:
//
// - for the left operand of all binary expressions, because
//   (in pg SQL) all binary expressions are left-associative.
//   This rewrites e.g. "(a + b) - c" -> "a + b - c"
//   and "(a - b) + c" -> "a - b + c"
// - for the right operand when the parent operator is known
//   to be fully associative, e.g.
//   "a + (b - c)" -> "a + b - c" because "+" is fully assoc,
//   but "a - (b + c)" cannot be simplified because "-" is not fully associative.
//
func (p *PrettyCfg) peelBinaryOperand(e Expr, sameLevel bool, parenPrio int) Expr {
	if !p.Simplify {
		return e
	}
	stripped := StripParens(e)
	switch te := stripped.(type) {
	case *BinaryExpr:
		childPrio := binaryOpPrio[te.Operator.Symbol]
		if childPrio < parenPrio || (sameLevel && childPrio == parenPrio) {
			return stripped
		}
	case *FuncExpr, *UnaryExpr, *AnnotateTypeExpr, *IndirectionExpr,
		*CastExpr, *ColumnItem, *UnresolvedName:
		// All these expressions have higher precedence than binary expressions.
		return stripped
	}
	// Everything else - we don't know. Be conservative and keep the
	// original form.
	return e
}

func (node *BinaryExpr) doc(p *PrettyCfg) pretty.Doc {
	// All the binary operators are at least left-associative.
	// So we can always simplify "(a OP b) OP c" to "a OP b OP c".
	parenPrio := binaryOpPrio[node.Operator.Symbol]
	leftOperand := p.peelBinaryOperand(node.Left, true /*sameLevel*/, parenPrio)
	// If the binary operator is also fully associative,
	// we can also simplify "a OP (b OP c)" to "a OP b OP c".
	opFullyAssoc := binaryOpFullyAssoc[node.Operator.Symbol]
	rightOperand := p.peelBinaryOperand(node.Right, opFullyAssoc, parenPrio)

	opDoc := pretty.Text(node.Operator.String())
	var res pretty.Doc
	if !node.Operator.Symbol.isPadded() {
		res = pretty.JoinDoc(opDoc, p.Doc(leftOperand), p.Doc(rightOperand))
	} else {
		pred := func(e Expr, recurse func(e Expr)) bool {
			if b, ok := e.(*BinaryExpr); ok && b.Operator == node.Operator {
				leftSubOperand := p.peelBinaryOperand(b.Left, true /*sameLevel*/, parenPrio)
				rightSubOperand := p.peelBinaryOperand(b.Right, opFullyAssoc, parenPrio)
				recurse(leftSubOperand)
				recurse(rightSubOperand)
				return true
			}
			return false
		}
		formatOperand := func(e Expr) pretty.Doc {
			return p.Doc(e)
		}
		operands := p.flattenOp(leftOperand, pred, formatOperand, nil)
		operands = p.flattenOp(rightOperand, pred, formatOperand, operands)
		res = pretty.JoinNestedRight(
			opDoc, operands...)
	}
	return pretty.Group(res)
}

func (node *ParenExpr) doc(p *PrettyCfg) pretty.Doc {
	return p.bracket("(", p.Doc(node.Expr), ")")
}

func (node *ParenSelect) doc(p *PrettyCfg) pretty.Doc {
	return p.bracket("(", p.Doc(node.Select), ")")
}

func (node *ParenTableExpr) doc(p *PrettyCfg) pretty.Doc {
	return p.bracket("(", p.Doc(node.Expr), ")")
}

func (node *Limit) doc(p *PrettyCfg) pretty.Doc {
	res := pretty.Nil
	for i, r := range node.docTable(p) {
		if r.Doc != nil {
			if i > 0 {
				res = pretty.Concat(res, pretty.Line)
			}
			res = pretty.Concat(res, p.nestUnder(pretty.Text(r.Label), r.Doc))
		}
	}
	return res
}

func (node *Limit) docTable(p *PrettyCfg) []pretty.TableRow {
	if node == nil {
		return nil
	}
	res := make([]pretty.TableRow, 0, 2)
	if node.Count != nil {
		e := node.Count
		if p.Simplify {
			e = StripParens(e)
		}
		res = append(res, p.row("LIMIT", p.Doc(e)))
	} else if node.LimitAll {
		res = append(res, p.row("LIMIT", pretty.Keyword("ALL")))
	}
	if node.Offset != nil {
		e := node.Offset
		if p.Simplify {
			e = StripParens(e)
		}
		res = append(res, p.row("OFFSET", p.Doc(e)))
	}
	return res
}

func (node *OrderBy) doc(p *PrettyCfg) pretty.Doc {
	return p.unrow(node.docRow(p))
}

func (node *OrderBy) docRow(p *PrettyCfg) pretty.TableRow {
	if node == nil || len(*node) == 0 {
		return emptyRow
	}
	d := make([]pretty.Doc, len(*node))
	for i, e := range *node {
		// Beware! The ORDER BY items should never be simplified,
		// because parentheses there are semantically important.
		d[i] = p.Doc(e)
	}
	return p.row("ORDER BY", p.commaSeparated(d...))
}

func (node *Select) doc(p *PrettyCfg) pretty.Doc {
	return p.rlTable(node.docTable(p)...)
}

func (node *Select) docTable(p *PrettyCfg) []pretty.TableRow {
	items := make([]pretty.TableRow, 0, 9)
	items = append(items, node.With.docRow(p))
	if s, ok := node.Select.(tableDocer); ok {
		items = append(items, s.docTable(p)...)
	} else {
		items = append(items, p.row("", p.Doc(node.Select)))
	}
	items = append(items, node.OrderBy.docRow(p))
	items = append(items, node.Limit.docTable(p)...)
	items = append(items, node.Locking.docTable(p)...)
	return items
}

func (node *SelectClause) doc(p *PrettyCfg) pretty.Doc {
	return p.rlTable(node.docTable(p)...)
}

func (node *SelectClause) docTable(p *PrettyCfg) []pretty.TableRow {
	if node.TableSelect {
		return []pretty.TableRow{p.row("TABLE", p.Doc(node.From.Tables[0]))}
	}
	exprs := node.Exprs.doc(p)
	if node.Distinct {
		if node.DistinctOn != nil {
			exprs = pretty.ConcatLine(p.Doc(&node.DistinctOn), exprs)
		} else {
			exprs = pretty.ConcatLine(pretty.Keyword("DISTINCT"), exprs)
		}
	}
	return []pretty.TableRow{
		p.row("SELECT", exprs),
		node.From.docRow(p),
		node.Where.docRow(p),
		node.GroupBy.docRow(p),
		node.Having.docRow(p),
		node.Window.docRow(p),
	}
}

func (node *From) doc(p *PrettyCfg) pretty.Doc {
	return p.unrow(node.docRow(p))
}

func (node *From) docRow(p *PrettyCfg) pretty.TableRow {
	if node == nil || len(node.Tables) == 0 {
		return emptyRow
	}
	d := node.Tables.doc(p)
	if node.AsOf.Expr != nil {
		d = p.nestUnder(
			d,
			p.Doc(&node.AsOf),
		)
	}
	return p.row("FROM", d)
}

func (node *Window) doc(p *PrettyCfg) pretty.Doc {
	return p.unrow(node.docRow(p))
}

func (node *Window) docRow(p *PrettyCfg) pretty.TableRow {
	if node == nil || len(*node) == 0 {
		return emptyRow
	}
	d := make([]pretty.Doc, len(*node))
	for i, e := range *node {
		d[i] = pretty.Fold(pretty.Concat,
			pretty.Text(e.Name.String()),
			p.keywordWithText(" ", "AS", " "),
			p.Doc(e),
		)
	}
	return p.row("WINDOW", p.commaSeparated(d...))
}

func (node *With) doc(p *PrettyCfg) pretty.Doc {
	return p.unrow(node.docRow(p))
}

func (node *With) docRow(p *PrettyCfg) pretty.TableRow {
	if node == nil {
		return emptyRow
	}
	d := make([]pretty.Doc, len(node.CTEList))
	for i, cte := range node.CTEList {
		asString := "AS"
		if cte.Mtr.Set {
			if !cte.Mtr.Materialize {
				asString += " NOT"
			}
			asString += " MATERIALIZED"
		}
		d[i] = p.nestUnder(
			p.Doc(&cte.Name),
			p.bracketKeyword(asString, " (", p.Doc(cte.Stmt), ")", ""),
		)
	}
	kw := "WITH"
	if node.Recursive {
		kw = "WITH RECURSIVE"
	}
	return p.row(kw, p.commaSeparated(d...))
}

func (node *Subquery) doc(p *PrettyCfg) pretty.Doc {
	d := pretty.Text("<unknown>")
	if node.Select != nil {
		d = p.Doc(node.Select)
	}
	if node.Exists {
		d = pretty.Concat(
			pretty.Keyword("EXISTS"),
			d,
		)
	}
	return d
}

func (node *AliasedTableExpr) doc(p *PrettyCfg) pretty.Doc {
	d := p.Doc(node.Expr)
	if node.Lateral {
		d = pretty.Concat(
			p.keywordWithText("", "LATERAL", " "),
			d,
		)
	}
	if node.IndexFlags != nil {
		d = pretty.Concat(
			d,
			p.Doc(node.IndexFlags),
		)
	}
	if node.Ordinality {
		d = pretty.Concat(
			d,
			p.keywordWithText(" ", "WITH ORDINALITY", ""),
		)
	}
	if node.As.Alias != "" {
		d = p.nestUnder(
			d,
			pretty.Concat(
				p.keywordWithText("", "AS", " "),
				p.Doc(&node.As),
			),
		)
	}
	return d
}

func (node *FuncExpr) doc(p *PrettyCfg) pretty.Doc {
	d := p.Doc(&node.Func)

	if len(node.Exprs) > 0 {
		args := node.Exprs.doc(p)
		if node.Type != 0 {
			args = pretty.ConcatLine(
				pretty.Text(funcTypeName[node.Type]),
				args,
			)
		}

		if node.AggType == GeneralAgg && len(node.OrderBy) > 0 {
			args = pretty.ConcatSpace(args, node.OrderBy.doc(p))
		}
		d = pretty.Concat(d, p.bracket("(", args, ")"))
	} else {
		d = pretty.Concat(d, pretty.Text("()"))
	}
	if node.AggType == OrderedSetAgg && len(node.OrderBy) > 0 {
		args := node.OrderBy.doc(p)
		d = pretty.Concat(d, p.bracket("WITHIN GROUP (", args, ")"))
	}
	if node.Filter != nil {
		d = pretty.Fold(pretty.ConcatSpace,
			d,
			pretty.Keyword("FILTER"),
			p.bracket("(",
				p.nestUnder(pretty.Keyword("WHERE"), p.Doc(node.Filter)),
				")"))
	}
	if window := node.WindowDef; window != nil {
		var over pretty.Doc
		if window.Name != "" {
			over = p.Doc(&window.Name)
		} else {
			over = p.Doc(window)
		}
		d = pretty.Fold(pretty.ConcatSpace,
			d,
			pretty.Keyword("OVER"),
			over,
		)
	}
	return d
}

func (node *WindowDef) doc(p *PrettyCfg) pretty.Doc {
	rows := make([]pretty.TableRow, 0, 4)
	if node.RefName != "" {
		rows = append(rows, p.row("", p.Doc(&node.RefName)))
	}
	if len(node.Partitions) > 0 {
		rows = append(rows, p.row("PARTITION BY", p.Doc(&node.Partitions)))
	}
	if len(node.OrderBy) > 0 {
		rows = append(rows, node.OrderBy.docRow(p))
	}
	if node.Frame != nil {
		rows = append(rows, node.Frame.docRow(p))
	}
	if len(rows) == 0 {
		return pretty.Text("()")
	}
	return p.bracket("(", p.rlTable(rows...), ")")
}

func (wf *WindowFrame) docRow(p *PrettyCfg) pretty.TableRow {
	kw := "RANGE"
	if wf.Mode == ROWS {
		kw = "ROWS"
	} else if wf.Mode == GROUPS {
		kw = "GROUPS"
	}
	d := p.Doc(wf.Bounds.StartBound)
	if wf.Bounds.EndBound != nil {
		d = p.rlTable(
			p.row("BETWEEN", d),
			p.row("AND", p.Doc(wf.Bounds.EndBound)),
		)
	}
	if wf.Exclusion != NoExclusion {
		d = pretty.Stack(d, p.Doc(wf.Exclusion))
	}
	return p.row(kw, d)
}

func (node *WindowFrameBound) doc(p *PrettyCfg) pretty.Doc {
	switch node.BoundType {
	case UnboundedPreceding:
		return pretty.Keyword("UNBOUNDED PRECEDING")
	case OffsetPreceding:
		return pretty.ConcatSpace(p.Doc(node.OffsetExpr), pretty.Keyword("PRECEDING"))
	case CurrentRow:
		return pretty.Keyword("CURRENT ROW")
	case OffsetFollowing:
		return pretty.ConcatSpace(p.Doc(node.OffsetExpr), pretty.Keyword("FOLLOWING"))
	case UnboundedFollowing:
		return pretty.Keyword("UNBOUNDED FOLLOWING")
	default:
		panic(errors.AssertionFailedf("unexpected type %d", errors.Safe(node.BoundType)))
	}
}

func (node *LockingClause) doc(p *PrettyCfg) pretty.Doc {
	return p.rlTable(node.docTable(p)...)
}

func (node *LockingClause) docTable(p *PrettyCfg) []pretty.TableRow {
	items := make([]pretty.TableRow, len(*node))
	for i, n := range *node {
		items[i] = p.row("", p.Doc(n))
	}
	return items
}

func (node *LockingItem) doc(p *PrettyCfg) pretty.Doc {
	return p.rlTable(node.docTable(p)...)
}

func (node *LockingItem) docTable(p *PrettyCfg) []pretty.TableRow {
	if node.Strength == ForNone {
		return nil
	}
	items := make([]pretty.TableRow, 0, 3)
	items = append(items, node.Strength.docTable(p)...)
	if len(node.Targets) > 0 {
		items = append(items, p.row("OF", p.Doc(&node.Targets)))
	}
	items = append(items, node.WaitPolicy.docTable(p)...)
	return items
}

func (node LockingStrength) doc(p *PrettyCfg) pretty.Doc {
	return p.rlTable(node.docTable(p)...)
}

func (node LockingStrength) docTable(p *PrettyCfg) []pretty.TableRow {
	str := node.String()
	if str == "" {
		return nil
	}
	return []pretty.TableRow{p.row("", pretty.Keyword(str))}
}

func (node LockingWaitPolicy) doc(p *PrettyCfg) pretty.Doc {
	return p.rlTable(node.docTable(p)...)
}

func (node LockingWaitPolicy) docTable(p *PrettyCfg) []pretty.TableRow {
	str := node.String()
	if str == "" {
		return nil
	}
	return []pretty.TableRow{p.row("", pretty.Keyword(str))}
}

func (p *PrettyCfg) peelCompOperand(e Expr) Expr {
	if !p.Simplify {
		return e
	}
	stripped := StripParens(e)
	switch stripped.(type) {
	case *FuncExpr, *IndirectionExpr, *UnaryExpr,
		*AnnotateTypeExpr, *CastExpr, *ColumnItem, *UnresolvedName:
		return stripped
	}
	return e
}

func (node *ComparisonExpr) doc(p *PrettyCfg) pretty.Doc {
	opStr := node.Operator.String()
	// IS and IS NOT are equivalent to IS NOT DISTINCT FROM and IS DISTINCT
	// FROM, respectively, when the RHS is true or false. We prefer the less
	// verbose IS and IS NOT in those cases.
	if node.Operator.Symbol == IsDistinctFrom && (node.Right == DBoolTrue || node.Right == DBoolFalse) {
		opStr = "IS NOT"
	} else if node.Operator.Symbol == IsNotDistinctFrom && (node.Right == DBoolTrue || node.Right == DBoolFalse) {
		opStr = "IS"
	}
	opDoc := pretty.Keyword(opStr)
	if node.Operator.Symbol.HasSubOperator() {
		opDoc = pretty.ConcatSpace(pretty.Text(node.SubOperator.String()), opDoc)
	}
	return pretty.Group(
		pretty.JoinNestedRight(
			opDoc,
			p.Doc(p.peelCompOperand(node.Left)),
			p.Doc(p.peelCompOperand(node.Right))))
}

func (node *AliasClause) doc(p *PrettyCfg) pretty.Doc {
	d := pretty.Text(node.Alias.String())
	if len(node.Cols) != 0 {
		d = p.nestUnder(d, p.bracket("(", p.Doc(&node.Cols), ")"))
	}
	return d
}

func (node *JoinTableExpr) doc(p *PrettyCfg) pretty.Doc {
	//  buf will contain the fully populated sequence of join keywords.
	var buf bytes.Buffer
	cond := pretty.Nil
	if _, isNatural := node.Cond.(NaturalJoinCond); isNatural {
		// Natural joins have a different syntax:
		//   "<a> NATURAL <join_type> [<join_hint>] JOIN <b>"
		buf.WriteString("NATURAL ")
	} else {
		// Regular joins:
		//   "<a> <join type> [<join hint>] JOIN <b>"
		if node.Cond != nil {
			cond = p.Doc(node.Cond)
		}
	}

	if node.JoinType != "" {
		buf.WriteString(node.JoinType)
		buf.WriteByte(' ')
		if node.Hint != "" {
			buf.WriteString(node.Hint)
			buf.WriteByte(' ')
		}
	}
	buf.WriteString("JOIN")

	return p.joinNestedOuter(
		buf.String(),
		p.Doc(node.Left),
		pretty.ConcatSpace(p.Doc(node.Right), cond))
}

func (node *OnJoinCond) doc(p *PrettyCfg) pretty.Doc {
	e := node.Expr
	if p.Simplify {
		e = StripParens(e)
	}
	return p.nestUnder(pretty.Keyword("ON"), p.Doc(e))
}

func (node *Insert) doc(p *PrettyCfg) pretty.Doc {
	items := make([]pretty.TableRow, 0, 8)
	items = append(items, node.With.docRow(p))
	kw := "INSERT"
	if node.OnConflict.IsUpsertAlias() {
		kw = "UPSERT"
	}
	items = append(items, p.row(kw, pretty.Nil))

	into := p.Doc(node.Table)
	if node.Columns != nil {
		into = p.nestUnder(into, p.bracket("(", p.Doc(&node.Columns), ")"))
	}
	items = append(items, p.row("INTO", into))

	if node.DefaultValues() {
		items = append(items, p.row("", pretty.Keyword("DEFAULT VALUES")))
	} else {
		items = append(items, node.Rows.docTable(p)...)
	}

	if node.OnConflict != nil && !node.OnConflict.IsUpsertAlias() {
		cond := pretty.Nil
		if len(node.OnConflict.Columns) > 0 {
			cond = p.bracket("(", p.Doc(&node.OnConflict.Columns), ")")
		}
		items = append(items, p.row("ON CONFLICT", cond))
		if node.OnConflict.ArbiterPredicate != nil {
			items = append(items, p.row("WHERE", p.Doc(node.OnConflict.ArbiterPredicate)))
		}

		if node.OnConflict.DoNothing {
			items = append(items, p.row("DO", pretty.Keyword("NOTHING")))
		} else {
			items = append(items, p.row("DO",
				p.nestUnder(pretty.Keyword("UPDATE SET"), p.Doc(&node.OnConflict.Exprs))))
			if node.OnConflict.Where != nil {
				items = append(items, node.OnConflict.Where.docRow(p))
			}
		}
	}

	items = append(items, p.docReturning(node.Returning))
	return p.rlTable(items...)
}

func (node *NameList) doc(p *PrettyCfg) pretty.Doc {
	d := make([]pretty.Doc, len(*node))
	for i, n := range *node {
		d[i] = p.Doc(&n)
	}
	return p.commaSeparated(d...)
}

func (node *CastExpr) doc(p *PrettyCfg) pretty.Doc {
	typ := pretty.Text(node.Type.SQLString())

	switch node.SyntaxMode {
	case CastPrepend:
		// This is a special case for things like INTERVAL '1s'. These only work
		// with string constats; if the underlying expression was changed, we fall
		// back to the short syntax.
		if _, ok := node.Expr.(*StrVal); ok {
			return pretty.Fold(pretty.Concat,
				typ,
				pretty.Text(" "),
				p.Doc(node.Expr),
			)
		}
		fallthrough
	case CastShort:
		if typ, ok := GetStaticallyKnownType(node.Type); ok {
			switch typ.Family() {
			case types.JsonFamily:
				if sv, ok := node.Expr.(*StrVal); ok && p.JSONFmt {
					return p.jsonCast(sv, "::", typ)
				}
			}
		}
		return pretty.Fold(pretty.Concat,
			p.exprDocWithParen(node.Expr),
			pretty.Text("::"),
			typ,
		)
	default:
		if nTyp, ok := GetStaticallyKnownType(node.Type); ok && nTyp.Family() == types.CollatedStringFamily {
			// COLLATE clause needs to go after CAST expression, so create
			// equivalent string type without the locale to get name of string
			// type without the COLLATE.
			strTyp := types.MakeScalar(
				types.StringFamily,
				nTyp.Oid(),
				nTyp.Precision(),
				nTyp.Width(),
				"", /* locale */
			)
			typ = pretty.Text(strTyp.SQLString())
		}

		ret := pretty.Fold(pretty.Concat,
			pretty.Keyword("CAST"),
			p.bracket(
				"(",
				p.nestUnder(
					p.Doc(node.Expr),
					pretty.Concat(
						p.keywordWithText("", "AS", " "),
						typ,
					),
				),
				")",
			),
		)

		if nTyp, ok := GetStaticallyKnownType(node.Type); ok && nTyp.Family() == types.CollatedStringFamily {
			ret = pretty.Fold(pretty.ConcatSpace,
				ret,
				pretty.Keyword("COLLATE"),
				pretty.Text(nTyp.Locale()))
		}
		return ret
	}
}

func (node *ValuesClause) doc(p *PrettyCfg) pretty.Doc {
	return p.rlTable(node.docTable(p)...)
}

func (node *ValuesClause) docTable(p *PrettyCfg) []pretty.TableRow {
	d := make([]pretty.Doc, len(node.Rows))
	for i := range node.Rows {
		d[i] = p.bracket("(", p.Doc(&node.Rows[i]), ")")
	}
	return []pretty.TableRow{p.row("VALUES", p.commaSeparated(d...))}
}

func (node *StatementSource) doc(p *PrettyCfg) pretty.Doc {
	return p.bracket("[", p.Doc(node.Statement), "]")
}

func (node *RowsFromExpr) doc(p *PrettyCfg) pretty.Doc {
	if p.Simplify && len(node.Items) == 1 {
		return p.Doc(node.Items[0])
	}
	return p.bracketKeyword("ROWS FROM", " (", p.Doc(&node.Items), ")", "")
}

func (node *Array) doc(p *PrettyCfg) pretty.Doc {
	return p.bracketKeyword("ARRAY", "[", p.Doc(&node.Exprs), "]", "")
}

func (node *Tuple) doc(p *PrettyCfg) pretty.Doc {
	exprDoc := p.Doc(&node.Exprs)
	if len(node.Exprs) == 1 {
		exprDoc = pretty.Concat(exprDoc, pretty.Text(","))
	}
	d := p.bracket("(", exprDoc, ")")
	if len(node.Labels) > 0 {
		labels := make([]pretty.Doc, len(node.Labels))
		for i, n := range node.Labels {
			labels[i] = p.Doc((*Name)(&n))
		}
		d = p.bracket("(", pretty.Stack(
			d,
			p.nestUnder(pretty.Keyword("AS"), p.commaSeparated(labels...)),
		), ")")
	}
	return d
}

func (node *UpdateExprs) doc(p *PrettyCfg) pretty.Doc {
	d := make([]pretty.Doc, len(*node))
	for i, n := range *node {
		d[i] = p.Doc(n)
	}
	return p.commaSeparated(d...)
}

func (p *PrettyCfg) exprDocWithParen(e Expr) pretty.Doc {
	if _, ok := e.(operatorExpr); ok {
		return p.bracket("(", p.Doc(e), ")")
	}
	return p.Doc(e)
}

func (node *Update) doc(p *PrettyCfg) pretty.Doc {
	items := make([]pretty.TableRow, 8)
	items = append(items,
		node.With.docRow(p),
		p.row("UPDATE", p.Doc(node.Table)),
		p.row("SET", p.Doc(&node.Exprs)))
	if len(node.From) > 0 {
		items = append(items,
			p.row("FROM", p.Doc(&node.From)))
	}
	items = append(items,
		node.Where.docRow(p),
		node.OrderBy.docRow(p))
	items = append(items, node.Limit.docTable(p)...)
	items = append(items, p.docReturning(node.Returning))
	return p.rlTable(items...)
}

func (node *Delete) doc(p *PrettyCfg) pretty.Doc {
	items := make([]pretty.TableRow, 6)
	items = append(items,
		node.With.docRow(p),
		p.row("DELETE FROM", p.Doc(node.Table)),
		node.Where.docRow(p),
		node.OrderBy.docRow(p))
	items = append(items, node.Limit.docTable(p)...)
	items = append(items, p.docReturning(node.Returning))
	return p.rlTable(items...)
}

func (p *PrettyCfg) docReturning(node ReturningClause) pretty.TableRow {
	switch r := node.(type) {
	case *NoReturningClause:
		return p.row("", nil)
	case *ReturningNothing:
		return p.row("RETURNING", pretty.Keyword("NOTHING"))
	case *ReturningExprs:
		return p.row("RETURNING", p.Doc((*SelectExprs)(r)))
	default:
		panic(errors.AssertionFailedf("unhandled case: %T", node))
	}
}

func (node *Order) doc(p *PrettyCfg) pretty.Doc {
	var d pretty.Doc
	if node.OrderType == OrderByColumn {
		d = p.Doc(node.Expr)
	} else {
		if node.Index == "" {
			d = pretty.ConcatSpace(
				pretty.Keyword("PRIMARY KEY"),
				p.Doc(&node.Table),
			)
		} else {
			d = pretty.ConcatSpace(
				pretty.Keyword("INDEX"),
				pretty.Fold(pretty.Concat,
					p.Doc(&node.Table),
					pretty.Text("@"),
					p.Doc(&node.Index),
				),
			)
		}
	}
	if node.Direction != DefaultDirection {
		d = p.nestUnder(d, pretty.Text(node.Direction.String()))
	}
	if node.NullsOrder != DefaultNullsOrder {
		d = p.nestUnder(d, pretty.Text(node.NullsOrder.String()))
	}
	return d
}

func (node *UpdateExpr) doc(p *PrettyCfg) pretty.Doc {
	d := p.Doc(&node.Names)
	if node.Tuple {
		d = p.bracket("(", d, ")")
	}
	e := node.Expr
	if p.Simplify {
		e = StripParens(e)
	}
	return p.nestUnder(d, pretty.ConcatSpace(pretty.Text("="), p.Doc(e)))
}

func (node *CreateTable) doc(p *PrettyCfg) pretty.Doc {
	// Final layout:
	//
	// CREATE [TEMP | UNLOGGED] TABLE [IF NOT EXISTS] name ( .... ) [AS]
	//     [SELECT ...] - for CREATE TABLE AS
	//     [INTERLEAVE ...]
	//     [PARTITION BY ...]
	//
	title := pretty.Keyword("CREATE")
	switch node.Persistence {
	case PersistenceTemporary:
		title = pretty.ConcatSpace(title, pretty.Keyword("TEMPORARY"))
	case PersistenceUnlogged:
		title = pretty.ConcatSpace(title, pretty.Keyword("UNLOGGED"))
	}
	title = pretty.ConcatSpace(title, pretty.Keyword("TABLE"))
	if node.IfNotExists {
		title = pretty.ConcatSpace(title, pretty.Keyword("IF NOT EXISTS"))
	}
	title = pretty.ConcatSpace(title, p.Doc(&node.Table))

	if node.As() {
		if len(node.Defs) > 0 {
			title = pretty.ConcatSpace(title,
				p.bracket("(", p.Doc(&node.Defs), ")"))
		}
		title = pretty.ConcatSpace(title, pretty.Keyword("AS"))
	} else {
		title = pretty.ConcatSpace(title,
			p.bracket("(", p.Doc(&node.Defs), ")"),
		)
	}

	clauses := make([]pretty.Doc, 0, 4)
	if node.As() {
		clauses = append(clauses, p.Doc(node.AsSource))
	}
	if node.Interleave != nil {
		clauses = append(clauses, p.Doc(node.Interleave))
	}
	if node.PartitionByTable != nil {
		clauses = append(clauses, p.Doc(node.PartitionByTable))
	}
	if node.Locality != nil {
		clauses = append(clauses, p.Doc(node.Locality))
	}
	if len(clauses) == 0 {
		return title
	}
	return p.nestUnder(title, pretty.Group(pretty.Stack(clauses...)))
}

func (node *CreateView) doc(p *PrettyCfg) pretty.Doc {
	// Final layout:
	//
	// CREATE [TEMP] VIEW name ( ... ) AS
	//     SELECT ...
	//
	title := pretty.Keyword("CREATE")
	if node.Replace {
		title = pretty.ConcatSpace(title, pretty.Keyword("OR REPLACE"))
	}
	if node.Persistence == PersistenceTemporary {
		title = pretty.ConcatSpace(title, pretty.Keyword("TEMPORARY"))
	}
	if node.Materialized {
		title = pretty.ConcatSpace(title, pretty.Keyword("MATERIALIZED"))
	}
	title = pretty.ConcatSpace(title, pretty.Keyword("VIEW"))
	if node.IfNotExists {
		title = pretty.ConcatSpace(title, pretty.Keyword("IF NOT EXISTS"))
	}
	d := pretty.ConcatSpace(
		title,
		p.Doc(&node.Name),
	)
	if len(node.ColumnNames) > 0 {
		d = pretty.ConcatSpace(
			d,
			p.bracket("(", p.Doc(&node.ColumnNames), ")"),
		)
	}
	return p.nestUnder(
		pretty.ConcatSpace(d, pretty.Keyword("AS")),
		p.Doc(node.AsSource),
	)
}

func (node *TableDefs) doc(p *PrettyCfg) pretty.Doc {
	// This groups column definitions using a table to get alignment of
	// column names, and separately comma-joins groups of column definitions
	// with constraint definitions.

	defs := *node
	colDefRows := make([]pretty.TableRow, 0, len(defs))
	items := make([]pretty.Doc, 0, len(defs))

	for i := 0; i < len(defs); i++ {
		if _, ok := defs[i].(*ColumnTableDef); ok {
			// Group all the subsequent column definitions into a table.
			j := i
			colDefRows = colDefRows[:0]
			for ; j < len(defs); j++ {
				cdef, ok := defs[j].(*ColumnTableDef)
				if !ok {
					break
				}
				colDefRows = append(colDefRows, cdef.docRow(p))
			}
			// Let the outer loop pick up where we left.
			i = j - 1

			// At this point the column definitions form a table, but the comma
			// is missing from each row. We need to add it here. However we
			// need to be careful. Since we're going to add a comma between the
			// set of all column definitions and the other table definitions
			// below (via commaSeparated), we need to ensure the last row does
			// not get a comma.
			for j = 0; j < len(colDefRows)-1; j++ {
				colDefRows[j].Doc = pretty.Concat(colDefRows[j].Doc, pretty.Text(","))
			}
			items = append(items, p.llTable(pretty.Text, colDefRows...))
		} else {
			// Not a column definition, just process normally.
			items = append(items, p.Doc(defs[i]))
		}
	}

	return p.commaSeparated(items...)
}

func (node *CaseExpr) doc(p *PrettyCfg) pretty.Doc {
	d := make([]pretty.Doc, 0, len(node.Whens)+3)
	c := pretty.Keyword("CASE")
	if node.Expr != nil {
		c = pretty.Group(pretty.ConcatSpace(c, p.Doc(node.Expr)))
	}
	d = append(d, c)
	for _, when := range node.Whens {
		d = append(d, p.Doc(when))
	}
	if node.Else != nil {
		d = append(d, pretty.Group(pretty.ConcatSpace(
			pretty.Keyword("ELSE"),
			p.Doc(node.Else),
		)))
	}
	d = append(d, pretty.Keyword("END"))
	return pretty.Stack(d...)
}

func (node *When) doc(p *PrettyCfg) pretty.Doc {
	return pretty.Group(pretty.ConcatLine(
		pretty.Group(pretty.ConcatSpace(
			pretty.Keyword("WHEN"),
			p.Doc(node.Cond),
		)),
		pretty.Group(pretty.ConcatSpace(
			pretty.Keyword("THEN"),
			p.Doc(node.Val),
		)),
	))
}

func (node *UnionClause) doc(p *PrettyCfg) pretty.Doc {
	op := node.Type.String()
	if node.All {
		op += " ALL"
	}
	return pretty.Stack(p.Doc(node.Left), p.nestUnder(pretty.Keyword(op), p.Doc(node.Right)))
}

func (node *IfErrExpr) doc(p *PrettyCfg) pretty.Doc {
	var s string
	if node.Else != nil {
		s = "IFERROR"
	} else {
		s = "ISERROR"
	}
	d := []pretty.Doc{p.Doc(node.Cond)}
	if node.Else != nil {
		d = append(d, p.Doc(node.Else))
	}
	if node.ErrCode != nil {
		d = append(d, p.Doc(node.ErrCode))
	}
	return p.bracketKeyword(s, "(", p.commaSeparated(d...), ")", "")
}

func (node *IfExpr) doc(p *PrettyCfg) pretty.Doc {
	return p.bracketKeyword("IF", "(",
		p.commaSeparated(
			p.Doc(node.Cond),
			p.Doc(node.True),
			p.Doc(node.Else),
		), ")", "")
}

func (node *NullIfExpr) doc(p *PrettyCfg) pretty.Doc {
	return p.bracketKeyword("NULLIF", "(",
		p.commaSeparated(
			p.Doc(node.Expr1),
			p.Doc(node.Expr2),
		), ")", "")
}

func (node *PartitionByTable) doc(p *PrettyCfg) pretty.Doc {
	// Final layout:
	//
	// PARTITION [ALL] BY NOTHING
	//
	// PARTITION [ALL] BY LIST (...)
	//    ( ..values.. )
	//
	// PARTITION [ALL] BY RANGE (...)
	//    ( ..values.. )
	var kw string
	kw = `PARTITION `
	if node.All {
		kw += `ALL `
	}
	return node.PartitionBy.docInner(p, kw+`BY `)
}

func (node *PartitionBy) doc(p *PrettyCfg) pretty.Doc {
	// Final layout:
	//
	// PARTITION BY NOTHING
	//
	// PARTITION BY LIST (...)
	//    ( ..values.. )
	//
	// PARTITION BY RANGE (...)
	//    ( ..values.. )
	return node.docInner(p, `PARTITION BY `)
}

func (node *PartitionBy) docInner(p *PrettyCfg, kw string) pretty.Doc {
	if node == nil {
		return pretty.Keyword(kw + `NOTHING`)
	}
	if len(node.List) > 0 {
		kw += `LIST`
	} else if len(node.Range) > 0 {
		kw += `RANGE`
	}
	title := pretty.ConcatSpace(pretty.Keyword(kw),
		p.bracket("(", p.Doc(&node.Fields), ")"))

	inner := make([]pretty.Doc, 0, len(node.List)+len(node.Range))
	for _, v := range node.List {
		inner = append(inner, p.Doc(&v))
	}
	for _, v := range node.Range {
		inner = append(inner, p.Doc(&v))
	}
	return p.nestUnder(title,
		p.bracket("(", p.commaSeparated(inner...), ")"),
	)
}

func (node *Locality) doc(p *PrettyCfg) pretty.Doc {
	// Final layout:
	//
	// LOCALITY [GLOBAL | REGIONAL BY [TABLE [IN [PRIMARY REGION|region]]|ROW]]
	localityKW := pretty.Keyword("LOCALITY")
	switch node.LocalityLevel {
	case LocalityLevelGlobal:
		return pretty.ConcatSpace(localityKW, pretty.Keyword("GLOBAL"))
	case LocalityLevelRow:
		ret := pretty.ConcatSpace(localityKW, pretty.Keyword("REGIONAL BY ROW"))
		if node.RegionalByRowColumn != "" {
			return pretty.ConcatSpace(
				ret,
				pretty.ConcatSpace(
					pretty.Keyword("AS"),
					p.Doc(&node.RegionalByRowColumn),
				),
			)
		}
		return ret
	case LocalityLevelTable:
		byTable := pretty.ConcatSpace(localityKW, pretty.Keyword("REGIONAL BY TABLE IN"))
		if node.TableRegion == "" {
			return pretty.ConcatSpace(
				byTable,
				pretty.Keyword("PRIMARY REGION"),
			)
		}
		return pretty.ConcatSpace(
			byTable,
			p.Doc(&node.TableRegion),
		)
	}
	panic(fmt.Sprintf("unknown locality: %v", *node))
}

func (node *ListPartition) doc(p *PrettyCfg) pretty.Doc {
	// Final layout:
	//
	// PARTITION name
	//   VALUES IN ( ... )
	//   [ .. subpartition ..]
	//
	title := pretty.ConcatSpace(pretty.Keyword("PARTITION"), p.Doc(&node.Name))

	clauses := make([]pretty.Doc, 1, 2)
	clauses[0] = pretty.ConcatSpace(
		pretty.Keyword("VALUES IN"),
		p.bracket("(", p.Doc(&node.Exprs), ")"),
	)
	if node.Subpartition != nil {
		clauses = append(clauses, p.Doc(node.Subpartition))
	}
	return p.nestUnder(title, pretty.Group(pretty.Stack(clauses...)))
}

func (node *RangePartition) doc(p *PrettyCfg) pretty.Doc {
	// Final layout:
	//
	// PARTITION name
	//   VALUES FROM (...)
	//   TO (...)
	//   [ .. subpartition ..]
	//
	title := pretty.ConcatSpace(
		pretty.Keyword("PARTITION"),
		p.Doc(&node.Name),
	)

	clauses := make([]pretty.Doc, 2, 3)
	clauses[0] = pretty.ConcatSpace(
		pretty.Keyword("VALUES FROM"),
		p.bracket("(", p.Doc(&node.From), ")"))
	clauses[1] = pretty.ConcatSpace(
		pretty.Keyword("TO"),
		p.bracket("(", p.Doc(&node.To), ")"))

	if node.Subpartition != nil {
		clauses = append(clauses, p.Doc(node.Subpartition))
	}

	return p.nestUnder(title, pretty.Group(pretty.Stack(clauses...)))
}

func (node *ShardedIndexDef) doc(p *PrettyCfg) pretty.Doc {
	// Final layout:
	//
	// USING HASH WITH BUCKET_COUNT = bucket_count
	//
	parts := []pretty.Doc{
		pretty.Keyword("USING HASH WITH BUCKET_COUNT = "),
		p.Doc(node.ShardBuckets),
	}
	return pretty.Fold(pretty.ConcatSpace, parts...)
}

func (node *InterleaveDef) doc(p *PrettyCfg) pretty.Doc {
	// Final layout:
	//
	// INTERLEAVE IN PARENT tbl (...) [RESTRICT|CASCADE]
	//
	parts := []pretty.Doc{
		pretty.Keyword("INTERLEAVE IN PARENT"),
		p.Doc(&node.Parent),
		p.bracket("(", p.Doc(&node.Fields), ")"),
	}
	if node.DropBehavior != DropDefault {
		parts = append(parts, pretty.Keyword(node.DropBehavior.String()))
	}
	return pretty.Fold(pretty.ConcatSpace, parts...)
}

func (node *CreateIndex) doc(p *PrettyCfg) pretty.Doc {
	// Final layout:
	// CREATE [UNIQUE] [INVERTED] INDEX [name]
	//    ON tbl (cols...)
	//    [STORING ( ... )]
	//    [INTERLEAVE ...]
	//    [PARTITION BY ...]
	//    [WITH ...]
	//    [WHERE ...]
	//
	title := make([]pretty.Doc, 0, 6)
	title = append(title, pretty.Keyword("CREATE"))
	if node.Unique {
		title = append(title, pretty.Keyword("UNIQUE"))
	}
	if node.Inverted {
		title = append(title, pretty.Keyword("INVERTED"))
	}
	title = append(title, pretty.Keyword("INDEX"))
	if node.Concurrently {
		title = append(title, pretty.Keyword("CONCURRENTLY"))
	}
	if node.IfNotExists {
		title = append(title, pretty.Keyword("IF NOT EXISTS"))
	}
	if node.Name != "" {
		title = append(title, p.Doc(&node.Name))
	}

	clauses := make([]pretty.Doc, 0, 5)
	clauses = append(clauses, pretty.Fold(pretty.ConcatSpace,
		pretty.Keyword("ON"),
		p.Doc(&node.Table),
		p.bracket("(", p.Doc(&node.Columns), ")")))

	if node.Sharded != nil {
		clauses = append(clauses, p.Doc(node.Sharded))
	}
	if len(node.Storing) > 0 {
		clauses = append(clauses, p.bracketKeyword(
			"STORING", " (",
			p.Doc(&node.Storing),
			")", "",
		))
	}
	if node.Interleave != nil {
		clauses = append(clauses, p.Doc(node.Interleave))
	}
	if node.PartitionByIndex != nil {
		clauses = append(clauses, p.Doc(node.PartitionByIndex))
	}
	if node.StorageParams != nil {
		clauses = append(clauses, p.bracketKeyword(
			"WITH", " (",
			p.Doc(&node.StorageParams),
			")", "",
		))
	}
	if node.Predicate != nil {
		clauses = append(clauses, p.nestUnder(pretty.Keyword("WHERE"), p.Doc(node.Predicate)))
	}
	return p.nestUnder(
		pretty.Fold(pretty.ConcatSpace, title...),
		pretty.Group(pretty.Stack(clauses...)))
}

func (node *FamilyTableDef) doc(p *PrettyCfg) pretty.Doc {
	// Final layout:
	// FAMILY [name] (columns...)
	//
	d := pretty.Keyword("FAMILY")
	if node.Name != "" {
		d = pretty.ConcatSpace(d, p.Doc(&node.Name))
	}
	return pretty.ConcatSpace(d, p.bracket("(", p.Doc(&node.Columns), ")"))
}

func (node *LikeTableDef) doc(p *PrettyCfg) pretty.Doc {
	d := pretty.Keyword("LIKE")
	d = pretty.ConcatSpace(d, p.Doc(&node.Name))
	for _, opt := range node.Options {
		word := "INCLUDING"
		if opt.Excluded {
			word = "EXCLUDING"
		}
		d = pretty.ConcatSpace(d, pretty.Keyword(word))
		d = pretty.ConcatSpace(d, pretty.Keyword(opt.Opt.String()))
	}
	return d
}

func (node *IndexTableDef) doc(p *PrettyCfg) pretty.Doc {
	// Final layout:
	// [INVERTED] INDEX [name] (columns...)
	//    [STORING ( ... )]
	//    [INTERLEAVE ...]
	//    [PARTITION BY ...]
	//    [WHERE ...]
	//
	title := pretty.Keyword("INDEX")
	if node.Name != "" {
		title = pretty.ConcatSpace(title, p.Doc(&node.Name))
	}
	if node.Inverted {
		title = pretty.ConcatSpace(pretty.Keyword("INVERTED"), title)
	}
	title = pretty.ConcatSpace(title, p.bracket("(", p.Doc(&node.Columns), ")"))

	clauses := make([]pretty.Doc, 0, 4)
	if node.Sharded != nil {
		clauses = append(clauses, p.Doc(node.Sharded))
	}
	if node.Storing != nil {
		clauses = append(clauses, p.bracketKeyword(
			"STORING", "(",
			p.Doc(&node.Storing),
			")", ""))
	}
	if node.Interleave != nil {
		clauses = append(clauses, p.Doc(node.Interleave))
	}
	if node.PartitionByIndex != nil {
		clauses = append(clauses, p.Doc(node.PartitionByIndex))
	}
	if node.StorageParams != nil {
		clauses = append(
			clauses,
			p.bracketKeyword("WITH", "(", p.Doc(&node.StorageParams), ")", ""),
		)
	}
	if node.Predicate != nil {
		clauses = append(clauses, p.nestUnder(pretty.Keyword("WHERE"), p.Doc(node.Predicate)))
	}

	if len(clauses) == 0 {
		return title
	}
	return p.nestUnder(title, pretty.Group(pretty.Stack(clauses...)))
}

func (node *UniqueConstraintTableDef) doc(p *PrettyCfg) pretty.Doc {
	// Final layout:
	// [CONSTRAINT name]
	//    [PRIMARY KEY|UNIQUE [WITHOUT INDEX]] ( ... )
	//    [STORING ( ... )]
	//    [INTERLEAVE ...]
	//    [PARTITION BY ...]
	//    [WHERE ...]
	//
	// or (no constraint name):
	//
	// [PRIMARY KEY|UNIQUE [WITHOUT INDEX]] ( ... )
	//    [STORING ( ... )]
	//    [INTERLEAVE ...]
	//    [PARTITION BY ...]
	//    [WHERE ...]
	//
	clauses := make([]pretty.Doc, 0, 5)
	var title pretty.Doc
	if node.PrimaryKey {
		title = pretty.Keyword("PRIMARY KEY")
	} else {
		title = pretty.Keyword("UNIQUE")
		if node.WithoutIndex {
			title = pretty.ConcatSpace(title, pretty.Keyword("WITHOUT INDEX"))
		}
	}
	title = pretty.ConcatSpace(title, p.bracket("(", p.Doc(&node.Columns), ")"))
	if node.Name != "" {
		clauses = append(clauses, title)
		title = pretty.ConcatSpace(pretty.Keyword("CONSTRAINT"), p.Doc(&node.Name))
	}
	if node.Sharded != nil {
		clauses = append(clauses, p.Doc(node.Sharded))
	}
	if node.Storing != nil {
		clauses = append(clauses, p.bracketKeyword(
			"STORING", "(",
			p.Doc(&node.Storing),
			")", ""))
	}
	if node.Interleave != nil {
		clauses = append(clauses, p.Doc(node.Interleave))
	}
	if node.PartitionByIndex != nil {
		clauses = append(clauses, p.Doc(node.PartitionByIndex))
	}
	if node.Predicate != nil {
		clauses = append(clauses, p.nestUnder(pretty.Keyword("WHERE"), p.Doc(node.Predicate)))
	}

	if len(clauses) == 0 {
		return title
	}
	return p.nestUnder(title, pretty.Group(pretty.Stack(clauses...)))
}

func (node *ForeignKeyConstraintTableDef) doc(p *PrettyCfg) pretty.Doc {
	// Final layout:
	// [CONSTRAINT name]
	//    FOREIGN KEY (...)
	//    REFERENCES tbl (...)
	//    [MATCH ...]
	//    [ACTIONS ...]
	//
	// or (no constraint name):
	//
	// FOREIGN KEY (...)
	//    REFERENCES tbl [(...)]
	//    [MATCH ...]
	//    [ACTIONS ...]
	//
	clauses := make([]pretty.Doc, 0, 4)
	title := pretty.ConcatSpace(
		pretty.Keyword("FOREIGN KEY"),
		p.bracket("(", p.Doc(&node.FromCols), ")"))

	if node.Name != "" {
		clauses = append(clauses, title)
		title = pretty.ConcatSpace(pretty.Keyword("CONSTRAINT"), p.Doc(&node.Name))
	}

	ref := pretty.ConcatSpace(
		pretty.Keyword("REFERENCES"), p.Doc(&node.Table))
	if len(node.ToCols) > 0 {
		ref = pretty.ConcatSpace(ref, p.bracket("(", p.Doc(&node.ToCols), ")"))
	}
	clauses = append(clauses, ref)

	if node.Match != MatchSimple {
		clauses = append(clauses, pretty.Keyword(node.Match.String()))
	}

	if actions := p.Doc(&node.Actions); ref != pretty.Nil {
		clauses = append(clauses, actions)
	}

	return p.nestUnder(title, pretty.Group(pretty.Stack(clauses...)))
}

func (p *PrettyCfg) maybePrependConstraintName(constraintName *Name, d pretty.Doc) pretty.Doc {
	if *constraintName != "" {
		return pretty.Fold(pretty.ConcatSpace,
			pretty.Keyword("CONSTRAINT"),
			p.Doc(constraintName),
			d)
	}
	return d
}

func (node *ColumnTableDef) doc(p *PrettyCfg) pretty.Doc {
	return p.unrow(node.docRow(p))
}

func (node *ColumnTableDef) docRow(p *PrettyCfg) pretty.TableRow {
	// Final layout:
	// colname
	//   type
	//   [AS ( ... ) STORED]
	//   [[CREATE [IF NOT EXISTS]] FAMILY [name]]
	//   [[CONSTRAINT name] DEFAULT expr]
	//   [[CONSTRAINT name] {NULL|NOT NULL}]
	//   [[CONSTRAINT name] {PRIMARY KEY|UNIQUE [WITHOUT INDEX]}]
	//   [[CONSTRAINT name] CHECK ...]
	//   [[CONSTRAINT name] REFERENCES tbl (...)
	//         [MATCH ...]
	//         [ACTIONS ...]
	//   ]
	//
	clauses := make([]pretty.Doc, 0, 7)

	// Column type.
	// ColumnTableDef node type will not be specified if it represents a CREATE
	// TABLE ... AS query.
	if node.Type != nil {
		clauses = append(clauses, pretty.Text(node.columnTypeString()))
	}

	// Compute expression (for computed columns).
	if node.IsComputed() {
		var typ string
		if node.Computed.Virtual {
			typ = "VIRTUAL"
		} else {
			typ = "STORED"
		}

		clauses = append(clauses, pretty.ConcatSpace(
			pretty.Keyword("AS"),
			pretty.ConcatSpace(
				p.bracket("(", p.Doc(node.Computed.Expr), ")"),
				pretty.Keyword(typ),
			),
		))
	}

	// Column family.
	if node.HasColumnFamily() {
		d := pretty.Keyword("FAMILY")
		if node.Family.Name != "" {
			d = pretty.ConcatSpace(d, p.Doc(&node.Family.Name))
		}
		if node.Family.Create {
			c := pretty.Keyword("CREATE")
			if node.Family.IfNotExists {
				c = pretty.ConcatSpace(c, pretty.Keyword("IF NOT EXISTS"))
			}
			d = pretty.ConcatSpace(c, d)
		}
		clauses = append(clauses, d)
	}

	// DEFAULT constraint.
	if node.HasDefaultExpr() {
		clauses = append(clauses, p.maybePrependConstraintName(&node.DefaultExpr.ConstraintName,
			pretty.ConcatSpace(pretty.Keyword("DEFAULT"), p.Doc(node.DefaultExpr.Expr))))
	}

	// [NOT] VISIBLE constraint.
	if node.Hidden {
		hiddenConstraint := pretty.Keyword("NOT VISIBLE")
		clauses = append(clauses, p.maybePrependConstraintName(&node.Nullable.ConstraintName, hiddenConstraint))
	}

	// NULL/NOT NULL constraint.
	nConstraint := pretty.Nil
	switch node.Nullable.Nullability {
	case Null:
		nConstraint = pretty.Keyword("NULL")
	case NotNull:
		nConstraint = pretty.Keyword("NOT NULL")
	}
	if nConstraint != pretty.Nil {
		clauses = append(clauses, p.maybePrependConstraintName(&node.Nullable.ConstraintName, nConstraint))
	}

	// PRIMARY KEY / UNIQUE constraint.
	pkConstraint := pretty.Nil
	if node.PrimaryKey.IsPrimaryKey {
		pkConstraint = pretty.Keyword("PRIMARY KEY")
	} else if node.Unique.IsUnique {
		pkConstraint = pretty.Keyword("UNIQUE")
		if node.Unique.WithoutIndex {
			pkConstraint = pretty.ConcatSpace(pkConstraint, pretty.Keyword("WITHOUT INDEX"))
		}
	}
	if pkConstraint != pretty.Nil {
		clauses = append(clauses, p.maybePrependConstraintName(&node.Unique.ConstraintName, pkConstraint))
	}

	if node.PrimaryKey.Sharded {
		clauses = append(clauses, pretty.Keyword("USING HASH WITH BUCKET_COUNT = "))
		clauses = append(clauses, p.Doc(node.PrimaryKey.ShardBuckets))
	}
	// CHECK expressions/constraints.
	for _, checkExpr := range node.CheckExprs {
		clauses = append(clauses, p.maybePrependConstraintName(&checkExpr.ConstraintName,
			pretty.ConcatSpace(pretty.Keyword("CHECK"), p.bracket("(", p.Doc(checkExpr.Expr), ")"))))
	}

	// FK constraints.
	if node.HasFKConstraint() {
		fkHead := pretty.ConcatSpace(pretty.Keyword("REFERENCES"), p.Doc(node.References.Table))
		if node.References.Col != "" {
			fkHead = pretty.ConcatSpace(fkHead, p.bracket("(", p.Doc(&node.References.Col), ")"))
		}
		fkDetails := make([]pretty.Doc, 0, 2)
		// We omit MATCH SIMPLE because it is the default.
		if node.References.Match != MatchSimple {
			fkDetails = append(fkDetails, pretty.Keyword(node.References.Match.String()))
		}
		if ref := p.Doc(&node.References.Actions); ref != pretty.Nil {
			fkDetails = append(fkDetails, ref)
		}
		fk := fkHead
		if len(fkDetails) > 0 {
			fk = p.nestUnder(fk, pretty.Group(pretty.Stack(fkDetails...)))
		}
		clauses = append(clauses, p.maybePrependConstraintName(&node.References.ConstraintName, fk))
	}

	// Prevents an additional space from being appended at the end of every column
	// name in the case of CREATE TABLE ... AS query. The additional space is
	// being caused due to the absence of column type qualifiers in CTAS queries.
	//
	// TODO(adityamaru): Consult someone with more knowledge about the pretty
	// printer architecture to find a cleaner solution.
	var tblRow pretty.TableRow
	if node.Type == nil {
		tblRow = pretty.TableRow{
			Label: node.Name.String(),
			Doc:   pretty.Stack(clauses...),
		}
	} else {
		tblRow = pretty.TableRow{
			Label: node.Name.String(),
			Doc:   pretty.Group(pretty.Stack(clauses...)),
		}
	}

	return tblRow
}

func (node *CheckConstraintTableDef) doc(p *PrettyCfg) pretty.Doc {
	// Final layout:
	//
	// CONSTRAINT name
	//    CHECK (...)
	//
	// or (no constraint name):
	//
	// CHECK (...)
	//
	d := pretty.ConcatSpace(pretty.Keyword("CHECK"),
		p.bracket("(", p.Doc(node.Expr), ")"))

	if node.Name != "" {
		d = p.nestUnder(
			pretty.ConcatSpace(
				pretty.Keyword("CONSTRAINT"),
				p.Doc(&node.Name),
			),
			d,
		)
	}
	return d
}

func (node *ReferenceActions) doc(p *PrettyCfg) pretty.Doc {
	var docs []pretty.Doc
	if node.Delete != NoAction {
		docs = append(docs,
			pretty.Keyword("ON DELETE"),
			pretty.Keyword(node.Delete.String()),
		)
	}
	if node.Update != NoAction {
		docs = append(docs,
			pretty.Keyword("ON UPDATE"),
			pretty.Keyword(node.Update.String()),
		)
	}
	return pretty.Fold(pretty.ConcatSpace, docs...)
}

func (node *Backup) doc(p *PrettyCfg) pretty.Doc {
	items := make([]pretty.TableRow, 0, 6)

	items = append(items, p.row("BACKUP", pretty.Nil))
	if node.Targets != nil {
		items = append(items, node.Targets.docRow(p))
	}
	if node.Nested {
		if node.Subdir != nil {
			items = append(items, p.row("INTO ", p.Doc(node.Subdir)))
			items = append(items, p.row(" IN ", p.Doc(&node.To)))
		} else if node.AppendToLatest {
			items = append(items, p.row("INTO LATEST IN", p.Doc(&node.To)))
		} else {
			items = append(items, p.row("INTO", p.Doc(&node.To)))
		}
	} else {
		items = append(items, p.row("TO", p.Doc(&node.To)))
	}

	if node.AsOf.Expr != nil {
		items = append(items, node.AsOf.docRow(p))
	}
	if node.IncrementalFrom != nil {
		items = append(items, p.row("INCREMENTAL FROM", p.Doc(&node.IncrementalFrom)))
	}
	if !node.Options.IsDefault() {
		items = append(items, p.row("WITH", p.Doc(&node.Options)))
	}
	return p.rlTable(items...)
}

func (node *Restore) doc(p *PrettyCfg) pretty.Doc {
	items := make([]pretty.TableRow, 0, 5)

	items = append(items, p.row("RESTORE", pretty.Nil))
	if node.DescriptorCoverage == RequestedDescriptors {
		items = append(items, node.Targets.docRow(p))
	}
	from := make([]pretty.Doc, len(node.From))
	for i := range node.From {
		from[i] = p.Doc(&node.From[i])
	}
	if node.Subdir != nil {
		items = append(items, p.row("FROM", p.Doc(node.Subdir)))
		items = append(items, p.row("IN", p.commaSeparated(from...)))
	} else {
		items = append(items, p.row("FROM", p.commaSeparated(from...)))
	}

	if node.AsOf.Expr != nil {
		items = append(items, node.AsOf.docRow(p))
	}
	if !node.Options.IsDefault() {
		items = append(items, p.row("WITH", p.Doc(&node.Options)))
	}
	return p.rlTable(items...)
}

func (node *TargetList) doc(p *PrettyCfg) pretty.Doc {
	return p.unrow(node.docRow(p))
}

func (node *TargetList) docRow(p *PrettyCfg) pretty.TableRow {
	if node.Databases != nil {
		return p.row("DATABASE", p.Doc(&node.Databases))
	}
	if node.Tenant != (roachpb.TenantID{}) {
		return p.row("TENANT", pretty.Text(strconv.FormatUint(node.Tenant.ToUint64(), 10)))
	}
	return p.row("TABLE", p.Doc(&node.Tables))
}

func (node *AsOfClause) doc(p *PrettyCfg) pretty.Doc {
	return p.unrow(node.docRow(p))
}

func (node *AsOfClause) docRow(p *PrettyCfg) pretty.TableRow {
	return p.row("AS OF SYSTEM TIME", p.Doc(node.Expr))
}

func (node *KVOptions) doc(p *PrettyCfg) pretty.Doc {
	var opts []pretty.Doc
	for _, opt := range *node {
		d := p.Doc(&opt.Key)
		if opt.Value != nil {
			d = pretty.Fold(pretty.ConcatSpace,
				d,
				pretty.Text("="),
				p.Doc(opt.Value),
			)
		}
		opts = append(opts, d)
	}
	return p.commaSeparated(opts...)
}

func (node *Import) doc(p *PrettyCfg) pretty.Doc {
	items := make([]pretty.TableRow, 0, 5)
	items = append(items, p.row("IMPORT", pretty.Nil))

	if node.Bundle {
		if node.Table != nil {
			items = append(items, p.row("TABLE", p.Doc(node.Table)))
			items = append(items, p.row("FROM", pretty.Nil))
		}
		items = append(items, p.row(node.FileFormat, p.Doc(&node.Files)))
	} else {
		if node.Into {
			into := p.Doc(node.Table)
			if node.IntoCols != nil {
				into = p.nestUnder(into, p.bracket("(", p.Doc(&node.IntoCols), ")"))
			}
			items = append(items, p.row("INTO", into))
		} else {
			if node.CreateFile != nil {
				items = append(items, p.row("TABLE", p.Doc(node.Table)))
				items = append(items, p.row("CREATE USING", p.Doc(node.CreateFile)))
			} else {
				table := p.bracketDoc(
					pretty.ConcatSpace(p.Doc(node.Table), pretty.Text("(")),
					p.Doc(&node.CreateDefs),
					pretty.Text(")"),
				)
				items = append(items, p.row("TABLE", table))
			}
		}

		data := p.bracketKeyword(
			"DATA", " (",
			p.Doc(&node.Files),
			")", "",
		)
		items = append(items, p.row(node.FileFormat, data))
	}
	if node.Options != nil {
		items = append(items, p.row("WITH", p.Doc(&node.Options)))
	}
	return p.rlTable(items...)
}

func (node *Export) doc(p *PrettyCfg) pretty.Doc {
	items := make([]pretty.TableRow, 0, 5)
	items = append(items, p.row("EXPORT", pretty.Nil))
	items = append(items, p.row("INTO "+node.FileFormat, p.Doc(node.File)))
	if node.Options != nil {
		items = append(items, p.row("WITH", p.Doc(&node.Options)))
	}
	items = append(items, p.row("FROM", p.Doc(node.Query)))
	return p.rlTable(items...)
}

func (node *NotExpr) doc(p *PrettyCfg) pretty.Doc {
	return p.nestUnder(
		pretty.Keyword("NOT"),
		p.exprDocWithParen(node.Expr),
	)
}

func (node *IsNullExpr) doc(p *PrettyCfg) pretty.Doc {
	return pretty.ConcatSpace(
		p.exprDocWithParen(node.Expr),
		pretty.Keyword("IS NULL"),
	)
}

func (node *IsNotNullExpr) doc(p *PrettyCfg) pretty.Doc {
	return pretty.ConcatSpace(
		p.exprDocWithParen(node.Expr),
		pretty.Keyword("IS NOT NULL"),
	)
}

func (node *CoalesceExpr) doc(p *PrettyCfg) pretty.Doc {
	return p.bracketKeyword(
		node.Name, "(",
		p.Doc(&node.Exprs),
		")", "",
	)
}

func (node *AlterTable) doc(p *PrettyCfg) pretty.Doc {
	title := pretty.Keyword("ALTER TABLE")
	if node.IfExists {
		title = pretty.ConcatSpace(title, pretty.Keyword("IF EXISTS"))
	}
	title = pretty.ConcatSpace(title, p.Doc(node.Table))
	return p.nestUnder(
		title,
		p.Doc(&node.Cmds),
	)
}

func (node *AlterTableCmds) doc(p *PrettyCfg) pretty.Doc {
	cmds := make([]pretty.Doc, len(*node))
	for i, c := range *node {
		cmds[i] = p.Doc(c)
	}
	return p.commaSeparated(cmds...)
}

func (node *AlterTableAddColumn) doc(p *PrettyCfg) pretty.Doc {
	title := pretty.Keyword("ADD COLUMN")
	if node.IfNotExists {
		title = pretty.ConcatSpace(title, pretty.Keyword("IF NOT EXISTS"))
	}
	return p.nestUnder(
		title,
		p.Doc(node.ColumnDef),
	)
}

func (node *Prepare) doc(p *PrettyCfg) pretty.Doc {
	return p.rlTable(node.docTable(p)...)
}

func (node *Prepare) docTable(p *PrettyCfg) []pretty.TableRow {
	name := p.Doc(&node.Name)
	if len(node.Types) > 0 {
		typs := make([]pretty.Doc, len(node.Types))
		for i, t := range node.Types {
			typs[i] = pretty.Text(t.SQLString())
		}
		name = pretty.ConcatSpace(name,
			p.bracket("(", p.commaSeparated(typs...), ")"),
		)
	}
	return []pretty.TableRow{
		p.row("PREPARE", name),
		p.row("AS", p.Doc(node.Statement)),
	}
}

func (node *Execute) doc(p *PrettyCfg) pretty.Doc {
	return p.rlTable(node.docTable(p)...)
}

func (node *Execute) docTable(p *PrettyCfg) []pretty.TableRow {
	name := p.Doc(&node.Name)
	if len(node.Params) > 0 {
		name = pretty.ConcatSpace(
			name,
			p.bracket("(", p.Doc(&node.Params), ")"),
		)
	}
	rows := []pretty.TableRow{p.row("EXECUTE", name)}
	if node.DiscardRows {
		rows = append(rows, p.row("", pretty.Keyword("DISCARD ROWS")))
	}
	return rows
}

func (node *AnnotateTypeExpr) doc(p *PrettyCfg) pretty.Doc {
	if node.SyntaxMode == AnnotateShort {
		if typ, ok := GetStaticallyKnownType(node.Type); ok {
			switch typ.Family() {
			case types.JsonFamily:
				if sv, ok := node.Expr.(*StrVal); ok && p.JSONFmt {
					return p.jsonCast(sv, ":::", typ)
				}
			}
		}
	}
	return p.docAsString(node)
}

// jsonCast attempts to pretty print a string that is cast or asserted as JSON.
func (p *PrettyCfg) jsonCast(sv *StrVal, op string, typ *types.T) pretty.Doc {
	return pretty.Fold(pretty.Concat,
		p.jsonString(sv.RawString()),
		pretty.Text(op),
		pretty.Text(typ.SQLString()),
	)
}

// jsonString parses s as JSON and pretty prints it.
func (p *PrettyCfg) jsonString(s string) pretty.Doc {
	j, err := json.ParseJSON(s)
	if err != nil {
		return pretty.Text(s)
	}
	return p.bracket(`'`, p.jsonNode(j), `'`)
}

// jsonNode pretty prints a JSON node.
func (p *PrettyCfg) jsonNode(j json.JSON) pretty.Doc {
	// Figure out what type this is.
	if it, _ := j.ObjectIter(); it != nil {
		// Object.
		elems := make([]pretty.Doc, 0, j.Len())
		for it.Next() {
			elems = append(elems, p.nestUnder(
				pretty.Concat(
					pretty.Text(json.FromString(it.Key()).String()),
					pretty.Text(`:`),
				),
				p.jsonNode(it.Value()),
			))
		}
		return p.bracket("{", p.commaSeparated(elems...), "}")
	} else if n := j.Len(); n > 0 {
		// Non-empty array.
		elems := make([]pretty.Doc, n)
		for i := 0; i < n; i++ {
			elem, err := j.FetchValIdx(i)
			if err != nil {
				return pretty.Text(j.String())
			}
			elems[i] = p.jsonNode(elem)
		}
		return p.bracket("[", p.commaSeparated(elems...), "]")
	}
	// Other.
	return pretty.Text(j.String())
}
