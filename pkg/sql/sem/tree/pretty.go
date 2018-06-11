// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package tree

import (
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/coltypes"
	"github.com/cockroachdb/cockroach/pkg/util/pretty"
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

// DefaultPrettyWidth is the default line width for pretty printed statements.
const DefaultPrettyWidth = 60

// Pretty pretty prints stmt with default options.
func Pretty(stmt NodeFormatter) string {
	return PrettyWithOpts(stmt, DefaultPrettyWidth, true, 4)
}

// PrettyWithOpts pretty prints stmt with specified options.
func PrettyWithOpts(stmt NodeFormatter, lineWidth int, useTabs bool, tabWidth int) string {
	var tab string
	if useTabs {
		tab = "\t"
	} else {
		tab = strings.Repeat(" ", tabWidth)
	}
	cfg := prettyCfg{
		tab:      tab,
		tabWidth: tabWidth,
	}
	doc := cfg.Doc(stmt)
	return pretty.Pretty(doc, lineWidth)
}

type prettyCfg struct {
	tab      string
	tabWidth int
}

func (p prettyCfg) Doc(f NodeFormatter) pretty.Doc {
	if f, ok := f.(docer); ok {
		doc := f.doc(p)
		return doc
	}
	return p.docAsString(f)
}

func (p prettyCfg) docAsString(f NodeFormatter) pretty.Doc {
	const prettyFlags = FmtShowPasswords | FmtParsable
	return pretty.Text(AsStringWithFlags(f, prettyFlags))
}

func (p prettyCfg) nestName(a, b pretty.Doc) pretty.Doc {
	return pretty.NestName(p.tabWidth, p.tab, a, b)
}

func (p prettyCfg) joinGroup(name, divider string, d ...pretty.Doc) pretty.Doc {
	return pretty.JoinGroup(p.tabWidth, p.tab, name, divider, d...)
}

func (p prettyCfg) bracket(l string, x pretty.Doc, r string) pretty.Doc {
	return pretty.Bracket(p.tabWidth, p.tab, l, x, r)
}

func (p prettyCfg) indentArgs(l pretty.Doc, op string, r pretty.Doc) pretty.Doc {
	return pretty.Fold(pretty.Concat,
		pretty.Nest(p.tabWidth, p.tab, l),
		pretty.Line,
		pretty.Text(op),
		pretty.Nest(p.tabWidth, p.tab, pretty.Concat(pretty.Line, r)),
	)
}

// docer is implemented by nodes that can convert themselves into
// pretty.Docs. If nodes cannot, node.Format is used instead as a Text Doc.
type docer interface {
	doc(prettyCfg) pretty.Doc
}

func (node SelectExprs) doc(p prettyCfg) pretty.Doc {
	d := make([]pretty.Doc, len(node))
	for i, e := range node {
		d[i] = e.doc(p)
	}
	return pretty.Join(",", d...)
}

func (node SelectExpr) doc(p prettyCfg) pretty.Doc {
	d := p.Doc(node.Expr)
	if node.As != "" {
		d = p.nestName(
			d,
			pretty.Concat(pretty.Text("AS "), p.Doc(&node.As)),
		)
	}
	return d
}

func (node TableExprs) doc(p prettyCfg) pretty.Doc {
	if len(node) == 0 {
		return pretty.Nil
	}
	d := make([]pretty.Doc, len(node))
	for i, e := range node {
		d[i] = p.Doc(e)
	}
	return p.joinGroup("FROM", ",", d...)
}

func (node *Where) doc(p prettyCfg) pretty.Doc {
	if node == nil {
		return pretty.Nil
	}
	return p.nestName(pretty.Text(node.Type), p.Doc(node.Expr))
}

func (node GroupBy) doc(p prettyCfg) pretty.Doc {
	if len(node) == 0 {
		return pretty.Nil
	}
	d := make([]pretty.Doc, len(node))
	for i, e := range node {
		d[i] = p.Doc(e)
	}
	return p.joinGroup("GROUP BY", ",", d...)
}

func (node *NormalizableTableName) doc(p prettyCfg) pretty.Doc {
	return p.Doc(node.TableNameReference)
}

func (node *AndExpr) doc(p prettyCfg) pretty.Doc {
	return p.docBinaryOp(node.Left, node.Right, "AND")
}

func (node *OrExpr) doc(p prettyCfg) pretty.Doc {
	return p.docBinaryOp(node.Left, node.Right, "OR")
}

func (p prettyCfg) docBinaryOp(l, r Expr, op string) pretty.Doc {
	return pretty.Group(pretty.Concat(
		p.exprDocWithParen(l),
		pretty.Concat(
			pretty.Line,
			pretty.Group(pretty.Fold(pretty.Concat,
				pretty.Text(op),
				pretty.Line,
				p.exprDocWithParen(r),
			)),
		),
	))
}

func (node *Exprs) doc(p prettyCfg) pretty.Doc {
	if node == nil || len(*node) == 0 {
		return pretty.Nil
	}
	d := make([]pretty.Doc, len(*node))
	for i, e := range *node {
		d[i] = p.Doc(e)
	}
	return pretty.Join(",", d...)
}

func (node *BinaryExpr) doc(p prettyCfg) pretty.Doc {
	var pad pretty.Doc = pretty.Nil
	if node.Operator.isPadded() {
		pad = pretty.Line
	}
	return pretty.Group(pretty.Concat(
		p.Doc(node.Left),
		pretty.Concat(
			pad,
			pretty.Group(pretty.Fold(pretty.Concat,
				pretty.Text(node.Operator.String()),
				pad,
				p.Doc(node.Right),
			)),
		),
	))
}

func (node *ParenExpr) doc(p prettyCfg) pretty.Doc {
	return p.bracket("(", p.Doc(node.Expr), ")")
}

func (node *ParenSelect) doc(p prettyCfg) pretty.Doc {
	return p.bracket("(", p.Doc(node.Select), ")")
}

func (node *ParenTableExpr) doc(p prettyCfg) pretty.Doc {
	return p.bracket("(", p.Doc(node.Expr), ")")
}

func (node *Limit) doc(p prettyCfg) pretty.Doc {
	if node == nil {
		return pretty.Nil
	}
	var count, offset pretty.Doc
	if node.Count != nil {
		count = p.nestName(pretty.Text("LIMIT"), p.Doc(node.Count))
	}
	if node.Offset != nil {
		offset = p.nestName(pretty.Text("OFFSET"), p.Doc(node.Offset))
	}
	return pretty.ConcatLine(count, offset)
}

func (node *OrderBy) doc(p prettyCfg) pretty.Doc {
	if node == nil || len(*node) == 0 {
		return pretty.Nil
	}
	d := make([]pretty.Doc, len(*node))
	for i, e := range *node {
		d[i] = p.Doc(e)
	}
	return p.joinGroup("ORDER BY", ",", d...)
}

func (node Select) doc(p prettyCfg) pretty.Doc {
	return pretty.Group(pretty.Stack(
		node.With.doc(p),
		p.Doc(node.Select),
		node.OrderBy.doc(p),
		node.Limit.doc(p),
	))
}

func (node SelectClause) doc(p prettyCfg) pretty.Doc {
	if node.TableSelect {
		return p.nestName(pretty.Text("TABLE"), p.Doc(node.From.Tables[0]))
	}
	sel := "SELECT"
	if node.Distinct {
		sel += " DISTINCT"
	}
	return pretty.Group(pretty.Stack(
		p.nestName(pretty.Text(sel), node.Exprs.doc(p)),
		node.From.doc(p),
		node.Where.doc(p),
		node.GroupBy.doc(p),
		node.Having.doc(p),
		node.Window.doc(p),
	))
}

func (node *From) doc(p prettyCfg) pretty.Doc {
	if node == nil || len(node.Tables) == 0 {
		return pretty.Nil
	}
	d := node.Tables.doc(p)
	if node.AsOf.Expr != nil {
		d = p.nestName(
			d,
			p.Doc(&node.AsOf),
		)
	}
	return d
}

func (node *Window) doc(p prettyCfg) pretty.Doc {
	if node == nil || len(*node) == 0 {
		return pretty.Nil
	}
	d := make([]pretty.Doc, len(*node))
	for i, e := range *node {
		d[i] = pretty.Fold(pretty.Concat,
			pretty.Text(e.Name.String()),
			pretty.Text(" AS "),
			p.Doc(e),
		)
	}
	return p.joinGroup("WINDOW", ",", d...)
}

func (node *With) doc(p prettyCfg) pretty.Doc {
	if node == nil {
		return pretty.Nil
	}
	d := make([]pretty.Doc, len(node.CTEList))
	for i, cte := range node.CTEList {
		d[i] = p.nestName(
			p.Doc(&cte.Name),
			p.bracket("AS (", p.Doc(cte.Stmt), ")"),
		)
	}
	return p.joinGroup("WITH", ",", d...)
}

func (node *Subquery) doc(p prettyCfg) pretty.Doc {
	d := pretty.Text("<unknown>")
	if node.Select != nil {
		d = p.Doc(node.Select)
	}
	if node.Exists {
		d = pretty.Concat(
			pretty.Text("EXISTS"),
			d,
		)
	}
	return d
}

func (node *AliasedTableExpr) doc(p prettyCfg) pretty.Doc {
	d := p.Doc(node.Expr)
	if node.Hints != nil {
		d = pretty.Concat(
			d,
			p.Doc(node.Hints),
		)
	}
	if node.Ordinality {
		d = pretty.Concat(
			d,
			pretty.Text(" WITH ORDINALITY"),
		)
	}
	if node.As.Alias != "" {
		d = p.nestName(
			d,
			pretty.Concat(
				pretty.Text("AS "),
				p.Doc(&node.As),
			),
		)
	}
	return d
}

func (node *FuncExpr) doc(p prettyCfg) pretty.Doc {
	d := node.Exprs.doc(p)
	if node.Type != 0 {
		d = pretty.Concat(
			pretty.Text(funcTypeName[node.Type]+" "),
			d,
		)
	}

	d = p.bracket(
		AsString(&node.Func)+"(",
		d,
		")",
	)

	if window := node.WindowDef; window != nil {
		var over pretty.Doc
		if window.Name != "" {
			over = p.Doc(&window.Name)
		} else {
			over = p.Doc(window)
		}
		d = pretty.Concat(
			d,
			pretty.Concat(
				pretty.Text(" OVER "),
				over,
			),
		)
	}
	if node.Filter != nil {
		d = pretty.Fold(pretty.Concat,
			d,
			pretty.Text(" FILTER (WHERE "),
			p.Doc(node.Filter),
			pretty.Text(")"),
		)
	}
	return d
}

func (node *ComparisonExpr) doc(p prettyCfg) pretty.Doc {
	if node.Operator.hasSubOperator() {
		return p.docAsString(node)
	}

	opStr := node.Operator.String()
	if node.Operator == IsDistinctFrom && (node.Right == DNull || node.Right == DBoolTrue || node.Right == DBoolFalse) {
		opStr = "IS NOT"
	} else if node.Operator == IsNotDistinctFrom && (node.Right == DNull || node.Right == DBoolTrue || node.Right == DBoolFalse) {
		opStr = "IS"
	}
	return p.docBinaryOp(node.Left, node.Right, opStr)
}

func (node *AliasClause) doc(p prettyCfg) pretty.Doc {
	d := pretty.Text(node.Alias.String())
	if len(node.Cols) != 0 {
		d = p.nestName(d, p.bracket("(", p.Doc(&node.Cols), ")"))
	}
	return d
}

func (node *JoinTableExpr) doc(p prettyCfg) pretty.Doc {
	var d []pretty.Doc
	if _, isNatural := node.Cond.(NaturalJoinCond); isNatural {
		// Natural joins have a different syntax: "<a> NATURAL <join_type> <b>"
		d = append(d,
			p.Doc(node.Cond),
			pretty.Text(node.Join),
			p.Doc(node.Right),
		)
	} else {
		// General syntax: "<a> <join_type> <b> <condition>"
		d = append(d, p.nestName(
			pretty.Text(node.Join),
			p.Doc(node.Right),
		))
		if node.Cond != nil {
			d = append(d, p.Doc(node.Cond))
		}
	}
	return p.nestName(p.Doc(node.Left), pretty.Stack(d...))
}

func (node *OnJoinCond) doc(p prettyCfg) pretty.Doc {
	return p.nestName(
		pretty.Text("ON"),
		p.Doc(node.Expr),
	)
}

func (node *Insert) doc(p prettyCfg) pretty.Doc {
	var d []pretty.Doc
	if node.With != nil {
		d = append(d, p.Doc(node.With))
	}
	if node.OnConflict.IsUpsertAlias() {
		d = append(d, pretty.Text("UPSERT"))
	} else {
		d = append(d, pretty.Text("INSERT"))
	}
	into := p.Doc(node.Table)
	if node.Columns != nil {
		into = p.nestName(into, p.bracket("(", p.Doc(&node.Columns), ")"))
	}
	d = append(d, p.nestName(pretty.Text("INTO"), into))
	if node.DefaultValues() {
		d = append(d, pretty.Text("DEFAULT VALUES"))
	} else {
		d = append(d, p.Doc(node.Rows))
	}
	if node.OnConflict != nil && !node.OnConflict.IsUpsertAlias() {
		d = append(d, pretty.Text("ON CONFLICT"))
		if len(node.OnConflict.Columns) > 0 {
			d = append(d, p.bracket("(", p.Doc(&node.OnConflict.Columns), ")"))
		}
		if node.OnConflict.DoNothing {
			d = append(d, pretty.Text("DO NOTHING"))
		} else {
			d = append(d,
				pretty.Text("DO UPDATE SET"),
				p.Doc(&node.OnConflict.Exprs),
			)
			if node.OnConflict.Where != nil {
				d = append(d, p.Doc(node.OnConflict.Where))
			}
		}
	}
	if r := p.Doc(node.Returning); r != pretty.Nil {
		d = append(d, r)
	}
	return pretty.Group(pretty.Stack(d...))
}

func (node *NameList) doc(p prettyCfg) pretty.Doc {
	d := make([]pretty.Doc, len(*node))
	for i, n := range *node {
		d[i] = p.Doc(&n)
	}
	return pretty.Join(",", d...)
}

func (node *CastExpr) doc(p prettyCfg) pretty.Doc {
	typ := pretty.Text(coltypes.ColTypeAsString(node.Type))

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
		return pretty.Fold(pretty.Concat,
			p.exprDocWithParen(node.Expr),
			pretty.Text("::"),
			typ,
		)
	default:
		return pretty.Fold(pretty.Concat,
			pretty.Text("CAST"),
			p.bracket(
				"(",
				p.nestName(
					p.Doc(node.Expr),
					pretty.Concat(
						pretty.Text("AS "),
						typ,
					),
				),
				")",
			),
		)
	}
}

func (node *ValuesClause) doc(p prettyCfg) pretty.Doc {
	d := make([]pretty.Doc, len(node.Tuples))
	for i, n := range node.Tuples {
		d[i] = p.Doc(n)
	}
	return p.joinGroup("VALUES", ",", d...)
}

func (node *StatementSource) doc(p prettyCfg) pretty.Doc {
	return p.bracket("[", p.Doc(node.Statement), "]")
}

func (node *RowsFromExpr) doc(p prettyCfg) pretty.Doc {
	return p.bracket("ROWS FROM (", p.Doc(&node.Items), ")")
}

func (node *Array) doc(p prettyCfg) pretty.Doc {
	return p.bracket("ARRAY[", p.Doc(&node.Exprs), "]")
}

func (node *Tuple) doc(p prettyCfg) pretty.Doc {
	var row string
	if node.Row {
		row = "ROW"
	}
	d := p.bracket(row+"(", p.Doc(&node.Exprs), ")")
	if len(node.Labels) > 0 {
		labels := make([]pretty.Doc, len(node.Labels))
		for i, n := range node.Labels {
			labels[i] = p.Doc((*Name)(&n))
		}
		d = p.bracket("(", pretty.Stack(
			d,
			p.nestName(pretty.Text("AS"), pretty.Join(",", labels...)),
		), ")")
	}
	return d
}

func (node *ReturningExprs) doc(p prettyCfg) pretty.Doc {
	return p.nestName(pretty.Text("RETURNING"), p.Doc((*SelectExprs)(node)))
}

func (node *UpdateExprs) doc(p prettyCfg) pretty.Doc {
	d := make([]pretty.Doc, len(*node))
	for i, n := range *node {
		d[i] = p.Doc(n)
	}
	return pretty.Join(",", d...)
}

func (p prettyCfg) exprDocWithParen(e Expr) pretty.Doc {
	if _, ok := e.(operatorExpr); ok {
		return p.bracket("(", p.Doc(e), ")")
	}
	return p.Doc(e)
}

func (node *Update) doc(p prettyCfg) pretty.Doc {
	return pretty.Group(pretty.Stack(
		p.Doc(node.With),
		p.nestName(pretty.Text("UPDATE"), p.Doc(node.Table)),
		p.nestName(pretty.Text("SET"), p.Doc(&node.Exprs)),
		p.Doc(node.Where),
		p.Doc(&node.OrderBy),
		p.Doc(node.Limit),
		p.Doc(node.Returning),
	))
}

func (node *Delete) doc(p prettyCfg) pretty.Doc {
	return pretty.Group(pretty.Stack(
		p.Doc(node.With),
		pretty.Text("DELETE"),
		p.nestName(pretty.Text("FROM"), p.Doc(node.Table)),
		p.Doc(node.Where),
		p.Doc(&node.OrderBy),
		p.Doc(node.Limit),
		p.Doc(node.Returning),
	))
}

func (node *Order) doc(p prettyCfg) pretty.Doc {
	var d pretty.Doc
	if node.OrderType == OrderByColumn {
		d = p.Doc(node.Expr)
	} else {
		if node.Index == "" {
			d = pretty.ConcatSpace(
				pretty.Text("PRIMARY KEY"),
				p.Doc(&node.Table),
			)
		} else {
			d = pretty.ConcatSpace(
				pretty.Text("INDEX"),
				pretty.Fold(pretty.Concat,
					p.Doc(&node.Table),
					pretty.Text("@"),
					p.Doc(&node.Index),
				),
			)
		}
	}
	if node.Direction != DefaultDirection {
		d = p.nestName(d, pretty.Text(node.Direction.String()))
	}
	return d
}

func (node *UpdateExpr) doc(p prettyCfg) pretty.Doc {
	d := p.Doc(&node.Names)
	if node.Tuple {
		d = p.bracket("(", d, ")")
	}
	return pretty.Stack(d, pretty.Text("="), p.Doc(node.Expr))
}

func (node *CreateTable) doc(p prettyCfg) pretty.Doc {
	title := "CREATE TABLE "
	if node.IfNotExists {
		title += "IF NOT EXISTS "
	}
	d := pretty.Fold(pretty.Concat,
		pretty.Text(title),
		p.Doc(&node.Table),
		pretty.Text(" "),
	)
	if node.As() {
		if len(node.AsColumnNames) > 0 {
			d = pretty.Concat(
				d,
				p.bracket("(", p.Doc(&node.AsColumnNames), ")"),
			)
		}
		d = p.nestName(
			pretty.Concat(
				d,
				pretty.Text(" AS"),
			),
			p.Doc(node.AsSource),
		)
	} else {
		d = pretty.Concat(
			d,
			p.bracket("(", p.Doc(&node.Defs), ")"),
		)
		if node.Interleave != nil {
			d = pretty.ConcatLine(d, p.Doc(node.Interleave))
		}
		if node.PartitionBy != nil {
			d = pretty.ConcatLine(d, p.Doc(node.PartitionBy))
		}
	}
	return d
}

func (node *TableDefs) doc(p prettyCfg) pretty.Doc {
	d := make([]pretty.Doc, len(*node))
	for i, n := range *node {
		d[i] = p.Doc(n)
	}
	return pretty.Join(",", d...)
}

func (node *CaseExpr) doc(p prettyCfg) pretty.Doc {
	var d []pretty.Doc
	c := pretty.Text("CASE")
	if node.Expr != nil {
		c = p.nestName(c, p.Doc(node.Expr))
	}
	d = append(d, c)
	for _, when := range node.Whens {
		d = append(d, p.Doc(when))
	}
	if node.Else != nil {
		d = append(d, p.nestName(
			pretty.Text("ELSE"),
			p.Doc(node.Else),
		))
	}
	d = append(d, pretty.Text("END"))
	return pretty.Stack(d...)
}

func (node *When) doc(p prettyCfg) pretty.Doc {
	return pretty.ConcatLine(
		p.nestName(
			pretty.Text("WHEN"),
			p.Doc(node.Cond),
		),
		p.nestName(
			pretty.Text("THEN"),
			p.Doc(node.Val),
		),
	)
}

func (node *UnionClause) doc(p prettyCfg) pretty.Doc {
	op := node.Type.String()
	if node.All {
		op += " ALL"
	}
	return p.indentArgs(p.Doc(node.Left), op, p.Doc(node.Right))
}

func (node *NoReturningClause) doc(prettyCfg) pretty.Doc { return pretty.Nil }
func (node *ReturningNothing) doc(prettyCfg) pretty.Doc  { return pretty.Text("RETURNING NOTHING") }
