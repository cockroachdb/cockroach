// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package plpgsqltree

import (
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/pretty"
)

// stmtListDoc converts a list of PL/pgSQL statements into a pretty.Doc by
// calling p.Doc on each statement and stacking them vertically.
func stmtListDoc(p *tree.PrettyCfg, stmts []Statement) pretty.Doc {
	if len(stmts) == 0 {
		return pretty.Nil
	}
	docs := make([]pretty.Doc, len(stmts))
	for i, s := range stmts {
		docs[i] = p.Doc(s)
	}
	return pretty.Stack(docs...)
}

// indentBody produces a NestT(HardLine + body) doc, which indents the body
// under the current position.
func indentBody(body pretty.Doc) pretty.Doc {
	if body == pretty.Nil {
		return pretty.Nil
	}
	return pretty.NestT(pretty.Concat(pretty.HardLine, body))
}

// labelDoc formats a <<label>> prefix, or returns pretty.Nil if empty.
func labelDoc(p *tree.PrettyCfg, label string) pretty.Doc {
	if label == "" {
		return pretty.Nil
	}
	name := tree.Name(label)
	return pretty.Fold(pretty.Concat,
		pretty.Text("<<"), p.Doc(&name), pretty.Text(">>"))
}

// endLoopDoc formats "END LOOP [label];" as a pretty.Doc.
func endLoopDoc(p *tree.PrettyCfg, label string) pretty.Doc {
	d := pretty.Keyword("END LOOP")
	if label != "" {
		name := tree.Name(label)
		d = pretty.ConcatSpace(d, p.Doc(&name))
	}
	return pretty.Concat(d, pretty.Text(";"))
}

func (s *Block) Doc(p *tree.PrettyCfg) pretty.Doc {
	parts := make([]pretty.Doc, 0, 5)
	if s.Label != "" {
		parts = append(parts, labelDoc(p, s.Label))
	}
	if s.Decls != nil {
		parts = append(parts,
			pretty.Concat(pretty.Keyword("DECLARE"), indentBody(stmtListDoc(p, s.Decls))))
	}
	parts = append(parts,
		pretty.Concat(pretty.Keyword("BEGIN"), indentBody(stmtListDoc(p, s.Body))))
	if s.Exceptions != nil {
		excDocs := make([]pretty.Doc, len(s.Exceptions))
		for i := range s.Exceptions {
			excDocs[i] = p.Doc(&s.Exceptions[i])
		}
		parts = append(parts,
			pretty.Concat(pretty.Keyword("EXCEPTION"), indentBody(pretty.Stack(excDocs...))))
	}
	end := pretty.Keyword("END")
	if s.Label != "" {
		name := tree.Name(s.Label)
		end = pretty.ConcatSpace(end, p.Doc(&name))
	}
	end = pretty.Concat(end, pretty.Text(";"))
	parts = append(parts, end)
	return pretty.Stack(parts...)
}

func (s *Declaration) Doc(p *tree.PrettyCfg) pretty.Doc {
	d := p.Doc(&s.Var)
	if s.Constant {
		d = pretty.ConcatSpace(d, pretty.Keyword("CONSTANT"))
	}
	d = pretty.ConcatSpace(d, p.FormatType(s.Typ))
	if s.Collate != "" {
		collate := tree.Name(s.Collate)
		d = pretty.ConcatSpace(d, pretty.ConcatSpace(
			pretty.Keyword("COLLATE"), p.Doc(&collate)))
	}
	if s.NotNull {
		d = pretty.ConcatSpace(d, pretty.Keyword("NOT NULL"))
	}
	if s.Expr != nil {
		d = pretty.ConcatSpace(d, pretty.NestUnder(pretty.Text(":="), p.Doc(s.Expr)))
	}
	return pretty.Concat(d, pretty.Text(";"))
}

func (s *CursorDeclaration) Doc(p *tree.PrettyCfg) pretty.Doc {
	d := p.Doc(&s.Name)
	switch s.Scroll {
	case tree.Scroll:
		d = pretty.ConcatSpace(d, pretty.Keyword("SCROLL"))
	case tree.NoScroll:
		d = pretty.ConcatSpace(d, pretty.Keyword("NO SCROLL"))
	}
	d = pretty.ConcatSpace(d, pretty.Keyword("CURSOR FOR"))
	d = pretty.NestUnder(d, p.Doc(s.Query))
	return pretty.Concat(d, pretty.Text(";"))
}

func (s *Assignment) Doc(p *tree.PrettyCfg) pretty.Doc {
	d := p.Doc(&s.Var)
	if s.Indirection != "" {
		d = pretty.Concat(d, pretty.Concat(pretty.Text("."), p.Doc(&s.Indirection)))
	}
	d = pretty.ConcatSpace(d, pretty.NestUnder(pretty.Text(":="), p.Doc(s.Value)))
	return pretty.Concat(d, pretty.Text(";"))
}

func (s *Return) Doc(p *tree.PrettyCfg) pretty.Doc {
	d := pretty.Keyword("RETURN")
	if s.Expr != nil {
		d = pretty.NestUnder(d, p.Doc(s.Expr))
	}
	return pretty.Concat(d, pretty.Text(";"))
}

func (s *ReturnNext) Doc(p *tree.PrettyCfg) pretty.Doc {
	d := pretty.Keyword("RETURN NEXT")
	if s.Expr != nil {
		d = pretty.NestUnder(d, p.Doc(s.Expr))
	}
	return pretty.Concat(d, pretty.Text(";"))
}

func (s *ReturnQuery) Doc(p *tree.PrettyCfg) pretty.Doc {
	d := pretty.Keyword("RETURN QUERY")
	if s.SqlStmt != nil {
		d = pretty.NestUnder(d, p.Doc(s.SqlStmt))
	}
	return pretty.Concat(d, pretty.Text(";"))
}

func (s *Exit) Doc(p *tree.PrettyCfg) pretty.Doc {
	d := pretty.Keyword("EXIT")
	if s.Label != "" {
		name := tree.Name(s.Label)
		d = pretty.ConcatSpace(d, p.Doc(&name))
	}
	if s.Condition != nil {
		d = pretty.NestUnder(d, pretty.ConcatSpace(
			pretty.Keyword("WHEN"), p.Doc(s.Condition)))
	}
	return pretty.Concat(d, pretty.Text(";"))
}

func (s *Continue) Doc(p *tree.PrettyCfg) pretty.Doc {
	d := pretty.Keyword("CONTINUE")
	if s.Label != "" {
		name := tree.Name(s.Label)
		d = pretty.ConcatSpace(d, p.Doc(&name))
	}
	if s.Condition != nil {
		d = pretty.NestUnder(d, pretty.ConcatSpace(
			pretty.Keyword("WHEN"), p.Doc(s.Condition)))
	}
	return pretty.Concat(d, pretty.Text(";"))
}

func (s *If) Doc(p *tree.PrettyCfg) pretty.Doc {
	parts := make([]pretty.Doc, 0, 4+len(s.ElseIfList))
	header := pretty.Fold(pretty.ConcatSpace,
		pretty.Keyword("IF"), p.Doc(s.Condition), pretty.Keyword("THEN"))
	parts = append(parts, pretty.Concat(header, indentBody(stmtListDoc(p, s.ThenBody))))
	for i := range s.ElseIfList {
		parts = append(parts, p.Doc(&s.ElseIfList[i]))
	}
	if len(s.ElseBody) > 0 {
		parts = append(parts,
			pretty.Concat(pretty.Keyword("ELSE"), indentBody(stmtListDoc(p, s.ElseBody))))
	}
	parts = append(parts, pretty.Concat(pretty.Keyword("END IF"), pretty.Text(";")))
	return pretty.Stack(parts...)
}

func (s *ElseIf) Doc(p *tree.PrettyCfg) pretty.Doc {
	header := pretty.Fold(pretty.ConcatSpace,
		pretty.Keyword("ELSIF"), p.Doc(s.Condition), pretty.Keyword("THEN"))
	return pretty.Concat(header, indentBody(stmtListDoc(p, s.Stmts)))
}

func (s *Loop) Doc(p *tree.PrettyCfg) pretty.Doc {
	parts := make([]pretty.Doc, 0, 3)
	if s.Label != "" {
		parts = append(parts, labelDoc(p, s.Label))
	}
	parts = append(parts,
		pretty.Concat(pretty.Keyword("LOOP"), indentBody(stmtListDoc(p, s.Body))))
	parts = append(parts, endLoopDoc(p, s.Label))
	return pretty.Stack(parts...)
}

func (s *While) Doc(p *tree.PrettyCfg) pretty.Doc {
	parts := make([]pretty.Doc, 0, 3)
	if s.Label != "" {
		parts = append(parts, labelDoc(p, s.Label))
	}
	header := pretty.Fold(pretty.ConcatSpace,
		pretty.Keyword("WHILE"), p.Doc(s.Condition), pretty.Keyword("LOOP"))
	parts = append(parts,
		pretty.Concat(header, indentBody(stmtListDoc(p, s.Body))))
	parts = append(parts, endLoopDoc(p, s.Label))
	return pretty.Stack(parts...)
}

func (s *ForLoop) Doc(p *tree.PrettyCfg) pretty.Doc {
	parts := make([]pretty.Doc, 0, 3)
	if s.Label != "" {
		parts = append(parts, labelDoc(p, s.Label))
	}
	targetDocs := make([]pretty.Doc, len(s.Target))
	for i := range s.Target {
		targetDocs[i] = p.Doc(&s.Target[i])
	}
	header := pretty.Fold(pretty.ConcatSpace,
		pretty.ConcatSpace(pretty.Keyword("FOR"), pretty.Join(",", targetDocs...)),
		pretty.Keyword("IN"), p.Doc(s.Control), pretty.Keyword("LOOP"))
	parts = append(parts,
		pretty.Concat(header, indentBody(stmtListDoc(p, s.Body))))
	parts = append(parts, endLoopDoc(p, s.Label))
	return pretty.Stack(parts...)
}

func (s *Call) Doc(p *tree.PrettyCfg) pretty.Doc {
	return pretty.Concat(
		pretty.NestUnder(pretty.Keyword("CALL"), p.Doc(s.Proc)),
		pretty.Text(";"))
}

func (s *Fetch) Doc(p *tree.PrettyCfg) pretty.Doc {
	var d pretty.Doc
	if s.IsMove {
		d = pretty.Keyword("MOVE")
	} else {
		d = pretty.Keyword("FETCH")
	}
	if dir := s.Cursor.FetchType.String(); dir != "" {
		d = pretty.ConcatSpace(d, pretty.Keyword(dir))
	}
	if s.Cursor.FetchType.HasCount() {
		d = pretty.ConcatSpace(d, pretty.Text(strconv.Itoa(int(s.Cursor.Count))))
	}
	curName := tree.Name(string(s.Cursor.Name))
	d = pretty.ConcatSpace(d, pretty.ConcatSpace(
		pretty.Keyword("FROM"), p.Doc(&curName)))
	if s.Target != nil {
		targetDocs := make([]pretty.Doc, len(s.Target))
		for i := range s.Target {
			targetDocs[i] = p.Doc(&s.Target[i])
		}
		d = pretty.NestUnder(d, pretty.NestUnder(
			pretty.Keyword("INTO"), pretty.Join(",", targetDocs...)))
	}
	return pretty.Concat(d, pretty.Text(";"))
}

func (s *Raise) Doc(p *tree.PrettyCfg) pretty.Doc {
	d := pretty.Keyword("RAISE")
	if s.LogLevel != "" {
		d = pretty.ConcatSpace(d, pretty.Keyword(strings.ToUpper(s.LogLevel)))
	}
	if s.Code != "" {
		d = pretty.ConcatSpace(d,
			pretty.ConcatSpace(pretty.Keyword("SQLSTATE"),
				pretty.Text(formatStringQuotes(p, s.Code))))
	}
	if s.CodeName != "" {
		d = pretty.ConcatSpace(d, pretty.Text(formatString(p, s.CodeName)))
	}
	if s.Message != "" {
		msgParts := make([]pretty.Doc, 1+len(s.Params))
		msgParts[0] = pretty.Text(formatStringQuotes(p, s.Message))
		for i := range s.Params {
			msgParts[i+1] = p.Doc(s.Params[i])
		}
		d = pretty.NestUnder(d, pretty.Join(",", msgParts...))
	}
	if len(s.Options) > 0 {
		optDocs := make([]pretty.Doc, len(s.Options))
		for i := range s.Options {
			optDocs[i] = p.Doc(&s.Options[i])
		}
		d = pretty.NestUnder(d, pretty.NestUnder(
			pretty.Keyword("USING"), pretty.Join(",", optDocs...)))
	}
	return pretty.Concat(d, pretty.Text(";"))
}

func (s *Execute) Doc(p *tree.PrettyCfg) pretty.Doc {
	d := p.Doc(s.SqlStmt)
	if s.Target != nil {
		into := pretty.Keyword("INTO")
		if s.Strict {
			into = pretty.ConcatSpace(into, pretty.Keyword("STRICT"))
		}
		targetDocs := make([]pretty.Doc, len(s.Target))
		for i := range s.Target {
			targetDocs[i] = p.Doc(&s.Target[i])
		}
		d = pretty.NestUnder(d, pretty.NestUnder(
			into, pretty.Join(",", targetDocs...)))
	}
	return pretty.Concat(d, pretty.Text(";"))
}

func (s *Open) Doc(p *tree.PrettyCfg) pretty.Doc {
	d := pretty.ConcatSpace(pretty.Keyword("OPEN"), p.Doc(&s.CurVar))
	switch s.Scroll {
	case tree.Scroll:
		d = pretty.ConcatSpace(d, pretty.Keyword("SCROLL"))
	case tree.NoScroll:
		d = pretty.ConcatSpace(d, pretty.Keyword("NO SCROLL"))
	}
	if s.Query != nil {
		d = pretty.ConcatSpace(d, pretty.Keyword("FOR"))
		d = pretty.NestUnder(d, p.Doc(s.Query))
	}
	return pretty.Concat(d, pretty.Text(";"))
}

func (s *Exception) Doc(p *tree.PrettyCfg) pretty.Doc {
	header := pretty.Keyword("WHEN")
	for i := range s.Conditions {
		if i > 0 {
			header = pretty.ConcatSpace(header, pretty.Keyword("OR"))
		}
		header = pretty.ConcatSpace(header, p.Doc(&s.Conditions[i]))
	}
	header = pretty.ConcatSpace(header, pretty.Keyword("THEN"))
	return pretty.Concat(header, indentBody(stmtListDoc(p, s.Action)))
}

func (s *DoBlock) Doc(p *tree.PrettyCfg) pretty.Doc {
	// Render the block to a string to find a dollar-quote delimiter that
	// doesn't collide with the body content.
	bodyStr := tree.AsStringWithFlags(s.Block, p.FmtFlagsWithDefaults())
	tag := "$" + tree.DollarQuoteDelimiter(bodyStr) + "$"
	return pretty.Fold(pretty.Concat,
		pretty.ConcatSpace(pretty.Keyword("DO"), pretty.Text(tag)),
		indentBody(p.Doc(s.Block)),
		pretty.HardLine,
		pretty.Text(tag+";"))
}
