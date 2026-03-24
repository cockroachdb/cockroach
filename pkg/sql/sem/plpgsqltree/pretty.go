// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package plpgsqltree

import (
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
func labelDoc(label string) pretty.Doc {
	if label == "" {
		return pretty.Nil
	}
	return pretty.Text("<<" + tree.NameString(label) + ">>")
}

// endLoopDoc formats "END LOOP [label];" as a pretty.Doc.
func endLoopDoc(label string) pretty.Doc {
	d := pretty.Keyword("END LOOP")
	if label != "" {
		d = pretty.ConcatSpace(d, pretty.Text(tree.NameString(label)))
	}
	return pretty.Concat(d, pretty.Text(";"))
}

func (s *Block) Doc(p *tree.PrettyCfg) pretty.Doc {
	parts := make([]pretty.Doc, 0, 5)
	if s.Label != "" {
		parts = append(parts, labelDoc(s.Label))
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
		end = pretty.ConcatSpace(end, pretty.Text(tree.NameString(s.Label)))
	}
	end = pretty.Concat(end, pretty.Text(";"))
	parts = append(parts, end)
	return pretty.Stack(parts...)
}

func (s *If) Doc(p *tree.PrettyCfg) pretty.Doc {
	parts := make([]pretty.Doc, 0, 4+len(s.ElseIfList))
	header := pretty.ConcatSpace(pretty.Keyword("IF"), p.Doc(s.Condition))
	header = pretty.ConcatSpace(header, pretty.Keyword("THEN"))
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
	header := pretty.ConcatSpace(pretty.Keyword("ELSIF"), p.Doc(s.Condition))
	header = pretty.ConcatSpace(header, pretty.Keyword("THEN"))
	return pretty.Concat(header, indentBody(stmtListDoc(p, s.Stmts)))
}

func (s *Loop) Doc(p *tree.PrettyCfg) pretty.Doc {
	parts := make([]pretty.Doc, 0, 3)
	if s.Label != "" {
		parts = append(parts, labelDoc(s.Label))
	}
	parts = append(parts,
		pretty.Concat(pretty.Keyword("LOOP"), indentBody(stmtListDoc(p, s.Body))))
	parts = append(parts, endLoopDoc(s.Label))
	return pretty.Stack(parts...)
}

func (s *While) Doc(p *tree.PrettyCfg) pretty.Doc {
	parts := make([]pretty.Doc, 0, 3)
	if s.Label != "" {
		parts = append(parts, labelDoc(s.Label))
	}
	header := pretty.ConcatSpace(pretty.Keyword("WHILE"), p.Doc(s.Condition))
	header = pretty.ConcatSpace(header, pretty.Keyword("LOOP"))
	parts = append(parts,
		pretty.Concat(header, indentBody(stmtListDoc(p, s.Body))))
	parts = append(parts, endLoopDoc(s.Label))
	return pretty.Stack(parts...)
}

func (s *ForLoop) Doc(p *tree.PrettyCfg) pretty.Doc {
	parts := make([]pretty.Doc, 0, 3)
	if s.Label != "" {
		parts = append(parts, labelDoc(s.Label))
	}
	header := pretty.Keyword("FOR ")
	for i, target := range s.Target {
		if i > 0 {
			header = pretty.Concat(header, pretty.Text(", "))
		}
		header = pretty.Concat(header, pretty.Text(tree.NameString(string(target))))
	}
	header = pretty.ConcatSpace(header, pretty.Keyword("IN"))
	header = pretty.ConcatSpace(header, p.Doc(s.Control))
	header = pretty.ConcatSpace(header, pretty.Keyword("LOOP"))
	parts = append(parts,
		pretty.Concat(header, indentBody(stmtListDoc(p, s.Body))))
	parts = append(parts, endLoopDoc(s.Label))
	return pretty.Stack(parts...)
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
		d = pretty.ConcatSpace(d, pretty.Text(formatStringQuotes(p, s.Message)))
		for i := range s.Params {
			d = pretty.Concat(d, pretty.Concat(pretty.Text(", "), p.Doc(s.Params[i])))
		}
	}
	if len(s.Options) > 0 {
		optDocs := make([]pretty.Doc, len(s.Options))
		for i := range s.Options {
			optDocs[i] = p.Doc(&s.Options[i])
		}
		d = pretty.ConcatSpace(d, pretty.ConcatSpace(
			pretty.Keyword("USING"), pretty.Join(",", optDocs...)))
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
	// The block is rendered twice: once to a string to determine a safe
	// dollar-quote delimiter, and once as a Doc for structured layout.
	bodyStr, err := p.Pretty(s.Block)
	if err != nil {
		bodyStr = tree.AsString(s.Block)
	}
	delimiter := tree.DollarQuoteDelimiter(bodyStr)
	openTag := "DO $" + delimiter + "$"
	closeTag := "$" + delimiter + "$;"
	return pretty.Concat(
		pretty.Text(openTag),
		pretty.Concat(indentBody(p.Doc(s.Block)),
			pretty.Concat(pretty.HardLine, pretty.Text(closeTag))))
}
