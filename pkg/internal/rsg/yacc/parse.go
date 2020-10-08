// Copyright 2011 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in licenses/BSD-golang.txt.

// Portions of this file are additionally subject to the following license
// and copyright.
//
// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Copied from Go's text/template/parse package and modified for yacc.

// Package yacc parses .y files.
package yacc

import (
	"fmt"
	"runtime"

	"github.com/cockroachdb/errors"
)

// Tree is the representation of a single parsed file.
type Tree struct {
	Name        string // name of the template represented by the tree.
	Productions []*ProductionNode
	text        string // text parsed to create the template (or its parent)
	// Parsing only; cleared after parse.
	lex       *lexer
	token     [2]item // two-token lookahead for parser.
	peekCount int
}

// Parse parses the yacc file text with optional name.
func Parse(name, text string) (t *Tree, err error) {
	t = New(name)
	t.text = text
	err = t.Parse(text)
	return
}

// next returns the next token.
func (t *Tree) next() item {
	if t.peekCount > 0 {
		t.peekCount--
	} else {
		t.token[0] = t.lex.nextItem()
	}
	return t.token[t.peekCount]
}

// backup backs the input stream up one token.
func (t *Tree) backup() {
	t.peekCount++
}

// peek returns but does not consume the next token.
func (t *Tree) peek() item {
	if t.peekCount > 0 {
		return t.token[t.peekCount-1]
	}
	t.peekCount = 1
	t.token[0] = t.lex.nextItem()
	return t.token[0]
}

// Parsing.

// New allocates a new parse tree with the given name.
func New(name string) *Tree {
	return &Tree{
		Name: name,
	}
}

// errorf formats the error and terminates processing.
func (t *Tree) errorf(format string, args ...interface{}) {
	err := fmt.Errorf(format, args...)
	err = errors.Wrapf(err, "parse: %s:%d", t.Name, t.lex.lineNumber())
	panic(err)
}

// expect consumes the next token and guarantees it has the required type.
func (t *Tree) expect(expected itemType, context string) item {
	token := t.next()
	if token.typ != expected {
		t.unexpected(token, context)
	}
	return token
}

// unexpected complains about the token and terminates processing.
func (t *Tree) unexpected(token item, context string) {
	t.errorf("unexpected %s in %s", token, context)
}

// recover is the handler that turns panics into returns from the top level of Parse.
func (t *Tree) recover(errp *error) {
	if e := recover(); e != nil {
		if _, ok := e.(runtime.Error); ok {
			panic(e)
		}
		if t != nil {
			t.stopParse()
		}
		*errp = e.(error)
	}
}

// startParse initializes the parser, using the lexer.
func (t *Tree) startParse(lex *lexer) {
	t.lex = lex
}

// stopParse terminates parsing.
func (t *Tree) stopParse() {
	t.lex = nil
}

// Parse parses the yacc string to construct a representation of
// the file for analysis.
func (t *Tree) Parse(text string) (err error) {
	defer t.recover(&err)
	t.startParse(lex(t.Name, text))
	t.text = text
	t.parse()
	t.stopParse()
	return nil
}

// parse is the top-level parser for a file.
// It runs to EOF.
func (t *Tree) parse() {
	for {
		switch token := t.next(); token.typ {
		case itemIdent:
			p := newProduction(token.pos, token.val)
			t.parseProduction(p)
			t.Productions = append(t.Productions, p)
		case itemEOF:
			return
		}
	}
}

func (t *Tree) parseProduction(p *ProductionNode) {
	const context = "production"
	t.expect(itemColon, context)
	if t.peek().typ == itemNL {
		t.next()
	}
	expectExpr := true
	for {
		token := t.next()
		switch token.typ {
		case itemComment, itemNL:
			// ignore
		case itemPipe:
			if expectExpr {
				t.unexpected(token, context)
			}
			expectExpr = true
		default:
			t.backup()
			if !expectExpr {
				return
			}
			e := newExpression(token.pos)
			t.parseExpression(e)
			p.Expressions = append(p.Expressions, e)
			expectExpr = false
		}
	}
}

func (t *Tree) parseExpression(e *ExpressionNode) {
	const context = "expression"
	for {
		switch token := t.next(); token.typ {
		case itemNL:
			peek := t.peek().typ
			if peek == itemPipe || peek == itemNL {
				return
			}
		case itemIdent:
			e.Items = append(e.Items, Item{token.val, TypToken})
		case itemLiteral:
			e.Items = append(e.Items, Item{token.val, TypLiteral})
		case itemExpr:
			e.Command = token.val
			if t.peek().typ == itemNL {
				t.next()
			}
			return
		case itemPct, itemComment:
			// ignore
		default:
			t.unexpected(token, context)
		}
	}
}
