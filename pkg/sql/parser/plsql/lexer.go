// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package plsql implements a plpgsql prototype.
package plsql

import (
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
)

// Prototype PL/SQL grammar:
//
// p ::= DECLARE d ; BEGIN s ;             -- END UDF body
// d ::= v τ | v τ := a | d ; d            -- variable declarations
// s ::= v := a                            --statements
//     | IF a THEN s ; [ ELSE s ; ] END IF
//     | LOOP s ; END LOOP
//     | EXIT | CONTINUE | RETURN a
//     | s ; s                             --statement sequence
// a ::= (⟨scalar SQL expression⟩)         --embedded SQL
// v ::= ⟨identifier⟩                      --variable name
// τ ::= ⟨scalar SQL type⟩                 --scalar value type
//
type lexer struct {
	in  string
	pos int
	res *tree.PLFuncBody
	err error
	str string
}

func newLexer(in string) *lexer {
	return &lexer{in: in}
}

func (l *lexer) setResult(decs []tree.PLDeclaration, stmts []tree.PLStatement) {
	l.res = &tree.PLFuncBody{
		Declarations: decs,
		Statements:   stmts,
	}
}

const eof = -1
const space = ' '
const newline = '\n'
const tab = '\t'

func (l *lexer) Lex(v *plsqlSymType) int {
	l.skipWhitespace()
	switch l.peek() {
	case eof:
		return eof
	case ';':
		l.pos++
		return ';'
	case ':':
		l.next()
		l.assertNext('=')
		return ASSIGNMENT
	case '(':
		return SQL_EXPR
	}
	var id int
	var str string
	for ch := l.peek(); ch != eof && ch != space && ch != newline && ch != tab && ch != ';'; ch = l.peek() {
		l.next()
		str += fmt.Sprintf("%c", ch)
	}
	if len(str) > 0 {
		id = getKeywordID(strings.ToLower(str))
	}
	l.str += str + " "
	v.str = str
	return id
}

func (l *lexer) skipWhitespace() {
	for ch := l.peek(); ch == space || ch == newline || ch == tab; ch = l.peek() {
		l.next()
	}
}

func (l *lexer) next() int {
	ch := l.peek()
	if ch != eof {
		l.pos++
	}
	return ch
}

func (l *lexer) peek() int {
	if l.err != nil || l.pos >= len(l.in) {
		return eof
	}
	return int(l.in[l.pos])
}

func (l *lexer) assertNext(ch int) {
	if l.next() != ch {
		l.Error("syntax error")
	}
}

func (l *lexer) Error(s string) {
	l.err = errors.Newf("plsql parser error: %s", s)
}

func Parse(in string) (*tree.PLFuncBody, error) {
	l := newLexer(in)
	_ = plsqlParse(l)
	return l.res, l.err
}

func (l *lexer) readSQLExpr() string {
	var res string
	var parenLevel int
	for ch := l.next(); ch != eof; ch = l.next() {
		if ch == '(' {
			parenLevel++
		}
		if ch == ')' {
			parenLevel--
		}
		res += fmt.Sprintf("%c", ch)
		if parenLevel < 0 {
			l.Error("invalid parenthesis nesting")
		}
		if parenLevel == 0 {
			break
		}
	}
	return res
}

func getKeywordID(kw string) int {
	switch kw {
	case "declare":
		return DECLARE
	case "begin":
		return BEGIN
	case "end":
		return END
	case "exit":
		return EXIT
	case "if":
		return IF
	case "in":
		return IN
	case "for":
		return FOR
	case "loop":
		return LOOP
	case "then":
		return THEN
	case "continue":
		return CONTINUE
	case "return":
		return RETURN
	case "else":
		return ELSE
	case "while":
		return WHILE
	case ":=":
		return ASSIGNMENT
	case "int":
		return INT
	case "float":
		return FLOAT
	case "text":
		return TEXT
	case "bool":
		return BOOL
	case "int[]":
		return INT_ARRAY
	case "float[]":
		return FLOAT_ARRAY
	case "text[]":
		return TEXT_ARRAY
	case "bool[]":
		return BOOL_ARRAY
	default:
		return IDENT
	}
}
