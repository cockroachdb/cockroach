// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package wkt

import (
	"fmt"
	"strconv"
	"strings"
	"unicode"

	"github.com/twpayne/go-geom"
)

// LexError is an error that occurs during lexing.
type LexError struct {
	problem string
	pos     int
}

func (e *LexError) Error() string {
	return fmt.Sprintf("lex error: %s at pos %d", e.problem, e.pos)
}

// ParseError is an error that occurs during parsing, which happens after lexing.
type ParseError struct {
	line string
}

func (e *ParseError) Error() string {
	return fmt.Sprintf("parse error: could not parse %q", e.line)
}

// Constant expected by parser when lexer reaches EOF.
const eof = 0

type wktLex struct {
	line    string
	pos     int
	ret     geom.T
	lastErr error
}

// Lex lexes a token from the input.
func (l *wktLex) Lex(yylval *wktSymType) int {
	// Skip leading spaces.
	l.trimLeft()

	// Lex a token.
	switch c := l.peek(); c {
	case eof:
		return eof
	case '(', ')', ',':
		return int(l.next())
	default:
		if unicode.IsLetter(c) {
			return l.keyword()
		} else if isNumRune(c) {
			return l.num(yylval)
		} else {
			l.lastErr = &LexError{
				problem: "unrecognized character",
				pos:     l.pos,
			}
			return eof
		}
	}
}

func getKeywordToken(tokStr string) int {
	switch tokStr {
	case "EMPTY":
		return EMPTY
	case "POINT":
		return POINT
	case "POINTZ":
		return POINTZ
	case "POINTM":
		return POINTM
	case "POINTZM":
		return POINTZM
	default:
		return eof
	}
}

// keyword lexes a string keyword.
func (l *wktLex) keyword() int {
	startPos := l.pos
	var b strings.Builder

	for {
		c := l.peek()
		if !unicode.IsLetter(c) {
			break
		}
		// Add the uppercase letter to the string builder.
		b.WriteRune(unicode.ToUpper(l.next()))
	}

	// Check for extra dimensions for geometry types.
	if b.String() != "EMPTY" {
		l.trimLeft()
		if unicode.ToUpper(l.peek()) == 'Z' {
			l.next()
			b.WriteRune('Z')
		}
		if unicode.ToUpper(l.peek()) == 'M' {
			l.next()
			b.WriteRune('M')
		}
	}

	ret := getKeywordToken(b.String())
	if ret == eof {
		l.lastErr = &LexError{
			problem: "invalid keyword",
			pos:     startPos,
		}
	}

	return ret
}

func isNumRune(r rune) bool {
	switch r {
	case '-', '.':
		return true
	default:
		return unicode.IsDigit(r)
	}
}

// num lexes a number.
func (l *wktLex) num(yylval *wktSymType) int {
	startPos := l.pos
	var b strings.Builder

	for {
		c := l.peek()
		if !isNumRune(c) {
			break
		}
		b.WriteRune(l.next())
	}

	fl, err := strconv.ParseFloat(b.String(), 64)
	if err != nil {
		l.lastErr = &LexError{
			problem: "invalid number",
			pos:     startPos,
		}
		return eof
	}
	yylval.coord = fl
	return NUM
}

func (l *wktLex) peek() rune {
	if l.pos == len(l.line) {
		return eof
	}
	return rune(l.line[l.pos])
}

func (l *wktLex) next() rune {
	c := l.peek()
	if c != eof {
		l.pos++
	}
	return c
}

func (l *wktLex) trimLeft() {
	for {
		c := l.peek()
		if c == eof || !unicode.IsSpace(c) {
			break
		}
		l.next()
	}
}

func (l *wktLex) Error(s string) {
	// Lex errors are set in the Lex function.
	// todo (ayang) improve parse error messages
	/* EMPTY */
}
