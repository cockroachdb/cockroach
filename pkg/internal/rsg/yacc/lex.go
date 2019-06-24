// Copyright 2011 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in licenses/BSD-golang.txt.

// Portions of this file are additionally subject to the following
// license and copyright.
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

package yacc

import (
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"
)

// item represents a token or text string returned from the scanner.
type item struct {
	typ itemType // The type of this item.
	pos Pos      // The starting position, in bytes, of this item in the input string.
	val string   // The value of this item.
}

func (i item) String() string {
	switch {
	case i.typ == itemEOF:
		return "EOF"
	case i.typ == itemError:
		return i.val
	case len(i.val) > 10:
		return fmt.Sprintf("%.10q...", i.val)
	}
	return fmt.Sprintf("%q", i.val)
}

// itemType identifies the type of lex items.
type itemType int

const (
	itemError itemType = iota // error occurred; value is text of error
	itemEOF
	itemComment
	itemPct
	itemDoublePct
	itemIdent
	itemColon
	itemLiteral
	itemExpr
	itemPipe
	itemNL
)

const eof = -1

// stateFn represents the state of the scanner as a function that returns the next state.
type stateFn func(*lexer) stateFn

// lexer holds the state of the scanner.
type lexer struct {
	name    string    // the name of the input; used only for error reports
	input   string    // the string being scanned
	state   stateFn   // the next lexing function to enter
	pos     Pos       // current position in the input
	start   Pos       // start position of this item
	width   Pos       // width of last rune read from input
	lastPos Pos       // position of most recent item returned by nextItem
	items   chan item // channel of scanned items
}

// next returns the next rune in the input.
func (l *lexer) next() rune {
	if int(l.pos) >= len(l.input) {
		l.width = 0
		return eof
	}
	r, w := utf8.DecodeRuneInString(l.input[l.pos:])
	l.width = Pos(w)
	l.pos += l.width
	return r
}

// peek returns but does not consume the next rune in the input.
func (l *lexer) peek() rune {
	r := l.next()
	l.backup()
	return r
}

// backup steps back one rune. Can only be called once per call of next.
func (l *lexer) backup() {
	l.pos -= l.width
}

// emit passes an item back to the client.
func (l *lexer) emit(t itemType) {
	l.items <- item{t, l.start, l.input[l.start:l.pos]}
	l.start = l.pos
}

// ignore skips over the pending input before this point.
func (l *lexer) ignore() {
	l.start = l.pos
}

// lineNumber reports which line we're on, based on the position of
// the previous item returned by nextItem. Doing it this way
// means we don't have to worry about peek double counting.
func (l *lexer) lineNumber() int {
	return 1 + strings.Count(l.input[:l.lastPos], "\n")
}

// errorf returns an error token and terminates the scan by passing
// back a nil pointer that will be the next state, terminating l.nextItem.
func (l *lexer) errorf(format string, args ...interface{}) stateFn {
	l.items <- item{itemError, l.start, fmt.Sprintf(format, args...)}
	return nil
}

// nextItem returns the next item from the input.
func (l *lexer) nextItem() item {
	i := <-l.items
	l.lastPos = i.pos
	return i
}

// lex creates a new scanner for the input string.
func lex(name, input string) *lexer {
	l := &lexer{
		name:  name,
		input: input,
		items: make(chan item),
	}
	go l.run()
	return l
}

// run runs the state machine for the lexer.
func (l *lexer) run() {
	for l.state = lexStart; l.state != nil; {
		l.state = l.state(l)
	}
}

// state functions

func lexStart(l *lexer) stateFn {
Loop:
	for {
		switch r := l.next(); {
		case r == '/':
			return lexComment
		case r == '%':
			return lexPct
		case r == '\n':
			l.emit(itemNL)
		case r == ':':
			l.emit(itemColon)
		case r == '|':
			l.emit(itemPipe)
		case r == '{':
			return lexExpr
		case isSpace(r):
			l.ignore()
		case isIdent(r):
			return lexIdent
		case r == '\'':
			return lexLiteral
		case r == eof:
			l.emit(itemEOF)
			break Loop
		default:
			return l.errorf("invalid character: %v", string(r))
		}
	}
	return nil
}

func lexLiteral(l *lexer) stateFn {
	for {
		switch l.next() {
		case '\'':
			l.emit(itemLiteral)
			return lexStart
		}
	}
}

func lexExpr(l *lexer) stateFn {
	ct := 1
	for {
		switch l.next() {
		case '{':
			ct++
		case '}':
			ct--
			if ct == 0 {
				l.emit(itemExpr)
				return lexStart
			}
		}
	}
}

func lexComment(l *lexer) stateFn {
	switch r := l.next(); r {
	case '/':
		for {
			switch l.next() {
			case '\n':
				l.backup()
				l.emit(itemComment)
				return lexStart
			}
		}
	case '*':
		for {
			switch l.next() {
			case '*':
				if l.peek() == '/' {
					l.next()
					l.emit(itemComment)
					return lexStart
				}
			}
		}
	default:
		return l.errorf("expected comment: %c", r)
	}
}

func lexPct(l *lexer) stateFn {
	switch l.next() {
	case '%':
		l.emit(itemDoublePct)
		return lexStart
	case '{':
		for {
			switch l.next() {
			case '%':
				if l.peek() == '}' {
					l.next()
					l.emit(itemPct)
					return lexStart
				}
			}
		}
	case 'p':
		if l.next() != 'r' || l.next() != 'e' || l.next() != 'c' || l.next() != ' ' {
			l.errorf("expected %%prec")
		}
		for {
			switch r := l.next(); {
			case isIdent(r):
				// absorb
			default:
				l.backup()
				l.emit(itemPct)
				return lexStart
			}
		}
	default:
		ct := 0
		for {
			switch l.next() {
			case ' ':
			case '{':
				ct++
			case '}':
				ct--
				if ct == 0 {
					l.emit(itemPct)
					return lexStart
				}
			case '\n':
				if ct == 0 {
					l.backup()
					l.emit(itemPct)
					return lexStart
				}
			}
		}
	}
}

func lexIdent(l *lexer) stateFn {
	for {
		switch r := l.next(); {
		case isIdent(r):
			// absorb
		default:
			l.backup()
			l.emit(itemIdent)
			return lexStart
		}
	}
}

func isSpace(r rune) bool {
	return r == ' ' || r == '\t'
}

func isIdent(r rune) bool {
	return r == '_' || unicode.IsLetter(r) || unicode.IsDigit(r)
}
