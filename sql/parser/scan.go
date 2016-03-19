// Copyright 2015 The Cockroach Authors.
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
//
// Author: Peter Mattis (peter@cockroachlabs.com)

package parser

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
	"unicode/utf8"
)

const eof = -1
const errUnterminated = "unterminated string"
const singleQuote = '\''

type scanner struct {
	in          string
	pos         int
	tokBuf      sqlSymType
	lastTok     sqlSymType
	nextTok     *sqlSymType
	lastError   string
	stmts       []Statement
	identQuote  int
	stringQuote int
	syntax      Syntax

	initialized bool
}

func makeScanner(str string, syntax Syntax) scanner {
	var s scanner
	s.init(str, syntax)
	return s
}

func (s *scanner) init(str string, syntax Syntax) {
	if s.initialized {
		panic("scanner already initialized; a scanner cannot be reused.")
	}
	s.initialized = true
	s.in = str
	s.syntax = syntax
	switch syntax {
	case Traditional:
		s.identQuote = '"'
	case Modern:
		s.identQuote = '`'
		s.stringQuote = '"'
	}
}

func (s *scanner) Lex(lval *sqlSymType) int {
	// The core lexing takes place in scan(). Here we do a small bit of post
	// processing of the lexical tokens so that the grammar only requires
	// one-token lookahead despite SQL requiring multi-token lookahead in some
	// cases. These special cases are handled below and the returned tokens are
	// adjusted to reflect the lookahead (LA) that occurred.

	if s.nextTok != nil {
		*lval = *s.nextTok
		s.nextTok = nil
	} else {
		s.scan(lval)
	}

	switch lval.id {
	case NOT, NULLS, WITH:
	default:
		s.lastTok = *lval
		return lval.id
	}

	s.nextTok = &s.tokBuf
	s.scan(s.nextTok)

	switch lval.id {
	case NOT:
		switch s.nextTok.id {
		case BETWEEN, IN, LIKE, SIMILAR:
			lval.id = NOT_LA
		}

	case WITH:
		switch s.nextTok.id {
		case TIME, ORDINALITY:
			lval.id = WITH_LA
		}
	}

	s.lastTok = *lval
	return lval.id
}

func (s *scanner) Error(e string) {
	var buf bytes.Buffer
	if s.lastTok.id == ERROR {
		fmt.Fprintf(&buf, "%s", s.lastTok.str)
	} else {
		fmt.Fprintf(&buf, "%s at or near \"%s\"", e, s.lastTok.str)
	}

	// Find the end of the line containing the last token.
	i := strings.Index(s.in[s.lastTok.pos:], "\n")
	if i == -1 {
		i = len(s.in)
	} else {
		i += s.lastTok.pos
	}
	// Find the beginning of the line containing the last token. Note that
	// LastIndex returns -1 if "\n" could not be found.
	j := strings.LastIndex(s.in[:s.lastTok.pos], "\n") + 1
	// Output everything up to and including the line containing the last token.
	fmt.Fprintf(&buf, "\n%s\n", s.in[:i])
	// Output a caret indicating where the last token starts.
	fmt.Fprintf(&buf, "%s^\n", strings.Repeat(" ", s.lastTok.pos-j))

	s.lastError = buf.String()
}

func (s *scanner) scan(lval *sqlSymType) {
	lval.id = 0
	lval.pos = s.pos
	lval.str = "EOF"

	if _, ok := s.skipWhitespace(lval, true); !ok {
		return
	}

	ch := s.next()
	if ch == eof {
		lval.pos = s.pos
		return
	}

	lval.id = int(ch)
	lval.pos = s.pos - 1
	lval.str = s.in[lval.pos:s.pos]

	switch ch {
	case '$':
		// param? $[0-9]+
		if isDigit(s.peek()) {
			s.scanParam(lval)
			return
		} else if s.syntax == Modern {
			// TODO(pmattis): This should really be prefixed with '@', but that
			// conflicts with using '@' for index indirection in qualified names.
			//
			// param? $<ident>
			if t := s.peek(); isIdentStart(t) {
				s.pos++
				s.scanIdent(lval, t)
				lval.id = PARAM
				return
			}
		}
		return

	case s.identQuote:
		// "[^"]"
		if s.scanString(lval, s.identQuote, false) {
			lval.id = IDENT
		}
		return

	case singleQuote:
		// '[^']'
		if s.scanString(lval, ch, s.syntax == Modern) {
			lval.id = SCONST
		}
		return

	case s.stringQuote:
		// '[^']'
		if s.scanString(lval, s.stringQuote, s.syntax == Modern) {
			lval.id = SCONST
		}
		return

	case 'b', 'B':
		// Bytes?
		if t := s.peek(); t == singleQuote || t == s.stringQuote {
			// [bB]'[^']'
			s.pos++
			if s.scanString(lval, t, true) {
				lval.id = BCONST
			}
			return
		} else if s.syntax == Modern && (t == 'r' || t == 'R') {
			// Raw bytes?
			if t := s.peekN(1); t == singleQuote || t == s.stringQuote {
				// [rRbB]'[^']'
				s.pos += 2
				if s.scanString(lval, t, false) {
					lval.id = BCONST
				}
				return
			}
		}
		s.scanIdent(lval, ch)
		return

	case 'r', 'R':
		if s.syntax == Modern {
			// Raw string?
			if t := s.peek(); t == singleQuote || t == s.stringQuote {
				// [rR]'[^']'
				s.pos++
				if s.scanString(lval, t, false) {
					lval.id = SCONST
				}
				return
			} else if t == 'b' || t == 'B' {
				// Raw bytes?
				if t := s.peekN(1); t == singleQuote || t == s.stringQuote {
					// [bBrR]'[^']'
					s.pos += 2
					if s.scanString(lval, t, false) {
						lval.id = BCONST
					}
					return
				}
			}
		}
		s.scanIdent(lval, ch)
		return

	case 'e', 'E':
		// Escaped string?
		if t := s.peek(); t == singleQuote || t == s.stringQuote {
			// [eE]'[^']'
			s.pos++
			if s.scanString(lval, t, true) {
				lval.id = SCONST
			}
			return
		}
		s.scanIdent(lval, ch)
		return

	case '.':
		switch t := s.peek(); {
		case t == '.': // ..
			s.pos++
			lval.id = DOT_DOT
			return
		case isDigit(t):
			s.scanNumber(lval, ch)
			return
		}
		return

	case '!':
		switch s.peek() {
		case '=': // !=
			s.pos++
			lval.id = NOT_EQUALS
			return
		}
		return

	case '<':
		switch s.peek() {
		case '<': // <<
			s.pos++
			lval.id = LSHIFT
			return
		case '>': // <>
			s.pos++
			lval.id = NOT_EQUALS
			return
		case '=': // <=
			s.pos++
			lval.id = LESS_EQUALS
			return
		}
		return

	case '>':
		switch s.peek() {
		case '>': // >>
			s.pos++
			lval.id = RSHIFT
			return
		case '=': // >=
			s.pos++
			lval.id = GREATER_EQUALS
			return
		}
		return

	case ':':
		switch s.peek() {
		case ':': // ::
			s.pos++
			lval.id = TYPECAST
			return
		}
		return

	case '|':
		switch s.peek() {
		case '|': // ||
			s.pos++
			lval.id = CONCAT
			return
		}
		return

	default:
		if isDigit(ch) {
			s.scanNumber(lval, ch)
			return
		}
		if isIdentStart(ch) {
			s.scanIdent(lval, ch)
			return
		}
	}

	// Everything else is a single character token which we already initialized
	// lval for above.
}

func (s *scanner) peek() int {
	if s.pos >= len(s.in) {
		return eof
	}
	return int(s.in[s.pos])
}

func (s *scanner) peekN(n int) int {
	pos := s.pos + n
	if pos >= len(s.in) {
		return eof
	}
	return int(s.in[pos])
}

func (s *scanner) next() int {
	ch := s.peek()
	if ch != eof {
		s.pos++
	}
	return ch
}

func (s *scanner) skipWhitespace(lval *sqlSymType, allowComments bool) (newline, ok bool) {
	newline = false
	for {
		ch := s.peek()
		if ch == '\n' {
			s.pos++
			newline = true
			continue
		}
		if ch == ' ' || ch == '\t' || ch == '\r' || ch == '\f' {
			s.pos++
			continue
		}
		if allowComments {
			if present, cok := s.scanComment(lval); !cok {
				return false, false
			} else if present {
				continue
			}
		}
		break
	}
	return newline, true
}

func (s *scanner) scanComment(lval *sqlSymType) (present, ok bool) {
	start := s.pos
	ch := s.peek()

	if ch == '/' {
		s.pos++
		if s.peek() != '*' {
			s.pos--
			return false, true
		}
		s.pos++
		depth := 1
		for {
			switch s.next() {
			case '*':
				if s.peek() == '/' {
					s.pos++
					depth--
					if depth == 0 {
						return true, true
					}
					continue
				}

			case '/':
				if s.peek() == '*' {
					s.pos++
					depth++
					continue
				}

			case eof:
				lval.id = ERROR
				lval.pos = start
				lval.str = "unterminated comment"
				return false, false
			}
		}
	}

	if ch == '-' {
		s.pos++
		if s.peek() != '-' {
			s.pos--
			return false, true
		}
		for {
			switch s.next() {
			case eof, '\n':
				return true, true
			}
		}
	}

	if s.syntax == Modern && ch == '#' {
		s.pos++
		for {
			switch s.next() {
			case eof, '\n':
				return true, true
			}
		}
	}

	return false, true
}

func (s *scanner) scanIdent(lval *sqlSymType, ch int) {
	start := s.pos - 1
	for {
		ch := s.peek()
		if isIdentMiddle(ch) {
			s.pos++
			continue
		}
		break
	}
	lval.str = s.in[start:s.pos]
	uppered := strings.ToUpper(lval.str)
	if id, ok := keywords[uppered]; ok {
		lval.id = id
		return
	}
	lval.id = IDENT
}

func (s *scanner) scanNumber(lval *sqlSymType, ch int) {
	start := s.pos - 1
	isHex := false
	hasDecimal := ch == '.'
	hasExponent := false

	for {
		ch := s.peek()
		if isHex && isHexDigit(ch) || isDigit(ch) {
			s.pos++
			continue
		}
		if ch == 'x' || ch == 'X' {
			if isHex || s.in[start] != '0' || s.pos != start+1 {
				lval.id = ERROR
				lval.str = "invalid hexadecimal literal"
				return
			}
			s.pos++
			isHex = true
			continue
		}
		if isHex {
			break
		}
		if ch == '.' {
			if hasDecimal || hasExponent {
				break
			}
			s.pos++
			if s.peek() == '.' {
				// Found ".." while scanning a number: back up to the end of the
				// integer.
				s.pos--
				break
			}
			hasDecimal = true
			continue
		}
		if ch == 'e' || ch == 'E' {
			if hasExponent {
				break
			}
			hasExponent = true
			s.pos++
			ch = s.peek()
			if ch == '-' || ch == '+' {
				s.pos++
			}
			ch = s.peek()
			if !isDigit(ch) {
				lval.id = ERROR
				lval.str = "invalid floating point literal"
				return
			}
			continue
		}
		break
	}

	lval.str = s.in[start:s.pos]
	if hasDecimal || hasExponent {
		lval.id = FCONST
		return
	}

	// We explicitly parse only decimal and hexadecimal literals here. We could
	// parse octal as well, but have decided for the present time not to.
	var err error
	var uval uint64
	if isHex {
		if s.pos == start+2 {
			lval.id = ERROR
			lval.str = "invalid hexadecimal literal"
			return
		}
		uval, err = strconv.ParseUint(lval.str[2:], 16, 64)
	} else {
		uval, err = strconv.ParseUint(lval.str, 10, 64)
	}
	if err == strconv.ErrRange || uval > 1<<63 {
		// Integers that would overflow an int64 should be parsed
		// as NumVals so they can have arbitrary precision.
		lval.id = FCONST
		return
	}
	// uval is now in the range [0, 1<<63]. Casting to an int64 leaves the range
	// [0, 1<<63 - 1] intact and moves 1<<63 to -1<<63 (a.k.a. math.MinInt64).
	lval.union.val = IntVal{Val: int64(uval), Str: lval.str}
	if err != nil {
		lval.id = ERROR
		lval.str = err.Error()
		return
	}

	lval.id = ICONST
}

func (s *scanner) scanParam(lval *sqlSymType) {
	start := s.pos
	for isDigit(s.peek()) {
		s.pos++
	}
	lval.str = s.in[start:s.pos]

	uval, err := strconv.ParseUint(lval.str, 10, 64)
	if err == nil && uval > 1<<63 {
		err = fmt.Errorf("integer value out of range: %d", uval)
	}
	// uval is now in the range [0, 1<<63]. Casting to an int64 leaves the range
	// [0, 1<<63 - 1] intact and moves 1<<63 to -1<<63 (a.k.a. math.MinInt64).
	lval.union.val = IntVal{Val: int64(uval), Str: lval.str}
	if err != nil {
		lval.id = ERROR
		lval.str = err.Error()
		return
	}

	lval.id = PARAM
}

func (s *scanner) scanString(lval *sqlSymType, ch int, allowEscapes bool) bool {
	var buf []byte
	var runeTmp [utf8.UTFMax]byte
	start := s.pos
	tripleQuoted := false

outer:
	for {
		switch s.next() {
		case ch:
			switch s.syntax {
			case Traditional:
				buf = append(buf, s.in[start:s.pos-1]...)
				if s.peek() == ch {
					// Double quote is translated into a single quote that is part of the
					// string.
					start = s.pos
					s.pos++
					continue
				}

				if newline, ok := s.skipWhitespace(lval, false); !ok {
					return false
				} else if !newline {
					break outer
				}
				// SQL allows joining adjacent strings separated by whitespace as long as
				// that whitespace contains at least one newline. Kind of strange to
				// require the newline, but that is the standard.
				if s.peek() != ch {
					break outer
				}
				s.pos++
				start = s.pos

			case Modern:
				if s.pos == start+1 && s.peek() == ch {
					// Triple-quotes at the start of the string.
					s.pos++
					start = s.pos
					tripleQuoted = true
					continue
				}
				if !tripleQuoted {
					buf = append(buf, s.in[start:s.pos-1]...)
					break outer
				}
				if s.peek() == ch && s.peekN(1) == ch {
					// Triple-quotes at the end of the string.
					buf = append(buf, s.in[start:s.pos-1]...)
					s.pos += 2
					break outer
				}
			}
			continue

		case '\n':
			if s.syntax == Modern && !tripleQuoted {
				lval.id = ERROR
				lval.str = fmt.Sprintf("invalid syntax: embedded newline")
				return false
			}

		case '\\':
			t := s.peek()

			if allowEscapes {
				buf = append(buf, s.in[start:s.pos-1]...)
				if t == ch {
					start = s.pos
					s.pos++
					continue
				}

				switch t {
				case 'a', 'b', 'f', 'n', 'r', 't', 'v', 'x', 'X', 'u', 'U', '\\',
					'0', '1', '2', '3', '4', '5', '6', '7':
					var tmp string
					if t == 'X' && len(s.in[s.pos:]) >= 3 {
						// UnquoteChar doesn't handle 'X' so we create a temporary string
						// for it to parse.
						tmp = "\\x" + s.in[s.pos+1:s.pos+3]
					} else {
						tmp = s.in[s.pos-1:]
					}
					v, multibyte, tail, err := strconv.UnquoteChar(tmp, byte(ch))
					if err != nil {
						lval.id = ERROR
						lval.str = err.Error()
						return false
					}
					if v < utf8.RuneSelf || !multibyte {
						buf = append(buf, byte(v))
					} else {
						n := utf8.EncodeRune(runeTmp[:], v)
						buf = append(buf, runeTmp[:n]...)
					}
					s.pos += len(tmp) - len(tail) - 1
					start = s.pos
					continue
				}

				// If we end up here, it's a redundant escape - simply drop the
				// backslash. For example, e'\"' is equivalent to e'"', and
				// e'\d\b' to e'd\b'. This is what Postgres does:
				// http://www.postgresql.org/docs/9.4/static/sql-syntax-lexical.html#SQL-SYNTAX-STRINGS-ESCAPE
				start = s.pos
			}

		case eof:
			lval.id = ERROR
			lval.str = errUnterminated
			return false
		}
	}

	lval.str = string(buf)
	return true
}

func isDigit(ch int) bool {
	return ch >= '0' && ch <= '9'
}

func isHexDigit(ch int) bool {
	return (ch >= '0' && ch <= '9') ||
		(ch >= 'a' && ch <= 'f') ||
		(ch >= 'A' && ch <= 'F')
}

func isIdent(s string) bool {
	if len(s) == 0 || !isIdentStart(int(s[0])) {
		return false
	}
	for i := 1; i < len(s); i++ {
		if !isIdentMiddle(int(s[i])) {
			return false
		}
	}
	return true
}

func isIdentStart(ch int) bool {
	return (ch >= 'A' && ch <= 'Z') ||
		(ch >= 'a' && ch <= 'z') ||
		(ch >= 128 && ch <= 255) ||
		(ch == '_')
}

func isIdentMiddle(ch int) bool {
	return isIdentStart(ch) || isDigit(ch) || ch == '$'
}
