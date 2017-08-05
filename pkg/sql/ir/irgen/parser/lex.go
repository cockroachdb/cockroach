// Copyright 2017 The Cockroach Authors.
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

package parser

import (
	"errors"
	"fmt"
	"io"
	"strconv"
	scn "text/scanner"
)

//go:generate make

// Pos represents a position in the input.
type Pos struct {
	pos scn.Position
}

func (pos Pos) String() string {
	return pos.pos.String()
}

// PosError wraps an error that occurred at a specific position in the input.
type PosError struct {
	Pos Pos
	Err error
}

func (e PosError) Error() string {
	return fmt.Sprintf("%s: %s", e.Pos.String(), e.Err.Error())
}

type lexer struct {
	scn      scn.Scanner
	defs     []Def
	firstErr error
}

func newLexer(filename string, src io.Reader) *lexer {
	var l lexer
	l.scn.Init(src)
	l.scn.Error = func(s *scn.Scanner, msg string) {
		if l.firstErr == nil {
			l.firstErr = PosError{Pos{pos: s.Pos()}, errors.New(msg)}
		}
	}
	l.scn.Filename = filename
	return &l
}

func (l *lexer) pos() Pos {
	return Pos{pos: l.scn.Position}
}

// Error implements the irgenLexer interface.
func (l *lexer) Error(e string) {
	if l.firstErr == nil {
		l.firstErr = PosError{l.pos(), errors.New(e)}
	}
}

// Lex implements the irgenLexer interface.
func (l *lexer) Lex(lval *irgenSymType) int {
	tok := l.scn.Scan()
	if l.firstErr != nil {
		return ERROR
	}
	s := l.scn.TokenText()
	switch tok {
	case scn.EOF:
		return 0
	case scn.Ident:
		switch l.scn.TokenText() {
		case "enum":
			return ENUM
		case "format":
			return FORMAT
		case "prim":
			return PRIM
		case "reserved":
			return RESERVED
		case "struct":
			return STRUCT
		case "sum":
			return SUM
		default:
			lval.str = strOccur{Str: s, Pos: l.pos()}
			return IDENT
		}
	case scn.Int:
		tag, err := strconv.Atoi(s)
		if err != nil {
			l.Error(err.Error())
			return ERROR
		}
		if tag < 1 || (19000 <= tag && tag <= 19999) || tag > (1<<29)-1 {
			l.Error(fmt.Sprintf("invalid tag number: %d", tag))
			return ERROR
		}
		lval.tag = TagOccur{Tag: Tag(tag), Pos: l.pos()}
		return TAG
	case scn.String, scn.RawString:
		lval.str = strOccur{Str: s, Pos: l.pos()}
		return STR
	case '*', '.', ';', '=', '[', ']', '{', '}':
		return int(tok)
	default:
		l.Error(fmt.Sprintf("invalid token: %q", s))
		return ERROR
	}
}
