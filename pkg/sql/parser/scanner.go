// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package parser

import (
	"github.com/cockroachdb/cockroach/pkg/sql/lexbase"
	"github.com/cockroachdb/cockroach/pkg/sql/scanner"
)

func makeSQLScanner(str string) scanner.SQLScanner {
	var s scanner.SQLScanner
	s.Init(str)
	return s
}

// SplitFirstStatement returns the length of the prefix of the string up to and
// including the first semicolon that separates statements. If there is no
// including the first semicolon that separates statements. If there is no
// semicolon, returns ok=false.
func SplitFirstStatement(sql string) (pos int, ok bool) {
	s := makeSQLScanner(sql)
	var lval = &sqlSymType{}
	for {
		s.Scan(lval)
		switch lval.ID() {
		case 0, lexbase.ERROR:
			return 0, false
		case ';':
			return s.Pos(), true
		}
	}
}

// Tokens decomposes the input into lexical tokens.
func Tokens(sql string) (tokens []TokenString, ok bool) {
	s := makeSQLScanner(sql)
	for {
		var lval = &sqlSymType{}
		s.Scan(lval)
		if lval.ID() == lexbase.ERROR {
			return nil, false
		}
		if lval.ID() == 0 {
			break
		}
		tokens = append(tokens, TokenString{TokenID: lval.ID(), Str: lval.Str()})
	}
	return tokens, true
}

// TokenString is the unit value returned by Tokens.
type TokenString struct {
	TokenID int32
	Str     string
}
