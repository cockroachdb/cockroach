// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package scanner

import (
	sqllex "github.com/cockroachdb/cockroach/pkg/sql/lexbase"
	"github.com/cockroachdb/cockroach/pkg/sql/plpgsql/parser/lexbase"
)

// PLpgSQLScanner is a scanner with a PLPGSQL specific scan function
type PLpgSQLScanner struct {
	Scanner
}

// Scan scans the next token and populates its information into lval.
// This scan function contains rules for plpgsql.
func (s *PLpgSQLScanner) Scan(lval ScanSymType) {
	ch, skipWhiteSpace := s.scanSetup(lval)

	if skipWhiteSpace {
		return
	}

	switch ch {
	case '$':
		if s.scanDollarQuotedString(lval) {
			lval.SetID(lexbase.SCONST)
			return
		}
		return

	case identQuote:
		// "[^"]"
		if s.scanString(lval, identQuote, false /* allowEscapes */, true /* requireUTF8 */) {
			lval.SetID(lexbase.IDENT)
		}
		return

	case singleQuote:
		// '[^']'
		if s.scanString(lval, ch, false /* allowEscapes */, true /* requireUTF8 */) {
			lval.SetID(lexbase.SCONST)
		}
		return

	case 'b':
		// Bytes?
		if s.peek() == singleQuote {
			// b'[^']'
			s.pos++
			if s.scanString(lval, singleQuote, true /* allowEscapes */, false /* requireUTF8 */) {
				lval.SetID(lexbase.BCONST)
			}
			return
		}
		s.scanIdent(lval)
		return

	case '.':
		switch t := s.peek(); {
		case t == '.': // ..
			s.pos++
			lval.SetID(lexbase.DOT_DOT)
			return
		case sqllex.IsDigit(t):
			s.scanNumber(lval, ch)
			return
		}
		return

	case '!':
		switch s.peek() {
		case '=': // !=
			s.pos++
			lval.SetID(lexbase.NOT_EQUALS)
			return
		}
		return

	case '<':
		switch s.peek() {
		case '<': // <<
			s.pos++
			lval.SetID(lexbase.LESS_LESS)
			return
		case '=': // <=
			s.pos++
			lval.SetID(lexbase.LESS_EQUALS)
			return
		}
		return

	case '>':
		switch s.peek() {
		case '>': // >>
			s.pos++
			lval.SetID(lexbase.GREATER_GREATER)
			return
		case '=': // >=
			s.pos++
			lval.SetID(lexbase.GREATER_EQUALS)
			return
		}
		return

	case ':':
		switch s.peek() {
		case ':':
			s.pos++
			lval.SetID(lexbase.TYPECAST)
			return
		case '=':
			s.pos++
			lval.SetID(lexbase.COLON_EQUALS)
			return
		}
		return

	default:
		if sqllex.IsDigit(ch) {
			s.scanNumber(lval, ch)
			return
		}
		if sqllex.IsIdentStart(ch) {
			s.scanIdent(lval)
			return
		}
	}
	// Everything else is a single character token which we already initialized
	// lval for above.
}

func (s *PLpgSQLScanner) scanIdent(lval ScanSymType) {
	s.lowerCaseAndNormalizeIdent(lval)
	lval.SetID(lexbase.GetKeywordID(lval.Str()))
}
