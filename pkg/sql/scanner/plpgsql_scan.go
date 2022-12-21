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
	"unicode/utf8"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/sql/plpgsql/parser/lexbase"
)

// PLPGSQLScanner is a scanner with a PLPGSQL specific scan function
type PLPGSQLScanner struct {
	Scanner
}

// Scan scans the next token and populates its information into lval.
// This scan function contains rules for plpgsql.
func (s *PLPGSQLScanner) Scan(lval ScanSymType) {
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
		case lexbase.IsDigit(t):
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
		if lexbase.IsDigit(ch) {
			s.scanNumber(lval, ch)
			return
		}
		if lexbase.IsIdentStart(ch) {
			s.scanIdent(lval)
			return
		}
	}
	// Everything else is a single character token which we already initialized
	// lval for above.
}

func (s *PLPGSQLScanner) scanIdent(lval ScanSymType) {
	s.pos--
	start := s.pos
	isASCII := true
	isLower := true

	// Consume the Scanner character by character, stopping after the last legal
	// identifier character. By the end of this function, we need to
	// lowercase and unicode normalize this identifier, which is expensive if
	// there are actual unicode characters in it. If not, it's quite cheap - and
	// if it's lowercase already, there's no work to do. Therefore, we keep track
	// of whether the string is only ASCII or only ASCII lowercase for later.
	for {
		ch := s.peek()
		//fmt.Println(ch, ch >= utf8.RuneSelf, ch >= 'A' && ch <= 'Z')

		if ch >= utf8.RuneSelf {
			isASCII = false
		} else if ch >= 'A' && ch <= 'Z' {
			isLower = false
		}

		if !lexbase.IsIdentMiddle(ch) {
			break
		}

		s.pos++
	}
	//fmt.Println("parsed: ", s.in[start:s.pos], isASCII, isLower)

	if isLower && isASCII {
		// Already lowercased - nothing to do.
		lval.SetStr(s.in[start:s.pos])
	} else if isASCII {
		// We know that the identifier we've seen so far is ASCII, so we don't need
		// to unicode normalize. Instead, just lowercase as normal.
		b := s.allocBytes(s.pos - start)
		_ = b[s.pos-start-1] // For bounds check elimination.
		for i, c := range s.in[start:s.pos] {
			if c >= 'A' && c <= 'Z' {
				c += 'a' - 'A'
			}
			b[i] = byte(c)
		}
		lval.SetStr(*(*string)(unsafe.Pointer(&b)))
	} else {
		// The string has unicode in it. No choice but to run Normalize.
		lval.SetStr(lexbase.NormalizeName(s.in[start:s.pos]))
	}

	lval.SetID(lexbase.GetKeywordID(lval.Str()))
}
