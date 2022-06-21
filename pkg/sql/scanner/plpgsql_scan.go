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
	"github.com/cockroachdb/cockroach/pkg/sql/lexbase"
	"github.com/cockroachdb/cockroach/pkg/sql/plpgsql/parser/plpgsqllexbase"
	"unicode/utf8"
	"unsafe"
)

// PLPGSQLScanner is a scanner with a PLPGSQL specific scan function
type PLPGSQLScanner struct {
	Scanner
}

// Scan scans the next token and populates its information into lval.
// This scan function contains rules for plpgsql.
func (s *PLPGSQLScanner) Scan(lval ScanSymType) {
	lval.SetID(0)
	lval.SetPos(int32(s.pos))
	lval.SetStr("EOF")

	if _, ok := s.skipWhitespace(lval, true); !ok {
		return
	}

	ch := s.next()
	if ch == eof {
		lval.SetPos(int32(s.pos))
		return
	}

	lval.SetID(int32(ch))
	lval.SetPos(int32(s.pos - 1))
	lval.SetStr(s.in[lval.Pos():s.pos])

	switch ch {
	case '$':
		if s.scanDollarQuotedString(lval) {
			lval.SetID(plpgsqllexbase.SCONST)
			return
		}
		return

	case identQuote:
		// "[^"]"
		if s.scanString(lval, identQuote, false /* allowEscapes */, true /* requireUTF8 */) {
			lval.SetID(plpgsqllexbase.IDENT)
		}
		return

	case singleQuote:
		// '[^']'
		if s.scanString(lval, ch, false /* allowEscapes */, true /* requireUTF8 */) {
			lval.SetID(plpgsqllexbase.SCONST)
		}
		return

	case 'b':
		// Bytes?
		if s.peek() == singleQuote {
			// b'[^']'
			s.pos++
			if s.scanString(lval, singleQuote, true /* allowEscapes */, false /* requireUTF8 */) {
				lval.SetID(plpgsqllexbase.BCONST)
			}
			return
		}
		s.scanIdent(lval)
		return

	case '.':
		switch t := s.peek(); {
		case t == '.': // ..
			s.pos++
			lval.SetID(plpgsqllexbase.DOT_DOT)
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
			lval.SetID(plpgsqllexbase.NOT_EQUALS)
			return
		}
		return

	case '<':
		switch s.peek() {
		case '<': // <<
			s.pos++
			lval.SetID(plpgsqllexbase.LESS_LESS)
			return
		case '=': // <=
			s.pos++
			lval.SetID(plpgsqllexbase.LESS_EQUALS)
			return
		}
		return

	case '>':
		switch s.peek() {
		case '>': // >>
			s.pos++
			lval.SetID(plpgsqllexbase.GREATER_GREATER)
			return
		case '=': // >=
			s.pos++
			lval.SetID(plpgsqllexbase.GREATER_EQUALS)
			return
		}
		return

	case ':':
		switch s.peek() {
		case ':':
			s.pos++
			lval.SetID(plpgsqllexbase.TYPECAST)
			return
		case '=':
			s.pos++
			lval.SetID(plpgsqllexbase.COLON_EQUALS)
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

	lval.SetID(plpgsqllexbase.GetKeywordID(lval.Str()))
}
