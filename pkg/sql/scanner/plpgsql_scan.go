// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package scanner

import (
	"fmt"
	"go/constant"
	"go/token"
	"strconv"
	"unicode/utf8"

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

	case 'e', 'E':
		// Escaped string?
		if s.peek() == singleQuote {
			// [eE]'[^']'
			s.lastAttemptedID = int32(lexbase.SCONST)
			s.pos++
			if s.scanString(lval, singleQuote, true /* allowEscapes */, true /* requireUTF8 */) {
				lval.SetID(lexbase.SCONST)
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

// scanString is similar to Scanner.scanString, but uses PL/pgSQL tokens.
func (s *PLpgSQLScanner) scanString(lval ScanSymType, ch int, allowEscapes, requireUTF8 bool) bool {
	buf := s.buffer()
	var runeTmp [utf8.UTFMax]byte
	start := s.pos
outer:
	for {
		switch s.next() {
		case ch:
			buf = append(buf, s.in[start:s.pos-1]...)
			if s.peek() == ch {
				// Double quote is translated into a single quote that is part of the
				// string.
				start = s.pos
				s.pos++
				continue
			}

			newline, ok := s.skipWhitespace(lval, false)
			if !ok {
				return false
			}

			// SQL allows joining adjacent single-quoted strings separated by
			// whitespace as long as that whitespace contains at least one
			// newline. Kind of strange to require the newline, but that is the
			// standard.
			if ch == singleQuote && s.peek() == singleQuote && newline {
				s.pos++
				start = s.pos
				continue
			}
			break outer

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
						lval.SetID(lexbase.ERROR)
						lval.SetStr(err.Error())
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
			lval.SetID(lexbase.ERROR)
			lval.SetStr(errUnterminated)
			return false
		}
	}

	if requireUTF8 && !utf8.Valid(buf) {
		lval.SetID(lexbase.ERROR)
		lval.SetStr(errInvalidUTF8)
		return false
	}

	if ch == identQuote {
		lval.SetStr(sqllex.NormalizeString(s.finishString(buf)))
	} else {
		lval.SetStr(s.finishString(buf))
	}
	return true
}

// scanDollarQuotedString is similar to Scanner.scanDollarQuotedString, but uses
// PL/pgSQL tokens.
func (s *PLpgSQLScanner) scanDollarQuotedString(lval ScanSymType) bool {
	s.lastAttemptedID = int32(lexbase.SCONST)
	buf := s.buffer()
	start := s.pos

	foundStartTag := false
	possibleEndTag := false
	startTagIndex := -1
	var startTag string

outer:
	for {
		ch := s.peek()
		switch ch {
		case '$':
			s.pos++
			if foundStartTag {
				if possibleEndTag {
					if len(startTag) == startTagIndex {
						// Found end tag.
						buf = append(buf, s.in[start+len(startTag)+1:s.pos-len(startTag)-2]...)
						break outer
					} else {
						// Was not the end tag but the current $ might be the start of the end tag we are looking for, so
						// just reset the startTagIndex.
						startTagIndex = 0
					}
				} else {
					possibleEndTag = true
					startTagIndex = 0
				}
			} else {
				startTag = s.in[start : s.pos-1]
				foundStartTag = true
			}

		case eof:
			if foundStartTag {
				// A start tag was found, therefore we expect an end tag before the eof, otherwise it is an error.
				lval.SetID(lexbase.ERROR)
				lval.SetStr(errUnterminated)
			} else {
				// This is not a dollar-quoted string, reset the pos back to the start.
				s.pos = start
			}
			return false

		default:
			// If we haven't found a start tag yet, check whether the current characters is a valid for a tag.
			if !foundStartTag && !sqllex.IsIdentStart(ch) && !sqllex.IsDigit(ch) {
				return false
			}
			s.pos++
			if possibleEndTag {
				// Check whether this could be the end tag.
				if startTagIndex >= len(startTag) || ch != int(startTag[startTagIndex]) {
					// This is not the end tag we are looking for.
					possibleEndTag = false
					startTagIndex = -1
				} else {
					startTagIndex++
				}
			}
		}
	}

	if !utf8.Valid(buf) {
		lval.SetID(lexbase.ERROR)
		lval.SetStr(errInvalidUTF8)
		return false
	}

	lval.SetStr(s.finishString(buf))
	return true
}

// scanNumber is similar to Scanner.scanNumber, but uses PL/pgSQL tokens.
func (s *PLpgSQLScanner) scanNumber(lval ScanSymType, ch int) {
	start := s.pos - 1
	isHex := false
	hasDecimal := ch == '.'
	hasExponent := false

	for {
		ch := s.peek()
		if (isHex && sqllex.IsHexDigit(ch)) || sqllex.IsDigit(ch) {
			s.pos++
			continue
		}
		if ch == 'x' || ch == 'X' {
			if isHex || s.in[start] != '0' || s.pos != start+1 {
				lval.SetID(lexbase.ERROR)
				lval.SetStr(errInvalidHexNumeric)
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
			if !sqllex.IsDigit(ch) {
				lval.SetID(lexbase.ERROR)
				lval.SetStr("invalid floating point literal")
				return
			}
			continue
		}
		break
	}

	lval.SetStr(s.in[start:s.pos])
	if hasDecimal || hasExponent {
		lval.SetID(lexbase.FCONST)
		floatConst := constant.MakeFromLiteral(lval.Str(), token.FLOAT, 0)
		if floatConst.Kind() == constant.Unknown {
			lval.SetID(lexbase.ERROR)
			lval.SetStr(fmt.Sprintf("could not make constant float from literal %q", lval.Str()))
			return
		}
		lval.SetUnionVal(NewNumValFn(floatConst, lval.Str(), false /* negative */))
	} else {
		if isHex && s.pos == start+2 {
			lval.SetID(lexbase.ERROR)
			lval.SetStr(errInvalidHexNumeric)
			return
		}

		// Strip off leading zeros from non-hex (decimal) literals so that
		// constant.MakeFromLiteral doesn't inappropriately interpret the
		// string as an octal literal. Note: we can't use strings.TrimLeft
		// here, because it will truncate '0' to ''.
		if !isHex {
			for len(lval.Str()) > 1 && lval.Str()[0] == '0' {
				lval.SetStr(lval.Str()[1:])
			}
		}

		lval.SetID(lexbase.ICONST)
		intConst := constant.MakeFromLiteral(lval.Str(), token.INT, 0)
		if intConst.Kind() == constant.Unknown {
			lval.SetID(lexbase.ERROR)
			lval.SetStr(fmt.Sprintf("could not make constant int from literal %q", lval.Str()))
			return
		}
		lval.SetUnionVal(NewNumValFn(intConst, lval.Str(), false /* negative */))
	}
}

// scanIdent is similar to Scanner.scanIdent, but uses PL/pgSQL tokens.
func (s *PLpgSQLScanner) scanIdent(lval ScanSymType) {
	s.normalizeIdent(lval, sqllex.IsIdentMiddle, true /* toLower */)
	lval.SetID(lexbase.GetKeywordID(lval.Str()))
}
