// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package scanner

import (
	"fmt"
	"go/constant"
	"go/token"

	sqllexbase "github.com/cockroachdb/cockroach/pkg/sql/lexbase"
	"github.com/cockroachdb/cockroach/pkg/util/jsonpath/parser/lexbase"
)

// JSONPathScanner is a scanner with a jsonpath-specific scan function.
type JSONPathScanner struct {
	Scanner
}

// Scan scans the next token and populates its information into lval.
// This scan function contains rules for jsonpath.
func (s *JSONPathScanner) Scan(lval ScanSymType) {
	ch, skipWhiteSpace := s.scanSetup(lval)
	if skipWhiteSpace {
		return
	}

	// TODO(normanchenn): We still need to handle $.Xe where X is any digit.
	switch ch {
	case '$':
		// Root path ($.)
		if s.peek() == '.' || s.peek() == eof || s.peek() == ' ' {
			return
		}

		// Handle variables like $var, $1a, $"var", etc.
		if s.peek() == identQuote {
			s.pos++
			if s.scanString(lval, identQuote, false /* allowEscapes */, true /* requireUTF8 */) {
				lval.SetID(lexbase.VARIABLE)
			}
			return
		}
		s.pos++
		s.scanIdent(lval)
		lval.SetID(lexbase.VARIABLE)
		return
	case identQuote:
		// "[^"]"
		if s.scanString(lval, identQuote, false /* allowEscapes */, true /* requireUTF8 */) {
			lval.SetID(lexbase.IDENT)
		}
		return
	default:
		if sqllexbase.IsDigit(ch) {
			s.scanNumber(lval, ch)
			return
		}
		if sqllexbase.IsIdentStart(ch) {
			s.scanIdent(lval)
			return
		}
	}
	// Everything else is a single character token which we already initialized
	// lval for above.
}

// isIdentMiddle returns true if the character is valid inside an identifier.
func isIdentMiddle(ch int) bool {
	return sqllexbase.IsIdentStart(ch) || sqllexbase.IsDigit(ch)
}

// scanIdent is similar to Scanner.scanIdent, but uses Jsonpath tokens.
func (s *JSONPathScanner) scanIdent(lval ScanSymType) {
	s.normalizeIdent(lval, isIdentMiddle, false /* toLower */)
	lval.SetID(lexbase.GetKeywordID(lval.Str()))
}

// scanNumber is similar to Scanner.scanNumber, but uses Jsonpath tokens.
func (s *JSONPathScanner) scanNumber(lval ScanSymType, ch int) {
	start := s.pos - 1
	isHex := false
	hasDecimal := ch == '.'
	hasExponent := false

	for {
		ch := s.peek()
		if (isHex && sqllexbase.IsHexDigit(ch)) || sqllexbase.IsDigit(ch) {
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
			if !sqllexbase.IsDigit(ch) {
				lval.SetID(lexbase.ERROR)
				lval.SetStr("invalid floating point literal")
				return
			}
			continue
		}
		break
	}

	// Disallow identifier after numerical constants e.g. "124foo".
	if sqllexbase.IsIdentStart(s.peek()) {
		lval.SetID(lexbase.ERROR)
		lval.SetStr(fmt.Sprintf("trailing junk after numeric literal at or near %q", s.in[start:s.pos+1]))
		return
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
