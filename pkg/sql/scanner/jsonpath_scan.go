// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package scanner

import (
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
		// Root path ($)
		if s.peek() == '.' || s.peek() == eof || s.peek() == ' ' || s.peek() == '[' || s.peek() == ')' {
			lval.SetID(lexbase.ROOT)
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
	case '=':
		if s.peek() == '=' { // ==
			s.pos++
			lval.SetID(lexbase.EQUAL)
			return
		}
		return
	case '!':
		if s.peek() == '=' { // !=
			s.pos++
			lval.SetID(lexbase.NOT_EQUAL)
			return
		}
		lval.SetID(lexbase.NOT)
		return
	case '>':
		if s.peek() == '=' { // >=
			s.pos++
			lval.SetID(lexbase.GREATER_EQUAL)
			return
		}
		lval.SetID(lexbase.GREATER)
		return
	case '<':
		if s.peek() == '=' { // <=
			s.pos++
			lval.SetID(lexbase.LESS_EQUAL)
			return
		}
		lval.SetID(lexbase.LESS)
		return
	case '&':
		if s.peek() == '&' { // &&
			s.pos++
			lval.SetID(lexbase.AND)
			return
		}
		return
	case '|':
		if s.peek() == '|' { // ||
			s.pos++
			lval.SetID(lexbase.OR)
			return
		}
		return
	case '@':
		lval.SetID(lexbase.CURRENT)
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
	// TODO(normanchenn): Allow any case for specific identifiers (strict, lax, to)
	s.normalizeIdent(lval, isIdentMiddle, false /* toLower */)
	lval.SetID(lexbase.GetKeywordID(lval.Str()))
}

// scanNumber is similar to Scanner.scanNumber, but uses Jsonpath tokens.
func (s *JSONPathScanner) scanNumber(lval ScanSymType, ch int) {
	s.scanNumberImpl(lval, ch, lexbase.ERROR, lexbase.FCONST, lexbase.ICONST)
}
