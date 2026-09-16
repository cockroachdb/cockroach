// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package scanner

import (
	"fmt"
	"strings"

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
	ch, skipWhiteSpace := s.scanSetup(lval, false /* allowComments */)
	if skipWhiteSpace {
		return
	}

	switch ch {
	case '$':
		// Root path ($)
		if s.peek() == '.' || s.peek() == eof || s.peek() == ' ' || s.peek() == '[' || s.peek() == ')' || s.peek() == '?' {
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
		// When scanning string literals for like_regex patterns, we need to
		// consider how to handle escape characters similarly to Postgres.
		// See: https://www.postgresql.org/docs/current/functions-json.html#JSONPATH-REGULAR-EXPRESSIONS,
		// "any backslashes you want to use in the regular expression must be doubled".
		//
		// With allowEscapes == true,
		//  - String literal input "^\\$" is scanned as "^\\$" (one escaped backslash)
		//  - This matches the behaviour of Postgres.
		// With allowEscapes == false,
		//  - String literal input "^\\$" is scanned as "^\\\\$" (two escaped backslashes)
		if s.scanString(lval, identQuote, true /* allowEscapes */, true /* requireUTF8 */) {
			lval.SetID(lexbase.STR)
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
	case '*':
		if s.peek() == '*' { // **
			s.pos++
			lval.SetID(lexbase.ANY)
			return
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
	// Postgres is case-insensitive for keywords, see
	// https://github.com/cockroachdb/cockroach/issues/144255.
	lval.SetID(lexbase.GetKeywordID(strings.ToLower(lval.Str())))
}

// scanNumber is similar to Scanner.scanNumber, but uses Jsonpath tokens.
func (s *JSONPathScanner) scanNumber(lval ScanSymType, ch int) {
	start := s.pos - 1
	s.scanNumberImpl(lval, ch, lexbase.ERROR, lexbase.FCONST, lexbase.ICONST)
	if lval.ID() != lexbase.ERROR {
		return
	}

	// scanNumberImpl explains a malformed literal in terms of the numeric
	// syntax it was trying to read, so `2x` is reported as a bad hexadecimal
	// literal and `2e` as a bad floating point literal. Neither reads well for
	// a jsonpath, where such a token is an accessor key that was written
	// without quotes. Postgres reports the numeric prefix together with the
	// first identifier character that follows it as trailing junk, which is
	// already how scanNumberImpl describes `2a`. Describe the remaining
	// digit-then-identifier cases the same way.
	junk := s.pos
	switch lval.Str() {
	case errInvalidHexNumeric:
		// s.pos is on the character that could not continue the literal.
	case errInvalidFloatLiteral:
		// s.pos is past the exponent marker, which is itself the junk unless a
		// sign follows it. A sign cannot appear in an identifier, so leaving
		// junk pointing at it keeps the more specific diagnostic below.
		junk--
	default:
		return
	}
	if junk < len(s.in) && isIdentMiddle(int(s.in[junk])) {
		lval.SetStr(fmt.Sprintf("trailing junk after numeric literal at or near %q", s.in[start:junk+1]))
	}
}
