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

	// TODO(normanchenn): This check will not work for valid JSONPath expressions
	// like '$.1key'. We don't support this case yet since expressions like
	// '$.1e' should fail due to being interpreted as a numeric literal.
	if sqllexbase.IsIdentStart(ch) {
		s.scanIdent(lval)
		return
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
