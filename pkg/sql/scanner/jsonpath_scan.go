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

	if isIdentStart(ch) {
		s.scanIdent(lval)
		return
	}
	// Everything else is a single character token which we already initialized
	// lval for above.
}

// isIdentStart returns true if the character is valid at the start of an identifier.
func isIdentStart(ch int) bool {
	return (ch >= 'A' && ch <= 'Z') ||
		(ch >= 'a' && ch <= 'z') ||
		sqllexbase.IsDigit(ch)
}

// scanIdent is similar to Scanner.scanIdent, but uses Jsonpath tokens.
func (s *JSONPathScanner) scanIdent(lval ScanSymType) {
	s.lowerCaseAndNormalizeIdent(lval, isIdentStart)
	lval.SetID(lexbase.GetKeywordID(lval.Str()))
}
