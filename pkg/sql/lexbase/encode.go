// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in licenses/BSD-vitess.txt.

// Portions of this file are additionally subject to the following
// license and copyright.
//
// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// This code was derived from https://github.com/youtube/vitess.

// Package lexbase contains utilities for lexing sql.
package lexbase

import (
	"bytes"
	"unicode/utf8"

	"github.com/cockroachdb/cockroach/pkg/util/stringencoding"
)

// EncodeFlags influence the formatting of strings and identifiers.
type EncodeFlags int

// HasFlags tests whether the given flags are set.
func (f EncodeFlags) HasFlags(subset EncodeFlags) bool {
	return f&subset == subset
}

const (
	// EncNoFlags indicates nothing special should happen while encoding.
	EncNoFlags EncodeFlags = 0

	// EncBareStrings indicates that strings will be rendered without
	// wrapping quotes if they contain no special characters.
	EncBareStrings EncodeFlags = 1 << iota

	// EncBareIdentifiers indicates that identifiers will be rendered
	// without wrapping quotes.
	EncBareIdentifiers

	// EncBareReservedKeywords indicates that reserved keywords will be rendered
	// without wrapping quotes.
	EncBareReservedKeywords

	// EncFirstFreeFlagBit needs to remain unused; it is used as base
	// bit offset for tree.FmtFlags.
	EncFirstFreeFlagBit
)

// EncodeRestrictedSQLIdent writes the identifier in s to buf. The
// identifier is quoted if either the flags ask for it, the identifier
// contains special characters, or the identifier is a reserved SQL
// keyword.
func EncodeRestrictedSQLIdent(buf *bytes.Buffer, s string, flags EncodeFlags) {
	if flags.HasFlags(EncBareIdentifiers) ||
		(IsBareIdentifier(s) && (flags.HasFlags(EncBareReservedKeywords) || !isReservedKeyword(s))) {
		buf.WriteString(s)
		return
	}
	EncodeEscapedSQLIdent(buf, s)
}

// EncodeUnrestrictedSQLIdent writes the identifier in s to buf.
// The identifier is only quoted if the flags don't tell otherwise and
// the identifier contains special characters.
func EncodeUnrestrictedSQLIdent(buf *bytes.Buffer, s string, flags EncodeFlags) {
	if flags.HasFlags(EncBareIdentifiers) || IsBareIdentifier(s) {
		buf.WriteString(s)
		return
	}
	EncodeEscapedSQLIdent(buf, s)
}

// EscapeSQLIdent ensures that the potential identifier in s is fully
// quoted, so that any special character it contains is not at risk
// of "spilling" in the surrounding syntax.
func EscapeSQLIdent(s string) string {
	var buf bytes.Buffer
	EncodeEscapedSQLIdent(&buf, s)
	return buf.String()
}

// EncodeEscapedSQLIdent writes the identifier in s to buf. The
// identifier is always quoted. Double quotes inside the identifier
// are escaped.
func EncodeEscapedSQLIdent(buf *bytes.Buffer, s string) {
	buf.WriteByte('"')
	start := 0
	for i, n := 0, len(s); i < n; i++ {
		ch := s[i]
		// The only character that requires escaping is a double quote.
		if ch == '"' {
			if start != i {
				buf.WriteString(s[start:i])
			}
			start = i + 1
			buf.WriteByte(ch)
			buf.WriteByte(ch) // add extra copy of ch
		}
	}
	if start < len(s) {
		buf.WriteString(s[start:])
	}
	buf.WriteByte('"')
}

const (
	minPrintableChar = 0x20 // ' '
	maxPrintableChar = 0x7E // '~'
)

var mustQuoteMap = [maxPrintableChar + 1]bool{
	' ': true,
	',': true,
	'{': true,
	'}': true,
}

// EncodeSQLString writes a string literal to buf. All unicode and
// non-printable characters are escaped.
func EncodeSQLString(buf *bytes.Buffer, in string) {
	EncodeSQLStringWithFlags(buf, in, EncNoFlags)
}

// EscapeSQLString returns an escaped SQL representation of the given
// string. This is suitable for safely producing a SQL string valid
// for input to the parser.
func EscapeSQLString(in string) string {
	var buf bytes.Buffer
	EncodeSQLString(&buf, in)
	return buf.String()
}

// EncodeSQLStringWithFlags writes a string literal to buf. All
// unicode and non-printable characters are escaped. flags controls
// the output format: if encodeBareString is set, the output string
// will not be wrapped in quotes if the strings contains no special
// characters.
func EncodeSQLStringWithFlags(buf *bytes.Buffer, in string, flags EncodeFlags) {
	// See http://www.postgresql.org/docs/9.4/static/sql-syntax-lexical.html
	start := 0
	escapedString := false
	bareStrings := flags.HasFlags(EncBareStrings)
	// Loop through each unicode code point.
	for i, r := range in {
		if i < start {
			continue
		}
		ch := byte(r)
		if r >= minPrintableChar && r <= maxPrintableChar {
			if mustQuoteMap[ch] {
				// We have to quote this string - ignore bareStrings setting
				bareStrings = false
			}
			if !stringencoding.NeedEscape(ch) && ch != '\'' {
				continue
			}
		}

		if !escapedString {
			buf.WriteString("e'") // begin e'xxx' string
			escapedString = true
		}
		buf.WriteString(in[start:i])

		ln := utf8.RuneLen(r)
		if ln < 0 {
			start = i + 1
		} else {
			start = i + ln
		}
		stringencoding.EncodeEscapedChar(buf, in, r, ch, i, '\'')
	}

	quote := !escapedString && !bareStrings
	if quote {
		buf.WriteByte('\'') // begin 'xxx' string if nothing was escaped
	}
	if start < len(in) {
		buf.WriteString(in[start:])
	}
	if escapedString || quote {
		buf.WriteByte('\'')
	}
}

// EncodeSQLBytes encodes the SQL byte array in 'in' to buf, to a
// format suitable for re-scanning. We don't use a straightforward hex
// encoding here with x'...'  because the result would be less
// compact. We are trading a little more time during the encoding to
// have a little less bytes on the wire.
func EncodeSQLBytes(buf *bytes.Buffer, in string) {
	buf.WriteString("b'")
	EncodeSQLBytesInner(buf, in)
	buf.WriteByte('\'')
}

// EncodeSQLBytesInner is like EncodeSQLBytes but does not include the
// outer quote delimiter and the 'b' prefix.
func EncodeSQLBytesInner(buf *bytes.Buffer, in string) {
	start := 0
	// Loop over the bytes of the string (i.e., don't use range over unicode
	// code points).
	for i, n := 0, len(in); i < n; i++ {
		ch := in[i]
		if encodedChar := stringencoding.EncodeMap[ch]; encodedChar != stringencoding.DontEscape {
			buf.WriteString(in[start:i])
			buf.WriteByte('\\')
			buf.WriteByte(encodedChar)
			start = i + 1
		} else if ch == '\'' {
			// We can't just fold this into stringencoding.EncodeMap because
			// stringencoding.EncodeMap is also used for strings which
			// aren't quoted with single-quotes
			buf.WriteString(in[start:i])
			buf.WriteByte('\\')
			buf.WriteByte(ch)
			start = i + 1
		} else if ch < 0x20 || ch >= 0x7F {
			buf.WriteString(in[start:i])
			// Escape non-printable characters.
			buf.Write(stringencoding.HexMap[ch])
			start = i + 1
		}
	}
	buf.WriteString(in[start:])
}
