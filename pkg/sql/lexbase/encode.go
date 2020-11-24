// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in licenses/BSD-vitess.txt.

// Portions of this file are additionally subject to the following
// license and copyright.
//
// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// This code was derived from https://github.com/youtube/vitess.

package lexbase

import "bytes"

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

	// EncFirstFreeFlagBit needs to remain unused; it is used as base
	// bit offset for tree.FmtFlags.
	EncFirstFreeFlagBit
)

// EncodeRestrictedSQLIdent writes the identifier in s to buf. The
// identifier is quoted if either the flags ask for it, the identifier
// contains special characters, or the identifier is a reserved SQL
// keyword.
func EncodeRestrictedSQLIdent(buf *bytes.Buffer, s string, flags EncodeFlags) {
	if flags.HasFlags(EncBareIdentifiers) || (!isReservedKeyword(s) && IsBareIdentifier(s)) {
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
