// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package lex

import (
	"unicode"
	"unicode/utf8"
)

// isASCII returns true if all the characters in s are ASCII.
func isASCII(s string) bool {
	for _, c := range s {
		if c > unicode.MaxASCII {
			return false
		}
	}
	return true
}

// IsDigit returns true if the character is between 0 and 9.
func IsDigit(ch int) bool {
	return ch >= '0' && ch <= '9'
}

// IsHexDigit returns true if the character is a valid hexadecimal digit.
func IsHexDigit(ch int) bool {
	return (ch >= '0' && ch <= '9') ||
		(ch >= 'a' && ch <= 'f') ||
		(ch >= 'A' && ch <= 'F')
}

// reservedOrLookaheadKeywords are the reserved keywords plus those keywords for
// which we need one token of lookahead extra to determine their token type.
var reservedOrLookaheadKeywords = make(map[string]struct{})

func init() {
	for s := range reservedKeywords {
		reservedOrLookaheadKeywords[s] = struct{}{}
	}
	for _, s := range []string{
		"between",
		"ilike",
		"in",
		"like",
		"of",
		"ordinality",
		"similar",
		"time",
	} {
		reservedOrLookaheadKeywords[s] = struct{}{}
	}
}

// isReservedKeyword returns true if the keyword is reserved, or needs
// one extra token of lookahead.
func isReservedKeyword(s string) bool {
	_, ok := reservedOrLookaheadKeywords[s]
	return ok
}

// isBareIdentifier returns true if the input string is a permissible bare SQL
// identifier.
func isBareIdentifier(s string) bool {
	if len(s) == 0 || !IsIdentStart(int(s[0])) || (s[0] >= 'A' && s[0] <= 'Z') {
		return false
	}
	// Keep track of whether the input string is all ASCII. If it is, we don't
	// have to bother running the full Normalize() function at the end, which is
	// quite expensive.
	isASCII := s[0] < utf8.RuneSelf
	for i := 1; i < len(s); i++ {
		if !IsIdentMiddle(int(s[i])) {
			return false
		}
		if s[i] >= 'A' && s[i] <= 'Z' {
			// Non-lowercase identifiers aren't permissible.
			return false
		}
		if s[i] >= utf8.RuneSelf {
			isASCII = false
		}
	}
	return isASCII || NormalizeName(s) == s
}

// IsIdentStart returns true if the character is valid at the start of an identifier.
func IsIdentStart(ch int) bool {
	return (ch >= 'A' && ch <= 'Z') ||
		(ch >= 'a' && ch <= 'z') ||
		(ch >= 128 && ch <= 255) ||
		(ch == '_')
}

// IsIdentMiddle returns true if the character is valid inside an identifier.
func IsIdentMiddle(ch int) bool {
	return IsIdentStart(ch) || IsDigit(ch) || ch == '$'
}
