// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package util

import (
	"bytes"
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/cockroachdb/errors"
)

// GetSingleRune decodes the string s as a single rune if possible.
func GetSingleRune(s string) (rune, error) {
	if s == "" {
		return 0, nil
	}
	r, sz := utf8.DecodeRuneInString(s)
	if r == utf8.RuneError {
		return 0, errors.Errorf("invalid character: %s", s)
	}
	if sz != len(s) {
		return r, errors.New("must be only one character")
	}
	return r, nil
}

// ToLowerSingleByte returns the lowercase of a given single ASCII byte.
// A non ASCII byte is returned unchanged.
func ToLowerSingleByte(b byte) byte {
	if b >= 'A' && b <= 'Z' {
		return 'a' + (b - 'A')
	}
	return b
}

// TruncateString truncates a string to a given number of runes.
func TruncateString(s string, maxRunes int) string {
	// This is a fast path (len(s) is an upper bound for RuneCountInString).
	if len(s) <= maxRunes {
		return s
	}
	n := utf8.RuneCountInString(s)
	if n <= maxRunes {
		return s
	}
	// Fast path for ASCII strings.
	if len(s) == n {
		return s[:maxRunes]
	}
	i := 0
	for pos := range s {
		if i == maxRunes {
			return s[:pos]
		}
		i++
	}
	// This code should be unreachable.
	return s
}

// RemoveTrailingSpaces splits the input string into lines, trims any trailing
// spaces from each line, then puts the lines back together.
//
// Any newlines at the end of the input string are ignored.
//
// The output string always ends in a newline.
func RemoveTrailingSpaces(input string) string {
	lines := strings.TrimRight(input, "\n")
	var buf bytes.Buffer
	for _, line := range strings.Split(lines, "\n") {
		fmt.Fprintf(&buf, "%s\n", strings.TrimRight(line, " "))
	}
	return buf.String()
}
