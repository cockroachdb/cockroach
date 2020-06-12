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
	"unicode/utf8"

	"github.com/pkg/errors"
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
