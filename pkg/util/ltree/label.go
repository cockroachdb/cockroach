// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package ltree

import (
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
)

// Postgres docs mention labels must be less than 256 bytes, but in practice,
// Postgres has a limit on up to 1000 characters.
const maxLabelLength = 1000

var errEmptyLabel = pgerror.New(pgcode.Syntax, "label cannot be empty")

type label string

func newLabel(l string) (label, error) {
	if len(l) > maxLabelLength {
		return "", pgerror.Newf(pgcode.NameTooLong, "label length is %d, must be at most %d", len(l), maxLabelLength)
	}
	if l == "" {
		return "", errEmptyLabel
	}
	for _, c := range l {
		if !isValidChar(byte(c)) {
			return "", pgerror.Newf(pgcode.Syntax, "label contains invalid character %c", c)
		}
	}
	return label(l), nil
}

// prevLabel returns the lexicographically previous label, or empty string if none exists.
func prevLabel(s string) string {
	if len(s) == 0 {
		return ""
	}

	lastChar := s[len(s)-1]
	if prev := prevChar(lastChar); prev != 0 {
		return s[:len(s)-1] + string(prev)
	}

	if len(s) > 1 {
		return s[:len(s)-1]
	}

	return ""
}

// validCharSet defines the lexicographic ordering of all valid characters in labels.
// Order: '-' < '0'-'9' < 'A'-'Z' < '_' < 'a'-'z'
const validCharSet = "-0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ_abcdefghijklmnopqrstuvwxyz"

// isValidChar returns true if the character is valid in an LTree label.
func isValidChar(c byte) bool {
	for i := 0; i < len(validCharSet); i++ {
		if validCharSet[i] == c {
			return true
		}
	}
	return false
}

// prevChar returns the previous valid character, or 0 if none exists.
func prevChar(c byte) byte {
	for i := 1; i < len(validCharSet); i++ {
		if validCharSet[i] == c {
			return validCharSet[i-1]
		}
	}
	return 0
}
