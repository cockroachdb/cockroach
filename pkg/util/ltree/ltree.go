// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package ltree

import (
	"bytes"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/errors"
)

const (
	PathSeparator = "."
	// Postgres imposes a 65535 limit on the number of labels in a ltree.
	maxNumOfLabels = 65535
	// Postgres docs mention labels must be less than 256 bytes, but in practice,
	// Postgres has a limit on up to 1000 characters.
	maxLabelLength = 1000
)

var (
	// Empty represents the LTree path "".
	Empty         = T{}
	errEmptyLabel = pgerror.New(pgcode.Syntax, "label cannot be empty")
)

// T represents a LTREE path.
type T struct {
	// path is an ordered slice of string labels that make up a path in a LTREE column.
	path []string
}

// ParseLTree parses a string representation of a path into a T struct.
func ParseLTree(pathStr string) (T, error) {
	labels := strings.Split(pathStr, PathSeparator)
	if len(labels) > maxNumOfLabels {
		return T{}, pgerror.Newf(pgcode.ProgramLimitExceeded, "number of ltree labels (%d) exceeds the maximum allowed (%d)", len(labels), maxNumOfLabels)
	}
	for _, label := range labels {
		err := validateLabel(label)
		if err != nil {
			if errors.Is(err, errEmptyLabel) && len(labels) == 1 {
				// If the only label is empty, we treat it as a valid empty path.
				return Empty, nil
			}
			return Empty, err
		}
	}
	return T{path: labels}, nil
}

// String returns the string representation of T.
func (lt T) String() string {
	var b bytes.Buffer
	lt.FormatToBuffer(&b)
	return b.String()
}

// FormatToBuffer formats the LTREE path into a bytes.Buffer,
// using the PathSeparator.
func (lt T) FormatToBuffer(buf *bytes.Buffer) {
	for i, l := range lt.path {
		if i > 0 {
			buf.WriteString(PathSeparator)
		}
		buf.WriteString(l)
	}
}

// ByteSize returns the size of the T in bytes, which is the sum of the label
// lengths and their path separators.
func (lt T) ByteSize() int {
	size := 0
	for i, l := range lt.path {
		if i > 0 {
			size += len(PathSeparator)
		}
		size += len(l)
	}
	return size
}

// Compare compares two LTrees lexicographically based on their labels.
func (lt T) Compare(other T) int {
	minLen := min(lt.Len(), other.Len())

	for i := 0; i < minLen; i++ {
		if cmp := strings.Compare(lt.path[i], other.path[i]); cmp != 0 {
			return cmp
		}
	}

	if lt.Len() < other.Len() {
		return -1
	} else if lt.Len() > other.Len() {
		return 1
	}
	return 0
}

// Len returns the number of labels in the T.
func (lt T) Len() int {
	return len(lt.path)
}

// Copy creates a copy of T.
func (lt T) Copy() T {
	copiedLabels := make([]string, lt.Len())
	copy(copiedLabels, lt.path)
	return T{path: copiedLabels}
}

// Prev returns the lexicographically previous LTree and a bool
// indicating whether it exists.
func (lt T) Prev() (T, bool) {
	if lt.Len() == 0 {
		return Empty, false
	}

	lastLabel := lt.path[lt.Len()-1]
	if l := prevLabel(lastLabel); l != "" {
		result := lt.Copy()
		result.path[lt.Len()-1] = l
		return result, true
	}

	if lt.Len() > 1 {
		return T{path: lt.path[:lt.Len()-1]}, true
	}

	return Empty, true
}

// validateLabel checks if a label is valid and returns an error if it is not,
// otherwise, it returns nil.
// A label is valid if it:
// - is not empty
// - does not exceed the maximum length
// - contains only valid characters: '-', '0'-'9', 'A'-'Z', '_', 'a'-'z'
func validateLabel(l string) error {
	if len(l) > maxLabelLength {
		return pgerror.Newf(pgcode.NameTooLong, "label length is %d, must be at most %d", len(l), maxLabelLength)
	}
	if l == "" {
		return errEmptyLabel
	}
	for _, c := range l {
		if !isValidChar(byte(c)) {
			return pgerror.Newf(pgcode.Syntax, "label contains invalid character %c", c)
		}
	}
	return nil
}

// prevLabel returns the lexicographically previous label or empty string if
// none exists.
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

// isValidChar returns true if the character is valid in an LTree label.
func isValidChar(c byte) bool {
	return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c == '_') || (c == '-')
}

var prevCharMap = map[byte]byte{
	'-': 0,
	'0': '-',
	'A': '9',
	'_': 'Z',
	'a': '_',
}

// prevChar returns the previous valid character assuming a given valid
// character, or 0 if none exists.
func prevChar(c byte) byte {
	if prev, ok := prevCharMap[c]; ok {
		return prev
	}
	return c - 1
}

// Contains returns true if an LTree path contains another LTree path.
// This implements the @> and <@ operators. This is equivalent to
// checking if an LTree path is an ancestor or descendant of another
// LTree path. This is also equivalent to matching a prefix of labels.
// Examples:
// 'a.b.c' @> 'a.b' returns false
// 'a.b' @> 'a.b.c' returns true
func (lt T) Contains(other T) bool {
	for i, label := range lt.path {
		if i >= other.Len() || label != other.path[i] {
			return false
		}
	}
	return true
}

// Concat returns a new LTree that is the concatenation of two valid LTree paths.
func Concat(l, other T) (T, error) {
	newLen := l.Len() + other.Len()
	if newLen > maxNumOfLabels {
		return Empty, pgerror.Newf(pgcode.ProgramLimitExceeded, "number of ltree levels (%d) exceeds the maximum allowed (%d)", newLen, maxNumOfLabels)
	}
	newPath := make([]string, 0, newLen)
	return T{path: append(append(newPath, l.path...), other.path...)}, nil
}
