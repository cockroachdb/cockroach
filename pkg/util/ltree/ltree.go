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

// ParseLTreeFromLabels parses a slice of labels into a T struct.
func ParseLTreeFromLabels(labels []string) (T, error) {
	if len(labels) > maxNumOfLabels {
		return T{}, pgerror.Newf(pgcode.ProgramLimitExceeded, "number of ltree labels (%d) exceeds the maximum allowed (%d)", len(labels), maxNumOfLabels)
	}
	for _, label := range labels {
		if err := validateLabel(label); err != nil {
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

// ForEachLabel iterates over each label in the LTREE path,
// calling the provided function with the index and label.
func (lt T) ForEachLabel(fn func(int, string)) {
	for i, l := range lt.path {
		fn(i, l)
	}
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

// isValidChar returns true if the character is valid in an LTree label.
func isValidChar(c byte) bool {
	return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c == '_') || (c == '-')
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

// SubPath returns a sub-path of the LTree starting from the given offset
// and of the specified length. If the offset is negative, it counts from the end of the path.
// If the length is negative, it extends to the end of the path from the offset.
func (lt T) SubPath(offset, length int) (T, error) {
	start := offset
	if start < 0 {
		start += lt.Len()
	}
	if length < 0 {
		length += lt.Len() - start
	}
	end := min(start+length, lt.Len())
	if start < 0 || start >= lt.Len() || length < 0 || end < start {
		// The end < start check is necessary to prevent overflows.
		return Empty, pgerror.Newf(pgcode.InvalidParameterValue, "invalid positions")
	}

	return T{
		path: lt.path[start:end:end],
	}, nil
}

// IndexOf returns the first index in l that matches the other l sub-ltree,
// starting from offset. If offset is negative, it counts from the end of the ltree.
// If the sub-ltree is not found, it returns -1.
func (lt T) IndexOf(other T, offset int) int {
	start := offset
	if start < 0 {
		start += lt.Len()
	}
	// The max() is necessary here, as start could still be negative even after
	// adding the length to `offset` since offset isn't restricted.
	for i := max(start, 0); i < lt.Len()-other.Len()+1; i++ {
		match := true
		for j := 0; j < other.Len(); j++ {
			if lt.path[i+j] != other.path[j] {
				match = false
				break
			}
		}
		if match {
			return i
		}
	}
	return -1
}

// LCA computes the Lowest Common proper Ancestor from a slice of LTree
// instances and returns the LCA and a bool indicating whether the result is
// NULL (the LCA of an Empty ltree is NULL). The LCA is the longest common
// proper prefix of the paths in the provided LTree slice. If the paths have
// no common prefix, it returns an empty LTree.
func LCA(ltrees []T) (_ T, isNull bool) { // lint: uppercase function OK
	if len(ltrees) == 0 {
		return Empty, true
	}
	var minLength = ltrees[0].Len()
	for _, ltree := range ltrees {
		minLength = min(minLength, ltree.Len())
	}
	if minLength == 0 {
		// If an Empty LTREE is in ltrees, then the LCA should be NULL.
		return Empty, true
	}
	var i int
	for i = 0; i < minLength; i++ {
		label := ltrees[0].path[i]
		equal := true
		for _, t := range ltrees[1:] {
			if t.path[i] != label {
				equal = false
				break
			}
		}
		if !equal {
			break
		}
	}

	// Enforcing proper ancestor
	// i.e: 'A.B' is the proper ancestor to 'A.B.C'.
	if minLength > 0 && i == minLength {
		i -= 1
	}

	return T{path: ltrees[0].path[:i:i]}, false
}
