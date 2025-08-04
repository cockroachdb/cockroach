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

const PathSeparator = "."

// Postgres imposes a 65535 limit on the number of labels in a ltree.
const maxNumOfLabels = 65535

// Empty represents the LTree path "".
var Empty = T{path: []label{}}

// T represents a LTREE path.
type T struct {
	// path is an ordered slice of label types that make up a path in a LTREE column.
	path []label
}

// ParseLTree parses a string representation of a path into a T struct.
func ParseLTree(pathStr string) (T, error) {
	labels := []label{}
	unparsedLabels := strings.Split(pathStr, PathSeparator)
	if len(unparsedLabels) > maxNumOfLabels {
		return T{}, pgerror.Newf(pgcode.ProgramLimitExceeded, "number of ltree labels (%d) exceeds the maximum allowed (%d)", len(unparsedLabels), maxNumOfLabels)
	}
	for _, labelStr := range unparsedLabels {
		l, err := newLabel(labelStr)
		if err != nil {
			if errors.Is(err, errEmptyLabel) && len(unparsedLabels) == 1 {
				// If the only label is empty, we treat it as a valid empty path.
				break
			}
			return T{}, err
		}
		labels = append(labels, l)
	}

	return T{path: labels}, nil
}

// String returns the string representation of T.
func (lt T) String() string {
	var b strings.Builder
	for i, l := range lt.path {
		if i > 0 {
			b.WriteString(PathSeparator)
		}
		b.WriteString(string(l))
	}
	return b.String()
}

// ForEachLabel iterates over each label in the LTREE path, calling the provided function with the index and label.
func (lt T) ForEachLabel(fn func(int, string)) {
	for i, l := range lt.path {
		fn(i, string(l))
	}
}

// LabelAt returns the label at the specified index in an LTree path.
func (lt T) LabelAt(idx int) (string, error) {
	if idx < 0 || idx >= lt.Len() {
		return "", pgerror.Newf(pgcode.InvalidParameterValue, "index %d out of bounds.", idx)
	}
	return string(lt.path[idx]), nil
}

// FormatToBuffer formats the LTREE path into a bytes.Buffer, using the PathSeparator.
func (lt T) FormatToBuffer(buf *bytes.Buffer) {
	for i, l := range lt.path {
		if i > 0 {
			buf.WriteString(PathSeparator)
		}
		buf.WriteString(string(l))
	}
}

// Compare compares two LTrees lexicographically based on their labels.
func (lt T) Compare(other T) int {
	minLen := min(lt.Len(), other.Len())
	for i := 0; i < minLen; i++ {
		if lt.path[i] < other.path[i] {
			return -1
		}
		if lt.path[i] > other.path[i] {
			return 1
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

// ByteSize returns the size of the T in bytes, which is the sum of the label lengths and their path separators.
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

// Copy creates a deep copy of T.
func (lt T) Copy() T {
	copiedLabels := make([]label, lt.Len())
	copy(copiedLabels, lt.path)
	return T{path: copiedLabels}
}

// Prev returns the lexicographically previous LTree and a bool representing
// whether a previous LTree exists.
func (lt T) Prev() (T, bool) {
	if lt.Len() == 0 {
		return Empty, false
	}

	lastLabel := string(lt.path[lt.Len()-1])
	if prevLabelStr := prevLabel(lastLabel); prevLabelStr != "" {
		newLbl, err := newLabel(prevLabelStr)
		if err == nil {
			result := lt.Copy()
			result.path[lt.Len()-1] = newLbl
			return result, true
		}
	}

	if lt.Len() > 1 {
		return T{path: lt.path[:lt.Len()-1]}, true
	}

	return Empty, true
}
