// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package ltree

import (
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/errors"
)

const PathSeparator = "."

// Postges imposes a 65535 limit on the number of labels in an ltree.
const MaxNumOfLabels = 65535

type LTree struct {
	// Path is an ordered slice of Label types that make up a path in a LTREE column.
	Path []Label
}

// ParseLTree parses a string representation of a path into a LTree struct.
func ParseLTree(pathStr string) (LTree, error) {
	labels := []Label{}
	unparsedLabels := strings.Split(pathStr, PathSeparator)
	if len(unparsedLabels) > MaxNumOfLabels {
		return LTree{}, pgerror.Newf(pgcode.ProgramLimitExceeded, "number of ltree labels (%d) exceeds the maximum allowed (%d)", len(unparsedLabels), MaxNumOfLabels)
	}
	for _, labelStr := range unparsedLabels {
		label, err := NewLabel(labelStr)
		if err != nil {
			if errors.Is(err, ErrEmptyLabel) && len(unparsedLabels) == 1 {
				// If the only label is empty, we treat it as a valid empty path.
				break
			}
			return LTree{}, err
		}
		labels = append(labels, label)
	}

	return LTree{Path: labels}, nil
}

// SubPath returns a sub-path of the LTree starting from the given offset
// and of the specified length. If the offset is negative, it counts from the end of the path.
// If the length is negative, it extends to the end of the path from the offset.
func (l LTree) SubPath(offset, length int) (LTree, error) {
	start := offset
	if start < 0 {
		start += l.Len()
	}
	if length < 0 {
		length += l.Len() - start
	}
	if start < 0 || start >= l.Len() || length < 0 {
		return LTree{}, pgerror.Newf(pgcode.InvalidParameterValue, "invalid positions")
	}

	end := min(start+length, l.Len())
	return LTree{
		Path: l.Path[start:end],
	}, nil
}

// IndexOf returns the first index in l that matches the other l sub-ltree,
// starting from offset. If offset is negative, it counts from the end of the ltree.
// If the sub-ltree is not found, it returns -1.
func (l LTree) IndexOf(other LTree, offset int) int {
	// TODO(paulniziolek): We could optimize this by using a more efficient search algorithm
	start := offset
	if start < 0 {
		start += l.Len()
	}
	for i := max(start, 0); i < l.Len()-other.Len()+1; i++ {
		match := true
		for j := 0; j < other.Len(); j++ {
			if l.Path[i+j] != other.Path[j] {
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

func (l LTree) String() string {
	var b strings.Builder
	for i, label := range l.Path {
		if i > 0 {
			b.WriteString(PathSeparator)
		}
		b.WriteString(string(label))
	}
	return b.String()
}

// Len returns the number of labels in the LTree.
func (l LTree) Len() int {
	return len(l.Path)
}

// Size returns the size of the LTree in bytes, which is the sum of the label lengths.
func (l LTree) Size() int {
	size := 0
	for _, label := range l.Path {
		size += len(label)
	}
	return size
}

// Copy creates a deep copy of the LTree.
func (l LTree) Copy() LTree {
	copiedLabels := make([]Label, len(l.Path))
	copy(copiedLabels, l.Path)
	return LTree{Path: copiedLabels}
}
