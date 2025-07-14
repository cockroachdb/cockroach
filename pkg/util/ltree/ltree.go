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
