// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package ltree

import "strings"

const PathSeparator = "."

type LTree struct {
	// Path is an ordered slice of Label types that make up a path in a LTREE column.
	Path []Label
}

// ParseLTree parses a string representation of a path into a LTree struct.
func ParseLTree(pathStr string) (LTree, error) {
	labels := []Label{}
	unparsedLabels := strings.Split(pathStr, PathSeparator)
	for _, labelStr := range unparsedLabels {
		label, err := NewLabel(labelStr)
		if err != nil {
			if err == ErrEmptyLabel && len(unparsedLabels) == 1 {
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
	labelStrs := make([]string, len(l.Path))
	for i, label := range l.Path {
		labelStrs[i] = string(label)
	}
	return strings.Join(labelStrs, PathSeparator)
}

func (l LTree) Len() int {
	return len(l.Path)
}

func (l LTree) Size() int {
	size := 0
	for _, label := range l.Path {
		size += len(label)
	}
	return size
}
