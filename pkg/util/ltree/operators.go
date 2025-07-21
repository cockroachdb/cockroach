// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package ltree

// Contains returns true if an LTree path contains another LTree path. This implements
// the @> and <@ operators. This is equivalent to checking if an LTree path is an ancestor
// or descendant of another LTree path.
// Examples:
// 'a.b.c' @> 'a.b' returns false
// 'a.b' @> 'a.b.c' returns true
// 'a.b.c' <@ 'a.b' returns true
// 'a.b' <@ 'a.b.c' returns false
// 'a' @> 'a' returns true
// 'a' <@ 'a' returns true
func Contains(l, other LTree) bool {
	for i, label := range l.Path {
		if i >= other.Len() || label != other.Path[i] {
			return false
		}
	}
	return true
}

// Concat returns a new LTree that is the concatenation of two valid LTree paths.
func Concat(l, other LTree) LTree {
	return LTree{Path: append(l.Path, other.Path...)}
}
