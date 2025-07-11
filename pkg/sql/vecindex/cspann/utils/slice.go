// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package utils

import "github.com/cockroachdb/cockroach/pkg/util/buildutil"

// ReplaceWithLast removes an element from a slice by replacing it with the last
// element and truncating the slice. This operation preserves the order of all
// elements except the removed one, which is replaced by the last element.
// The operation completes in O(1) time.
func ReplaceWithLast[T any](s []T, i int) []T {
	l := len(s) - 1
	s[i] = s[l]
	return s[:l]
}

// EnsureSliceLen returns a slice of the given length and generic type. If the
// existing slice has enough capacity, that slice is returned after adjusting
// its length. Otherwise, a new, larger slice is allocated.
// NOTE: Every element of the new slice is uninitialized; callers are
// responsible for initializing the memory.
func EnsureSliceLen[T any](s []T, l int) []T {
	// In test builds, always allocate new memory, to catch bugs where callers
	// assume existing slice elements will be copied.
	if cap(s) < l || buildutil.CrdbTestBuild {
		return make([]T, l)
	}
	return s[:l]
}
