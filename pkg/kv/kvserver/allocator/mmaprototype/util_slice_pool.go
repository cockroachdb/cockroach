// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package mmaprototype

import "sync"

// slicePool pools reusable slices of T. Callers receive a *[]T from get; the
// pointer indirection lets the pool retain capacity even when the caller grows
// the slice. put clears the elements and resets the length to zero before
// returning it to the pool, so that stale pointers don't prevent GC.
//
// A slicePool must not be copied after first use.
type slicePool[T any] struct {
	p sync.Pool
}

func newSlicePool[T any](initCap int) *slicePool[T] {
	return &slicePool[T]{
		p: sync.Pool{
			New: func() any {
				s := make([]T, 0, initCap)
				return &s
			},
		},
	}
}

// get returns a pointer to a zero-length slice backed by a pooled array.
func (sp *slicePool[T]) get() *[]T {
	return sp.p.Get().(*[]T)
}

// put clears the slice elements, resets the length to zero, and returns it to
// the pool.
func (sp *slicePool[T]) put(s *[]T) {
	clear(*s)
	*s = (*s)[:0]
	sp.p.Put(s)
}
