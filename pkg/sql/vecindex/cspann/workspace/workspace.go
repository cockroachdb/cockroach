// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package workspace

import (
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/vector"
)

// T provides temporary per-thread memory for routines that only need to use it
// within the context of their current stack frame. Allocated memory is stack-
// allocated and must be explicitly freed in the same order it was allocated and
// should never be referenced again once freed. For example:
//
//	var workspace workspace.T
//	tempVector := workspace.AllocVector(2)
//	defer workspace.FreeVector(tempVector)
//	... use tempVector only within this scope
//
// T is not thread-safe.
type T struct {
	floatStack  stackAlloc[float32]
	uint64Stack stackAlloc[uint64]
}

// IsClear returns true if there is no temp memory currently in use (i.e. all
// memory has been freed). This can be called to validate that there are no
// leaks.
func (w *T) IsClear() bool {
	return w.floatStack.IsEmpty() && w.uint64Stack.IsEmpty()
}

// AllocVector returns a temporary vector having the given number of dimensions.
// NOTE: Vector data is undefined; callers should not assume it's zeroed.
func (w *T) AllocVector(dims int) vector.T {
	return w.AllocFloats(dims)
}

// FreeVector reclaims a temporary vector that was previously allocated.
func (w *T) FreeVector(vec vector.T) {
	w.FreeFloats(vec)
}

// AllocVectorSet returns a temporary vector set having the given number of
// vectors with the given number of dimensions.
// NOTE: Vector data is undefined; callers should not assume it's zeroed.
func (w *T) AllocVectorSet(count, dims int) vector.Set {
	floats := w.AllocFloats(count * dims)
	return vector.MakeSetFromRawData(floats, dims)
}

// FreeVectorSet reclaims a temporary vector set that was previously allocated.
func (w *T) FreeVectorSet(vectors vector.Set) {
	w.FreeFloats(vectors.Data)
}

// AllocFloats returns a temporary slice of float32 values of the given size.
// NOTE: Slice data is undefined; callers should not assume it's zeroed.
func (w *T) AllocFloats(count int) []float32 {
	ret := w.floatStack.Alloc(count)
	if buildutil.CrdbTestBuild {
		// Write non-zero values to allocated memory.
		for i := 0; i < len(ret); i++ {
			ret[i] = 0xBADF00D
		}
	}
	return ret
}

// FreeFloats reclaims a temporary float32 slice that was previously allocated.
func (w *T) FreeFloats(floats []float32) {
	if buildutil.CrdbTestBuild {
		// Write non-zero values to allocated memory.
		for i := 0; i < len(floats); i++ {
			floats[i] = 0xBADF00D
		}
	}
	w.floatStack.Free(floats)
}

// AllocUint64s returns a temporary slice of uint64 values of the given size.
// NOTE: Slice data is undefined; callers should not assume it's zeroed.
func (w *T) AllocUint64s(count int) []uint64 {
	ret := w.uint64Stack.Alloc(count)
	if buildutil.CrdbTestBuild {
		// Write non-zero values to allocated memory.
		for i := 0; i < len(ret); i++ {
			ret[i] = 0xBADF00D
		}
	}
	return ret
}

// FreeUint64s reclaims a temporary uint64 slice that was previously allocated.
func (w *T) FreeUint64s(uint64s []uint64) {
	if buildutil.CrdbTestBuild {
		// Write non-zero values to allocated memory.
		for i := 0; i < len(uint64s); i++ {
			uint64s[i] = 0xBADF00D
		}
	}
	w.uint64Stack.Free(uint64s)
}

// stackAlloc allocates memory using a stack. Callers must deallocate memory in
// the inverse order of allocation. For example, if a caller allocates objects
// A and then B, it must free B and then A.
type stackAlloc[T any] []T

func (s *stackAlloc[T]) Alloc(count int) []T {
	start := len(*s)
	end := start + count
	if end > cap(*s) {
		// Need a new, larger array. Note that it's not necessary to copy the
		// existing data, as it's temporary.
		*s = make([]T, end, max(end*3/2, 16))
	}
	*s = (*s)[:end]
	return (*s)[start:end]
}

func (s *stackAlloc[T]) Free(t []T) {
	*s = (*s)[:len(*s)-len(t)]
}

func (s *stackAlloc[T]) IsEmpty() bool {
	return len(*s) == 0
}
