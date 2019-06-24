// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package stringarena

import (
	"context"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/util/mon"
)

// Arena provides arena allocation of a string from a []byte, reducing
// allocation overhead and GC pressure significantly if a large number of
// strings with a similar lifetime are being created. The reduction in
// allocation overhead should be obvious because we're replacing general
// allocation with what is essentially bump-pointer allocation. The reduction
// in GC pressure is due to the decrease in objects that the GC has to
// consider.
type Arena struct {
	alloc []byte
	acc   *mon.BoundAccount
	size  int64
}

// Make creates a new Arena with the specified monitoring account. If acc is
// nil, memory monitoring will be disabled.
func Make(acc *mon.BoundAccount) Arena {
	return Arena{acc: acc}
}

// UnsafeReset informs the memory account that previously allocated strings will
// no longer be used, and moves the current block pointer to the front of the
// buffer. Prefer to use this over creating a new arena in situations where we
// know that none of the strings currently on the arena will be referenced
// again.
//
// NOTE: Do NOT use this if you cannot guarantee that previously allocated
// strings will never be referenced again! If we cannot guarantee that, we run
// the risk of overwriting their contents with new data, which violates
// assumptions about the immutability of Go's strings.
//
// To prevent overwriting, we theoretically only need to ensure that strings
// allocated on the current block are never used again, but callers are
// oblivious to which block the string they get is allocated on, so it is easier
// to ensure that no previously allocated strings are used again.
func (a *Arena) UnsafeReset(ctx context.Context) error {
	newSize := int64(cap(a.alloc))
	if a.acc != nil {
		if err := a.acc.Resize(ctx, a.size, newSize); err != nil {
			return err
		}
	}
	a.alloc = a.alloc[:0]
	a.size = newSize
	return nil
}

// AllocBytes allocates a string in the arena with contents specified by
// b. Returns an error on memory accounting failure. The returned string can be
// used for as long as desired, but it will pin the entire underlying chunk of
// memory it was allocated from which can be significantly larger than the
// string itself. The primary use case for arena allocation is when all of the
// allocated strings will be released in mass.
func (a *Arena) AllocBytes(ctx context.Context, b []byte) (string, error) {
	n := len(b)
	if cap(a.alloc)-len(a.alloc) < n {
		if err := a.reserve(ctx, n); err != nil {
			return "", err
		}
	}

	// Note that we're carving the bytes for the string from the end of
	// a.alloc. This allows us to use cap(a.alloc) to reserve large chunk sizes
	// up to a maximum. See reserve().
	pos := len(a.alloc)
	data := a.alloc[pos : pos+n : pos+n]
	a.alloc = a.alloc[:pos+n]

	copy(data, b)
	// NB: string and []byte have the same layout for the first 2 fields. This
	// use of unsafe avoids an allocation. We're promising to never mutate "data"
	// at this point, which is true because we carved it off from ss.alloc.
	return *(*string)(unsafe.Pointer(&data)), nil
}

// TODO(peter): This is copied from util/bufalloc in order to support memory
// accounting. Perhaps figure out how to share the code.
func (a *Arena) reserve(ctx context.Context, n int) error {
	const chunkAllocMinSize = 512
	const chunkAllocMaxSize = 16384

	allocSize := cap(a.alloc) * 2
	if allocSize < chunkAllocMinSize {
		allocSize = chunkAllocMinSize
	} else if allocSize > chunkAllocMaxSize {
		allocSize = chunkAllocMaxSize
	}
	if allocSize < n {
		allocSize = n
	}
	if a.acc != nil {
		if err := a.acc.Grow(ctx, int64(allocSize)); err != nil {
			return err
		}
	}
	a.alloc = make([]byte, 0, allocSize)
	a.size += int64(allocSize)
	return nil
}
