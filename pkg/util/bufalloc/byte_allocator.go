// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package bufalloc

// ByteAllocator provides chunk allocation of []byte, amortizing the overhead
// of each allocation. Because the underlying storage for the slices is shared,
// they should share a similar lifetime in order to avoid pinning large amounts
// of memory unnecessarily. The allocator itself is a []byte where cap()
// indicates the total amount of memory and len() is the amount already
// allocated. The size of the buffer to allocate from is grown exponentially
// when it runs out of room up to a maximum size (chunkAllocMaxSize).
type ByteAllocator struct {
	b []byte
}

const chunkAllocMinSize = 512
const chunkAllocMaxSize = 16384

func (a ByteAllocator) reserve(n int) ByteAllocator {
	allocSize := cap(a.b) * 2
	if allocSize < chunkAllocMinSize {
		allocSize = chunkAllocMinSize
	} else if allocSize > chunkAllocMaxSize {
		allocSize = chunkAllocMaxSize
	}
	if allocSize < n {
		allocSize = n
	}
	a.b = make([]byte, 0, allocSize)
	return a
}

// Alloc allocates a new chunk of memory with the specified length. extraCap
// indicates additional zero bytes that will be present in the returned []byte,
// but not part of the length.
func (a ByteAllocator) Alloc(n int, extraCap int) (ByteAllocator, []byte) {
	if cap(a.b)-len(a.b) < n+extraCap {
		a = a.reserve(n + extraCap)
	}
	p := len(a.b)
	r := a.b[p : p+n : p+n+extraCap]
	a.b = a.b[:p+n+extraCap]
	return a, r
}

// Copy allocates a new chunk of memory, initializing it from src.
// extraCap indicates additional zero bytes that will be present in the returned
// []byte, but not part of the length.
func (a ByteAllocator) Copy(src []byte, extraCap int) (ByteAllocator, []byte) {
	var alloc []byte
	a, alloc = a.Alloc(len(src), extraCap)
	copy(alloc, src)
	return a, alloc
}

// Truncate resets the length of the underlying buffer to zero, allowing the
// reserved capacity in the buffer to be written over and reused.
func (a ByteAllocator) Truncate() ByteAllocator {
	a.b = a.b[:0]
	return a
}
