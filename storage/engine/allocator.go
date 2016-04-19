// Copyright 2016 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.
//
// Author: Peter Mattis (peter@cockroachlabs.com)

package engine

// chunkAllocator provides chunk allocation of []byte, amortizing the overhead
// of each allocation. Because the underlying storage for the slices is shared,
// they should share a similar lifetime in order to avoid pinning large amounts
// of memory unnecessarily. The allocator itself is a []byte where cap()
// indicates the total amount of memory and len() is the amount already
// allocated. The size of the buffer to allocate from is grown exponentially
// when it runs out of room up to a maximum size (chunkAllocMaxSize).
type chunkAllocator []byte

const chunkAllocMinSize = 512
const chunkAllocMaxSize = 16384

func (a chunkAllocator) reserve(n int) chunkAllocator {
	allocSize := cap(a) * 2
	if allocSize < chunkAllocMinSize {
		allocSize = chunkAllocMinSize
	} else if allocSize > chunkAllocMaxSize {
		allocSize = chunkAllocMaxSize
	}
	if allocSize < n {
		allocSize = n
	}
	return make([]byte, 0, allocSize)
}

// newChunk allocates a new chunk of memory, initializing it from src. extra
// indicates additional zero bytes that will be present in the returned []byte,
// but not part of the length.
func (a chunkAllocator) newChunk(src []byte, extra int) (chunkAllocator, []byte) {
	n := len(src)
	if cap(a)-len(a) < n+extra {
		a = a.reserve(n + extra)
	}
	p := len(a)
	r := a[p : p+n : p+n+extra]
	a = a[:p+n+extra]
	copy(r, src)
	return a, r
}
