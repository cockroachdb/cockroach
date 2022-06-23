// Copyright 2015-2018 trivago N.V.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tcontainer

import (
	"reflect"
	"runtime"
	"sync/atomic"
	"unsafe"
)

const (
	tiny   = 64
	small  = 512
	medium = 1024
	large  = 1024 * 10
	huge   = 1024 * 100

	tinyCount   = 16384 // 1 MB
	smallCount  = 2048  // 1 MB
	mediumCount = 1024  // 1 MB
	largeCount  = 102   // ~1 MB
	hugeCount   = 10    // ~1 MB
)

type byteSlab struct {
	buffer     []byte
	bufferSize uintptr
	stride     uintptr
	basePtr    *uintptr
	nextPtr    *uintptr
}

// BytePool is a fragmentation friendly way to allocated byte slices.
type BytePool struct {
	tinySlab   byteSlab
	smallSlab  byteSlab
	mediumSlab byteSlab
	largeSlab  byteSlab
	hugeSlab   byteSlab
}

func newByteSlab(size, count int) byteSlab {
	bufferSize := count * size
	buffer := make([]byte, bufferSize)
	basePtr := (*reflect.SliceHeader)(unsafe.Pointer(&buffer)).Data
	nextPtr := basePtr + uintptr(bufferSize)

	return byteSlab{
		buffer:     buffer,
		bufferSize: uintptr(bufferSize),
		stride:     uintptr(size),
		basePtr:    &basePtr,
		nextPtr:    &nextPtr,
	}
}

func (slab *byteSlab) getSlice(size int) (chunk []byte) {
	chunkHeader := (*reflect.SliceHeader)(unsafe.Pointer(&chunk))
	chunkHeader.Len = size
	chunkHeader.Cap = int(slab.stride)

	for {
		// WARNING: The following two lines are order sensitive
		basePtr := atomic.LoadUintptr(slab.basePtr)
		nextPtr := atomic.AddUintptr(slab.nextPtr, -slab.stride)
		lastPtr := basePtr + slab.bufferSize

		switch {
		case nextPtr < basePtr || nextPtr >= lastPtr:
			// out of range either means alloc while realloc or race between
			// base and next during realloc. In the latter case we lose a chunk.
			runtime.Gosched()

		case nextPtr == basePtr:
			// Last item: realloc
			slab.buffer = make([]byte, slab.bufferSize)
			dataPtr := (*reflect.SliceHeader)(unsafe.Pointer(&slab.buffer)).Data

			// WARNING: The following two lines are order sensitive
			atomic.StoreUintptr(slab.nextPtr, dataPtr+slab.bufferSize)
			atomic.StoreUintptr(slab.basePtr, dataPtr)
			fallthrough

		default:
			chunkHeader.Data = nextPtr
			return
		}
	}
}

// NewBytePool creates a new BytePool with each slab using 1 MB of storage.
// The pool contains 5 slabs of different sizes: 64B, 512B, 1KB, 10KB and 100KB.
// Allocations above 100KB will be allocated directly.
func NewBytePool() BytePool {
	return BytePool{
		tinySlab:   newByteSlab(tiny, tinyCount),
		smallSlab:  newByteSlab(small, smallCount),
		mediumSlab: newByteSlab(medium, mediumCount),
		largeSlab:  newByteSlab(large, largeCount),
		hugeSlab:   newByteSlab(huge, hugeCount),
	}
}

// NewBytePoolWithSize creates a new BytePool with each slab size using n MB of
// storage. See NewBytePool() for slab size details.
func NewBytePoolWithSize(n int) BytePool {
	if n <= 0 {
		n = 1
	}
	return BytePool{
		tinySlab:   newByteSlab(tiny, tinyCount*n),
		smallSlab:  newByteSlab(small, smallCount*n),
		mediumSlab: newByteSlab(medium, mediumCount*n),
		largeSlab:  newByteSlab(large, largeCount*n),
		hugeSlab:   newByteSlab(huge, hugeCount*n),
	}
}

// Get returns a slice allocated to a normalized size.
// Sizes are organized in evenly sized buckets so that fragmentation is kept low.
func (b *BytePool) Get(size int) []byte {
	switch {
	case size == 0:
		return []byte{}

	case size <= tiny:
		return b.tinySlab.getSlice(size)

	case size <= small:
		return b.smallSlab.getSlice(size)

	case size <= medium:
		return b.mediumSlab.getSlice(size)

	case size <= large:
		return b.largeSlab.getSlice(size)

	case size <= huge:
		return b.hugeSlab.getSlice(size)

	default:
		return make([]byte, size)
	}
}
