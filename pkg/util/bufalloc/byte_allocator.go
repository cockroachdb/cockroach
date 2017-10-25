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

package bufalloc

import (
	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

// ByteAllocator provides chunk allocation of []byte, amortizing the overhead
// of each allocation. Because the underlying storage for the slices is shared,
// they should share a similar lifetime in order to avoid pinning large amounts
// of memory unnecessarily. The allocator itself is a []byte where cap()
// indicates the total amount of memory and len() is the amount already
// allocated. The size of the buffer to allocate from is grown exponentially
// when it runs out of room up to a maximum size (chunkAllocMaxSize).
type ByteAllocator []byte

const chunkAllocMinSize = 512
const chunkAllocMaxSize = 16384

func (a ByteAllocator) reserve(n int) ByteAllocator {
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

// Alloc allocates a new chunk of memory with the specified length. extraCap
// indicates additional zero bytes that will be present in the returned []byte,
// but not part of the length.
func (a ByteAllocator) Alloc(n int, extraCap int) (ByteAllocator, []byte) {
	if cap(a)-len(a) < n+extraCap {
		a = a.reserve(n + extraCap)
	}
	p := len(a)
	r := a[p : p+n : p+n+extraCap]
	a = a[:p+n+extraCap]
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

// CopySpan performs a deep copy of the provided Span. The returned Span
// should be treated as immutable.
func (a ByteAllocator) CopySpan(src roachpb.Span) (ByteAllocator, roachpb.Span) {
	if len(src.Key) == 0 {
		return a, roachpb.Span{}
	}
	var dst roachpb.Span
	extraCap := 0
	if len(src.EndKey) > 0 {
		a, dst.EndKey = a.Copy(src.EndKey, 0)
	} else {
		// In a few places including in the TimestampCache we translate
		// [Key, nil) -> [Key, Key.Next()). If the start key has enough
		// extra capacity, the Next call does not need to allocate and
		// can instead share the same memory with the start key.
		extraCap = 1
	}
	a, dst.Key = a.Copy(src.Key, extraCap)
	return a, dst
}

// CopyRSpan performs a deep copy of the provided RSpan. The returned RSpan
// should be treated as immutable.
func (a ByteAllocator) CopyRSpan(src roachpb.RSpan) (ByteAllocator, roachpb.RSpan) {
	var dst roachpb.Span
	a, dst = a.CopySpan(src.AsRawSpanWithNoLocals())
	return a, roachpb.RSpan{
		Key:    roachpb.RKey(dst.Key),
		EndKey: roachpb.RKey(dst.EndKey),
	}
}

// SpanSize returns the amount of memory required to copy the provided Span.
func SpanSize(src roachpb.Span) int {
	if len(src.Key) == 0 {
		return 0
	}
	endSize := 1
	if len(src.EndKey) > 0 {
		endSize = len(src.EndKey)
	}
	return len(src.Key) + endSize
}

// RSpanSize returns the amount of memory required to copy the provided RSpan.
func RSpanSize(src roachpb.RSpan) int {
	return SpanSize(src.AsRawSpanWithNoLocals())
}
