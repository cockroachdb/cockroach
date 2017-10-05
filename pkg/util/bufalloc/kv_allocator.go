// Copyright 2017 The Cockroach Authors.
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
	"sync"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

const kvSliceMinExp = 4
const kvSliceMin = 1 << kvSliceMinExp // 2^minExp

const kvSliceAllocFactorLog = 2                       // log2(kvSliceAllocFactor)
const kvSliceAllocFactor = 1 << kvSliceAllocFactorLog // re-alloc growth factor, 2^2 = 4

const kvSliceAllocSize = 6

var kvSliceAlloc [kvSliceAllocSize]sync.Pool

func init() {
	for i := range kvSliceAlloc {
		idx := i
		kvSliceAlloc[i] = sync.Pool{
			New: func() interface{} {
				return make([]roachpb.KeyValue, 0, indexToSize(idx))
			},
		}
	}
}

func newKVSliceForIdx(idx int) []roachpb.KeyValue {
	return kvSliceAlloc[idx].Get().([]roachpb.KeyValue)
}

func newKVSlice() []roachpb.KeyValue {
	return newKVSliceForIdx(0)
}

// 16    -> 0
// 64    -> 1
// 256   -> 2
// 1024  -> 3
// 4096  -> 4
// 16384 -> 5
func sizeToIndex(size int) int {
	size >>= kvSliceMinExp
	idx := 0
	for ; size > 1; idx++ {
		size >>= kvSliceAllocFactorLog
	}
	return idx
}

// 0 -> 16
// 1 -> 64
// 2 -> 256
// 3 -> 1024
// 4 -> 4096
// 5 -> 16384
func indexToSize(idx int) int {
	return kvSliceMin << (kvSliceAllocFactorLog * uint(idx))
}

// AppendToKVSlice behaves the same as: append(s, kv).
//
// This function allows us to catch append-induced reallocations and release the
// original slice back into our allocator pools.
func AppendToKVSlice(s []roachpb.KeyValue, kv roachpb.KeyValue) []roachpb.KeyValue {
	c := cap(s)
	if c == 0 {
		s = newKVSlice()
	} else if len(s) == c {
		idx := sizeToIndex(c)
		var realloc []roachpb.KeyValue
		if idx+1 >= kvSliceAllocSize {
			realloc = make([]roachpb.KeyValue, 0, kvSliceAllocFactor*c)
		} else {
			realloc = newKVSliceForIdx(idx + 1)
		}
		realloc = realloc[:c+1]
		copy(realloc[:c], s)
		realloc[c] = kv
		ReleaseKVSlice(s)
		return realloc
	}
	return append(s, kv)
}

// ReleaseKVSlice releases the KeyValue slice, allowing it to be re-used.
func ReleaseKVSlice(s []roachpb.KeyValue) {
	idx := sizeToIndex(cap(s))
	switch {
	case idx < kvSliceAllocSize:
		kvSliceAlloc[idx].Put(s[:0])
	case idx == kvSliceAllocSize:
		splitAndReleaseAllSizes(s, idx)
	case idx > kvSliceAllocSize:
		splitAndRelease(s, idx)
	}
}

// splitAndRelease splits s into kvSliceAllocFactor evenly sized chunks and
// release each one.
//
// Ex:
//  - len(s) == 64
//  - kvSliceAllocFactor == 4
//  - kvSliceMin = 16
// the function will release 4 slices of cap 16.
func splitAndRelease(s []roachpb.KeyValue, idx int) {
	chunkSize := indexToSize(idx - 1)
	for i := 0; i < kvSliceAllocFactor; i++ {
		ReleaseKVSlice(s[:0:chunkSize])
		s = s[chunkSize:]
	}
	if cap(s) != 0 {
		panic("s should be empty")
	}
}

// splitAndReleaseAllSizes splits s into kvSliceAllocFactor evenly sized chunks.
// It releases kvSliceAllocFactor - 1 of those chunks immediately, and recurses
// with the last chunk to release smaller chunks.
//
// Ex:
//  - len(s) == 256
//  - kvSliceAllocFactor == 4
//  - kvSliceMin = 4
// the function will release 3 slices of cap 64, 3 slices of cap 16, and 4
// slices of cap 4.
func splitAndReleaseAllSizes(s []roachpb.KeyValue, idx int) {
	for ; idx > 1; idx-- {
		chunkSize := indexToSize(idx - 1)
		for i := 0; i < kvSliceAllocFactor-1; i++ {
			kvSliceAlloc[idx-1].Put(s[:0:chunkSize])
			s = s[chunkSize:]
		}
	}
	splitAndRelease(s, idx)
}
