// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package unique

import (
	"bytes"
	"reflect"
	"sort"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// ProcessUniqueID is an ID which is unique to this process in the cluster.
// It is used to generate unique integer keys via GenerateUniqueInt. Generally
// it is the node ID of a system tenant or the sql instance ID of a secondary
// tenant.
//
// Note that for its uniqueness property to hold, the value must use no more
// than 15 bits. Nothing enforces this for node IDs, but, in practice, they
// do not generally get to be more than 16k unless nodes are being added and
// removed frequently. In order to eliminate this bug, we ought to use the
// leased SQLInstanceID instead of the NodeID to generate these unique integers
// in all cases.
type ProcessUniqueID int32

// NodeIDBits is the number of bits stored in the lower portion of
// GenerateUniqueInt.
const NodeIDBits = 15

var uniqueIntState struct {
	syncutil.Mutex
	timestamp uint64
}

var uniqueIntEpoch = time.Date(2015, time.January, 1, 0, 0, 0, 0, time.UTC).UnixNano()

// GenerateUniqueInt creates a unique int composed of the current time at a
// 10-microsecond granularity and the instance-id. The instance-id is stored in the
// lower 15 bits of the returned value and the timestamp is stored in the upper
// 48 bits. The top-bit is left empty so that negative values are not returned.
// The 48-bit timestamp field provides for 89 years of timestamps. We use a
// custom epoch (Jan 1, 2015) in order to utilize the entire timestamp range.
//
// Note that GenerateUniqueInt() imposes a limit on instance IDs while
// generateUniqueBytes() does not.
//
// TODO(pmattis): Do we have to worry about persisting the milliseconds value
// periodically to avoid the clock ever going backwards (e.g. due to NTP
// adjustment)?
func GenerateUniqueInt(instanceID ProcessUniqueID) uint64 {
	const precision = uint64(10 * time.Microsecond)

	// TODO(andrei): For tenants we need to validate that the current time is
	// within the validity of the sqlliveness session to which the instanceID is
	// bound. Without this validation, two different nodes might be calling this
	// function with the same instanceID at the same time, and both would generate
	// the same unique int. See #90459.
	nowNanos := timeutil.Now().UnixNano()
	// Paranoia: nowNanos should never be less than uniqueIntEpoch.
	if nowNanos < uniqueIntEpoch {
		nowNanos = uniqueIntEpoch
	}
	timestamp := uint64(nowNanos-uniqueIntEpoch) / precision

	uniqueIntState.Lock()
	if timestamp <= uniqueIntState.timestamp {
		timestamp = uniqueIntState.timestamp + 1
	}
	uniqueIntState.timestamp = timestamp
	uniqueIntState.Unlock()

	return GenerateUniqueID(int32(instanceID), timestamp)
}

// GenerateUniqueID encapsulates the logic to generate a unique number from
// a nodeID and timestamp.
func GenerateUniqueID(instanceID int32, timestamp uint64) uint64 {
	// We xor in the instanceID so that instanceIDs larger than 32K will flip bits
	// in the timestamp portion of the final value instead of always setting them.
	id := (timestamp << NodeIDBits) ^ uint64(instanceID)
	return id
}

// UniquifyByteSlices takes as input a slice of slices of bytes, and
// deduplicates them using a sort and unique. The output will not contain any
// duplicates but it will be sorted.
func UniquifyByteSlices(slices [][]byte) [][]byte {
	if len(slices) == 0 {
		return slices
	}
	// First sort:
	sort.Slice(slices, func(i int, j int) bool {
		return bytes.Compare(slices[i], slices[j]) < 0
	})
	// Then distinct: (wouldn't it be nice if Go had generics?)
	lastUniqueIdx := 0
	for i := 1; i < len(slices); i++ {
		if !bytes.Equal(slices[i], slices[lastUniqueIdx]) {
			// We found a unique entry, at index i. The last unique entry in the array
			// was at lastUniqueIdx, so set the entry after that one to our new unique
			// entry, and bump lastUniqueIdx for the next loop iteration.
			lastUniqueIdx++
			slices[lastUniqueIdx] = slices[i]
		}
	}
	slices = slices[:lastUniqueIdx+1]
	return slices
}

// UniquifyAcrossSlices removes elements from both slices that are duplicated
// across both of the slices. For example, inputs [1,2,3], [2,3,4] would remove
// 2 and 3 from both lists.
// It assumes that both slices are pre-sorted using the same comparison metric
// as cmpFunc provides, and also already free of duplicates internally. It
// returns the slices, which will have also been sorted as a side effect.
// cmpFunc compares the lth index of left to the rth index of right. It must
// return less than 0 if the left element is less than the right element, 0 if
// equal, and greater than 0 otherwise.
// setLeft sets the ith index of left to the jth index of left.
// setRight sets the ith index of right to the jth index of right.
// The function returns the new lengths of both input slices, whose elements
// will have been mutated, but whose lengths must be set the new lengths by
// the caller.
func UniquifyAcrossSlices(
	left interface{},
	right interface{},
	cmpFunc func(l, r int) int,
	setLeft func(i, j int),
	setRight func(i, j int),
) (leftLen, rightLen int) {
	leftSlice := reflect.ValueOf(left)
	rightSlice := reflect.ValueOf(right)

	lLen := leftSlice.Len()
	rLen := rightSlice.Len()

	var lIn, lOut int
	var rIn, rOut int

	// Remove entries that are duplicated across both entry lists.
	// This loop walks through both lists using a merge strategy. Two pointers per
	// list are maintained. One is the "input pointer", which is always the ith
	// element of the input list. One is the "output pointer", which is the index
	// after the most recent unique element in the list. Every time we bump the
	// input pointer, we also set the element at the output pointer to that at
	// the input pointer, so we don't have to use extra space - we're
	// deduplicating in-place.
	for rIn < rLen || lIn < lLen {
		var cmp int
		if lIn == lLen {
			cmp = 1
		} else if rIn == rLen {
			cmp = -1
		} else {
			cmp = cmpFunc(lIn, rIn)
		}
		if cmp < 0 {
			setLeft(lOut, lIn)
			lIn++
			lOut++
		} else if cmp > 0 {
			setRight(rOut, rIn)
			rIn++
			rOut++
		} else {
			// Elements are identical - we want to remove them from the list. So
			// we increment our input indices without touching our output indices.
			// Next time through the loop, we'll shift the next element back to
			// the last output index which is now lagging behind the input index.
			lIn++
			rIn++
		}
	}
	return lOut, rOut
}
