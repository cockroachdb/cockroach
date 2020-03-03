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
	"sort"
)

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
