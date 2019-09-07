// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colexec

import "github.com/cockroachdb/cockroach/pkg/col/coldata"

// vecComparator is a helper for the ordered synchronizer. It stores multiple
// column vectors of a single type and facilitates comparing values between
// them. The implementations are templated by vec_comparators_gen.go.
type vecComparator interface {
	// compare compares values from two vectors. vecIdx is the index of the vector
	// and valIdx is the index of the value in that vector to compare. Returns -1,
	// 0, or 1.
	compare(vecIdx1, vecIdx2 int, valIdx1, valIdx2 uint16) int

	// set sets the value of the vector at dstVecIdx at index dstValIdx to the value
	// at the vector at srcVecIdx at index srcValIdx.
	set(srcVecIdx, dstVecIdx int, srcValIdx, dstValIdx uint16)

	// setVec updates the vector at idx.
	setVec(idx int, vec coldata.Vec)
}
