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

// projConstOpBase contains all of the fields for binary projections with a
// constant, except for the constant itself.
type projConstOpBase struct {
	OneInputNode
	allocator      *Allocator
	colIdx         int
	outputIdx      int
	decimalScratch decimalOverloadScratch
}

// projOpBase contains all of the fields for non-constant binary projections.
type projOpBase struct {
	OneInputNode
	allocator      *Allocator
	col1Idx        int
	col2Idx        int
	outputIdx      int
	decimalScratch decimalOverloadScratch
}
