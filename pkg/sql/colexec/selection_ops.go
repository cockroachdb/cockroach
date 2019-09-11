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

// selConstOpBase contains all of the fields for binary selections with a
// constant, except for the constant itself.
type selConstOpBase struct {
	OneInputNode
	colIdx int
}

// selOpBase contains all of the fields for non-constant binary selections.
type selOpBase struct {
	OneInputNode
	col1Idx int
	col2Idx int
}
