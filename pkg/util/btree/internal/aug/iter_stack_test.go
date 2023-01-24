// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package aug

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/btree/ordered"
	"github.com/stretchr/testify/require"
)

// TestIterStack tests the interface of the iterStack type.
func TestIterStack(t *testing.T) {
	type (
		cmp           = ordered.ValueComparator[int]
		iterFrameT    = iterFrame[cmp, int, int, int]
		iterStackT    = iterStack[cmp, int, int, int]
		iterStackArrT = iterStackArr[cmp, int, int, int]
	)
	f := func(i int) iterFrameT { return iterFrameT{Pos: int16(i)} }
	var is iterStackT
	for i := 1; i <= 2*len(iterStackArrT{}); i++ {
		var j int
		for j = 0; j < i; j++ {
			is.push(f(j))
		}
		require.Equal(t, j, is.len())
		for j--; j >= 0; j-- {
			require.Equal(t, f(j), is.pop())
		}
		is.reset()
	}
}
