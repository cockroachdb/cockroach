// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package opt

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util"
)

func BenchmarkColSet(b *testing.B) {
	// Verify that the wrapper doesn't add overhead (as was the case with earlier
	// go versions which couldn't do mid-stack inlining).
	const n = 50
	b.Run("fastintset", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			var c util.FastIntSet
			for j := 0; j < n; j++ {
				c.Add(j)
			}
		}
	})
	b.Run("colset", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			var c ColSet
			for j := 0; j < n; j++ {
				c.Add(ColumnID(j))
			}
		}
	})
}

func TestTranslateColSet(t *testing.T) {
	test := func(t *testing.T, colSetIn ColSet, from ColList, to ColList, expected ColSet) {
		t.Helper()

		actual := TranslateColSet(colSetIn, from, to)
		if !actual.Equals(expected) {
			t.Fatalf("\nexpected: %s\nactual  : %s", expected, actual)
		}
	}

	colSetIn, from, to := MakeColSet(1, 2, 3), ColList{1, 2, 3}, ColList{4, 5, 6}
	test(t, colSetIn, from, to, MakeColSet(4, 5, 6))

	colSetIn, from, to = MakeColSet(2, 3), ColList{1, 2, 3}, ColList{4, 5, 6}
	test(t, colSetIn, from, to, MakeColSet(5, 6))

	// colSetIn and colSetOut might not be the same length.
	colSetIn, from, to = MakeColSet(1, 2), ColList{1, 1, 2}, ColList{4, 5, 6}
	test(t, colSetIn, from, to, MakeColSet(4, 5, 6))

	colSetIn, from, to = MakeColSet(1, 2, 3), ColList{1, 2, 3}, ColList{4, 5, 4}
	test(t, colSetIn, from, to, MakeColSet(4, 5))

	colSetIn, from, to = MakeColSet(2), ColList{1, 2, 2}, ColList{4, 5, 6}
	test(t, colSetIn, from, to, MakeColSet(5, 6))
}
