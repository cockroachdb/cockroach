// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package colconv

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestVecsToStringWithRowPrefix(t *testing.T) {
	defer leaktest.AfterTest(t)()

	vec := coldata.NewVec(types.String, coldata.BatchSize(), coldata.StandardColumnFactory)
	input := []string{"one", "two", "three"}
	for i := range input {
		vec.Bytes().Set(i, []byte(input[i]))
	}
	getExpected := func(length int, sel []int, prefix string) []string {
		result := make([]string, length)
		for i := 0; i < length; i++ {
			rowIdx := i
			if sel != nil {
				rowIdx = sel[i]
			}
			result[i] = prefix + "['" + input[rowIdx] + "']"
		}
		return result
	}
	for _, tc := range []struct {
		length int
		sel    []int
		prefix string
	}{
		{length: 3},
		{length: 2, sel: []int{0, 2}},
		{length: 3, prefix: "row: "},
		{length: 2, sel: []int{0, 2}, prefix: "row: "},
	} {
		require.Equal(t, getExpected(tc.length, tc.sel, tc.prefix), vecsToStringWithRowPrefix([]*coldata.Vec{vec}, tc.length, tc.sel, tc.prefix))
	}
}
