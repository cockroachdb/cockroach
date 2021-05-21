// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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

	vec := coldata.NewMemColumn(types.String, coldata.BatchSize(), coldata.StandardColumnFactory)
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
		require.Equal(t, getExpected(tc.length, tc.sel, tc.prefix), vecsToStringWithRowPrefix([]coldata.Vec{vec}, tc.length, tc.sel, tc.prefix))
	}
}
