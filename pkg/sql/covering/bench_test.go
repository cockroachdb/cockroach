// Copyright 2017 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package covering

import "testing"

func BenchmarkOverlapCoveringMerge(b *testing.B) {
	testCaseInput := [][]byte{
		{1, 100}, {1, 100}, {1, 100}, {1, 100}, {1, 100}, {1, 100}, {1, 100}, {1, 100}, {1, 100}, {1, 100},
		{1, 100}, {1, 100}, {1, 100}, {1, 100}, {1, 100}, {1, 100}, {1, 100}, {1, 100}, {1, 100}, {1, 100},
		{1, 100}, {1, 100}, {1, 100}, {1, 100}, {1, 100}, {1, 100}, {1, 100}, {1, 100}, {1, 100}, {1, 100},
		{1, 100}, {1, 100}, {1, 100}, {1, 100}, {1, 100}, {1, 100}, {1, 100}, {1, 100}, {1, 100}, {1, 100},
		{1, 100}, {1, 100}, {1, 100}, {1, 100}, {1, 100}, {1, 100}, {1, 100}, {1, 100}, {1, 100}, {1, 100},
		{1, 100}, {1, 100}, {1, 100}, {1, 100}, {1, 100}, {1, 100}, {1, 100}, {1, 100}, {1, 100}, {1, 100},
		{1, 100}, {1, 100}, {1, 100}, {1, 100}, {1, 100}, {1, 100}, {1, 100}, {1, 100}, {1, 100}, {1, 100},
		{1, 100}, {1, 100}, {1, 100}, {1, 100}, {1, 100}, {1, 100}, {1, 100}, {1, 100}, {1, 100}, {1, 100},
		{1, 100}, {1, 100}, {1, 100}, {1, 100}, {1, 100}, {1, 100}, {1, 100}, {1, 100}, {1, 100}, {1, 100},
		{1, 100}, {1, 100}, {1, 100}, {1, 100}, {1, 100}, {1, 100}, {1, 100}, {1, 100}, {1, 100}, {1, 100},
		{
			1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 3, 24, 25, 26, 27, 28, 29, 30,
			31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 52, 52, 53, 54, 55, 56, 57, 58, 59, 60,
			61, 61, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90,
			91, 92, 93, 94, 95, 06, 97, 98, 99, 100},
	}

	var inputs []Covering
	for _, endpoints := range testCaseInput {
		var payload int
		var c Covering
		for i := 0; i < len(endpoints); i += 2 {
			c = append(c, Range{
				Start:   []byte{endpoints[i]},
				End:     []byte{endpoints[i+1]},
				Payload: payload,
			})
			payload++
		}
		inputs = append(inputs, c)
	}

	for i := 0; i < b.N; i++ {
		OverlapCoveringMerge(inputs)
	}
}
