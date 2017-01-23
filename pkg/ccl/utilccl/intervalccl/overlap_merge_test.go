// Copyright 2016 The Cockroach Authors.
//
// Licensed under the Cockroach Community Licence (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/pkg/ccl/LICENSE

package intervalccl

import (
	"bytes"
	"fmt"
	"reflect"
	"testing"
)

func TestOverlapCoveringMerge(t *testing.T) {
	tests := []struct {
		name string
		// inputs is a slice of coverings. The inner slice represents a covering
		// as an even number of ints, which are pairwise endpoints. So (1, 2, 2,
		// 4) means [/1, /2) and [/2, /4).
		inputs [][]byte
		// expectedIntervals is the output ranges in the same format as an input
		// covering.
		expectedIntervals []byte
		// expectedPayloads is the output payloads, corresponding 1:1 with
		// entries in expectedIntervals. Each input range is given an int
		// payload from 0..N-1 where N is the total number of ranges in all
		// input coverings for this test. Each of these is formatted as a string
		// concatenation of these ints.
		//
		// So for covering [1, 2), [2, 3) and covering [1, 3), the output
		// payloads would be "02" for [1, 2) and "12" for [2, 3).
		expectedPayloads []string
	}{
		{"no input",
			[][]byte{},
			nil, nil,
		},
		{"one empty covering",
			[][]byte{{}},
			nil, nil,
		},
		{"two empty coverings",
			[][]byte{{}, {}},
			nil, nil,
		},
		{"one",
			[][]byte{{1, 2}, {}},
			[]byte{1, 2}, []string{"0"},
		},
		{"same",
			[][]byte{{1, 2}, {1, 2}},
			[]byte{1, 2}, []string{"01"},
		},
		{"overlap",
			[][]byte{{1, 3}, {2, 3}},
			[]byte{1, 2, 2, 3}, []string{"0", "01"}},
		{"overlap reversed",
			[][]byte{{2, 3}, {1, 3}},
			[]byte{1, 2, 2, 3}, []string{"1", "01"}},
		{"no overlap",
			[][]byte{{1, 2, 5, 6}, {3, 4}},
			[]byte{1, 2, 3, 4, 5, 6}, []string{"0", "2", "1"},
		},
		{"cockroach range splits and merges",
			[][]byte{{1, 3, 3, 4}, {1, 4}, {1, 2, 2, 4}},
			[]byte{1, 2, 2, 3, 3, 4}, []string{"023", "024", "124"},
		},
		{"godoc example",
			[][]byte{{1, 2, 3, 4, 6, 7}, {1, 5}},
			[]byte{1, 2, 2, 3, 3, 4, 4, 5, 6, 7}, []string{"03", "3", "13", "3", "2"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var payload int
			var inputs []Covering
			for _, endpoints := range test.inputs {
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
			var outputIntervals []byte
			var outputPayloads []string
			for _, r := range OverlapCoveringMerge(inputs) {
				outputIntervals = append(outputIntervals, r.Start[0], r.End[0])
				var payload bytes.Buffer
				for _, p := range r.Payload.([]interface{}) {
					fmt.Fprintf(&payload, "%d", p.(int))
				}
				outputPayloads = append(outputPayloads, payload.String())
			}
			if !reflect.DeepEqual(outputIntervals, test.expectedIntervals) {
				t.Errorf("intervals got\n%v\nexpected\n%v", outputIntervals, test.expectedIntervals)
			}
			if !reflect.DeepEqual(outputPayloads, test.expectedPayloads) {
				t.Errorf("payloads got\n%v\nexpected\n%v", outputPayloads, test.expectedPayloads)
			}
		})
	}
}
