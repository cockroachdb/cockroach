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
	"fmt"
	"reflect"
	"testing"
)

func TestUniquifyByteSlices(t *testing.T) {
	tests := []struct {
		input    []string
		expected []string
	}{
		{
			input:    []string{"foo", "foo"},
			expected: []string{"foo"},
		},
		{
			input:    []string{},
			expected: []string{},
		},
		{
			input:    []string{"", ""},
			expected: []string{""},
		},
		{
			input:    []string{"foo"},
			expected: []string{"foo"},
		},
		{
			input:    []string{"foo", "bar", "foo"},
			expected: []string{"bar", "foo"},
		},
		{
			input:    []string{"foo", "bar"},
			expected: []string{"bar", "foo"},
		},
		{
			input:    []string{"bar", "bar", "foo"},
			expected: []string{"bar", "foo"},
		},
	}
	for i, tt := range tests {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			input := make([][]byte, len(tt.input))
			expected := make([][]byte, len(tt.expected))
			for i := range tt.input {
				input[i] = []byte(tt.input[i])
			}
			for i := range tt.expected {
				expected[i] = []byte(tt.expected[i])
			}
			if got := UniquifyByteSlices(input); !reflect.DeepEqual(got, expected) {
				t.Errorf("UniquifyByteSlices() = %v, expected %v", got, expected)
			}
		})
	}
}
