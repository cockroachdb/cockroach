// Copyright 2016 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.
//
// Author: Radu Berinde

package log

import (
	"testing"

	"golang.org/x/net/context"
)

func TestLogContext(t *testing.T) {
	ctx := context.Background()
	ctxA := WithLogTagInt(ctx, "NodeID", 5)
	ctxB := WithLogTagInt64(ctxA, "ReqID", 123)
	ctxC := WithLogTag(ctxB, "aborted", nil)
	ctxD := WithLogTag(ctxC, "slice", []int{1, 2, 3})

	ctxB1 := WithLogTagStr(ctxA, "branch", "meh")

	testCases := []struct{ value, expected string }{
		{
			value:    makeMessage(ctx, "test", nil),
			expected: "test",
		},
		{
			value:    makeMessage(ctxA, "test", nil),
			expected: "[NodeID=5] test",
		},
		{
			value:    makeMessage(ctxB, "test", nil),
			expected: "[NodeID=5,ReqID=123] test",
		},
		{
			value:    makeMessage(ctxC, "test", nil),
			expected: "[NodeID=5,ReqID=123,aborted] test",
		},
		{
			value:    makeMessage(ctxD, "test", nil),
			expected: "[NodeID=5,ReqID=123,aborted,slice=[1 2 3]] test",
		},
		{
			value:    makeMessage(ctxB1, "test", nil),
			expected: "[NodeID=5,branch=meh] test",
		},
	}

	for i, tc := range testCases {
		if tc.value != tc.expected {
			t.Errorf("Test case %d failed: expected '%s', got '%s'", i, tc.expected, tc.value)
		}
	}
}
