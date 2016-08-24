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

	testCases := []struct {
		ctx      context.Context
		expected string
	}{
		{
			ctx:      ctx,
			expected: "test",
		},
		{
			ctx:      ctxA,
			expected: "[NodeID=5] test",
		},
		{
			ctx:      ctxB,
			expected: "[NodeID=5,ReqID=123] test",
		},
		{
			ctx:      ctxC,
			expected: "[NodeID=5,ReqID=123,aborted] test",
		},
		{
			ctx:      ctxD,
			expected: "[NodeID=5,ReqID=123,aborted,slice=[1 2 3]] test",
		},
		{
			ctx:      ctxB1,
			expected: "[NodeID=5,branch=meh] test",
		},
	}

	for i, tc := range testCases {
		if value := makeMessage(tc.ctx, "test", nil); value != tc.expected {
			t.Errorf("Test case %d failed: expected '%s', got '%s'", i, tc.expected, value)
		}
	}
}

func TestWithLogTagsFromCtx(t *testing.T) {
	ctx1 := context.Background()
	ctx1A := WithLogTagInt(ctx1, "1A", 1)
	ctx1B := WithLogTag(ctx1A, "1B", nil)

	ctx2 := context.Background()
	ctx2A := WithLogTagInt(ctx2, "2A", 1)
	ctx2B := WithLogTag(ctx2A, "2B", nil)

	testCases := []struct {
		ctx      context.Context
		expected string
	}{
		{
			ctx:      WithLogTagsFromCtx(ctx1, ctx2),
			expected: "test",
		},

		{
			ctx:      WithLogTagsFromCtx(ctx1, ctx2A),
			expected: "[2A=1] test",
		},

		{
			ctx:      WithLogTagsFromCtx(ctx1, ctx2B),
			expected: "[2A=1,2B] test",
		},

		{
			ctx:      WithLogTagsFromCtx(ctx1A, ctx2),
			expected: "[1A=1] test",
		},

		{
			ctx:      WithLogTagsFromCtx(ctx1A, ctx2A),
			expected: "[1A=1,2A=1] test",
		},

		{
			ctx:      WithLogTagsFromCtx(ctx1A, ctx2B),
			expected: "[1A=1,2A=1,2B] test",
		},

		{
			ctx:      WithLogTagsFromCtx(ctx1B, ctx2),
			expected: "[1A=1,1B] test",
		},

		{
			ctx:      WithLogTagsFromCtx(ctx1B, ctx2A),
			expected: "[1A=1,1B,2A=1] test",
		},

		{
			ctx:      WithLogTagsFromCtx(ctx1B, ctx2B),
			expected: "[1A=1,1B,2A=1,2B] test",
		},
	}

	for i, tc := range testCases {
		if value := makeMessage(tc.ctx, "test", nil); value != tc.expected {
			t.Errorf("Test case %d failed: expected '%s', got '%s'", i, tc.expected, value)
		}
	}
}
