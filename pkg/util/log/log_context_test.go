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

	otlog "github.com/opentracing/opentracing-go/log"
	"github.com/pkg/errors"

	"golang.org/x/net/context"
)

func TestLogContext(t *testing.T) {
	ctx := context.Background()
	ctxA := WithLogTagInt(ctx, "NodeID", 5)
	ctxB := WithLogTagInt64(ctxA, "r", 123)
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
			expected: "[NodeID=5,r123] test",
		},
		{
			ctx:      ctxC,
			expected: "[NodeID=5,r123,aborted] test",
		},
		{
			ctx:      ctxD,
			expected: "[NodeID=5,r123,aborted,slice=[1 2 3]] test",
		},
		{
			ctx:      ctxB1,
			expected: "[NodeID=5,branch=meh] test",
		},
	}

	for i, tc := range testCases {
		if value := makeMessage(tc.ctx, "test", nil); value != tc.expected {
			t.Errorf("test case %d failed: expected '%s', got '%s'", i, tc.expected, value)
		}
	}
}

// withLogTagsFromCtx returns a context based on ctx with fromCtx's log tags
// added on.
//
// The result is equivalent to replicating the WithLogTag* calls that were
// used to obtain fromCtx and applying them to ctx in the same order - but
// skipping those for which ctx already has a tag with the same name.
func withLogTagsFromCtx(ctx, fromCtx context.Context) context.Context {
	if bottomTag := contextBottomTag(fromCtx); bottomTag != nil {
		return augmentTagChain(ctx, bottomTag)
	}
	return ctx
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
			ctx:      withLogTagsFromCtx(ctx1, ctx2),
			expected: "test",
		},

		{
			ctx:      withLogTagsFromCtx(ctx1, ctx2A),
			expected: "[2A=1] test",
		},

		{
			ctx:      withLogTagsFromCtx(ctx1, ctx2B),
			expected: "[2A=1,2B] test",
		},

		{
			ctx:      withLogTagsFromCtx(ctx1A, ctx2),
			expected: "[1A=1] test",
		},

		{
			ctx:      withLogTagsFromCtx(ctx1A, ctx2A),
			expected: "[1A=1,2A=1] test",
		},

		{
			ctx:      withLogTagsFromCtx(ctx1A, ctx2B),
			expected: "[1A=1,2A=1,2B] test",
		},

		{
			ctx:      withLogTagsFromCtx(ctx1B, ctx2),
			expected: "[1A=1,1B] test",
		},

		{
			ctx:      withLogTagsFromCtx(ctx1B, ctx2A),
			expected: "[1A=1,1B,2A=1] test",
		},

		{
			ctx:      withLogTagsFromCtx(ctx1B, ctx2B),
			expected: "[1A=1,1B,2A=1,2B] test",
		},
	}

	for i, tc := range testCases {
		t.Run("", func(t *testing.T) {
			if value := makeMessage(tc.ctx, "test", nil); value != tc.expected {
				t.Errorf("test case %d failed: expected '%s', got '%s'", i, tc.expected, value)
			}
		})
	}
}

type chain struct {
	head, tail *logTag
}

func (c *chain) Append(t logTag) *chain {
	if t.parent != nil {
		panic("can't append a chain")
	}
	if c.head == nil {
		c.head = &t
		c.tail = c.head
	} else {
		c.tail.parent = &t
		c.tail = &t
	}
	return c
}

func makeTag(key string, val int) logTag {
	return logTag{Field: otlog.Int(key, val)}
}

func checkChain(expected *logTag, actual *logTag) error {
	e, a := expected, actual
	for {
		if e == nil && a == nil {
			return nil
		}
		if e == nil && a != nil {
			return errors.Errorf("expected done, actual has extra nodes starting with %s", a)
		}
		if e != nil && a == nil {
			return errors.Errorf("actual done, expected has extra nodes starting with %s", e)
		}
		if e.Key() != a.Key() || e.Value() != a.Value() {
			return errors.Errorf("%s != %s", e, a)
		}
		e = e.parent
		a = a.parent
	}
}

func TestMergeChains(t *testing.T) {
	var c1, c2 chain
	c1.Append(makeTag("A", 1)).Append(makeTag("B", 1)).Append(makeTag("C", 1)).Append(makeTag("D", 1))
	c2.Append(makeTag("A", 2)).Append(makeTag("B", 2)).Append(makeTag("D", 2)).Append(makeTag("E", 2))
	r := mergeChains(c1.head, c2.head)
	var expected chain
	if err := checkChain(r,
		expected.Append(makeTag("A", 2)).Append(makeTag("B", 2)).Append(makeTag("D", 2)).Append(
			makeTag("E", 2)).Append(makeTag("C", 1)).head); err != nil {
		t.Fatal(err)
	}
}
