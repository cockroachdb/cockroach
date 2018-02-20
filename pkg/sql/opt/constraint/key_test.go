// Copyright 2018 The Cockroach Authors.
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
// This file implements data structures used by index constraints generation.

package constraint

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

func TestKey(t *testing.T) {
	testKey(t, EmptyKey, "")

	k := MakeKey(tree.NewDInt(1))
	testKey(t, k, "/1")

	k = MakeCompositeKey(tree.NewDInt(2))
	testKey(t, k, "/2")

	k = MakeCompositeKey(tree.NewDString("foo"), tree.NewDInt(3))
	testKey(t, k, "/'foo'/3")
}

func TestKeyCompare(t *testing.T) {
	test := func(
		t *testing.T, evalCtx *tree.EvalContext, k, l Key, kExt, lExt KeyExtension, expected int,
	) {
		t.Helper()
		if actual := k.Compare(evalCtx, l, kExt, lExt); actual != expected {
			t.Errorf("k: %s, l %s, expected: %d, actual: %d", k, l, expected, actual)
		}
	}

	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)

	key0 := MakeKey(tree.NewDInt(0))
	key1 := MakeKey(tree.NewDInt(1))
	key01 := MakeCompositeKey(tree.NewDInt(0), tree.NewDInt(1))
	keyNull := MakeKey(tree.DNull)

	test(t, &evalCtx, EmptyKey, keyNull, ExtendLow, ExtendLow, -1)
	test(t, &evalCtx, EmptyKey, keyNull, ExtendLow, ExtendHigh, -1)
	test(t, &evalCtx, EmptyKey, keyNull, ExtendHigh, ExtendLow, 1)
	test(t, &evalCtx, EmptyKey, keyNull, ExtendHigh, ExtendHigh, 1)

	test(t, &evalCtx, key0, key0, ExtendLow, ExtendLow, 0)
	test(t, &evalCtx, key0, key0, ExtendLow, ExtendHigh, -1)
	test(t, &evalCtx, key0, key0, ExtendHigh, ExtendLow, 1)
	test(t, &evalCtx, key0, key0, ExtendHigh, ExtendHigh, 0)

	test(t, &evalCtx, key0, key1, ExtendLow, ExtendLow, -1)
	test(t, &evalCtx, key0, key1, ExtendLow, ExtendHigh, -1)
	test(t, &evalCtx, key0, key1, ExtendHigh, ExtendLow, -1)
	test(t, &evalCtx, key0, key1, ExtendHigh, ExtendHigh, -1)

	test(t, &evalCtx, key1, key0, ExtendLow, ExtendLow, 1)
	test(t, &evalCtx, key1, key0, ExtendLow, ExtendHigh, 1)
	test(t, &evalCtx, key1, key0, ExtendHigh, ExtendLow, 1)
	test(t, &evalCtx, key1, key0, ExtendHigh, ExtendHigh, 1)

	test(t, &evalCtx, key01, key0, ExtendLow, ExtendLow, 1)
	test(t, &evalCtx, key01, key0, ExtendLow, ExtendHigh, -1)
	test(t, &evalCtx, key01, key0, ExtendHigh, ExtendLow, 1)
	test(t, &evalCtx, key01, key0, ExtendHigh, ExtendHigh, -1)

	test(t, &evalCtx, key0, key01, ExtendLow, ExtendLow, -1)
	test(t, &evalCtx, key0, key01, ExtendLow, ExtendHigh, -1)
	test(t, &evalCtx, key0, key01, ExtendHigh, ExtendLow, 1)
	test(t, &evalCtx, key0, key01, ExtendHigh, ExtendHigh, 1)

	test(t, &evalCtx, keyNull, key0, ExtendHigh, ExtendLow, -1)
}

func TestKeyConcat(t *testing.T) {
	k := EmptyKey

	// Empty + empty.
	k = k.Concat(EmptyKey)
	testKey(t, k, "")

	// Empty + single value.
	k = k.Concat(MakeKey(tree.NewDInt(1)))
	testKey(t, k, "/1")

	// Single value + empty.
	k = k.Concat(EmptyKey)
	testKey(t, k, "/1")

	// Single value + single value.
	k = k.Concat(MakeKey(tree.NewDInt(2)))
	testKey(t, k, "/1/2")

	// Multiple values + empty.
	k = k.Concat(EmptyKey)
	testKey(t, k, "/1/2")

	// Multiple values + single value.
	k = k.Concat(MakeKey(tree.NewDInt(3)))
	testKey(t, k, "/1/2/3")

	// Multiple values + multiple values.
	k = k.Concat(MakeCompositeKey(tree.NewDString("bar"), tree.DBoolTrue))
	testKey(t, k, "/1/2/3/'bar'/true")
}

func testKey(t *testing.T, k Key, expected string) {
	t.Helper()
	if k.String() != expected {
		t.Errorf("expected: %s, actual: %s", expected, k.String())
	}
}
