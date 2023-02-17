// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.
//

package pool_test

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/pool"
	"github.com/stretchr/testify/require"
)

func TestTypedPool(t *testing.T) {
	defer leaktest.AfterTest(t)()

	intPool := pool.MakePool[int](func(i *int) {
		*i = 0
	})

	intPool2 := pool.MakePool[int](func(i *int) {
		*i = 42
	})

	type rec struct {
		a, b int
	}

	type rec2 struct {
		a, b int
	}
	recPool := pool.MakePool[rec](func(r *rec) {
		r.a = 1
		r.b = -1
	})
	recPool2 := pool.MakePool[rec2](func(r *rec2) {
		r.a = 1
		r.b = -1
	})

	// Underlying sync.Pool are reused per type.
	require.Equal(t, intPool.Pool, intPool2.Pool)
	require.NotEqual(t, recPool.Pool, recPool2.Pool)
	require.NotEqual(t, intPool.Pool, recPool.Pool)

	// returned objects are initialized as per init function.
	a := intPool.Get()
	b := intPool2.Get()
	require.NotEqual(t, a, b) // a, b are different objects.
	require.Equal(t, 0, *a)
	require.Equal(t, 42, *b)
	intPool.Put(a)
	intPool2.Put(b)

	require.Equal(t, &rec{a: 1, b: -1}, recPool.Get())
}
