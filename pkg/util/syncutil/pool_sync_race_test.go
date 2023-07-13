// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

//go:build race
// +build race

package syncutil

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPool(t *testing.T) {
	var p Pool
	require.Nil(t, p.Get())

	p.Put("a")
	p.Put("b")
	require.Equal(t, "a", p.Get())
	require.Equal(t, "b", p.Get())
	require.Nil(t, p.Get())
}

func TestPoolNew(t *testing.T) {
	i := 0
	p := Pool{
		New: func() any {
			i++
			return i
		},
	}
	require.Equal(t, 1, p.Get())
	require.Equal(t, 2, p.Get())

	p.Put(42)
	require.Equal(t, 42, p.Get())

	require.Equal(t, 3, p.Get())
}

func TestPoolCapacity(t *testing.T) {
	var p Pool
	for i := 0; i < 2*maxPoolCap; i++ {
		p.Put(i)
	}
	for i := 0; i < maxPoolCap; i++ {
		require.Equal(t, maxPoolCap+i, p.Get())
	}
	require.Nil(t, p.Get())
}

func TestPoolDoubleFree(t *testing.T) {
	var p Pool
	var x int

	// Non-pointer types are not checked against double-free.
	p.Put(x)
	p.Put(x)

	// Pointer types are checked against double-free.
	p.Put(&x)
	require.Panics(t, func() {
		p.Put(&x)
	})
}
