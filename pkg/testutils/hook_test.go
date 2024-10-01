// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package testutils

import (
	"testing"

	"github.com/stretchr/testify/require"
)

var someFunc = func(n int) int {
	return n - 1
}

type someType struct {
	funcField func(int) int
}

func TestTestingHookWithGlobal(t *testing.T) {
	require.Equal(t, 9, someFunc(10))
	restoreHook := TestingHook(&someFunc, func(n int) int {
		return n + 1
	})
	require.Equal(t, 11, someFunc(10))
	restoreHook()
	require.Equal(t, 9, someFunc(10))
}

func TestTestingHookWithStruct(t *testing.T) {
	s := someType{
		funcField: func(n int) int {
			return n - 1
		},
	}
	require.Equal(t, 9, s.funcField(10))
	restoreHook := TestingHook(&s.funcField, func(n int) int {
		return n + 1
	})
	require.Equal(t, 11, s.funcField(10))
	restoreHook()
	require.Equal(t, 9, s.funcField(10))
}
