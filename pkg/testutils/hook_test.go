// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
