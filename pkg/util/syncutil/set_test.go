// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package syncutil

import (
	"cmp"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSetBasic(t *testing.T) {
	var s Set[int]

	require.Nil(t, asSlice(&s))
	require.False(t, s.Contains(1))
	require.False(t, s.Remove(1))

	require.True(t, s.Add(1))
	require.False(t, s.Add(1))
	require.True(t, s.Add(2))
	require.Equal(t, []int{1, 2}, asSlice(&s))
	require.True(t, s.Contains(1))
	require.True(t, s.Contains(2))
	require.False(t, s.Contains(3))

	require.True(t, s.Remove(1))
	require.False(t, s.Remove(1))
	require.False(t, s.Remove(3))
	require.Equal(t, []int{2}, asSlice(&s))
	require.False(t, s.Contains(1))
	require.True(t, s.Contains(2))
	require.False(t, s.Contains(3))
}

func asSlice[V cmp.Ordered](s *Set[V]) []V {
	var res []V
	s.Range(func(v V) bool {
		res = append(res, v)
		return true
	})
	slices.Sort(res)
	return res
}
