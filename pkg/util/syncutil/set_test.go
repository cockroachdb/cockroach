// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package syncutil

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func (s *Set[V]) asSlice() []V {
	var res []V
	s.Range(func(v V) bool {
		res = append(res, v)
		return true
	})
	return res
}

func TestSetBasic(t *testing.T) {
	var s Set[int]

	require.Nil(t, s.asSlice())
	require.False(t, s.Contains(1))
	require.False(t, s.Remove(1))

	require.True(t, s.Add(1))
	require.False(t, s.Add(1))
	require.True(t, s.Add(2))
	require.ElementsMatch(t, []int{1, 2}, s.asSlice())
	require.True(t, s.Contains(1))
	require.True(t, s.Contains(2))
	require.False(t, s.Contains(3))

	require.True(t, s.Remove(1))
	require.False(t, s.Remove(1))
	require.False(t, s.Remove(3))
	require.ElementsMatch(t, []int{2}, s.asSlice())
	require.False(t, s.Contains(1))
	require.True(t, s.Contains(2))
	require.False(t, s.Contains(3))
}

func TestSetPointerValue(t *testing.T) {
	var s Set[*int]
	ptr1, ptr2, ptr3 := new(int), new(int), new(int)

	require.Nil(t, s.asSlice())
	require.False(t, s.Contains(nil))
	require.False(t, s.Contains(ptr1))
	require.False(t, s.Remove(nil))
	require.False(t, s.Remove(ptr1))

	require.True(t, s.Add(nil))
	require.False(t, s.Add(nil))
	require.True(t, s.Add(ptr1))
	require.False(t, s.Add(ptr1))
	require.True(t, s.Add(ptr2))
	require.ElementsMatch(t, []*int{nil, ptr1, ptr2}, s.asSlice())
	require.True(t, s.Contains(nil))
	require.True(t, s.Contains(ptr1))
	require.True(t, s.Contains(ptr2))
	require.False(t, s.Contains(ptr3))

	require.True(t, s.Remove(nil))
	require.False(t, s.Remove(nil))
	require.True(t, s.Remove(ptr1))
	require.False(t, s.Remove(ptr1))
	require.False(t, s.Remove(ptr3))
	require.ElementsMatch(t, []*int{ptr2}, s.asSlice())
	require.False(t, s.Contains(nil))
	require.False(t, s.Contains(ptr1))
	require.True(t, s.Contains(ptr2))
	require.False(t, s.Contains(ptr3))
}
