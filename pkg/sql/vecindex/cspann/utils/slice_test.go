// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package utils

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/stretchr/testify/require"
)

func TestReplaceWithLast(t *testing.T) {
	// Test basic replacement
	s := []int{1, 2, 3, 4}
	result := ReplaceWithLast(s, 1)
	require.Equal(t, []int{1, 4, 3}, result)

	// Test single element
	s = []int{1}
	result = ReplaceWithLast(s, 0)
	require.Empty(t, result)
}

func TestEnsureSliceLen(t *testing.T) {
	// Test growing slice
	s := make([]int, 2)
	result := EnsureSliceLen(s, 4)
	require.Equal(t, 4, len(result))
	require.Equal(t, 4, cap(result))

	// Test shrinking slice
	s = make([]int, 4)
	result = EnsureSliceLen(s, 2)
	require.Equal(t, 2, len(result))
	if buildutil.CrdbTestBuild {
		require.Equal(t, 2, cap(result))
	} else {
		require.Equal(t, 4, cap(result))
	}
}
