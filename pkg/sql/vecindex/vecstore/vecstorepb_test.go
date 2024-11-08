// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package vecstore

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestChildKey(t *testing.T) {
	childKey1 := ChildKey{PartitionKey: 10}
	childKey2 := ChildKey{PartitionKey: 20}
	childKey3 := ChildKey{ForeignKey: []byte{1, 2, 3}}
	childKey4 := ChildKey{ForeignKey: []byte{1, 10, 3}}
	childKey5 := ChildKey{PartitionKey: 10, ForeignKey: []byte{1, 10, 3}}

	// Equal method.
	require.True(t, childKey1.Equal(childKey1))
	require.False(t, childKey1.Equal(childKey2))
	require.True(t, childKey3.Equal(childKey3))
	require.False(t, childKey3.Equal(childKey4))
	require.False(t, childKey1.Equal(childKey5))
	require.False(t, childKey4.Equal(childKey5))
}
