// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package execstatspb

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIntValue(t *testing.T) {
	var v IntValue
	require.False(t, v.HasValue())
	require.Equal(t, uint64(0), v.Value())

	v.Set(0)
	require.True(t, v.HasValue())
	require.Equal(t, uint64(0), v.Value())

	v.Set(10)
	require.True(t, v.HasValue())
	require.Equal(t, uint64(10), v.Value())

	v.Add(100)
	require.True(t, v.HasValue())
	require.Equal(t, uint64(110), v.Value())

	v.Clear()
	require.False(t, v.HasValue())
	require.Equal(t, uint64(0), v.Value())

	v.Add(100)
	require.True(t, v.HasValue())
	require.Equal(t, uint64(100), v.Value())
}
