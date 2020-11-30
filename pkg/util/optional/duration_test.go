// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package optional_test

import (
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/optional"
	"github.com/stretchr/testify/require"
)

func TestDuration(t *testing.T) {
	var v optional.Duration
	require.False(t, v.HasValue())
	require.Equal(t, time.Duration(0), v.Value())
	require.Equal(t, v.String(), "<unset>")

	v.Set(0)
	require.True(t, v.HasValue())
	require.Equal(t, time.Duration(0), v.Value())
	require.Equal(t, v.String(), "0s")

	v.Set(10)
	require.True(t, v.HasValue())
	require.Equal(t, time.Duration(10), v.Value())
	require.Equal(t, v.String(), "10ns")

	v.Add(100)
	require.True(t, v.HasValue())
	require.Equal(t, time.Duration(110), v.Value())
	require.Equal(t, v.String(), "110ns")

	v.Clear()
	require.False(t, v.HasValue())
	require.Equal(t, time.Duration(0), v.Value())
	require.Equal(t, v.String(), "<unset>")

	v.Add(100)
	require.True(t, v.HasValue())
	require.Equal(t, time.Duration(100), v.Value())
	require.Equal(t, v.String(), "100ns")
}
