// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package execinfrapb

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestTimeValue(t *testing.T) {
	// TODO(asubiotto): Is it worth it to introduce a testing interface and try
	//  reuse TestIntValue?
	var v TimeValue
	require.False(t, v.HasValue())
	require.Equal(t, time.Duration(0), v.Value())

	v.Set(0)
	require.True(t, v.HasValue())
	require.Equal(t, time.Duration(0), v.Value())

	v.Set(10)
	require.True(t, v.HasValue())
	require.Equal(t, time.Duration(10), v.Value())

	v.Add(100)
	require.True(t, v.HasValue())
	require.Equal(t, time.Duration(110), v.Value())

	v.Clear()
	require.False(t, v.HasValue())
	require.Equal(t, time.Duration(0), v.Value())

	v.Add(100)
	require.True(t, v.HasValue())
	require.Equal(t, time.Duration(100), v.Value())
}
