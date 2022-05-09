// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.
//

package skip

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRegistry(t *testing.T) {
	r := Registry{}
	require.NoError(t, r.Import(strings.NewReader(`
pkg/foo/bar.TestFoo flaky test



pkg/util.TestBar broken test
`)))

	{
		reason, ok := r.IsSkipped("pkg/foo/bar", "TestFoo")
		require.True(t, ok)
		require.Equal(t, "flaky test", reason)
	}

	{
		reason, ok := r.IsSkipped("pkg/util", "TestBar")
		require.True(t, ok)
		require.Equal(t, "broken test", reason)
	}

	{
		reason, ok := r.IsSkipped("pkg/util", "TestFoo")
		require.False(t, ok)
		require.Zero(t, reason)
	}

	{
		// NB: Registry doesn't try to reason about subtests. If a subtest is queried,
		// the caller is assumed to already have verified that the parent test is not
		// also skipped.
		reason, ok := r.IsSkipped("pkg/foo/bar", "TestFoo/TestGoo")
		require.False(t, ok)
		require.Zero(t, reason)
	}
}
