// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rel

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestOrdinalSetCapacity ensures that ordinalSet operations
func TestOrdinalSetCapacity(t *testing.T) {
	var os ordinalSet
	for o := ordinal(0); o <= ordinalSetMaxOrdinal; o++ {
		added := os.add(o)
		require.NotEqual(t, os, added)
		added = os
	}

	// Adding a value which is out-of-range should be a no-op.
	require.Equal(t, os, os.add(ordinalSetMaxOrdinal+1))
}
