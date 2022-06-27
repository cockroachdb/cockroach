// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
