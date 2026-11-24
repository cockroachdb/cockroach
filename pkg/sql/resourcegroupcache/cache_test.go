// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package resourcegroupcache

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestLookupByName(t *testing.T) {
	defer leaktest.AfterTest(t)()

	c := New()

	t.Run("hit", func(t *testing.T) {
		id, ok := c.LookupByName(CRDBInternalResourceGroupHigh)
		require.True(t, ok)
		require.Equal(t, uint64(1), id)

		id, ok = c.LookupByName(CRDBInternalResourceGroupLow)
		require.True(t, ok)
		require.Equal(t, uint64(2), id)
	})

	t.Run("miss", func(t *testing.T) {
		id, ok := c.LookupByName("does_not_exist")
		require.False(t, ok)
		require.Zero(t, id)

		id, ok = c.LookupByName("")
		require.False(t, ok)
		require.Zero(t, id)
	})

	t.Run("case sensitive", func(t *testing.T) {
		// Lookup is exact-match; a differently-cased name misses.
		_, ok := c.LookupByName("CRDB_INTERNAL_RESOURCE_GROUP_HIGH")
		require.False(t, ok)
	})
}
