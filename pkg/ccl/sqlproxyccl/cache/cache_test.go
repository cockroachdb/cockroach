// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package cache

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/stretchr/testify/require"
)

func TestCappedConnCache_Eviction(t *testing.T) {
	keys := []ConnKey{
		{TenantID: roachpb.MakeTenantID(1)},
		{TenantID: roachpb.MakeTenantID(2)},
		{TenantID: roachpb.MakeTenantID(3)},
	}
	newKey := ConnKey{TenantID: roachpb.MakeTenantID(4)}
	c := NewCappedConnCache(3).(*cappedConnCache)

	for _, key := range keys {
		c.Insert(&key)
		require.True(t, c.Exists(&key))
	}
	// Inserting a new key forces an existing key to be evicted to keep size at 3.
	c.Insert(&newKey)
	require.Equal(t, 3, len(c.mu.conns))
	// the new key should be in the cache.
	require.True(t, c.Exists(&newKey))

	var cnt int
	for _, key := range keys {
		if c.Exists(&key) {
			cnt++
		}
	}
	// One of the old keys should not be in the cache.
	require.Equal(t, 3-1, cnt)
}
