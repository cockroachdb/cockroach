// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tochar

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFormatCache(t *testing.T) {
	c := NewFormatCache(1)
	hh24 := parseFormat("HH24")

	// Cache is correctly populated.
	result := c.lookup("HH24")
	require.Equal(t, hh24, result)
	rInt, ok := c.mu.cache.Get("HH24")
	require.True(t, ok)
	require.Equal(t, hh24, rInt.([]formatNode))

	// Cache does not cache large format strings.
	longStr := strings.Repeat(",", maxCacheKeySize+1)
	longFmt := parseFormat(longStr)
	// result should still be the same.
	result = c.lookup(longStr)
	require.Equal(t, longFmt, result)
	// But the entry should not be in the cache.
	_, ok = c.mu.cache.Get(longStr)
	require.False(t, ok)
	_, ok = c.mu.cache.Get("HH24")
	require.True(t, ok)
}
