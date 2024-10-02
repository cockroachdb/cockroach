// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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

// TestFormatCacheLookupThreadSafe is non-deterministic. Flakes indicate that
// FormatCache.lookup is not thread safe.
// See https://github.com/cockroachdb/cockroach/issues/95424
func TestFormatCacheLookupThreadSafe(t *testing.T) {
	formats := []string{"HH12", "HH24", "MI"}
	c := NewFormatCache(len(formats) - 1)
	for i := 0; i < 100_000; i++ {
		go func(i int) {
			format := formats[i%len(formats)]
			c.lookup(format)
		}(i)
	}
}
