// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package ctxutil

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

var k1 = RegisterFastValueKey()
var k2 = RegisterFastValueKey()
var k3 = RegisterFastValueKey()

func TestFastValues(t *testing.T) {
	ctx := context.Background()

	c1 := WithFastValue(ctx, k1, "val1")
	require.Equal(t, "val1", FastValue(c1, k1))
	require.Nil(t, FastValue(c1, k2))

	c2 := WithFastValue(c1, k2, "val2")
	require.Equal(t, "val1", FastValue(c2, k1))
	require.Equal(t, "val2", FastValue(c2, k2))

	// Verify that the values are propagated through other contexts.
	c3, cancel := context.WithTimeout(c2, time.Hour)
	defer cancel()
	require.Equal(t, "val1", FastValue(c3, k1))
	require.Equal(t, "val2", FastValue(c3, k2))

	b := WithFastValues(c3)
	b.Set(k1, "val1-updated")
	b.Set(k3, "val3")
	c4 := b.Finish()
	require.Equal(t, "val1-updated", FastValue(c4, k1))
	require.Equal(t, "val2", FastValue(c4, k2))
	require.Equal(t, "val3", FastValue(c4, k3))

	c5 := WithFastValue(c4, k1, nil)
	require.Nil(t, FastValue(c5, k1))
	require.Equal(t, "val2", FastValue(c5, k2))
	require.Equal(t, "val3", FastValue(c5, k3))
}
