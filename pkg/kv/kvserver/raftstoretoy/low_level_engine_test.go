// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package raftstoretoy

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLowLevelEngine(t *testing.T) {
	ctx := context.Background()
	lle := &mockEngine{}

	k := func(s string) []byte {
		return []byte(s)
	}

	{
		b := lle.NewBatch()
		b.Put(ctx, k("hello"), []byte("version0"))
		require.NoError(t, b.Commit(true))
		b.Put(ctx, k("hello"), []byte("version1"))
		require.NoError(t, b.Commit(false))

		var buf strings.Builder
		require.NoError(t, lle.Dump(&buf))
		t.Log("\n" + buf.String())
	}

	require.NoError(t, lle.Flush())

	{
		b := lle.NewBatch()
		b.Put(ctx, k("hello"), []byte("version2"))
		b.Put(ctx, k("goodbye"), []byte("version2"))
		require.NoError(t, b.Commit(false))

		var buf strings.Builder
		require.NoError(t, lle.Dump(&buf))
		t.Log("\n" + buf.String())
	}
}
