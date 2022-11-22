// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package pprofutil

import (
	"context"
	"runtime/pprof"
	"testing"

	"github.com/cockroachdb/logtags"
	"github.com/stretchr/testify/require"
)

func TestSimple(t *testing.T) {
	ctx := context.Background()
	newCtx, undo := SetProfilerLabels(ctx, "label_key", "label_value")
	defer undo()

	require.NotEqual(t, context.Background(), newCtx)

	v, ok := pprof.Label(ctx, "label_key")
	require.False(t, ok)
	require.Equal(t, "", v)

	v, ok = pprof.Label(newCtx, "label_key")
	require.True(t, ok)
	require.Equal(t, "label_value", v)
}

func TestSimpleFromContext(t *testing.T) {
	ctx := logtags.AddTag(context.Background(), "tag_key", "tag_value")
	newCtx, undo := SetProfilerLabelsFromCtxTags(ctx)
	defer undo()

	require.NotEqual(t, context.Background(), newCtx)

	v, ok := pprof.Label(ctx, "tag_key")
	require.False(t, ok)
	require.Equal(t, "", v)

	v, ok = pprof.Label(newCtx, "tag_key")
	require.True(t, ok)
	require.Equal(t, "tag_value", v)
}

func TestNoTags(t *testing.T) {
	newCtx, undo := SetProfilerLabelsFromCtxTags(context.Background())
	defer undo()
	require.Equal(t, context.Background(), newCtx)
}
