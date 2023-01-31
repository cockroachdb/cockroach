// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package grpcutil

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"
)

func TestFastFromIncomingContext(t *testing.T) {
	defer leaktest.AfterTest(t)()

	md := metadata.MD{"hello": []string{"world", "universe"}}

	ctx := metadata.NewIncomingContext(context.Background(), md)
	md2, ok := FastFromIncomingContext(ctx)
	require.True(t, ok)
	require.Equal(t, md2, md)

	ok, v := FastFirstValueFromIncomingContext(ctx, "hello")
	require.True(t, ok)
	require.Equal(t, v, "world")
}

func TestFastGetAndRemoveValue(t *testing.T) {
	md := metadata.MD{"hello": []string{"world", "universe"}}
	ctx := metadata.NewIncomingContext(context.Background(), md)

	found, _, newCtx := FastGetAndDeleteValueFromIncomingContext(ctx, "woo")
	require.False(t, found)
	require.Equal(t, ctx, newCtx)

	found, val, newCtx := FastGetAndDeleteValueFromIncomingContext(ctx, "hello")
	require.True(t, found)
	require.Equal(t, "world", val)

	found, _, newCtx2 := FastGetAndDeleteValueFromIncomingContext(newCtx, "hello")
	require.False(t, found)
	require.Equal(t, newCtx, newCtx2)
}

func BenchmarkFromIncomingContext(b *testing.B) {
	md := metadata.MD{"hello": []string{"world", "universe"}}
	ctx := metadata.NewIncomingContext(context.Background(), md)
	b.Run("stdlib", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, _ = metadata.FromIncomingContext(ctx)
		}
	})
	b.Run("fast", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, _ = FastFromIncomingContext(ctx)
		}
	})
}

func slowRemoveValueIfExists1(
	ctx context.Context, key string,
) (found bool, val string, newCtx context.Context) {
	vals := metadata.ValueFromIncomingContext(ctx, key)
	if len(vals) > 0 {
		md, _ := metadata.FromIncomingContext(ctx)
		delete(md, key)
		return true, vals[0], metadata.NewIncomingContext(ctx, md)
	}
	return false, "", ctx
}

func slowRemoveValueIfExists2(
	ctx context.Context, key string,
) (found bool, val string, newCtx context.Context) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return false, "", ctx
	}
	if v, ok := md[key]; ok {
		if len(v) > 0 {
			found = true
			val = v[0]
		}
		delete(md, key)
		return found, val, metadata.NewIncomingContext(ctx, md)
	}
	return false, "", ctx
}

func BenchmarkGetAndRemoveValue(b *testing.B) {
	md := metadata.MD{"hello": []string{"world", "universe"}}
	ctx := metadata.NewIncomingContext(context.Background(), md)

	b.Run("stdlib1/exists", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, _, _ = slowRemoveValueIfExists1(ctx, "hello")
		}
	})
	b.Run("stdlib2/exists", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, _, _ = slowRemoveValueIfExists1(ctx, "hello")
		}
	})
	b.Run("fast/exists", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, _, _ = FastGetAndDeleteValueFromIncomingContext(ctx, "hello")
		}
	})
	b.Run("stdlib1/notexists", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, _, _ = slowRemoveValueIfExists1(ctx, "foo")
		}
	})
	b.Run("stdlib2/notexists", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, _, _ = slowRemoveValueIfExists1(ctx, "foo")
		}
	})
	b.Run("fast/notexists", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, _, _ = FastGetAndDeleteValueFromIncomingContext(ctx, "foo")
		}
	})
}
