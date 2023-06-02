// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package ctxutil

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/logtags"
	"github.com/stretchr/testify/require"
)

func TestWhenDone(t *testing.T) {
	parent, cancelParent := context.WithCancel(context.Background())
	done := make(chan struct{})
	require.NoError(t, WhenDone(parent, func(err error) { close(done) }))
	cancelParent()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("timeout")
	}
}

func TestWhenDoneNested(t *testing.T) {
	var val1Key, val2Key int
	ctx := context.WithValue(context.Background(), &val1Key, 0)
	ctx = context.WithValue(ctx, &val2Key, 0)
	parent, cancelParent := context.WithCancel(ctx)
	defer cancelParent()
	ctx = logtags.AddTag(parent, "foo", "bar")

	done := make(chan struct{})
	require.NoError(t, WhenDone(ctx, func(err error) { close(done) }))
	cancelParent()

	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("timeout")
	}
}

func BenchmarkFastDoneChecker(b *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b.Run("err", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = ctx.Err()
		}
	})

	b.Run("fast", func(b *testing.B) {
		var f FastDoneCheckerContext
		f.Init(ctx)
		for i := 0; i < b.N; i++ {
			_ = f.ContextDone()
		}
	})
}
