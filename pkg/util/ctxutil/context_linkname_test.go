// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.
//
//go:build bazel || (gc && !go1.23)

package ctxutil

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestCanPropagateCancellation(t *testing.T) {
	defer leaktest.AfterTest(t)()

	t.Run("withCancel", func(t *testing.T) {
		parent, cancelParent := context.WithCancel(context.Background())
		defer cancelParent()
		require.True(t, CanDirectlyDetectCancellation(parent))
	})

	t.Run("withCancelEmbed", func(t *testing.T) {
		parent, cancelParent := context.WithCancel(context.Background())
		defer cancelParent()
		ctx := &myCtx{parent}
		require.True(t, CanDirectlyDetectCancellation(ctx))
	})

	t.Run("nested", func(t *testing.T) {
		parent, cancelParent := context.WithCancel(context.Background())
		defer cancelParent()
		timeoutCtx, cancelTimeout := context.WithTimeout(parent, time.Hour)
		defer cancelTimeout()
		ctx := &myCtx{timeoutCtx}
		require.True(t, CanDirectlyDetectCancellation(ctx))
	})

	t.Run("nonCancellable", func(t *testing.T) {
		require.False(t, CanDirectlyDetectCancellation(context.Background()))
	})

	t.Run("nonCancellableCustom", func(t *testing.T) {
		require.False(t, CanDirectlyDetectCancellation(&noOpCtx{}))
	})

	t.Run("nonCancellableCustomEmbed", func(t *testing.T) {
		require.False(t, CanDirectlyDetectCancellation(&myCtx{&noOpCtx{}}))
	})
}

type myCtx struct {
	context.Context
}

type noOpCtx struct{}

var _ context.Context = (*noOpCtx)(nil)

func (n *noOpCtx) Deadline() (deadline time.Time, ok bool) {
	return time.Time{}, false
}

func (n *noOpCtx) Done() <-chan struct{} {
	return nil
}

func (n *noOpCtx) Err() error {
	return nil
}

func (n *noOpCtx) Value(key any) any {
	return nil
}
