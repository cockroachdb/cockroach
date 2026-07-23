// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/cancelchecker"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestVirtualTableGenerators(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	stopper := stop.NewStopper()
	ctx := context.Background()
	defer stopper.Stop(ctx)
	t.Run("test cleanup", func(t *testing.T) {
		worker := func(ctx context.Context, pusher rowPusher) error {
			if err := pusher.pushRow(tree.NewDInt(1)); err != nil {
				return err
			}
			if err := pusher.pushRow(tree.NewDInt(2)); err != nil {
				return err
			}
			return nil
		}
		next, cleanup, setupError := setupGenerator(ctx, worker, stopper)
		require.NoError(t, setupError)
		d, err := next()
		if err != nil {
			t.Fatal(err)
		}
		require.Equal(t, tree.Datums{tree.NewDInt(1)}, d)

		// Check that we can safely cleanup in the middle of execution.
		cleanup(ctx)
	})

	t.Run("test worker error", func(t *testing.T) {
		// Test that if the worker returns an error we catch it.
		worker := func(ctx context.Context, pusher rowPusher) error {
			if err := pusher.pushRow(tree.NewDInt(1)); err != nil {
				return err
			}
			if err := pusher.pushRow(tree.NewDInt(2)); err != nil {
				return err
			}
			return errors.New("dummy error")
		}
		next, cleanup, setupError := setupGenerator(ctx, worker, stopper)
		require.NoError(t, setupError)
		_, err := next()
		require.NoError(t, err)
		_, err = next()
		require.NoError(t, err)
		_, err = next()
		require.Error(t, err)
		cleanup(ctx)
	})

	t.Run("test no next", func(t *testing.T) {
		// Test we don't leak anything if we call cleanup before next.
		worker := func(ctx context.Context, pusher rowPusher) error {
			return nil
		}
		_, cleanup, setupError := setupGenerator(ctx, worker, stopper)
		require.NoError(t, setupError)
		cleanup(ctx)
	})

	t.Run("test context cancellation", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		// Test cancellation before asking for any rows.
		worker := func(ctx context.Context, pusher rowPusher) error {
			if err := pusher.pushRow(tree.NewDInt(1)); err != nil {
				return err
			}
			if err := pusher.pushRow(tree.NewDInt(2)); err != nil {
				return err
			}
			return nil
		}
		next, cleanup, setupError := setupGenerator(ctx, worker, stopper)
		require.NoError(t, setupError)
		cancel()
		_, err := next()
		// There is a small chance that we race and don't return
		// a query canceled here. So, only check the error if
		// it is non-nil.
		if err != nil {
			require.Equal(t, cancelchecker.QueryCanceledError, err)
		}
		cleanup(ctx)

		// Test cancellation after asking for a row.
		ctx, cancel = context.WithCancel(context.Background())
		next, cleanup, setupError = setupGenerator(ctx, worker, stopper)
		require.NoError(t, setupError)
		row, err := next()
		require.NoError(t, err)
		require.Equal(t, tree.Datums{tree.NewDInt(1)}, row)
		cancel()
		_, err = next()
		require.Equal(t, cancelchecker.QueryCanceledError, err)
		cleanup(ctx)

		// Test cancellation after asking for all the rows.
		ctx, cancel = context.WithCancel(context.Background())
		next, cleanup, setupError = setupGenerator(ctx, worker, stopper)
		require.NoError(t, setupError)
		_, err = next()
		require.NoError(t, err)
		_, err = next()
		require.NoError(t, err)
		cancel()
		cleanup(ctx)
	})

	t.Run("test worker panic", func(t *testing.T) {
		worker := func(ctx context.Context, pusher rowPusher) error {
			panic(errors.New("worker panic"))
		}
		next, cleanup, setupError := setupGenerator(ctx, worker, stopper)
		require.NoError(t, setupError)
		defer cleanup(ctx)
		_, err := next()
		require.Error(t, err)
		require.True(t, testutils.IsError(err, "worker panic"))
	})
}

func BenchmarkVirtualTableGenerators(b *testing.B) {
	defer leaktest.AfterTest(b)()
	defer log.Scope(b).Close(b)
	stopper := stop.NewStopper()
	ctx := context.Background()
	defer stopper.Stop(ctx)
	worker := func(ctx context.Context, pusher rowPusher) error {
		for {
			if err := pusher.pushRow(tree.NewDInt(tree.DInt(1))); err != nil {
				return err
			}
		}
	}
	b.Run("bench read", func(b *testing.B) {
		next, cleanup, setupError := setupGenerator(ctx, worker, stopper)
		require.NoError(b, setupError)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, err := next()
			require.NoError(b, err)
		}
		cleanup(ctx)
	})
}
