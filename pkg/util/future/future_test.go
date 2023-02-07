// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.
//

package future_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/contextutil"
	"github.com/cockroachdb/cockroach/pkg/util/future"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestSimpleFuture(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	f := future.Make(ctx,
		func(ctx context.Context) (string, error) { return "hello future", nil },
	)
	defer f.Join()

	v, err := f.Get()
	require.NoError(t, err)
	require.Equal(t, "hello future", v)

	f.WhenReady(func(v string, err error) {
		require.NoError(t, err)
		require.Equal(t, "hello future", v)
	})
}

func TestCompletedFuture(t *testing.T) {
	defer leaktest.AfterTest(t)()

	f := future.MakeCompletedFuture("future is here")
	defer f.Join()

	v, err := f.Get()
	require.NoError(t, err)
	require.Equal(t, "future is here", v)

	expectErr := errors.New("something bad happened")
	f = future.MakeErrFuture[string](expectErr)
	defer f.Join()
	_, err = f.Get()
	require.ErrorIs(t, err, expectErr)
}

func TestPropagatesCancellation(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	f := future.Make(ctx,
		func(ctx context.Context) (string, error) {
			<-ctx.Done()
			return "", ctx.Err()
		},
	)
	defer f.Join()

	awaitErr := contextutil.RunWithTimeout(ctx, "try-get", 500*time.Microsecond,
		func(ctx context.Context) error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-f.Done():
				_, err := f.Get()
				return err
			}
		})
	require.True(t, errors.HasType(awaitErr, (*contextutil.TimeoutError)(nil)))

	cancel()
	_, err := f.Get()
	require.True(t, errors.Is(err, context.Canceled))
}

// Verify panic in the future function are recovered as errors.
func TestKeepCalmAndCarryOn(t *testing.T) {
	defer leaktest.AfterTest(t)()

	var div int
	f := future.Make(context.Background(), func(ctx context.Context) (int, error) {
		return 1 / div, nil
	})
	defer f.Join()
	_, err := f.Get()
	require.EqualError(t, err, "panic recovered: runtime error: integer divide by zero")
}

func TestPromise(t *testing.T) {
	defer leaktest.AfterTest(t)()
	f := future.MakePromise[int]()
	defer f.Join() // theoretically, not needed; but let's keep it clean.

	// More than one WhenReady handler may be added.
	ready1 := make(chan struct{})
	ready2 := make(chan struct{})
	f.WhenReady(func(v int, err error) {
		close(ready1)
	})
	f.WhenReady(func(v int, err error) {
		close(ready2)
	})
	fErr := future.MakePromise[int]()
	defer fErr.Join()

	var wg sync.WaitGroup
	wg.Add(1)
	defer wg.Wait()
	go func() {
		defer wg.Done()
		f.SetValue(42)
		fErr.SetErr(context.Canceled)
	}()

	v, err := f.Get()
	require.NoError(t, err)
	require.Equal(t, 42, v)
	<-ready1
	<-ready2

	_, err = fErr.Get()
	require.ErrorIs(t, err, context.Canceled)

	// Producing a value again is a no-op
	fErr.SetValue(42)
	v, err = fErr.Get()
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 0, v)
}
