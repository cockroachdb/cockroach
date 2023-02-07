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

	"github.com/cockroachdb/cockroach/pkg/util/future"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestCompletedFuture(t *testing.T) {
	defer leaktest.AfterTest(t)()

	f := future.MakeCompletedFuture("future is here")

	require.Equal(t, "future is here", future.MakeAwaitableFuture(f).Get())

	expectErr := errors.New("something bad happened")
	ef := future.MakeCompletedErrorFuture(expectErr)
	require.ErrorIs(t, future.MakeAwaitableFuture(ef).Get(), expectErr)
}

func TestPromise(t *testing.T) {
	defer leaktest.AfterTest(t)()
	f := future.MakePromise[int]()

	// More than one WhenReady handler may be added.
	ready1 := make(chan struct{})
	ready2 := make(chan struct{})
	f.WhenReady(func(v int) { close(ready1) })
	f.WhenReady(func(v int) { close(ready2) })
	var fErr future.ErrorFuture

	var wg sync.WaitGroup
	wg.Add(1)
	defer wg.Wait()
	go func() {
		defer wg.Done()
		f.Set(42)
		fErr.Set(context.Canceled)
	}()

	require.Equal(t, 42, future.MakeAwaitableFuture(f).Get())
	<-ready1
	<-ready2

	require.ErrorIs(t, future.MakeAwaitableFuture(&fErr).Get(), context.Canceled)

	// Producing a value again is a no-op
	fErr.Set(errors.New("should be ignored"))
	require.ErrorIs(t, future.MakeAwaitableFuture(&fErr).Get(), context.Canceled)
}

func TestErrorFuture(t *testing.T) {
	defer leaktest.AfterTest(t)()

	f := future.MakeAwaitableFuture(future.MakePromise[error]())

	select {
	case <-f.Done():
		t.Fatal("surprisingly, the future is here")
	default:
	}

	errCh := make(chan error, 1)
	f.WhenReady(func(err error) {
		errCh <- err
	})
	expectErr := errors.Newf("test error")
	f.Set(expectErr)
	require.True(t, errors.Is(<-errCh, expectErr))

	errNow := future.MakeAwaitableFuture(future.MakeCompletedErrorFuture(expectErr))
	require.True(t, errors.Is(errNow.Get(), expectErr))

	// Once prepared with error value, setting another error
	// is a no-op.
	errNow.Set(errors.New("should be ignored"))
	require.True(t, errors.Is(errNow.Get(), expectErr))
}
