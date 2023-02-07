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

	v, err := f.Get()
	require.NoError(t, err)
	require.Equal(t, "future is here", v)

	expectErr := errors.New("something bad happened")
	f = future.MakeErrFuture[string](expectErr)
	_, err = f.Get()
	require.ErrorIs(t, err, expectErr)
}

func TestPromise(t *testing.T) {
	defer leaktest.AfterTest(t)()
	f := future.MakePromise[int]()

	// More than one WhenReady handler may be added.
	ready1 := make(chan struct{})
	ready2 := make(chan struct{})
	f.WhenReadyAsync(func(v int, err error) {
		close(ready1)
	})
	f.WhenReady(func(v int, err error) { // Could have used Defer instead.
		close(ready2)
	})
	fErr := future.MakePromise[int]()

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
