// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package syncutil

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestCondMutex(t *testing.T) {
	alwaysTrue := func() bool { return true }

	t.Run("zero initialized", func(t *testing.T) {
		var mu CondMutex
		mu.Lock()
		_ = 1 // defeat empty critical section warning.
		mu.Unlock()
		mu.LockWhen(alwaysTrue)
		mu.Unlock()
	})

	t.Run("timeout", func(t *testing.T) {
		mu := struct {
			CondMutex
			state bool
		}{}

		lockResult := make(chan error, 1)
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
			defer cancel()
			lockResult <- mu.LockWhenCtx(ctx, func() bool { return mu.state })
		}()

		select {
		case err := <-lockResult:
			require.True(t, errors.Is(err, context.DeadlineExceeded))
		case <-time.After(30 * time.Second):
			t.Fatalf("timeout reading lock result")
		}
	})

	t.Run("timeout same thread", func(t *testing.T) {
		mu := struct {
			CondMutex
			state bool
		}{}
		// This is a silly test where we try to lock twice; but the second attempt should time out.
		mu.Lock()
		defer mu.Unlock()

		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()
		require.True(t, errors.Is(context.DeadlineExceeded, mu.LockCtx(ctx)))
	})
}
