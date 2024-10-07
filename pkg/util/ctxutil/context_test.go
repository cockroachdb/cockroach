// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package ctxutil

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestWhenDone(t *testing.T) {
	defer leaktest.AfterTest(t)()

	makeDoneWatcher := func(t *testing.T) (whenDone func(), waitDone func()) {
		done := make(chan struct{})
		return func() { close(done) }, func() {
			select {
			case <-done:
			case <-time.After(30 * time.Second):
				t.Fatal("timeout")
			}
		}
	}

	t.Run("whenDone", func(t *testing.T) {
		parent, cancelParent := context.WithCancel(context.Background())
		whenDone, waitDone := makeDoneWatcher(t)
		require.True(t, WhenDone(parent, whenDone))
		cancelParent()
		waitDone()
	})

	t.Run("whenDoneAlreadyCancelled", func(t *testing.T) {
		parent, cancelParent := context.WithCancel(context.Background())
		cancelParent() // immediately cancel parent.
		whenDone, waitDone := makeDoneWatcher(t)
		require.True(t, WhenDone(parent, whenDone))
		waitDone()
	})
}
