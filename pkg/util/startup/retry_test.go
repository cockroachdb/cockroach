// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package startup

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/stretchr/testify/require"
)

func TestRetryIsInterruptedByStopper(t *testing.T) {
	ctx := context.Background()
	quiesce := make(chan struct{})
	stop := make(chan struct{})
	go func() {
		_ = RunIdempotentWithRetry(ctx, quiesce, "trying hard",
			func(ctx context.Context) error {
				// Throttle tiny bit to avoid busy loop.
				<-time.After(time.Millisecond)
				return kvpb.NewError(kvpb.NewAmbiguousResultErrorf("test error")).GoError()
			})
		close(stop)
	}()
	close(quiesce)
	select {
	case <-stop:
	case <-time.After(30 * time.Second):
		t.Fatal("test retry didn't stop after 30 sec after quiesce")
	}
}

func TestRetryOnlyAmbiguousErrors(t *testing.T) {
	ctx := context.Background()
	stop := make(chan struct{})
	go func() {
		_ = RunIdempotentWithRetry(ctx, make(chan struct{}), "trying hard",
			func(ctx context.Context) error {
				// Throttle tiny bit to avoid busy loop.
				<-time.After(time.Millisecond)
				return errors.New("non retryable")
			})
		close(stop)
	}()
	select {
	case <-stop:
	case <-time.After(30 * time.Second):
		t.Fatal("retry didn't stop on non-retryable error")
	}
}

func TestRetry(t *testing.T) {
	ctx := context.Background()
	const tries = 2
	stop := make(chan int)
	go func() {
		count := 0
		_ = RunIdempotentWithRetry(ctx, make(chan struct{}), "trying hard",
			func(ctx context.Context) error {
				count++
				if count < tries {
					return kvpb.NewError(kvpb.NewAmbiguousResultErrorf("test error")).GoError()
				}
				return nil
			})
		stop <- count
	}()
	select {
	case count := <-stop:
		require.Equal(t, tries, count, "expected number of retries before success")
	case <-time.After(30 * time.Second):
		t.Fatal("retry didn't stop on non-retryable error")
	}
}
