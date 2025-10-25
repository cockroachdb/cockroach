// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package scheduler

import (
	"context"
	"errors"
	"testing"
	"time"

	mtasks "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/models/tasks"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/logger"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/stretchr/testify/require"
)

type recordingScheduler struct {
	mu      syncutil.Mutex
	calls   int
	errOnce error
	task    mtasks.ITask
	callCh  chan struct{}
}

func (r *recordingScheduler) ScheduleSyncTaskIfNeeded(
	context.Context, *logger.Logger,
) (mtasks.ITask, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls++
	if r.callCh != nil {
		select {
		case r.callCh <- struct{}{}:
		default:
		}
	}
	if r.errOnce != nil {
		err := r.errOnce
		r.errOnce = nil
		return nil, err
	}
	return r.task, nil
}

func TestStartPeriodicRefreshSchedulesAndStops(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	recordCh := make(chan struct{}, 4)
	s := &recordingScheduler{callCh: recordCh}
	errCh := make(chan error, 1)
	doneCh := make(chan struct{}, 1)

	StartPeriodicRefresh(ctx, logger.DefaultLogger, errCh, 5*time.Millisecond, s, func() {
		doneCh <- struct{}{}
	})

	require.Eventually(t, func() bool {
		select {
		case <-recordCh:
			return true
		default:
			return false
		}
	}, time.Second, 5*time.Millisecond)

	cancel()

	require.Eventually(t, func() bool {
		select {
		case <-doneCh:
			return true
		default:
			return false
		}
	}, time.Second, 5*time.Millisecond)

	select {
	case err := <-errCh:
		require.NoError(t, err)
	default:
	}

	s.mu.Lock()
	called := s.calls
	s.mu.Unlock()
	require.Greater(t, called, 0)
}

func TestStartPeriodicRefreshPropagatesError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	expectedErr := errors.New("boom")
	s := &recordingScheduler{errOnce: expectedErr}
	errCh := make(chan error, 1)

	StartPeriodicRefresh(ctx, logger.DefaultLogger, errCh, 5*time.Millisecond, s, nil)

	require.Eventually(t, func() bool {
		select {
		case err := <-errCh:
			require.ErrorIs(t, err, expectedErr)
			return true
		default:
			return false
		}
	}, time.Second, 5*time.Millisecond)
}
