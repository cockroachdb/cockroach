// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package storeliveness

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

type callbackScheduler interface {
	registerCallback(debugName string, f func()) callbackSchedulerHandle
}

type callbackSchedulerHandle interface {
	runCallbackAfterDuration(d time.Duration)
	unregister()
}

type timerCallbackScheduler struct {
	stopper *stop.Stopper
}

func (tcs *timerCallbackScheduler) registerCallback(
	debugName string, f func(),
) callbackSchedulerHandle {
	h := &timerCallbackSchedulerHandle{
		unregisterCh: make(chan struct{}),
	}
	// Hack: if we don't reset the timer, the h.timer.C is a nil channel that
	// blocks forever.
	h.timer.Reset(365 * 24 * time.Hour)
	err := tcs.stopper.RunAsyncTask(context.Background(), debugName, func(_ context.Context) {
		for {
			select {
			case <-h.timer.C:
				h.timer.Read = true
				f()
			case <-tcs.stopper.ShouldQuiesce():
				return
			case <-h.unregisterCh:
				return
			}
		}
	})
	if err != nil {
		panic(err)
	}
	return h
}

type timerCallbackSchedulerHandle struct {
	timer        timeutil.Timer
	unregisterCh chan struct{}
}

func (h *timerCallbackSchedulerHandle) runCallbackAfterDuration(d time.Duration) {
	h.timer.Reset(d)
}

func (h *timerCallbackSchedulerHandle) unregister() {
	h.timer.Stop()
	close(h.unregisterCh)
}
