// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rangefeed

import (
	"context"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/queue"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/logtags"
)

// Scheduler is responsible for scheduling event processing for
// rangefeed registrations.
type Scheduler struct {
	wg         sync.WaitGroup
	numWorkers int
	metrics    *Metrics
	mu         struct {
		syncutil.Mutex
		cv   *sync.Cond
		stop bool
		q    queue.Queue[ioReq]
	}
}

// NewScheduler creates new scheduler.  Returned scheduler must be Start()ed.
func NewScheduler(numWorkers int, metrics *Metrics) *Scheduler {
	s := &Scheduler{numWorkers: numWorkers, metrics: metrics}
	s.mu.cv = sync.NewCond(&s.mu.Mutex)

	return s
}

// Start starts workers responsible for performing IO.
func (s *Scheduler) Start(ctx context.Context, stopper *stop.Stopper) {
	s.wg.Add(s.numWorkers)
	for i := 0; i < s.numWorkers; i++ {
		go func(idx int) {
			ctx = logtags.AddTag(ctx, "iow", idx)
			s.worker(ctx)
		}(i)
	}
	stopper.AddCloser(stop.CloserFn(func() {
		s.Stop()
	}))
}

// Stop signals workers to exit and waits for their termination.
func (s *Scheduler) Stop() {
	s.mu.Lock()
	s.mu.stop = true
	s.mu.cv.Broadcast()
	s.mu.q.Release()
	s.mu.Unlock()
	s.wg.Wait()
}

func (s *Scheduler) schedule(r *registration) {
	s.mu.Lock()
	s.mu.q.Push(ioReq{r: r, ts: timeutil.Now()})
	s.mu.cv.Broadcast()
	s.mu.Unlock()
}

func (s *Scheduler) worker(ctx context.Context) {
	defer s.wg.Done()

	for {
		var req ioReq
		s.mu.Lock()
		for {
			if s.mu.stop {
				s.mu.Unlock()
				return
			}

			var ok bool
			if req, ok = s.mu.q.PopFront(); ok {
				s.metrics.SchedulerQueueDepth.Update(int64(s.mu.q.Len()))
				break
			}
			s.mu.cv.Wait()
		}
		s.mu.Unlock()
		s.metrics.SchedulerBatchDelay.RecordValue(timeutil.Since(req.ts).Nanoseconds())

		if err := req.r.outputLoop(ctx); err != nil {
			req.r.disconnect(roachpb.NewError(err))
			req.r.onDisconnect()
		}
	}
}

type ioReq struct {
	r  *registration
	ts time.Time
}
