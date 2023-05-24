// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.
//

package concurrent

import (
	"context"
	"fmt"
	"runtime/pprof"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/util/queue"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// Executor is an interface describing asynchronous
// function execution -- much like go keyword.
// Executor does not provide any execution order guarantees.
type Executor interface {
	// GoCtx executes provided function with the specified context.
	GoCtx(ctx context.Context, f func(ctx context.Context))
}

// DefaultExecutor is the default executor which simply
// executes functions on a new go routine.
var DefaultExecutor = sysExecutor{}

// Option is an Executor configuration option.
type Option interface {
	apply(cfg *config)
}

// NewWorkQueue returns an Executor that queues work items.
//
// Work queue is suitable in cases where the caller has potentially many
// blocking operations that it wants to perform asynchronously.
// If this sounds like a "go func(){...}" -- it is.  The problem here is that
// if the number of such go functions is very large, and happen in bursts,
// the creation of many go routines will put load on Go runtime, and this
// in turn causes elevated latencies for other go routines (i.e there is
// an impact on foreground SQL latency).
//
// To address unbounded burst of goroutine creation, we could instead utilize
// fixed number of workers to bound the parallelism.
//
// Work queue implements a standard Go fan-out pattern. It differs from the
// standard implementation in that it does not rely on the bounded buffered
// channels to submit the work to the executors. Instead, it has a queue, which
// fronts one or more executor workers.
//
// If the number of workers is not set, defaults to a single worker.
//
// All closures submitted to this queue will execute at some point. However, if
// the stopper is signaled to quiesce, those closures will execute with their
// context canceled.
func NewWorkQueue(
	ctx context.Context, name string, stopper *stop.Stopper, opts ...Option,
) (Executor, error) {
	var cfg config
	for _, o := range opts {
		o.apply(&cfg)
	}

	if cfg.numWorkers <= 0 {
		cfg.numWorkers = 1
	}

	wq := workQueue{withCancelOnQuiesce: stopper.WithCancelOnQuiesce}
	wq.mu.cv = sync.NewCond(&wq.mu.Mutex)
	wq.wg.Add(cfg.numWorkers)

	for i := 0; i < cfg.numWorkers; i++ {
		workerName := fmt.Sprintf("wq-%s-%d", name, i)
		if err := stopper.RunAsyncTask(ctx, workerName, func(ctx context.Context) {
			defer pprof.SetGoroutineLabels(ctx)
			ctx = pprof.WithLabels(ctx, pprof.Labels("name", workerName))
			pprof.SetGoroutineLabels(ctx)
			wq.worker(ctx)
		}); err != nil {
			return nil, err
		}
	}

	stopperName := fmt.Sprintf("wq-%s-stopper", name)
	if err := stopper.RunAsyncTask(ctx, stopperName, func(ctx context.Context) {
		defer pprof.SetGoroutineLabels(ctx)
		pprof.SetGoroutineLabels(pprof.WithLabels(ctx, pprof.Labels("name", stopperName)))

		<-stopper.ShouldQuiesce()
		wq.stop()
	}); err != nil {
		return nil, err
	}
	return &wq, nil
}

// WithNumWorkers returns an option to configure the number of workers used to
// process callbacks.
func WithNumWorkers(n int) Option {
	return funcOpt(func(cfg *config) {
		cfg.numWorkers = n
	})
}

type workQueue struct {
	wg                  sync.WaitGroup
	withCancelOnQuiesce func(ctx context.Context) (context.Context, func())

	mu struct {
		syncutil.Mutex
		cv   *sync.Cond
		stop bool
		work queue.Queue[func()]
	}
}

// GoCtx implements Executor interface.
func (q *workQueue) GoCtx(ctx context.Context, f func(ctx context.Context)) {
	ctx, _ = q.withCancelOnQuiesce(ctx)
	q.mu.Lock()
	q.mu.work.Push(func() { f(ctx) })
	q.mu.cv.Signal()
	q.mu.Unlock()
}

func (q *workQueue) stop() {
	q.mu.Lock()
	q.mu.work.Pop()
	q.mu.stop = true
	q.mu.cv.Broadcast()
	q.mu.Unlock()
	q.wg.Wait()
}

func (q *workQueue) worker(ctx context.Context) {
	defer q.wg.Done()

	for {
		q.mu.Lock()
		var fn func()
		for fn == nil {
			var ok bool
			fn, ok = q.mu.work.PopFront()
			if ok {
				break
			}
			if q.mu.stop {
				q.mu.Unlock()
				return
			}
			q.mu.cv.Wait()
		}
		q.mu.Unlock()

		fn()
	}
}

type funcOpt func(cfg *config)

func (f funcOpt) apply(cfg *config) {
	f(cfg)
}

type config struct {
	numWorkers int
}

type sysExecutor struct{}

// GoCtx implements Executor interface.
func (sysExecutor) GoCtx(ctx context.Context, f func(ctx context.Context)) {
	go func() { f(ctx) }()
}
