// Copyright 2020-2021 Buf Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package thread

import (
	"context"
	"runtime"
	"sync"

	"go.uber.org/multierr"
)

var (
	globalParallelism = runtime.NumCPU()
	globalLock        sync.RWMutex
)

// Parallelism returns the current parellism.
//
// This defaults to the number of CPUs.
func Parallelism() int {
	globalLock.RLock()
	parallelism := globalParallelism
	globalLock.RUnlock()
	return parallelism
}

// SetParallelism sets the parallelism.
//
// If parallelism < 1, this sets the parallelism to 1.
func SetParallelism(parallelism int) {
	if parallelism < 1 {
		parallelism = 1
	}
	globalLock.Lock()
	globalParallelism = parallelism
	globalLock.Unlock()
}

// Parallelize runs the jobs in parallel.
//
// A max of Parallelism jobs will be run at once.
// Returns the combined error from the jobs.
func Parallelize(ctx context.Context, jobs []func(context.Context) error, options ...ParallelizeOption) error {
	parallelizeOptions := newParallelizeOptions()
	for _, option := range options {
		option(parallelizeOptions)
	}
	switch len(jobs) {
	case 0:
		return nil
	case 1:
		return jobs[0](ctx)
	default:
		multiplier := parallelizeOptions.multiplier
		if multiplier < 1 {
			multiplier = 1
		}
		semaphoreC := make(chan struct{}, Parallelism()*multiplier)
		var retErr error
		var wg sync.WaitGroup
		var lock sync.Mutex
		stop := false
		for _, job := range jobs {
			if stop {
				break
			}
			job := job
			// First, check if the context is done before even hitting a new job start.
			// We don't want to do the select with the semaphore as we want the context
			// being done to take precedence.
			select {
			case <-ctx.Done():
				stop = true
			default:
			}
			if stop {
				// We want to break the for loop without using labels/gotos but if
				// we put a break inside the select then it only breaks the select
				break
			}
			// Next, we still do a select between the context and the semaphore, so that
			// if we are blocking on the semaphore, and then the context is cancelled,
			// we will then end up breaking.
			select {
			case <-ctx.Done():
				stop = true
			case semaphoreC <- struct{}{}:
				wg.Add(1)
				go func() {
					if err := job(ctx); err != nil {
						lock.Lock()
						retErr = multierr.Append(retErr, err)
						lock.Unlock()
						if parallelizeOptions.cancel != nil {
							parallelizeOptions.cancel()
						}
					}
					// This will never block.
					<-semaphoreC
					wg.Done()
				}()
			}
		}
		wg.Wait()
		return retErr
	}
}

// ParallelizeOption is an option to Parallelize.
type ParallelizeOption func(*parallelizeOptions)

// ParallelizeWithMultiplier returns a new ParallelizeOption that will use a multiple
// of Parallelism() for the number of jobs that can be run at once.
//
// The default is to only do Parallelism() number of jobs.
// A multiplier of <1 has no meaning.
func ParallelizeWithMultiplier(multiplier int) ParallelizeOption {
	return func(parallelizeOptions *parallelizeOptions) {
		parallelizeOptions.multiplier = multiplier
	}
}

// ParallelizeWithCancel returns a new ParallelizeOption that will call the
// given context.CancelFunc if any job fails.
func ParallelizeWithCancel(cancel context.CancelFunc) ParallelizeOption {
	return func(parallelizeOptions *parallelizeOptions) {
		parallelizeOptions.cancel = cancel
	}
}

type parallelizeOptions struct {
	multiplier int
	cancel     context.CancelFunc
}

func newParallelizeOptions() *parallelizeOptions {
	return &parallelizeOptions{}
}
