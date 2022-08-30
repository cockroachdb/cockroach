// Copyright 2017 Google LLC
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

package pubsub

import (
	"context"
	"sync/atomic"

	"golang.org/x/sync/semaphore"
)

// flowController implements flow control for Subscription.Receive.
type flowController struct {
	maxCount          int
	maxSize           int                 // max total size of messages
	semCount, semSize *semaphore.Weighted // enforces max number and size of messages
	// Number of calls to acquire - number of calls to release. This can go
	// negative if semCount == nil and a large acquire is followed by multiple
	// small releases.
	// Atomic.
	countRemaining int64
	// Number of outstanding bytes remaining. Atomic.
	bytesRemaining int64
}

// newFlowController creates a new flowController that ensures no more than
// maxCount messages or maxSize bytes are outstanding at once. If maxCount or
// maxSize is < 1, then an unlimited number of messages or bytes is permitted,
// respectively.
func newFlowController(maxCount, maxSize int) *flowController {
	fc := &flowController{
		maxCount: maxCount,
		maxSize:  maxSize,
		semCount: nil,
		semSize:  nil,
	}
	if maxCount > 0 {
		fc.semCount = semaphore.NewWeighted(int64(maxCount))
	}
	if maxSize > 0 {
		fc.semSize = semaphore.NewWeighted(int64(maxSize))
	}
	return fc
}

// acquire blocks until one message of size bytes can proceed or ctx is done.
// It returns nil in the first case, or ctx.Err() in the second.
//
// acquire allows large messages to proceed by treating a size greater than maxSize
// as if it were equal to maxSize.
func (f *flowController) acquire(ctx context.Context, size int) error {
	if f.semCount != nil {
		if err := f.semCount.Acquire(ctx, 1); err != nil {
			return err
		}
	}
	if f.semSize != nil {
		if err := f.semSize.Acquire(ctx, f.bound(size)); err != nil {
			if f.semCount != nil {
				f.semCount.Release(1)
			}
			return err
		}
	}
	outstandingMessages := atomic.AddInt64(&f.countRemaining, 1)
	recordStat(ctx, OutstandingMessages, outstandingMessages)
	outstandingBytes := atomic.AddInt64(&f.bytesRemaining, f.bound(size))
	recordStat(ctx, OutstandingBytes, outstandingBytes)

	return nil
}

// tryAcquire returns false if acquire would block. Otherwise, it behaves like
// acquire and returns true.
//
// tryAcquire allows large messages to proceed by treating a size greater than
// maxSize as if it were equal to maxSize.
func (f *flowController) tryAcquire(ctx context.Context, size int) bool {
	if f.semCount != nil {
		if !f.semCount.TryAcquire(1) {
			return false
		}
	}
	if f.semSize != nil {
		if !f.semSize.TryAcquire(f.bound(size)) {
			if f.semCount != nil {
				f.semCount.Release(1)
			}
			return false
		}
	}
	outstandingMessages := atomic.AddInt64(&f.countRemaining, 1)
	recordStat(ctx, OutstandingMessages, outstandingMessages)
	outstandingBytes := atomic.AddInt64(&f.bytesRemaining, f.bound(size))
	recordStat(ctx, OutstandingBytes, outstandingBytes)

	return true
}

// release notes that one message of size bytes is no longer outstanding.
func (f *flowController) release(ctx context.Context, size int) {
	outstandingMessages := atomic.AddInt64(&f.countRemaining, -1)
	recordStat(ctx, OutstandingMessages, outstandingMessages)
	outstandingBytes := atomic.AddInt64(&f.bytesRemaining, -1*f.bound(size))
	recordStat(ctx, OutstandingBytes, outstandingBytes)

	if f.semCount != nil {
		f.semCount.Release(1)
	}
	if f.semSize != nil {
		f.semSize.Release(f.bound(size))
	}
}

func (f *flowController) bound(size int) int64 {
	if size > f.maxSize {
		return int64(f.maxSize)
	}
	return int64(size)
}

func (f *flowController) count() int {
	return int(atomic.LoadInt64(&f.countRemaining))
}
