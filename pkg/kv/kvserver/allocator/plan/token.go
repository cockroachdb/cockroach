// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package plan

import (
	"context"
	"errors"

	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// AllocatorToken is a token which provides mutual exclusion for allocator
// execution. When the token is acquired, other acquirers will fail until
// release, but may register a callback which is called after release.
//
// The leaseholder replica should acquire an allocator token before beginning
// replica or lease changes on a range. After the changes have
// failed/succeeded, the token should be released. The goal is to limit the
// amount of concurrent reshuffling activity of a range.
type AllocatorToken struct {
	mu struct {
		syncutil.Mutex
		acquired       bool
		acquiredName   string
		onReleaseNamed []string
		onReleaseMap   map[string]func(context.Context)
	}
}

// Acquire tries to acquire the token, returning true if successful and false
// otherwise. When acquisition fails, the onFail callback will be registered
// and called upon release.
func (at *AllocatorToken) Acquire(
	ctx context.Context, name string, onFail func(context.Context),
) error {
	at.mu.Lock()
	defer at.mu.Unlock()

	if !at.mu.acquired {
		at.mu.onReleaseMap = map[string]func(context.Context){}
		at.mu.onReleaseNamed = nil
		at.mu.acquired = true
		at.mu.acquiredName = name
		log.KvDistribution.VEventf(ctx, 1, "acquired allocator token")
		return nil
	}

	// Add the onFail callback if the acquirer doesn't currently hold the token.
	// If the acquirer doesn't have any callbacks registered, then add them to
	// end of the list and update the associated callback.
	if onFail != nil && at.mu.acquiredName != name {
		if _, ok := at.mu.onReleaseMap[name]; !ok {
			at.mu.onReleaseNamed = append(at.mu.onReleaseNamed, name)
		}
		at.mu.onReleaseMap[name] = onFail
	}

	log.KvDistribution.VEventf(ctx,
		1, "failed to acquire allocator token, held by %v", at.mu.acquiredName)
	return ErrAllocatorToken
}

// Release releases the token and calls all callbacks registered by failed
// acquisitions.
func (at *AllocatorToken) Release(ctx context.Context) {
	onRelease := []func(context.Context){}
	at.mu.Lock()
	defer func() {
		at.mu.Unlock()
		// The onRelease callbacks are called outside of the mutex to avoid
		// deadlocking on the releaser.
		for _, f := range onRelease {
			f(ctx)
		}
	}()

	if !at.mu.acquired {
		panic("expected allocator token to be held before release")
	}

	// The registered onFail callbacks are cleared on the next acquisition, see
	// Acquire().
	for _, name := range at.mu.onReleaseNamed {
		onRelease = append(onRelease, at.mu.onReleaseMap[name])
	}
	at.mu.acquired = false
	log.KvDistribution.VEventf(ctx, 1, "released allocator token")
}

// ErrAllocatorToken occurs when the allocator token cannot be acquired.
var ErrAllocatorToken = errors.New(`failed to acquire allocator token`)
