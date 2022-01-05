// Copyright 2017 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package backupccl

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// memoryAccumulator is a thin wrapper around a BoundAccount that only releases memory
// from the bound account when it is closed, otherwise it accumulates and
// re-uses resources.
// This is useful when resources, once accumulated should not be returned as
// they may be needed later to make progress.
// It is safe for concurrent use.
type memoryAccumulator struct {
	syncutil.Mutex
	ba       *mon.BoundAccount
	reserved int64
}

// newMemoryAccumulator creates a new accumulator backed by a bound account created
// from the given memory monitor.
func newMemoryAccumulator(mm *mon.BytesMonitor) *memoryAccumulator {
	if mm == nil {
		return &memoryAccumulator{}
	}
	ba := mm.MakeBoundAccount()
	return &memoryAccumulator{ba: &ba}
}

// request checks that the given number of bytes is available, requesting some
// from the backing monitor if necessary.
func (acc *memoryAccumulator) request(ctx context.Context, requested int64) error {
	acc.Lock()
	defer acc.Unlock()

	if acc.reserved >= requested {
		acc.reserved -= requested
		return nil
	}

	requested -= acc.reserved
	acc.reserved = 0

	return acc.ba.Grow(ctx, requested)
}

// release releases a number of bytes back into the internal reserved pool.
func (acc *memoryAccumulator) release(released int64) {
	acc.Lock()
	defer acc.Unlock()

	acc.reserved += released
}

// close returns all accumulated memory to the backing monitor.
func (acc *memoryAccumulator) close(ctx context.Context) {
	acc.Lock()
	defer acc.Unlock()

	acc.reserved = 0
	acc.ba.Close(ctx)
}
