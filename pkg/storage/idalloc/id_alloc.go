// Copyright 2014 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package idalloc

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/pkg/errors"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
)

// An Allocator is used to increment a key in allocation blocks
// of arbitrary size starting at a minimum ID.
//
// Note: if all you want is to increment a key and retry on retryable errors,
// see client.IncrementValRetryable().
type Allocator struct {
	log.AmbientContext

	idKey     atomic.Value
	db        *client.DB
	minID     uint32      // Minimum ID to return
	blockSize uint32      // Block allocation size
	ids       chan uint32 // Channel of available IDs
	stopper   *stop.Stopper
	once      sync.Once
}

// NewAllocator creates a new ID allocator which increments the
// specified key in allocation blocks of size blockSize, with
// allocated IDs starting at minID. Allocated IDs are positive
// integers.
func NewAllocator(
	ambient log.AmbientContext,
	idKey roachpb.Key,
	db *client.DB,
	minID uint32,
	blockSize uint32,
	stopper *stop.Stopper,
) (*Allocator, error) {
	// minID can't be the zero value because reads from closed channels return
	// the zero value.
	if minID == 0 {
		return nil, errors.Errorf("minID must be a positive integer: %d", minID)
	}
	if blockSize == 0 {
		return nil, errors.Errorf("blockSize must be a positive integer: %d", blockSize)
	}
	ia := &Allocator{
		AmbientContext: ambient,
		db:             db,
		minID:          minID,
		blockSize:      blockSize,
		ids:            make(chan uint32, blockSize/2+1),
		stopper:        stopper,
	}
	ia.idKey.Store(idKey)

	return ia, nil
}

// Allocate allocates a new ID from the global KV DB.
func (ia *Allocator) Allocate(ctx context.Context) (uint32, error) {
	ia.once.Do(ia.start)

	select {
	case id := <-ia.ids:
		// when the channel is closed, the zero value is returned.
		if id == 0 {
			return id, errors.Errorf("could not allocate ID; system is draining")
		}
		return id, nil
	case <-ctx.Done():
		return 0, ctx.Err()
	}
}

func (ia *Allocator) start() {
	ctx := ia.AnnotateCtx(context.Background())
	ia.stopper.RunWorker(ctx, func(ctx context.Context) {
		defer close(ia.ids)

		for {
			var newValue int64
			for newValue <= int64(ia.minID) {
				var err error
				var res client.KeyValue
				for r := retry.Start(base.DefaultRetryOptions()); r.Next(); {
					idKey := ia.idKey.Load().(roachpb.Key)
					if err := ia.stopper.RunTask(ctx, "storage.Allocator: allocating block", func(ctx context.Context) {
						res, err = ia.db.Inc(ctx, idKey, int64(ia.blockSize))
					}); err != nil {
						log.Warning(ctx, err)
						return
					}
					if err == nil {
						newValue = res.ValueInt()
						break
					}

					log.Warningf(ctx, "unable to allocate %d ids from %s: %s", ia.blockSize, idKey, err)
				}
				if err != nil {
					panic(fmt.Sprintf("unexpectedly exited id allocation retry loop: %s", err))
				}
			}

			end := newValue + 1
			start := end - int64(ia.blockSize)

			if start < int64(ia.minID) {
				start = int64(ia.minID)
			}

			// Add all new ids to the channel for consumption.
			for i := start; i < end; i++ {
				select {
				case ia.ids <- uint32(i):
				case <-ia.stopper.ShouldStop():
					return
				}
			}
		}
	})
}
