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
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package storage

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/retry"
	"github.com/cockroachdb/cockroach/util/stop"
)

// idAllocationRetryOpts sets the retry options for handling RangeID
// allocation errors.
var idAllocationRetryOpts = retry.Options{
	InitialBackoff: 50 * time.Millisecond,
	MaxBackoff:     5 * time.Second,
	Multiplier:     2,
}

// An idAllocator is used to increment a key in allocation blocks
// of arbitrary size starting at a minimum ID.
type idAllocator struct {
	idKey     atomic.Value
	db        *client.DB
	minID     uint32      // Minimum ID to return
	blockSize uint32      // Block allocation size
	ids       chan uint32 // Channel of available IDs
	stopper   *stop.Stopper
	once      sync.Once
}

// newIDAllocator creates a new ID allocator which increments the
// specified key in allocation blocks of size blockSize, with
// allocated IDs starting at minID. Allocated IDs are positive
// integers.
func newIDAllocator(
	idKey roachpb.Key, db *client.DB, minID uint32, blockSize uint32, stopper *stop.Stopper,
) (*idAllocator, error) {
	// minID can't be the zero value because reads from closed channels return
	// the zero value.
	if minID == 0 {
		return nil, util.Errorf("minID must be a positive integer: %d", minID)
	}
	if blockSize == 0 {
		return nil, util.Errorf("blockSize must be a positive integer: %d", blockSize)
	}
	ia := &idAllocator{
		db:        db,
		minID:     minID,
		blockSize: blockSize,
		ids:       make(chan uint32, blockSize/2+1),
		stopper:   stopper,
	}
	ia.idKey.Store(idKey)

	return ia, nil
}

// Allocate allocates a new ID from the global KV DB.
func (ia *idAllocator) Allocate() (uint32, error) {
	ia.once.Do(ia.start)

	id := <-ia.ids
	// when the channel is closed, the zero value is returned.
	if id == 0 {
		return id, util.Errorf("could not allocate ID; system is draining")
	}
	return id, nil
}

func (ia *idAllocator) start() {
	ia.stopper.RunWorker(func() {
		defer close(ia.ids)

		for {
			var newValue int64
			for newValue <= int64(ia.minID) {
				var (
					pErr *roachpb.Error
					res  client.KeyValue
				)
				for r := retry.Start(idAllocationRetryOpts); r.Next(); {
					idKey := ia.idKey.Load().(roachpb.Key)
					if !ia.stopper.RunTask(func() {
						res, pErr = ia.db.Inc(idKey, int64(ia.blockSize))
					}) {
						return
					}
					if pErr == nil {
						newValue = res.ValueInt()
						break
					}

					log.Warningf("unable to allocate %d ids from %s: %s", ia.blockSize, idKey, pErr)
				}
				if pErr != nil {
					panic(fmt.Sprintf("unexpectedly exited id allocation retry loop: %s", pErr))
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
