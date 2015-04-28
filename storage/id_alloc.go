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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package storage

import (
	"fmt"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/log"
)

// allocationTrigger is a special ID which if encountered,
// causes allocation of the next block of IDs.
const allocationTrigger = 0

// IDAllocationRetryOpts sets the retry options for handling RaftID
// allocation errors.
var IDAllocationRetryOpts = util.RetryOptions{
	Backoff:     50 * time.Millisecond,
	MaxBackoff:  5 * time.Second,
	Constant:    2,
	MaxAttempts: 0,
}

// An IDAllocator is used to increment a key in allocation blocks
// of arbitrary size starting at a minimum ID.
type IDAllocator struct {
	idKey     atomic.Value
	db        *client.KV
	minID     int64      // Minimum ID to return
	blockSize int64      // Block allocation size
	ids       chan int64 // Channel of available IDs
	closed    int32      // Atomically updated closed "bool"
	stopper   *util.Stopper
}

// NewIDAllocator creates a new ID allocator which increments the
// specified key in allocation blocks of size blockSize, with
// allocated IDs starting at minID. Allocated IDs are positive
// integers.
func NewIDAllocator(idKey proto.Key, db *client.KV, minID int64, blockSize int64,
	stopper *util.Stopper) (*IDAllocator, error) {
	if minID <= allocationTrigger {
		return nil, util.Errorf("minID must be > %d", allocationTrigger)
	}
	if blockSize < 1 {
		return nil, util.Errorf("blockSize must be a positive integer: %d", blockSize)
	}
	ia := &IDAllocator{
		db:        db,
		minID:     minID,
		blockSize: blockSize,
		ids:       make(chan int64, blockSize+blockSize/2+1),
		stopper:   stopper,
	}
	ia.idKey.Store(idKey)
	ia.ids <- allocationTrigger
	return ia, nil
}

// Allocate allocates a new ID from the global KV DB.
func (ia *IDAllocator) Allocate() (int64, error) {
	for {
		id := <-ia.ids
		if id == allocationTrigger {
			if !ia.stopper.StartTask() {
				if atomic.CompareAndSwapInt32(&ia.closed, 0, 1) {
					close(ia.ids)
				}
				return 0, util.Errorf("could not allocate ID; system is draining")
			}
			go func() {
				ia.allocateBlock(ia.blockSize)
				ia.stopper.FinishTask()
			}()
		} else {
			return id, nil
		}
	}
}

// allocateBlock allocates a block of IDs using db.Increment and
// sends all IDs on the ids channel. Midway through the block, a
// special allocationTrigger ID is inserted which causes allocation
// to occur before IDs run out to hide Increment latency.
func (ia *IDAllocator) allocateBlock(incr int64) {
	var ir *proto.IncrementResponse
	retryOpts := IDAllocationRetryOpts
	err := util.RetryWithBackoff(retryOpts, func() (util.RetryStatus, error) {
		idKey := ia.idKey.Load().(proto.Key)
		call := client.IncrementCall(idKey, incr)
		ir = call.Reply.(*proto.IncrementResponse)
		if err := ia.db.Run(call); err != nil {
			log.Warningf("unable to allocate %d ids from %s: %s", incr, ia.idKey, err)
			return util.RetryContinue, err
		}
		return util.RetryBreak, nil
	})
	if err != nil {
		panic(fmt.Sprintf("unexpectedly exited id allocation retry loop: %s", err))
	}

	if ir.NewValue <= ia.minID {
		log.Warningf("allocator key is currently set at %d; minID is %d; allocating again to skip %d IDs",
			ir.NewValue, ia.minID, ia.minID-ir.NewValue)
		ia.allocateBlock(ia.minID - ir.NewValue + ia.blockSize - 1)
		return
	}

	// Add all new ids to the channel for consumption.
	start := ir.NewValue - ia.blockSize + 1
	end := ir.NewValue + 1
	if start < ia.minID {
		start = ia.minID
	}

	for i := start; i < end; i++ {
		ia.ids <- i
		if i == (start+end)/2 {
			ia.ids <- allocationTrigger
		}
	}
}
