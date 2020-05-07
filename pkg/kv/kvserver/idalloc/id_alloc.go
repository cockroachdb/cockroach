// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package idalloc

import (
	"context"
	"fmt"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/errors"
)

// DBIncrementer wraps a suitable subset of *kv.DB for use with an allocator.
func DBIncrementer(
	db interface {
		Inc(ctx context.Context, key interface{}, value int64) (kv.KeyValue, error)
	},
) Incrementer {
	return func(ctx context.Context, key roachpb.Key, inc int64) (int64, error) {
		res, err := db.Inc(ctx, key, inc)
		if err != nil {
			return 0, err
		}
		return res.Value.GetInt()
	}
}

// Incrementer abstracts over the database which holds the key counter.
type Incrementer func(_ context.Context, _ roachpb.Key, inc int64) (new int64, _ error)

// Options are the options passed to NewAllocator.
type Options struct {
	AmbientCtx  log.AmbientContext
	Key         roachpb.Key
	Incrementer Incrementer
	BlockSize   uint32
	Stopper     *stop.Stopper
}

// An Allocator is used to increment a key in allocation blocks of arbitrary
// size.
type Allocator struct {
	log.AmbientContext
	opts Options

	ids  chan uint32 // Channel of available IDs
	once sync.Once
}

// NewAllocator creates a new ID allocator which increments the specified key in
// allocation blocks of size blockSize. If the key exists, it's assumed to have
// an int value (and it needs to be positive since id 0 is a sentinel used
// internally by the allocator that can't be generated). The first value
// returned is the existing value + 1, or 1 if the key did not previously exist.
func NewAllocator(opts Options) (*Allocator, error) {
	if opts.BlockSize == 0 {
		return nil, errors.Errorf("blockSize must be a positive integer: %d", opts.BlockSize)
	}
	opts.AmbientCtx.AddLogTag("idalloc", nil)
	return &Allocator{
		AmbientContext: opts.AmbientCtx,
		opts:           opts,
		ids:            make(chan uint32, opts.BlockSize/2+1),
	}, nil
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
	ia.opts.Stopper.RunWorker(ctx, func(ctx context.Context) {
		defer close(ia.ids)

		for {
			var newValue int64
			var err error
			for r := retry.Start(base.DefaultRetryOptions()); r.Next(); {
				if stopperErr := ia.opts.Stopper.RunTask(ctx, "idalloc: allocating block",
					func(ctx context.Context) {
						newValue, err = ia.opts.Incrementer(ctx, ia.opts.Key, int64(ia.opts.BlockSize))
					}); stopperErr != nil {
					return
				}
				if err == nil {
					break
				}

				log.Warningf(
					ctx,
					"unable to allocate %d ids from %s: %+v",
					ia.opts.BlockSize,
					ia.opts.Key,
					err,
				)
			}
			if err != nil {
				panic(fmt.Sprintf("unexpectedly exited id allocation retry loop: %s", err))
			}

			end := newValue + 1
			start := end - int64(ia.opts.BlockSize)
			if start <= 0 {
				log.Fatalf(ctx, "allocator initialized with negative key")
			}

			// Add all new ids to the channel for consumption.
			for i := start; i < end; i++ {
				select {
				case ia.ids <- uint32(i):
				case <-ia.opts.Stopper.ShouldStop():
					return
				}
			}
		}
	})
}
