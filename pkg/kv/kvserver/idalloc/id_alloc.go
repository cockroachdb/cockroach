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
	"sync"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/errors"
)

// Incrementer abstracts over the database which holds the key counter.
type Incrementer func(_ context.Context, _ roachpb.Key, inc int64) (updated int64, _ error)

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

// Options are the options passed to NewAllocator.
type Options struct {
	AmbientCtx  log.AmbientContext
	Key         roachpb.Key
	Incrementer Incrementer
	BlockSize   int64
	Stopper     *stop.Stopper
	Fatalf      func(context.Context, string, ...interface{}) // defaults to log.Fatalf
}

// An Allocator is used to increment a key in allocation blocks of arbitrary
// size.
type Allocator struct {
	log.AmbientContext
	opts Options

	ids  chan int64 // Channel of available IDs
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
	if opts.Fatalf == nil {
		opts.Fatalf = log.Fatalf
	}
	opts.AmbientCtx.AddLogTag("idalloc", nil)
	return &Allocator{
		AmbientContext: opts.AmbientCtx,
		opts:           opts,
		ids:            make(chan int64, opts.BlockSize/2+1),
	}, nil
}

// Allocate allocates a new ID from the global KV DB.
func (ia *Allocator) Allocate(ctx context.Context) (int64, error) {
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
	if err := ia.opts.Stopper.RunAsyncTask(ctx, "id-alloc", func(ctx context.Context) {
		defer close(ia.ids)

		var prevValue int64 // for assertions
		for {
			var newValue int64
			var err error
			for r := retry.Start(base.DefaultRetryOptions()); r.Next(); {
				if stopperErr := ia.opts.Stopper.RunTask(ctx, "idalloc: allocating block",
					func(ctx context.Context) {
						newValue, err = ia.opts.Incrementer(ctx, ia.opts.Key, ia.opts.BlockSize)
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
				ia.opts.Fatalf(ctx, "unexpectedly exited id allocation retry loop: %s", err)
				return
			}
			if prevValue != 0 && newValue < prevValue+ia.opts.BlockSize {
				ia.opts.Fatalf(
					ctx,
					"counter corrupt: incremented to %d, expected at least %d + %d",
					newValue, prevValue, ia.opts.BlockSize,
				)
				return
			}

			end := newValue + 1
			start := end - ia.opts.BlockSize
			if start <= 0 {
				ia.opts.Fatalf(ctx, "allocator initialized with negative key")
				return
			}
			prevValue = newValue

			// Add all new ids to the channel for consumption.
			for i := start; i < end; i++ {
				select {
				case ia.ids <- i:
				case <-ia.opts.Stopper.ShouldQuiesce():
					return
				}
			}
		}
	}); err != nil {
		close(ia.ids)
	}
}
