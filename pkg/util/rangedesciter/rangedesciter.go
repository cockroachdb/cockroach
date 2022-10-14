// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rangedesciter

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/errors"
)

// Iterator paginates through every range descriptor in the system.
type Iterator interface {
	// Iterate paginates through range descriptors in the system using the given
	// page size. It's important to note that the closure is being executed in
	// the context of a distributed transaction that may be automatically
	// retried. So something like the following is an anti-pattern:
	//
	//     processed := 0
	//     _ = rdi.Iterate(...,
	//         func(descriptors ...roachpb.RangeDescriptor) error {
	//             processed += len(descriptors) // we'll over count if retried
	//             log.Infof(ctx, "processed %d ranges", processed)
	//         },
	//     )
	//
	// Instead we allow callers to pass in a callback to signal on every attempt
	// (including the first). This lets us salvage the example above:
	//
	//     var processed int
	//     init := func() { processed = 0 }
	//     _ = rdi.Iterate(..., init,
	//         func(descriptors ...roachpb.RangeDescriptor) error {
	//             processed += len(descriptors)
	//             log.Infof(ctx, "processed %d ranges", processed)
	//         },
	//     )
	Iterate(
		ctx context.Context, pageSize int, init func(),
		fn func(descriptors ...roachpb.RangeDescriptor) error,
	) error
}

// DB is a database handle to a CRDB cluster.
type DB interface {
	Txn(ctx context.Context, retryable func(context.Context, *kv.Txn) error) error
}

// iteratorImpl is a concrete (private) implementation of the Iterator
// interface.
type iteratorImpl struct {
	db DB
}

// New returns an Iterator.
func New(db DB) Iterator {
	return &iteratorImpl{db: db}
}

var _ Iterator = &iteratorImpl{}

// Iterate implements the Iterator interface.
func (i *iteratorImpl) Iterate(
	ctx context.Context,
	pageSize int,
	init func(),
	fn func(descriptors ...roachpb.RangeDescriptor) error,
) error {
	return i.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		// Inform the caller that we're starting a fresh attempt to page in
		// range descriptors.
		init()

		// Iterate through meta{1,2} to pull out all the range descriptors.
		var lastRangeIDInMeta1 roachpb.RangeID
		return txn.Iterate(ctx, keys.MetaMin, keys.MetaMax, pageSize,
			func(rows []kv.KeyValue) error {
				descriptors := make([]roachpb.RangeDescriptor, 0, len(rows))
				var desc roachpb.RangeDescriptor
				for _, row := range rows {
					err := row.ValueProto(&desc)
					if err != nil {
						return errors.Wrapf(err, "unable to unmarshal range descriptor from %s", row.Key)
					}

					// In small enough clusters it's possible for the same range
					// descriptor to be stored in both meta1 and meta2. This
					// happens when some range spans both the meta and the user
					// keyspace. Consider when r1 is [/Min,
					// /System/NodeLiveness); we'll store the range descriptor
					// in both /Meta2/<r1.EndKey> and in /Meta1/KeyMax[1].
					//
					// As part of iterator we'll de-duplicate this descriptor
					// away by checking whether we've seen it before in meta1.
					// Since we're scanning over the meta range in sorted
					// order, it's enough to check against the last range
					// descriptor we've seen in meta1.
					//
					// [1]: See kvserver.rangeAddressing.
					if desc.RangeID == lastRangeIDInMeta1 {
						continue
					}

					descriptors = append(descriptors, desc)
					if keys.InMeta1(keys.RangeMetaKey(desc.StartKey)) {
						lastRangeIDInMeta1 = desc.RangeID
					}
				}

				// Invoke fn with the current chunk (of size ~blockSize) of
				// range descriptors.
				return fn(descriptors...)
			})
	})
}
