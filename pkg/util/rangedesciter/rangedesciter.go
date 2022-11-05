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
	"github.com/cockroachdb/cockroach/pkg/util/iterutil"
	"github.com/cockroachdb/errors"
)

// Iterator paginates through range descriptors in the system.
type Iterator interface {
	// Iterate paginates through range descriptors in the system that overlap
	// with the given span. When doing so it uses the given page size. It's
	// important to note that the closure is being executed in the context of a
	// distributed transaction that may be automatically retried. So something
	// like the following is an anti-pattern:
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
	//
	//
	// When the query span is something other than keys.EverythingSpan, the page
	// size is also approximately haw many extra keys/range descriptors we may
	// be reading. Callers are expected to pick a page size accordingly
	// (page sizes that are much larger than expected # of descriptors would
	// lead to wasted work).
	Iterate(
		ctx context.Context, pageSize int, init func(), span roachpb.Span,
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
	span roachpb.Span,
	fn func(descriptors ...roachpb.RangeDescriptor) error,
) error {
	rspan := roachpb.RSpan{
		Key:    keys.MustAddr(span.Key),
		EndKey: keys.MustAddr(span.EndKey),
	}

	return i.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		// Inform the caller that we're starting a fresh attempt to page in
		// range descriptors.
		init()

		// Bound the start key of the meta{1,2} scan as much as possible. If the
		// start key < keys.Meta1KeyMax (we're also interested in the meta1
		// range descriptor), start our scan at keys.MetaMin. If not, start it
		// at the relevant range meta key (in meta1 if the start key sits within
		// meta2, in meta2 if the start key is an ordinary key).
		metaScanStartKey := keys.MetaMin
		if rangeMetaKey := keys.RangeMetaKey(rspan.Key); !rangeMetaKey.Equal(roachpb.RKeyMin) {
			metaScanStartKey = rangeMetaKey.AsRawKey()
		}

		// Iterate through meta{1,2} to pull out relevant range descriptors.
		// We'll keep scanning until we've found a range descriptor outside the
		// scan of interest.
		var lastRangeIDInMeta1 roachpb.RangeID
		return iterutil.Map(txn.Iterate(ctx, metaScanStartKey, keys.MetaMax, pageSize,
			func(rows []kv.KeyValue) error {
				descriptors := make([]roachpb.RangeDescriptor, 0, len(rows))
				stopMetaIteration := false

				var desc roachpb.RangeDescriptor
				for _, row := range rows {
					if err := row.ValueProto(&desc); err != nil {
						return errors.Wrapf(err, "unable to unmarshal range descriptor from %s", row.Key)
					}

					// In small enough clusters, it's possible for the same
					// range descriptor to be stored in both meta1 and meta2.
					// This happens when some range spans both the meta and the
					// user keyspace. Consider when r1 is
					// [/Min, /System/NodeLiveness); we'll store the range
					// descriptor in both /Meta2/<r1.EndKey> and in
					// /Meta1/KeyMax[1].
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

					if desc.EndKey.Equal(rspan.Key) {
						// Keys in meta ranges are encoded using the end keys of
						// range descriptors. It's possible then the first
						// descriptor/key we read has the same (exclusive) end
						// key as our query start. We'll want to skip over this
						// descriptor.
						continue
					}
					if _, err := desc.KeySpan().Intersect(rspan); err != nil {
						// We're past the last range descriptor that overlaps
						// with the given span.
						stopMetaIteration = true
						break
					}

					// This descriptor's span intersects with our query span, so
					// collect it for the callback.
					descriptors = append(descriptors, desc)

					if keys.InMeta1(keys.RangeMetaKey(desc.StartKey)) {
						lastRangeIDInMeta1 = desc.RangeID
					}
				}

				if len(descriptors) != 0 {
					// Invoke fn with the current chunk (of size ~pageSize) of
					// range descriptors.
					if err := fn(descriptors...); err != nil {
						return err
					}
				}
				if stopMetaIteration {
					return iterutil.StopIteration() // we're done here
				}
				return nil
			}),
		)
	})
}
