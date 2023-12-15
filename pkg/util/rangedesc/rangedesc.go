// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rangedesc

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/iterutil"
	"github.com/cockroachdb/errors"
)

// Scanner paginates through range descriptors in the system.
type Scanner interface {
	// Scan paginates through range descriptors in the system that overlap
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
	Scan(
		ctx context.Context, pageSize int, init func(), span roachpb.Span,
		fn func(descriptors ...roachpb.RangeDescriptor) error,
	) error
}

// Iterator iterates through range descriptors.
type Iterator interface {
	// Valid returns whether the iterator is pointing at a valid element.
	Valid() bool
	// Next advances the iterator. Must not be called if Valid is false.
	Next()
	// CurRangeDescriptor returns the range descriptor the iterator is currently
	// pointing at. Must not be called if Valid is false.
	CurRangeDescriptor() roachpb.RangeDescriptor
}

type LazyIterator interface {
	Iterator
	Error() error
}

// IteratorFactory is used to construct Iterators over arbitrary spans.
type IteratorFactory interface {
	// NewIterator constructs an iterator to iterate over range descriptors for
	// ranges that overlap with the supplied span.
	// NB: While this looks like an iterator rather than batch API, note that the
	// constructor fetches the entirety of the result set and the returned iter is
	// merely iterating over that buffered result set, so this is actually better
	// treated as if it is a batch API, eg GetAllRangeDescs(span). Callers should
	// generally avoid this API and prefer NewLazyIterator which fetches as during
	// iteration instead. This eager version will be removed once all callers have
	// been extended to expect/check for errors during iteration.
	NewIterator(ctx context.Context, span roachpb.Span) (Iterator, error)
	// NewLazyIterator constructs an iterator to iterate over range descriptors
	// for ranges that overlap with the supplied span.
	NewLazyIterator(ctx context.Context, span roachpb.Span, pageSize int) (LazyIterator, error)
}

// DB is a database handle to a CRDB cluster.
type DB interface {
	Txn(ctx context.Context, retryable func(context.Context, *kv.Txn) error) error
}

// impl is a concrete (private) implementation of the Scanner interface. It also
// serves as the system tenant's implementation for the IteratorFactory
// interface.
type impl struct {
	db DB
}

// NewScanner returns a Scanner.
func NewScanner(db DB) Scanner {
	return &impl{db: db}
}

// NewIteratorFactory returns an IteratorFactory.
func NewIteratorFactory(db DB) IteratorFactory {
	return &impl{db: db}
}

var _ Scanner = &impl{}
var _ IteratorFactory = &impl{}

// Scan implements the Scanner interface.
func (i *impl) Scan(
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
		// at the relevant range meta key -- in meta1 if the start key sits
		// within meta2, in meta2 if the start key is an ordinary key.
		//
		// So what exactly is the "relevant range meta key"? Since keys in meta
		// ranges are encoded using the end keys of range descriptors, we're
		// looking for the lowest existing range meta key that's strictly
		// greater than RangeMetaKey(start key).
		rangeMetaKeyForStart := keys.RangeMetaKey(rspan.Key)
		metaScanBoundsForStart, err := keys.MetaScanBounds(rangeMetaKeyForStart)
		if err != nil {
			return err
		}
		metaScanStartKey := metaScanBoundsForStart.Key.AsRawKey()

		// Scan through meta{1,2} to pull out relevant range descriptors.
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

// NewIterator implements the IteratorFactory interface.
func (i *impl) NewIterator(ctx context.Context, span roachpb.Span) (Iterator, error) {
	rangeDescriptors, err := i.getPage(ctx, span, 0)
	return NewSliceIterator(rangeDescriptors), err
}

// NewLazyIterator implements the IteratorFactory interface.
func (i *impl) NewLazyIterator(ctx context.Context, span roachpb.Span, pageSize int) (LazyIterator, error) {
	return NewPaginatedIter(ctx, span, pageSize, i.getPage)
}

func (i *impl) getPage(
	ctx context.Context, span roachpb.Span, pageSize int,
) ([]roachpb.RangeDescriptor, error) {
	var rangeDescriptors []roachpb.RangeDescriptor
	err := iterutil.Map(i.Scan(ctx, pageSize, func() {
		rangeDescriptors = rangeDescriptors[:0] // retryable
	}, span, func(descriptors ...roachpb.RangeDescriptor) error {
		rangeDescriptors = append(rangeDescriptors, descriptors...)
		if len(rangeDescriptors) >= pageSize {
			return iterutil.StopIteration()
		}
		return nil
	}))
	if err != nil {
		return nil, err
	}
	return rangeDescriptors, nil
}

func NewSliceIterator(descs []roachpb.RangeDescriptor) Iterator {
	return &iterator{rangeDescs: descs}
}

// iterator is a concrete (private) implementation of the Iterator interface.
type iterator struct {
	rangeDescs []roachpb.RangeDescriptor
	curIdx     int
}

// Valid implements the Iterator interface.
func (i *iterator) Valid() bool {
	return i.curIdx < len(i.rangeDescs)
}

// Next implements the Iterator interface.
func (i *iterator) Next() {
	i.curIdx++
}

// CurRangeDescriptor implements the Iterator interface.
func (i *iterator) CurRangeDescriptor() roachpb.RangeDescriptor {
	return i.rangeDescs[i.curIdx]
}

// NewPaginatedIter returns a LazyIterator backed by the passed page fetch fn.
func NewPaginatedIter(
	ctx context.Context,
	span roachpb.Span,
	pageSize int,
	fn func(context.Context, roachpb.Span, int) ([]roachpb.RangeDescriptor, error),
) (LazyIterator, error) {
	it := &paginated{ctx: ctx, fetch: fn, pageSize: pageSize, span: span}
	it.fill()
	return it, it.Error()
}

type paginated struct {
	ctx      context.Context
	span     roachpb.Span
	curPage  []roachpb.RangeDescriptor
	curIdx   int
	fetch    func(context.Context, roachpb.Span, int) ([]roachpb.RangeDescriptor, error)
	pageSize int
	err      error
}

// Valid implements the LazyIterator interface.
func (i *paginated) Valid() bool {
	return i.err == nil && i.curIdx < len(i.curPage)
}

// Error implements the LazyIterator interface.
func (i *paginated) Error() error {
	return i.err
}

// Next implements the LazyIterator interface.
func (i *paginated) Next() {
	i.curIdx++
	if i.curIdx >= len(i.curPage) {
		i.fill()
	}
}

func (i *paginated) fill() {
	if len(i.curPage) > 0 {
		i.span.Key = i.curPage[len(i.curPage)-1].EndKey.AsRawKey()
	}
	if i.span.Valid() {
		i.curPage, i.err = i.fetch(i.ctx, i.span, i.pageSize)
		i.curIdx = 0
	}
}

// CurRangeDescriptor implements the LazyIterator interface.
func (i *paginated) CurRangeDescriptor() roachpb.RangeDescriptor {
	return i.curPage[i.curIdx]
}
