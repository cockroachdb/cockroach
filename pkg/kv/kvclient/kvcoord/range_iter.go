// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kv

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
)

// A RangeIterator provides a mechanism for iterating over all ranges
// in a key span. A new RangeIterator must be positioned with Seek()
// to begin iteration.
//
// RangeIterator is not thread-safe.
type RangeIterator struct {
	ds      *DistSender
	scanDir ScanDirection
	key     roachpb.RKey
	desc    *roachpb.RangeDescriptor
	token   *EvictionToken
	init    bool
	pErr    *roachpb.Error
}

// RangeIteratorGen is a generator of RangeIterators.
type RangeIteratorGen func() *RangeIterator

// NewRangeIterator creates a new RangeIterator.
func NewRangeIterator(ds *DistSender) *RangeIterator {
	return &RangeIterator{
		ds: ds,
	}
}

// ScanDirection determines the semantics of RangeIterator.Next() and
// RangeIterator.NeedAnother().
type ScanDirection byte

const (
	// Ascending means Next() will advance towards keys that compare higher.
	Ascending ScanDirection = iota
	// Descending means Next() will advance towards keys that compare lower.
	Descending
)

// Key returns the current key. The iterator must be valid.
func (ri *RangeIterator) Key() roachpb.RKey {
	if !ri.Valid() {
		panic(ri.Error())
	}
	return ri.key
}

// Desc returns the descriptor of the range at which the iterator is
// currently positioned. The iterator must be valid.
func (ri *RangeIterator) Desc() *roachpb.RangeDescriptor {
	if !ri.Valid() {
		panic(ri.Error())
	}
	return ri.desc
}

// Token returns the eviction token corresponding to the range
// descriptor for the current iteration. The iterator must be valid.
func (ri *RangeIterator) Token() *EvictionToken {
	if !ri.Valid() {
		panic(ri.Error())
	}
	return ri.token
}

// NeedAnother checks whether the iteration needs to continue to cover
// the remainder of the ranges described by the supplied key span. The
// iterator must be valid.
func (ri *RangeIterator) NeedAnother(rs roachpb.RSpan) bool {
	if !ri.Valid() {
		panic(ri.Error())
	}
	if rs.EndKey == nil {
		panic("NeedAnother() undefined for spans representing a single key")
	}
	if ri.scanDir == Ascending {
		return ri.desc.EndKey.Less(rs.EndKey)
	}
	return rs.Key.Less(ri.desc.StartKey)
}

// Valid returns whether the iterator is valid. To be valid, the
// iterator must be have been seeked to an initial position using
// Seek(), and must not have encountered an error.
func (ri *RangeIterator) Valid() bool {
	return ri.Error() == nil
}

// Error returns the error the iterator encountered, if any. If
// the iterator has not been initialized, returns iterator error.
func (ri *RangeIterator) Error() *roachpb.Error {
	if !ri.init {
		return roachpb.NewErrorf("range iterator not intialized with Seek()")
	}
	return ri.pErr
}

// Reset resets the RangeIterator to its initial state.
func (ri *RangeIterator) Reset() {
	*ri = RangeIterator{ds: ri.ds}
}

// Silence unused warning.
var _ = (*RangeIterator)(nil).Reset

// Next advances the iterator to the next range. The direction of
// advance is dependent on whether the iterator is reversed. The
// iterator must be valid.
func (ri *RangeIterator) Next(ctx context.Context) {
	if !ri.Valid() {
		panic(ri.Error())
	}
	// Determine next span when the current range is subtracted.
	if ri.scanDir == Ascending {
		ri.Seek(ctx, ri.desc.EndKey, ri.scanDir)
	} else {
		ri.Seek(ctx, ri.desc.StartKey, ri.scanDir)
	}
}

// Seek positions the iterator at the specified key.
func (ri *RangeIterator) Seek(ctx context.Context, key roachpb.RKey, scanDir ScanDirection) {
	if log.HasSpanOrEvent(ctx) {
		rev := ""
		if scanDir == Descending {
			rev = " (rev)"
		}
		log.Eventf(ctx, "querying next range at %s%s", key, rev)
	}
	ri.scanDir = scanDir
	ri.init = true // the iterator is now initialized
	ri.pErr = nil  // clear any prior error
	ri.key = key   // set the key

	if (scanDir == Ascending && key.Equal(roachpb.RKeyMax)) ||
		(scanDir == Descending && key.Equal(roachpb.RKeyMin)) {
		ri.pErr = roachpb.NewErrorf("RangeIterator seek to invalid key %s", key)
		return
	}

	// Retry loop for looking up next range in the span. The retry loop
	// deals with retryable range descriptor lookups.
	for r := retry.StartWithCtx(ctx, ri.ds.rpcRetryOptions); r.Next(); {
		var err error
		ri.desc, ri.token, err = ri.ds.getDescriptor(
			ctx, ri.key, ri.token, ri.scanDir == Descending)

		if log.V(2) {
			log.Infof(ctx, "key: %s, desc: %s err: %v", ri.key, ri.desc, err)
		}

		// getDescriptor may fail retryably if, for example, the first
		// range isn't available via Gossip. Assume that all errors at
		// this level are retryable. Non-retryable errors would be for
		// things like malformed requests which we should have checked
		// for before reaching this point.
		if err != nil {
			log.VEventf(ctx, 1, "range descriptor lookup failed: %s", err)
			continue
		}

		// It's possible that the returned descriptor misses parts of the
		// keys it's supposed to include after it's truncated to match the
		// descriptor. Example revscan [a,g), first desc lookup for "g"
		// returns descriptor [c,d) -> [d,g) is never scanned.
		// We evict and retry in such a case.
		// TODO: this code is subject to removal. See
		// https://groups.google.com/d/msg/cockroach-db/DebjQEgU9r4/_OhMe7atFQAJ
		reverse := ri.scanDir == Descending
		if (reverse && !ri.desc.ContainsKeyInverted(ri.key)) ||
			(!reverse && !ri.desc.ContainsKey(ri.key)) {
			log.Eventf(ctx, "addressing error: %s does not include key %s", ri.desc, ri.key)
			if err := ri.token.Evict(ctx); err != nil {
				ri.pErr = roachpb.NewError(err)
				return
			}
			// On addressing errors, don't backoff; retry immediately.
			r.Reset()
			continue
		}
		if log.V(2) {
			log.Infof(ctx, "returning; key: %s, desc: %s", ri.key, ri.desc)
		}
		return
	}

	// Check for an early exit from the retry loop.
	if pErr := ri.ds.deduceRetryEarlyExitError(ctx); pErr != nil {
		ri.pErr = pErr
	} else {
		ri.pErr = roachpb.NewErrorf("RangeIterator failed to seek to %s", key)
	}
}
