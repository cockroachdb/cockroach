// Copyright 2016 The Cockroach Authors.
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

package kv

import (
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/retry"
)

// A RangeIterator provides a mechanism for iterating over all ranges
// in a key span. A new RangeIterator must be positioned to begin
//
// RangeIterator is not thread-safe.
type RangeIterator struct {
	ds        *DistSender
	isReverse bool
	key       roachpb.RKey
	desc      *roachpb.RangeDescriptor
	token     *EvictionToken
	init      bool
	pErr      *roachpb.Error
}

// NewRangeIterator creates a new RangeIterator which will iterate
// over the ranges specified by the rs key span.
func NewRangeIterator(ds *DistSender, isReverse bool) *RangeIterator {
	return &RangeIterator{
		ds:        ds,
		isReverse: isReverse,
	}
}

// Key returns the current key. The iterator must be valid.
func (ri *RangeIterator) Key() roachpb.RKey {
	if !ri.Valid() {
		panic("iterator not valid")
	}
	return ri.key
}

// Desc returns the descriptor of the range at which the iterator is
// currently positioned. The iterator must be valid.
func (ri *RangeIterator) Desc() *roachpb.RangeDescriptor {
	if !ri.Valid() {
		panic("iterator not valid")
	}
	return ri.desc
}

// Token returns the eviction token corresponding to the range
// descriptor for the current iteration. The iterator must be valid.
func (ri *RangeIterator) Token() *EvictionToken {
	if !ri.Valid() {
		panic("iterator not valid")
	}
	return ri.token
}

// NeedAnother checks whether the iteration needs to continue to cover
// the remainder of the ranges described by the supplied key span. The
// iterator must be valid.
func (ri *RangeIterator) NeedAnother(rs roachpb.RSpan) bool {
	if !ri.Valid() {
		panic("iterator not valid")
	}
	if ri.isReverse {
		return rs.Key != nil && rs.Key.Less(ri.desc.StartKey)
	}
	return rs.EndKey != nil && ri.desc.EndKey.Less(rs.EndKey)
}

// Valid returns true if the iterator is valid. To be valid, the
// iterator must be have been seeked to an initial position using
// Seek(), and must not have encountered an error.
func (ri *RangeIterator) Valid() bool {
	return ri.init && ri.pErr == nil
}

// Error returns the error the iterator encountered, if any.
func (ri *RangeIterator) Error() *roachpb.Error {
	return ri.pErr
}

// Next advances the iterator to the next range. The direction of
// advance is dependent on whether the iterator is reversed. The
// iterator must be valid.
func (ri *RangeIterator) Next(ctx context.Context) {
	if !ri.Valid() {
		panic("iterator not valid")
	}
	// Determine next span when the current range is subtracted.
	if ri.isReverse {
		ri.Seek(ctx, ri.desc.StartKey)
	} else {
		ri.Seek(ctx, ri.desc.EndKey)
	}
}

// Seek positions the iterator at the specified key.
func (ri *RangeIterator) Seek(ctx context.Context, key roachpb.RKey) {
	log.Eventf(ctx, "querying next range at %s", key)
	ri.init = true // the iterator is now initialized
	ri.pErr = nil  // clear any prior error
	ri.key = key   // set the key

	// Retry loop for looking up next range in the span. The retry loop
	// deals with retryable range descriptor lookups.
	for r := retry.StartWithCtx(ctx, ri.ds.rpcRetryOptions); r.Next(); {
		log.Event(ctx, "meta descriptor lookup")
		var err error
		ri.desc, ri.token, err = ri.ds.getDescriptor(ctx, ri.key, ri.token, ri.isReverse)

		// getDescriptor may fail retryably if, for example, the first
		// range isn't available via Gossip. Assume that all errors at
		// this level are retryable. Non-retryable errors would be for
		// things like malformed requests which we should have checked
		// for before reaching this point.
		if err != nil {
			log.VEventf(1, ctx, "range descriptor lookup failed: %s", err)
			continue
		}

		// It's possible that the returned descriptor misses parts of the
		// keys it's supposed to include after it's truncated to match the
		// descriptor. Example revscan [a,g), first desc lookup for "g"
		// returns descriptor [c,d) -> [d,g) is never scanned.
		// We evict and retry in such a case.
		if (ri.isReverse && !ri.desc.ContainsExclusiveEndKey(ri.key)) ||
			(!ri.isReverse && !ri.desc.ContainsKey(ri.key)) {
			log.Eventf(ctx, "addressing error: %s does not include key %s", ri.desc, ri.key)
			if err := ri.token.Evict(ctx); err != nil {
				ri.pErr = roachpb.NewError(err)
				return
			}
			// On addressing errors, don't backoff; retry immediately.
			r.Reset()
			continue
		}
		break
	}

	// Check for an early exit from the retry loop.
	if pErr := ri.ds.deduceRetryEarlyExitError(ctx); pErr != nil {
		ri.pErr = pErr
		return
	}
}
