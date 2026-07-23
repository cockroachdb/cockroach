// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvcoord

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangecache"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/errors"
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
	// token represents the results of the latest cache lookup.
	token rangecache.EvictionToken
	init  bool
	err   error
}

// MakeRangeIterator creates a new RangeIterator.
func MakeRangeIterator(ds *DistSender) RangeIterator {
	return RangeIterator{
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

func (d ScanDirection) String() string {
	if d == Ascending {
		return "asc"
	}
	return "desc"
}

// Key returns the current key. The iterator must be valid.
func (ri *RangeIterator) Key() roachpb.RKey {
	if !ri.Valid() {
		panic(ri.Error())
	}
	return ri.key
}

// Desc returns the descriptor of the range at which the iterator is
// currently positioned. The iterator must be valid.
//
// The returned descriptor is immutable.
func (ri *RangeIterator) Desc() *roachpb.RangeDescriptor {
	if !ri.Valid() {
		panic(ri.Error())
	}
	return ri.token.Desc()
}

// Leaseholder returns information about the leaseholder of the range at which
// the iterator is currently positioned. The iterator must be valid.
//
// The lease information comes from a cache, and so it can be stale. Returns nil
// if no lease information is known.
//
// The returned lease is immutable.
func (ri *RangeIterator) Leaseholder() *roachpb.ReplicaDescriptor {
	if !ri.Valid() {
		panic(ri.Error())
	}
	return ri.token.Leaseholder()
}

// ClosedTimestampPolicy returns the closed timestamp policy of the range at
// which the iterator is currently positioned. The iterator must be valid.
//
// The policy information comes from a cache, and so it can be stale. Returns
// the default policy of LAG_BY_CLUSTER_SETTING if no policy information is
// known.
func (ri *RangeIterator) ClosedTimestampPolicy() roachpb.RangeClosedTimestampPolicy {
	if !ri.Valid() {
		panic(ri.Error())
	}
	// TODO(ajwerner): We default the closed timestamp policy here to
	// LAG_BY_CLUSTER_SETTING, which is pessimistic. When sending batch requests,
	// we default the policy to LEAD_FOR_GLOBAL_READS. The reasoning for this
	// difference is not deeply principled. Consider unifying them.
	const defaultPolicy = roachpb.LAG_BY_CLUSTER_SETTING
	return ri.token.ClosedTimestampPolicy(defaultPolicy)
}

// Token returns the eviction token corresponding to the range
// descriptor for the current iteration. The iterator must be valid.
func (ri *RangeIterator) Token() rangecache.EvictionToken {
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
		return ri.Desc().EndKey.Less(rs.EndKey)
	}
	return rs.Key.Less(ri.Desc().StartKey)
}

// Valid returns whether the iterator is valid. To be valid, the
// iterator must be have been seeked to an initial position using
// Seek(), and must not have encountered an error.
func (ri *RangeIterator) Valid() bool {
	return ri.Error() == nil
}

var errRangeIterNotInitialized = errors.New("range iterator not intialized with Seek()")

// Error returns the error the iterator encountered, if any. If
// the iterator has not been initialized, returns iterator error.
func (ri *RangeIterator) Error() error {
	if !ri.init {
		return errRangeIterNotInitialized // hot path
	}
	return ri.err
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
		ri.Seek(ctx, ri.Desc().EndKey, ri.scanDir)
	} else {
		ri.Seek(ctx, ri.Desc().StartKey, ri.scanDir)
	}
}

// Seek positions the iterator at the specified key.
func (ri *RangeIterator) Seek(ctx context.Context, key roachpb.RKey, scanDir ScanDirection) {
	logEvents := log.HasSpan(ctx)
	if logEvents {
		rev := ""
		if scanDir == Descending {
			rev = " (rev)"
		}
		log.Eventf(ctx, "querying next range at %s%s", key, rev)
	}
	ri.scanDir = scanDir
	ri.init = true // the iterator is now initialized
	ri.err = nil   // clear any prior error
	ri.key = key   // set the key

	if (scanDir == Ascending && key.Equal(roachpb.RKeyMax)) ||
		(scanDir == Descending && key.Equal(roachpb.RKeyMin)) {
		ri.err = errors.Errorf("RangeIterator seek to invalid key %s", key)
		return
	}

	// Retry loop for looking up next range in the span. The retry loop
	// deals with retryable range descriptor lookups.
	var err error
	for r := retry.StartWithCtx(ctx, ri.ds.rpcRetryOptions); r.Next(); {
		// Note that we pass an empty eviction token here because ri.token
		// corresponds to the previous range.
		var rngInfo rangecache.EvictionToken
		rngInfo, err = ri.ds.getRoutingInfo(
			ctx, ri.key, rangecache.EvictionToken{}, ri.scanDir == Descending,
		)

		// getRoutingInfo may fail retryably if, for example, the first
		// range isn't available via Gossip. Assume that all errors at
		// this level are retryable. Non-retryable errors would be for
		// things like malformed requests which we should have checked
		// for before reaching this point.
		if err != nil {
			log.VEventf(ctx, 1, "range descriptor lookup failed: %s", err)
			if !rangecache.IsRangeLookupErrorRetryable(err) {
				break
			}
			continue
		}
		if logEvents {
			log.Eventf(ctx, "key: %s, desc: %s", ri.key, rngInfo.Desc())
		}

		ri.token = rngInfo
		return
	}

	// Check for an early exit from the retry loop.
	if deducedErr := ri.ds.deduceRetryEarlyExitError(ctx, err); deducedErr != nil {
		ri.err = deducedErr
	} else {
		ri.err = errors.Wrapf(err, "RangeIterator failed to seek to %s", key)
	}
}
