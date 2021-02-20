// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tracker

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

// Tracker tracks the lower bound of a set of timestamps (called the tracked
// set). Timestamps can be added and removed from the tracked set. A
// conservative estimation of the set's lower bound can be queried; the result
// will be lower or equal to all the tracked timestamps; it might not be equal
// to the lowest timestamp currently in the set (i.e. it might not be precise).
//
// For context, the Tracker is used to track the write timestamps of requests
// currently evaluating on a range. The lower bound is used to figure out what
// timestamps can be closed: we can only close timestamps below that of any
// request that's currently evaluating. The Tracker itself does not know
// anything about closed timestamps.
//
// Track can be called concurrently. Other methods cannot be called concurrently
// (with themselves or with any other method, including Track).
//
// The usage pattern is:
//
// Start of request evaluation:
//
// externalLock.RLock()
// tok := Tracker.Track(request.writeTimestamp)
// externalLock.RUnlock()
//
// Proposal buffer flush:
//
// externalLock.Lock()
// for each command being proposed:
// 		Tracker.Untrack(tok)
// newClosedTimestamp := min(now() - kv.closed_timestamp.target_duration, Tracker.LowerBound() - 1)
// externalLock.Unlock()
//
// The production implementation of the interface is the lockfreeTracker, which
// trades accuracy for performance. There's also a more pedestrian HeapTracker
// reference implementation.
type Tracker interface {

	// Track inserts a timestamps into the tracked set. The returned token must
	// later be passed to Untrack() to remove the timestamps from the set.
	//
	// While `ts` is tracked, LowerBound() will return values less or equal to
	// `ts`.
	//
	// Track can be called concurrently with other Track calls.
	//
	// Implementations should pay attention so that the returned interface doesn't
	// cause an allocation (when the implementation's concrete token type is
	// wrapped in the RemovalToken interface). If their token implementation is
	// smaller or equal to a pointer size, the allocation will be avoided.
	Track(_ context.Context, ts hlc.Timestamp) RemovalToken

	// Untrack removes a timestamp from the tracked set - the timestamp that created
	// `tok`. This might advance the result of future LowerBound() calls.
	//
	// Untrack cannot be called concurrently with other operations.
	Untrack(_ context.Context, tok RemovalToken)

	// LowerBound returns a conservative estimate of the lower bound of the
	// tracked set of timestamps. If the tracked set is currently empty, an empty
	// timestamp is returned.
	//
	// The returned timestamp might be smaller than the lowest timestamp ever
	// inserted into the set. Implementations are allowed to round timestamps
	// down.
	//
	// Synthetic timestamps: The Tracker doesn't necessarily track synthetic /
	// physical timestamps precisely; the only guarantee implementations need to
	// make is that, if no synthethic timestamp is inserted into the tracked set
	// for a while, eventually the LowerBound value will not be synthetic.
	LowerBound(context.Context) hlc.Timestamp

	// Count returns the current size of the tracked set.
	//
	// Count cannot be called concurrently with other methods.
	Count() int
}

// RemovalToken represents the result of Track: a token to be later used with
// Untrack() for removing the respective timestamp from the tracked set.
type RemovalToken interface {
	// RemovalTokenMarker is a dummy marker method.
	RemovalTokenMarker()
}
