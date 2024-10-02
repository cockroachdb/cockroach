// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rditer

import (
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

// ReplicatedSpansFilter is used to declare filters when selecting replicated
// spans.
//
// TODO(arul): we could consider using a bitset here instead. Note that a lot of
// the fields here are mutually exclusive (e.g. ReplicatedSpansExcludeLocks and
// ReplicatedSpansLocksOnly), so we'd need some sort of validation here.
type ReplicatedSpansFilter int

const (
	// ReplicatedSpansAll includes all replicated spans, including user keys,
	// range descriptors, and lock keys.
	ReplicatedSpansAll ReplicatedSpansFilter = iota
	// ReplicatedSpansExcludeUser includes all replicated spans except for user
	// keys.
	ReplicatedSpansExcludeUser
	// ReplicatedSpansUserOnly includes just user keys, and no other replicated
	// spans.
	ReplicatedSpansUserOnly
	// ReplicatedSpansExcludeLocks includes all replicated spans except for the
	// lock table.
	ReplicatedSpansExcludeLocks
	// ReplicatedSpansLocksOnly includes just spans for the lock table, and no
	// other replicated spans.
	ReplicatedSpansLocksOnly
)

// SelectOpts configures which spans for a Replica to return from Select.
// A Replica comprises replicated (i.e. belonging to the state machine) spans
// and unreplicated spans, and depending on external circumstances one may want
// to read or erase only certain components of a Replica.
type SelectOpts struct {
	// ReplicatedBySpan selects all replicated key Spans that are keyed by a user
	// key. This includes user keys, range descriptors, and locks (separated
	// intents).
	ReplicatedBySpan roachpb.RSpan
	// ReplicatedSpansFilter specifies which of the replicated spans indicated by
	// ReplicatedBySpan should be returned or excluded. The zero value,
	// ReplicatedSpansAll, returns all replicated spans.
	ReplicatedSpansFilter ReplicatedSpansFilter
	// ReplicatedByRangeID selects all RangeID-keyed replicated keys. An example
	// of a key that falls into this Span is the GCThresholdKey.
	ReplicatedByRangeID bool
	// UnreplicatedByRangeID selects all RangeID-keyed unreplicated keys. Examples
	// of keys that fall into this Span are the HardStateKey (and generally all
	// Raft state) and the RangeTombstoneKey.
	UnreplicatedByRangeID bool
}

// Select returns a slice of disjoint sorted[^1] Spans describing all or a part
// of a Replica's keyspace, depending on the supplied SelectOpts.
//
// [^1]: lexicographically (bytes.Compare), which means they are compatible with
// pebble's CockroachDB-specific sort order (storage.EngineComparer).
func Select(rangeID roachpb.RangeID, opts SelectOpts) []roachpb.Span {
	var sl []roachpb.Span

	if opts.ReplicatedByRangeID {
		sl = append(sl, makeRangeIDReplicatedSpan(rangeID))
	}

	if opts.UnreplicatedByRangeID {
		sl = append(sl, makeRangeIDUnreplicatedSpan(rangeID))
	}

	if !opts.ReplicatedBySpan.Equal(roachpb.RSpan{}) {
		// r1 "really" only starts at LocalMax. But because we use a StartKey of
		// RKeyMin for r1, we actually do anchor range descriptors (and their locks
		// and txn records) at RKeyMin as well. On the other hand, the "user key
		// space" cannot start at RKeyMin because then it encompasses the
		// special-cased prefix \x02... (/Local/). So awkwardly for key-based local
		// keyspace we must not call KeySpan, for user keys we have to.
		//
		// See also the comment on KeySpan.
		in := opts.ReplicatedBySpan
		adjustedIn := in.KeySpan()
		if opts.ReplicatedSpansFilter != ReplicatedSpansUserOnly {
			if opts.ReplicatedSpansFilter != ReplicatedSpansLocksOnly {
				sl = append(sl, makeRangeLocalKeySpan(in))
			}

			// Lock table.
			if opts.ReplicatedSpansFilter != ReplicatedSpansExcludeLocks {
				// Handle doubly-local lock table keys since range descriptor key
				// is a range local key that can have a replicated lock acquired on it.
				startRangeLocal, _ := keys.LockTableSingleKey(keys.MakeRangeKeyPrefix(in.Key), nil)
				endRangeLocal, _ := keys.LockTableSingleKey(keys.MakeRangeKeyPrefix(in.EndKey), nil)
				// Need adjusted start key to avoid overlapping with the local lock span
				// right above.
				startGlobal, _ := keys.LockTableSingleKey(adjustedIn.Key.AsRawKey(), nil)
				endGlobal, _ := keys.LockTableSingleKey(adjustedIn.EndKey.AsRawKey(), nil)
				sl = append(sl, roachpb.Span{
					Key:    startRangeLocal,
					EndKey: endRangeLocal,
				}, roachpb.Span{
					Key:    startGlobal,
					EndKey: endGlobal,
				})
			}
		}
		if opts.ReplicatedSpansFilter != ReplicatedSpansExcludeUser &&
			opts.ReplicatedSpansFilter != ReplicatedSpansLocksOnly {
			// Adjusted span because r1's "normal" keyspace starts only at LocalMax,
			// not RKeyMin.
			sl = append(sl, adjustedIn.AsRawSpanWithNoLocals())
		}
	}
	return sl
}
