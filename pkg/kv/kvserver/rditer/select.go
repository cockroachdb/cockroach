// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rditer

import (
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/errors"
)

// ReplicatedSpansFilter is used to declare filters when selecting replicated
// spans.
//
// TODO(arul): we could consider using a bitset here instead. Note that a lot of
// the fields here are mutually exclusive (e.g. ReplicatedSpansExcludeLocks and
// ReplicatedSpansLocksOnly), so we'd need some sort of validation here.
type ReplicatedSpansFilter int

const (
	// Throw away the zero value for easier migration away from this deprecated
	// field.
	_ ReplicatedSpansFilter = iota
	// ReplicatedSpansAll includes all replicated spans, including user keys,
	// range descriptors, and lock keys.
	ReplicatedSpansAll
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

// SelectRangedOptions configures span-based selection for replicated keys.
type SelectRangedOptions struct {
	// Span describes the range bounds. This must be set if any of the other
	// fields in this struct are.
	Span roachpb.RSpan
	// SystemKeys includes replicated range-local system keys such as range
	// descriptors, transaction records, queue processing state, and probe keys.
	// These keys are stored under the /Local/Range prefix and are part of the
	// replicated state machine.
	//
	// \x01 k <encoded-key> rdsc
	//  [1][2]    [3]       [4]
	//
	// [1]: system prefix byte
	// [2]: system prefix byte ("range local prefix byte")
	// [3]: encoded key, which is the EncodeBytesAscending of the anchor key
	// [4]: four-byte suffix (in this case, rsdc for RangeDescriptor)
	SystemKeys bool
	// LockTable includes lock table keys that track separated intents and other
	// locking metadata.
	//
	// Encoded as:
	// \x01 z k  <enc-key>
	// [1] [2][3]   [4]
	//
	// [1]: system prefix byte
	// [2]: lock table prefix byte
	// [3]: key prefix byte (currently always `k` since we don't have ranged intents)
	// [4]: encoded key, which is the EncodeBytesAscending of the locked key
	// Notably, the locked key can be a system key (intent on range descriptor).
	LockTable bool
	// UserKeys includes user data keys - the actual key-value pairs stored by
	// applications. These are the keys that fall into Span according to the
	// ordering used by the Engine (which matches byte-wise order).
	UserKeys bool
}

// SelectOpts configures which spans for a Replica to return from Select.
// A Replica comprises replicated (i.e. belonging to the state machine) spans
// and unreplicated spans, and depending on external circumstances one may want
// to read or erase only certain components of a Replica.
type SelectOpts struct {
	// ReplicatedBySpan selects all replicated key Spans that are keyed by a user
	// key. This includes user keys, range descriptors, and locks (separated
	// intents).
	//
	// DEPRECATED: Use Ranged.Span instead.
	// TODO(tbg): remove.
	ReplicatedBySpan roachpb.RSpan
	// Ranged selects spans based on key ranges. If Span is empty, no range-based
	// selection is performed.
	Ranged SelectRangedOptions
	// ReplicatedSpansFilter specifies which of the replicated spans indicated by
	// ReplicatedBySpan should be returned or excluded. The zero value,
	// ReplicatedSpansAll, returns all replicated spans.
	//
	// DEPRECATED: Use the individual Ranged fields instead.
	// TODO(tbg): remove.
	ReplicatedSpansFilter ReplicatedSpansFilter
	// ReplicatedByRangeID selects all RangeID-keyed replicated keys. An example
	// of a key that falls into this Span is the GCThresholdKey.
	ReplicatedByRangeID bool
	// UnreplicatedByRangeID selects all RangeID-keyed unreplicated keys. Examples
	// of keys that fall into this Span are the HardStateKey (and generally all
	// Raft state) and the RangeTombstoneKey.
	UnreplicatedByRangeID bool
}

func MakeLegacySelectOpts(
	sp roachpb.RSpan, replicatedOnly bool, filter ReplicatedSpansFilter,
) SelectOpts {
	opts := SelectOpts{
		Ranged: SelectRangedOptions{
			Span: sp,
		}.Filtered(filter),
	}
	if replicatedOnly {
		// NB: We exclude ReplicatedByRangeID if only user keys are requested.
		opts.ReplicatedByRangeID = filter != ReplicatedSpansUserOnly
	} else {
		opts.ReplicatedByRangeID = true
		opts.UnreplicatedByRangeID = true
	}
	return opts
}

// Select returns a slice of disjoint sorted[^1] Spans describing all or a part
// of a Replica's keyspace, depending on the supplied SelectOpts.
//
// [^1]: lexicographically (bytes.Compare), which means they are compatible with
// pebble's CockroachDB-specific sort order (storage.EngineComparer).
func Select(rangeID roachpb.RangeID, opts SelectOpts) []roachpb.Span {
	// Handle backward compatibility: determine which span and boolean fields to use.
	// Priority: Ranged fields > ReplicatedSpansFilter.

	if opts.ReplicatedSpansFilter > 0 {
		// TODO(tbg): remove this once we've migrated to the new fields.
		opts.Ranged = opts.Ranged.Filtered(opts.ReplicatedSpansFilter)
	}

	if len(opts.ReplicatedBySpan.EndKey) > 0 {
		opts.Ranged.Span = opts.ReplicatedBySpan
	}

	var sl []roachpb.Span

	if opts.ReplicatedByRangeID {
		sl = append(sl, makeRangeIDReplicatedSpan(rangeID))
	}

	if opts.UnreplicatedByRangeID {
		sl = append(sl, makeRangeIDUnreplicatedSpan(rangeID))
	}

	if !opts.Ranged.Span.Equal(roachpb.RSpan{}) {
		// r1 "really" only starts at LocalMax. But because we use a StartKey of
		// RKeyMin for r1, we actually do anchor range descriptors (and their locks
		// and txn records) at RKeyMin as well. On the other hand, the "user key
		// space" cannot start at RKeyMin because then it encompasses the
		// special-cased prefix \x02... (/Local/). So awkwardly for key-based local
		// keyspace we must not call KeySpan, for user keys we have to.
		//
		// See also the comment on KeySpan.
		in := opts.Ranged.Span
		adjustedIn := in.KeySpan()

		// System keys (range descriptors, txn records, probes, etc).
		if opts.Ranged.SystemKeys {
			sl = append(sl, makeRangeLocalKeySpan(in))
		}

		if opts.Ranged.LockTable {
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

		if opts.Ranged.UserKeys {
			// Adjusted span because r1's "normal" keyspace starts only at LocalMax,
			// not RKeyMin.
			sl = append(sl, adjustedIn.AsRawSpanWithNoLocals())
		}
	}
	return sl
}

// Filtered returns a copy of the SelectRangedOptions with boolean fields set
// according to the ReplicatedSpansFilter.
func (opts SelectRangedOptions) Filtered(filter ReplicatedSpansFilter) SelectRangedOptions {
	switch filter {
	case ReplicatedSpansAll:
		opts.SystemKeys = true
		opts.UserKeys = true
		opts.LockTable = true
	case ReplicatedSpansExcludeUser:
		opts.SystemKeys = true
		opts.UserKeys = false
		opts.LockTable = true
	case ReplicatedSpansUserOnly:
		opts.SystemKeys = false
		opts.UserKeys = true
		opts.LockTable = false
	case ReplicatedSpansExcludeLocks:
		opts.SystemKeys = true
		opts.UserKeys = true
		opts.LockTable = false
	case ReplicatedSpansLocksOnly:
		opts.SystemKeys = false
		opts.UserKeys = false
		opts.LockTable = true
	default:
		panic(errors.AssertionFailedf("unknown ReplicatedSpansFilter: %d", filter))
	}

	return opts
}
