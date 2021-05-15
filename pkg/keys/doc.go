// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package keys manages the construction of keys for CockroachDB's key-value
// layer.
//
// The keys package is necessarily tightly coupled to the storage package. In
// theory, it is oblivious to higher levels of the stack. In practice, it
// exposes several functions that blur abstraction boundaries to break
// dependency cycles. For example, EnsureSafeSplitKey knows far too much about
// how to decode SQL keys.
//
// 1. Overview
//
// This is the ten-thousand foot view of the keyspace:
//
//    +------------------+
//    | (empty)          | /Min
//    | \x01...          | /Local            ---------------------+
//    |                  |                                        |
//    | ...              |                                        | local keys
//    |                  |                                        |
//    |                  |                   ---------------------+
//    |                  |                   ---------------------+
//    | \x02...          | /Meta1            ----+                |
//    | \x03...          | /Meta2                |                |
//    | \x04...          | /System               |                |
//    |                  |                       | system keys    |
//    | ...              |                       |                |
//    |                  |                   ----+                |
//    | \x89...          | /Table/1          ----+                |
//    | \x8a...          | /Table/2              |                |
//    |                  |                       | system tenant  |
//    | ...              |                       |                | global keys
//    |                  |                   ----+                |
//    | \xfe\x8a\x89...  | /Tenant/2/Table/1 ----+                |
//    | \xfe\x8a\x8a...  | /Tenant/2/Table/2     |                |
//    |                  |                       | tenant 2       |
//    | ...              |                       |                |
//    |                  |                   ----+                |
//    | \xfe...          | /Tenant/...       ----+                |
//    | \xfe...          |                       |                |
//    |                  |                       | tenant ...     |
//    | ...              |                       |                |
//    |                  |                   ----+                |
//    | \xff\xff         | /Max              ---------------------+
//    +------------------+
//
// When keys are pretty printed, the logical name to the right of the table is
// shown instead of the raw byte sequence.
//
//
// 1. Key Ranges
//
// The keyspace is divided into contiguous, non-overlapping chunks called
// "ranges." A range is defined by its start and end keys. For example, a range
// might span from [/Table/1, /Table/2), where the lower bound is inclusive and
// the upper bound is exclusive. Any key that begins with /Table/1, like
// /Table/1/SomePrimaryKeyValue..., would belong to this range. Key ranges
// exist over the "resolved" keyspace, refer to the "Key Addressing" section
// below for more details.
//
//
// 2. Local vs. Global Keys
//
// There are broadly two types of keys, "local" and "global":
//
//  (i) Local keys, such as store- and range-specific metadata, are keys that
//  must be physically collocated with the store and/or ranges they refer to but
//  also logically separated so that they do not pollute the user key space.
//  This is further elaborated on in the "Key Addressing" section below. Local
//  data also includes data "local" to a node, such as the store metadata and
//  the raft log, which is where the name originated.
//
//  (ii) Non-local keys (for e.g. meta1, meta2, system, and SQL keys) are
//  collectively referred to as "global" keys.
//
// NB: The empty key (/Min) is a special case. No data is stored there, but it
// is used as the start key of the first range descriptor and as the starting
// point for some scans, in which case it acts like a global key.
//
// (Check `keymap` below for a more precise breakdown of the local and global
// keyspace.)
//
//
// 2. Key Addressing
//
// We also have this concept of the "address" for a key. Keys get "resolved"
// using `keys.Addr`, through which we're able to lookup the range "containing"
// the key. For global keys, the resolved key is the key itself.
//
// Local keys are special. For certain kinds of local keys (namely, addressable
// ones), the resolved key is obtained by stripping out the local key prefix,
// suffix, and optional details (refer to `keymap` below to understand how local
// keys are constructed). This level of indirection was introduced so that we
// could logically sort these local keys into a range other than what a
// strictly physical key based sort would entail. For example, the key
// /Local/Range/Table/1 would naturally sort into the range [/Min, /System), but
// its "address" is /Table/1, so it actually belongs to a range like [/Table1,
// /Table/2).
//
// Consider the motivating example: we want to store a copy of the range
// descriptor in a key that's both (a) a part of the range, and (b) does not
// require us to remove a portion of the keyspace from the user (say by
// reserving some key suffix). Storing this information in the global keyspace
// would place the data on an arbitrary set of stores, with no guarantee of
// collocation. By being able to logically sort the range descriptor key next to
// the range itself, we're able to collocate the two.
//
//
// 3. (replicated) Range-ID local keys vs. Range local keys
//
// Deciding between replicated range-ID local keys and range local keys is not
// entirely straightforward, as the two key types serve similar purposes.
// Range-ID keys, as the name suggests, use the range-ID in the key. Range local
// keys instead use a key within the range bounds. Range-ID keys are not
// addressable whereas range-local keys are. Note that only addressable keys can
// be the target of KV operations, unaddressable keys can only be written as a
// side-effect of other KV operations. This can often makes the choice between
// the two clear (range descriptor keys needing to be addressable, and therefore
// being a range local key is one example of this). Not being addressable also
// implies not having multiple versions, and therefore never having intents.
//
// The "behavioral" difference between range local keys and range-id local keys
// is that range local keys split and merge along range boundaries while
// range-id local keys don't. We want to move as little data as possible during
// splits and merges (in fact, we don't re-write any data during splits), and
// that generally determines which data sits where. If we want the split point
// of a range to dictate where certain keys end up, then they're likely meant to
// be range local keys. If not, they're meant to be range-ID local keys. Any key
// we need to re-write during splits/merges will needs to go through Raft. We
// have limits set on the size of Raft proposals so we generally don’t want to
// be re-writing lots of data. Range lock keys (see below) are separate from
// range local keys, but behave similarly in that they split and merge along
// range boundaries.
//
// This naturally leads to range-id local keys being used to store metadata
// about a specific Range and range local keys being used to store metadata
// about specific "global" keys. Let us consider transaction record keys for
// example (ignoring for a second we also need them to be addressable). Hot
// ranges could potentially have lots of transaction keys. Keys destined for the
// RHS of the split need to be collocated with the RHS range. By categorizing
// them as as range local keys, we avoid needing to re-write them during splits
// as they automatically sort into the new range boundaries. If they were
// range-ID local keys, we'd have to update each transaction key with the new
// range ID.
package keys

// NB: The sorting order of the symbols below map to the physical layout.
// Preserve group-wise ordering when adding new constants.
var _ = [...]interface{}{
	MinKey,

	// There are five types of local key data enumerated below: replicated
	// range-ID, unreplicated range-ID, range local, store-local, and range lock
	// keys. Range lock keys are required to be last category of keys in the
	// lock key space.
	// Local keys are constructed using a prefix, an optional infix, and a
	// suffix. The prefix and infix are used to disambiguate between the four
	// types of local keys listed above, and determines inter-group ordering.
	// The string comment next to each symbol below is the suffix pertaining to
	// the corresponding key (and determines intra-group ordering).
	// 	  - RangeID replicated keys all share `LocalRangeIDPrefix` and
	// 		`LocalRangeIDReplicatedInfix`.
	// 	  - RangeID unreplicated keys all share `LocalRangeIDPrefix` and
	// 		`localRangeIDUnreplicatedInfix`.
	// 	  - Range local keys all share `LocalRangePrefix`.
	//	  - Store keys all share `localStorePrefix`.
	// 	  - Range lock (which are also local keys) all share
	//	  `LocalRangeLockTablePrefix`.
	//
	// `LocalRangeIDPrefix`, `localRangePrefix`, `localStorePrefix`, and
	// `LocalRangeLockTablePrefix` all in turn share `LocalPrefix`.
	// `LocalPrefix` was chosen arbitrarily. Local keys would work just as well
	// with a different prefix, like 0xff, or even with a suffix.

	//   1. Replicated range-ID local keys: These store metadata pertaining to a
	//   range as a whole. Though they are replicated, they are unaddressable.
	//   Typical examples are MVCC stats and the abort span. They all share
	//   `LocalRangeIDPrefix` and `LocalRangeIDReplicatedInfix`.
	AbortSpanKey,                // "abc-"
	RangeGCThresholdKey,         // "lgc-"
	RangeAppliedStateKey,        // "rask"
	RaftAppliedIndexLegacyKey,   // "rfta"
	RaftTruncatedStateLegacyKey, // "rftt"
	RangeLeaseKey,               // "rll-"
	LeaseAppliedIndexLegacyKey,  // "rlla"
	RangePriorReadSummaryKey,    // "rprs"
	RangeVersionKey,             // "rver"
	RangeStatsLegacyKey,         // "stat"

	//   2. Unreplicated range-ID local keys: These contain metadata that
	//   pertain to just one replica of a range. They are unreplicated and
	//   unaddressable. The typical example is the Raft log. They all share
	//   `LocalRangeIDPrefix` and `localRangeIDUnreplicatedInfix`.
	RangeTombstoneKey,              // "rftb"
	RaftHardStateKey,               // "rfth"
	RaftLogKey,                     // "rftl"
	RaftTruncatedStateKey,          // "rftt"
	RangeLastReplicaGCTimestampKey, // "rlrt"

	//   3. Range local keys: These also store metadata that pertains to a range
	//   as a whole. They are replicated and addressable. Typical examples are
	//   the range descriptor and transaction records. They all share
	//   `LocalRangePrefix`.
	QueueLastProcessedKey, // "qlpt"
	RangeDescriptorKey,    // "rdsc"
	TransactionKey,        // "txn-"

	//   4. Store local keys: These contain metadata about an individual store.
	//   They are unreplicated and unaddressable. The typical example is the
	//   store 'ident' record. They all share `localStorePrefix`.
	StoreClusterVersionKey, // "cver"
	StoreGossipKey,         // "goss"
	StoreHLCUpperBoundKey,  // "hlcu"
	StoreIdentKey,          // "iden"
	StoreNodeTombstoneKey,  // "ntmb"
	StoreLastUpKey,         // "uptm"
	StoreCachedSettingsKey, // "stng"

	//   5. Range lock keys for all replicated locks. All range locks share
	//   LocalRangeLockTablePrefix. Locks can be acquired on global keys and on
	//   range local keys. Currently, locks are only on single keys, i.e., not
	//   on a range of keys. Only exclusive locks are currently supported, and
	//   these additionally function as pointers to the provisional MVCC values.
	//   Single key locks use a byte, LockTableSingleKeyInfix, that follows
	//   the LocalRangeLockTablePrefix. This is to keep the single-key locks
	//   separate from (future) range locks.
	LockTableSingleKey,

	// The global keyspace includes the meta{1,2}, system, system tenant SQL
	// keys, and non-system tenant SQL keys.
	//
	// 	1. Meta keys: This is where we store all key addressing data.
	MetaMin,
	Meta1Prefix,
	Meta2Prefix,
	MetaMax,

	// 	2. System keys: This is where we store global, system data which is
	// 	replicated across the cluster.
	SystemPrefix,
	NodeLivenessPrefix,  // "\x00liveness-"
	BootstrapVersionKey, // "bootstrap-version"
	descIDGenerator,     // "desc-idgen"
	NodeIDGenerator,     // "node-idgen"
	RangeIDGenerator,    // "range-idgen"
	StatusPrefix,        // "status-"
	StatusNodePrefix,    // "status-node-"
	StoreIDGenerator,    // "store-idgen"
	MigrationPrefix,     // "system-version/"
	MigrationLease,      // "system-version/lease"
	TimeseriesPrefix,    // "tsd"
	SystemMax,

	// 	3. System tenant SQL keys: This is where we store all system-tenant
	// 	table data.
	TableDataMin,
	NamespaceTableMin,
	UserTableDataMin,
	TableDataMax,

	//  4. Non-system tenant SQL keys: This is where we store all non-system
	//  tenant table data.
	TenantTableDataMin,
	TenantTableDataMax,

	MaxKey,
}

// Unused, deprecated keys.
var _ = [...]interface{}{
	localRaftLastIndexSuffix,
	localRangeFrozenStatusSuffix,
	localRangeLastVerificationTimestampSuffix,
	localRemovedLeakedRaftEntriesSuffix,
	localTxnSpanGCThresholdSuffix,
}
