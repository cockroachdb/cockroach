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
// This is the ten-thousand foot view of the keyspace:
//
//    +----------+
//    | (empty)  | /Min
//    | \x01...  | /Local
//    | \x02...  | /Meta1    ----+
//    | \x03...  | /Meta2        |
//    | \x04...  | /System       |
//    |          |               |
//    | ...      |               |
//    |          |               | global keys
//    | \x89...  | /Table/1      |
//    | \x8a...  | /Table/2      |
//    |          |               |
//    | ...      |               |
//    |          |               |
//    | \xff\xff | /Max      ----+
//    +----------+
//
// When keys are pretty printed, the logical name to the right of the table is
// shown instead of the raw byte sequence.
//
// Non-local key types (i.e., meta1, meta2, system, and SQL keys) are
// collectively referred to as "global" keys. The empty key (/Min) is a special
// case. No data is stored there, but it is used as the start key of the first
// range descriptor and as the starting point for some scans, in which case it
// acts like a global key.
//
// Key addressing
//
// The keyspace is divided into contiguous, non-overlapping chunks called
// "ranges." A range is defined by its start and end keys. For example, a range
// might span from [/Table/1, /Table/2), where the lower bound is inclusive and
// the upper bound is exclusive. Any key that begins with /Table/1, like
// /Table/1/SomePrimaryKeyValue..., would belong to this range. Range boundaries
// are always global keys.
//
// Local keys are special. They do not belong to the range they would naturally
// sort into. Instead, they have an "address" that is used to determine which
// range they sort into. For example, the key /Local/Range/Table/1 would
// naturally sort into the range [/Min, /System), but its address is /Table/1,
// so it actually belongs to a range like [/Table1, /Table/2). Note that not
// every local key has an address.
//
// Global keys have an address too, but the address is simply the key itself.
//
// To retrieve a key's address, use the Addr function.
//
// Local keys
//
// Local keys hold data local to a RocksDB instance, such as store- and
// range-specific metadata which must not pollute the user key space, but must
// be collocated with the store and/or ranges which they refer to. Storing this
// information in the global keyspace would place the data on an arbitrary set
// of stores, with no guarantee of collocation. Local data includes store
// metadata, range metadata, abort cache values and transaction records.
//
// The local key prefix was chosen arbitrarily. Local keys would work just as
// well with a different prefix, like 0xff, or even with a suffix.
//
// There are four kinds of local keys.
//
//   - Store local keys contain metadata about an individual store. They are
//     unreplicated and unaddressable. The typical example is the store 'ident'
//     record.
//
//   - Unreplicated range-ID local keys contain metadata that pertains to just
//     one replica of a range. They are unreplicated and unaddressable. The
//     typical example is the Raft log.
//
//   - Replicated range-ID local keys store metadata that pertains to a range as
//     a whole. Though they are replicated, they are unaddressable. Typical
//     examples are MVCC stats and the abort span.
//
//   - Range local keys also store metadata that pertains to a range as a whole.
//     They are replicated and addressable. Typical examples are the local
//     range descriptor and transaction records.
//
// Deciding between replicated range-ID local keys and (replicated) range local
// keys is not entirely straightforward, as the two key types serve similar
// purposes. Note that only addressable keys, like range local keys, can be the
// target of KV operations. Unaddressable keys, like range-ID local keys, can
// only be written as a side-effect of other KV operations. This often makes the
// choice between the two clear.
package keys
