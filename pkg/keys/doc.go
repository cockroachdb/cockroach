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
// collectively referred to as "global" keys.
//
// Key addressing
//
// The keyspace is divided into contiguous, non-overlapping chunks called
// "ranges." A range is defined by its start and end keys. For example, a range
// might span from [/Table/1, /Table/2), where the lower bound is inclusive and
// the upper bound is exclusive. Any key that begins with /Table/1, like
// /Table/1/SomePrimaryKeyValue..., would belong to this range.
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
// metadata, range metadata, abort cache values, transaction records,
// range-spanning binary tree node pointers, and message queues.
//
// The local key prefix was deliberately chosen to sort before the meta key
// prefixes, because these local keys are not addressable via the meta range
// addressing indexes.
//
// Some local data is not replicated, such as the store's 'ident' record. Most
// local data is replicated, such as AbortSpan entries and transaction rows, but
// is not addressable as normal MVCC values as part of transactions. Finally,
// some local data are stored as MVCC values and are addressable as part of
// distributed transactions, such as range metadata, range-spanning binary tree
// node pointers, and message queues.
//
// Deciding what type of local key to use is not entirely straightforward.
// Unreplicated local keys must be unaddressable, but replicated local keys can
// be either addressable or unaddressable. We do not have clear guidance on
// which type of replicated key to choose. Note, however, that only addressable
// keys can be the target of KV operations; unaddressable keys can only be
// written as a side-effect of other KV operations. This makes unaddressable
// replicated keys impractical in many circumstances.
package keys
