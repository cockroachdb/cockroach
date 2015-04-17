// Copyright 2014 The Cockroach Authors.
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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)

/*
Package engine provides low-level storage. It interacts with storage
backends (e.g. LevelDB, RocksDB, etc.) via the Engine interface. At
one level higher, MVCC provides multi-version concurrency control
capability on top of an Engine instance.

The Engine interface provides an API for key-value stores. InMem
implements an in-memory engine using a sorted map. RocksDB implements
an engine for data stored to local disk using RocksDB, a variant of
LevelDB.

MVCC provides a multi-version concurrency control system on top of an
engine. MVCC is the basis for Cockroach's support for distributed
transactions. It is intended for direct use from storage.Range
objects.

Notes on MVCC architecture

Each MVCC value contains a metadata key/value pair and one or more
version key/value pairs. The MVCC metadata key is the actual key for
the value, binary encoded using the SQL binary encoding scheme which
contains a sentinel byte of 0x25, following by a 7-bit encoding of the
key data with 1s in the high bit and terminated by a nil byte. The
MVCC metadata value is of type proto.MVCCMetadata and contains the
most recent version timestamp and an optional proto.Transaction
message. If set, the most recent version of the MVCC value is a
transactional "intent". It also contains some information on the size
of the most recent version's key and value for efficient stat counter
computations.

Each MVCC version key/value pair has a key which is also
binary-encoded, but is suffixed with a decreasing, big-endian encoding
of the timestamp (8 bytes for the nanosecond wall time, followed by 4
bytes for the logical time). The MVCC version value is a message of
type proto.MVCCValue which indicates whether the version is a deletion
timestamp and if not, contains a proto.Value object which holds the
actual value. The decreasing encoding on the timestamp sorts the most
recent version directly after the metadata key. This increases the
likelihood that an Engine.Get() of the MVCC metadata will get the same
block containing the most recent version, even if there are many
versions. We rely on getting the MVCC metadata key/value and then
using it to directly get the MVCC version using the metadata's most
recent version timestamp. This avoids using an expensive merge
iterator to scan the most recent version. It also allows us to
leverage RocksDB's bloom filters.

The binary encoding used on the MVCC keys allows arbitrary keys to be
stored in the map (no restrictions on intermediate nil-bytes, for
example), while still sorting lexicographically and guaranteeing that
all timestamp-suffixed MVCC version keys sort consecutively with the
metadata key. We use an escape-based encoding which transforms all nul
("\x00") characters in the key and is terminated with the sequence
"\x00\x01", which is guaranteed to not occur elsewhere in the encoded
value.

We considered inlining the most recent MVCC version in the
MVCCMetadata. This would reduce the storage overhead of storing the
same key twice (which is small due to block compression), and the
runtime overhead of two separate DB lookups. On the other hand, all
writes that create a new version of an existing key would incur a
double write as the previous value is moved out of the MVCCMetadata
into its versioned key. Preliminary benchmarks have not shown enough
performance improvement to justify this change, although we may
revisit this decision if it turns out that multiple versions of the
same key are rare in practice.

However, we do allow inlining in order to use the MVCC interface to
store non-versioned values. It turns out that not everything which
Cockroach needs to store would be efficient or possible using MVCC.
Examples include transaction records, response cache entries, stats
counters, time series data, and system-local config values. However,
supporting a mix of encodings is problematic in terms of resulting
complexity. So Cockroach treats an MVCC timestamp of zero to mean an
inlined, non-versioned value. These values are replaced if they exist
on a Put operation and are cleared from the engine on a delete.
Importantly, zero-timestamped MVCC values may be merged, as is
necessary for stats counters and time series data.
*/
package engine
