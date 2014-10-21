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
The Engine interface provides an API for key-value stores. InMem
implements an in-memory engine using a sorted map. RocksDB implements
an engine for data stored to local disk using RocksDB, a variant of
LevelDB.

MVCC provides a multi-version concurrency control system on top of an
engine. MVCC is the basis for Cockroach's support for distributed
transactions. It is intended for direct use from storage.Range
objects.

Some notes on MVCC architecture

Each MVCC value contains a metadata key/value pair and one more more
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
of timestamp. 8 bytes for the nanosecond wall time, followed by 4
bytes for the logical time. The MVCC version value is a message of
type proto.MVCCValue which indicates whether the version is a deletion
timestamp and if not, contains a proto.Value object which holds the
actual value. The decreasing encoding on the timestamp sorts the most
recent version directly after the metadata key. This increasing the
likelihood that an Engine.Get() of the MVCC metadata will get the same
block containing the most recent version, even in the event there are
many versions. We rely on getting the MVCC metadata key/value and then
using it to directly get the MVCC version using the metadata's most
recent version timestamp to avoid using an expensive merge iterator to
scan the most recent version. This also allows us to leverage leveldb's
bloom filters.

The 7-bit binary encoding used on the MVCC keys allows arbitrary keys
to be stored in the map (no restrictions on intermediate nil-bytes,
for example), while still sorting lexicographically and guaranteeing
that all timestamp-suffixed MVCC version keys sort consecutively with
the metadata key. It should be noted that the 7-bit binary encoding is
distasteful and we'd like to substitute it with something which
preserves at least 7-bit ascii visibility, but has the same sort
properties. We considered using leveldb's custom key comparator
functionality, but the attendant risks seemed too great. What risks?
Mostly that RocksDB is unlikely to have tested custom key comparators
with their more advanced (and ever-growing) functionality. Further,
bugs in our code (both C++ and Go) related to the custom comparator
seemed more likely to be painful than just dealing with the 7-bit
binary encoding.

Another design point with addressing: does it make sense to inline the
most recent MVCC version with the MVCCMetadata. The pros are that the
total byte count would more closely track the "expected" byte count
when writing single key/value pairs to the store. Also, you might
expected better scan efficiency, though this wasn't confirmed through
benchmarks. It might also be the case that multiple versions in many
databases turns out to be the exception, no the rule. Instead,
Cockroach always writes the version separately from the metadata to
avoid a double write when adding all versions beyond the first (the
original value needs to be rewritten using a version key with
timestamp appended, and new version written as inlined metadata).
Further, on the topic of byte counts, the duplicate key prefixes
between metadata and version keys largely disappears through block
compression.
*/
package engine
