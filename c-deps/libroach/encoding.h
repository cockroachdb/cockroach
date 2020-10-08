// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#pragma once

#include <libroach.h>
#include <rocksdb/slice.h>
#include <stdint.h>
#include "defines.h"
#include "protos/storage/enginepb/mvcc.pb.h"

namespace cockroach {

const int kIntZero = 136;
const int kIntSmall = 109;
const int kIntMax = 253;
const int kIntMin = 128;

// EncodeUint32 encodes the uint32 value using a big-endian 4 byte
// representation. The bytes are appended to the supplied buffer.
void EncodeUint32(std::string* buf, uint32_t v);

// EncodeUint64 encodes the uint64 value using a big-endian 8 byte
// representation. The encoded bytes are appended to the supplied buffer.
void EncodeUint64(std::string* buf, uint64_t v);

// EncodeUvarint64 encodes the uint64 value using a variable-length
// representation. The encoded bytes are appended to the supplied buffer.
void EncodeUvarint64(std::string* buf, uint64_t v);

// DecodedUint32 decodes a fixed-length encoded uint32 from a buffer, returning
// true on a successful decode. The decoded value is returned in *value.
bool DecodeUint32(rocksdb::Slice* buf, uint32_t* value);

// DecodedUint64 decodes a fixed-length encoded uint64 from a buffer, returning
// true on a successful decode. The decoded value is returned in *value.
bool DecodeUint64(rocksdb::Slice* buf, uint64_t* value);

// DecodeUvarint64 decodes a variable-length encoded uint64 from a buffer,
// returning true on a successful decode. The decoded value is returned in
// *value.
bool DecodeUvarint64(rocksdb::Slice* buf, uint64_t* value);

const int kMVCCVersionTimestampSize = 12;

void EncodeTimestamp(std::string& s, int64_t wall_time, int32_t logical);
std::string EncodeTimestamp(DBTimestamp ts);

// MVCC keys are encoded as <key>\x00[<wall_time>[<logical>]]<#timestamp-bytes>. A
// custom RocksDB comparator (DBComparator) is used to maintain the desired
// ordering as these keys do not sort lexicographically correctly.
std::string EncodeKey(const rocksdb::Slice& key, int64_t wall_time, int32_t logical);

// MVCC keys are encoded as <key>\x00[<wall_time>[<logical>]]<#timestamp-bytes>. A
// custom RocksDB comparator (DBComparator) is used to maintain the desired
// ordering as these keys do not sort lexicographically correctly.
std::string EncodeKey(DBKey k);

// SplitKey splits an MVCC key into key and timestamp slices. See also
// DecodeKey if you want to decode the timestamp. Returns true on
// success and false on any decoding error.
WARN_UNUSED_RESULT inline bool SplitKey(rocksdb::Slice buf, rocksdb::Slice* key,
                                        rocksdb::Slice* timestamp) {
  if (buf.empty()) {
    return false;
  }
  const char ts_size = buf[buf.size() - 1];
  if (ts_size >= buf.size()) {
    return false;
  }
  *key = rocksdb::Slice(buf.data(), buf.size() - ts_size - 1);
  *timestamp = rocksdb::Slice(key->data() + key->size(), ts_size);
  return true;
}

// DecodeTimestamp an MVCC encoded timestamp. Returns true on success
// and false on any decoding error.
WARN_UNUSED_RESULT bool DecodeTimestamp(rocksdb::Slice* timestamp, int64_t* wall_time,
                                        int32_t* logical);
WARN_UNUSED_RESULT bool DecodeTimestamp(rocksdb::Slice buf,
                                        cockroach::util::hlc::Timestamp* timestamp);

// EmptyTimestamp returns whether ts represents an empty timestamp where both
// the wall_time and logical components are zero.
bool EmptyTimestamp(DBTimestamp ts);

// DecodeKey splits an MVCC key into a key slice and decoded
// timestamp. See also SplitKey if you want to do not need to decode
// the timestamp. Returns true on success and false on any decoding
// error.
WARN_UNUSED_RESULT bool DecodeKey(rocksdb::Slice buf, rocksdb::Slice* key, int64_t* wall_time,
                                  int32_t* logical);
WARN_UNUSED_RESULT inline bool DecodeKey(rocksdb::Slice buf, rocksdb::Slice* key, DBTimestamp* ts) {
  return DecodeKey(buf, key, &ts->wall_time, &ts->logical);
}

const int kLocalSuffixLength = 4;

// DecodeRangeIDKey parses a local range ID key into range ID, infix,
// suffix, and detail.
WARN_UNUSED_RESULT bool DecodeRangeIDKey(rocksdb::Slice buf, int64_t* range_id,
                                         rocksdb::Slice* infix, rocksdb::Slice* suffix,
                                         rocksdb::Slice* detail);

// KeyPrefix strips the timestamp from an MVCC encoded key, returning
// a slice that is still MVCC encoded. This is used by the prefix
// extractor used to build bloom filters on the prefix.
rocksdb::Slice KeyPrefix(const rocksdb::Slice& src);

// IsInt peeks at the type of the value encoded at the start of buf and checks
// if it is of type int.
WARN_UNUSED_RESULT bool IsInt(rocksdb::Slice* buf);

// StripTenantPrefix validates that the given key has a tenant prefix. On
// completion, buf holds the remainder of the key (with the prefix removed).
WARN_UNUSED_RESULT bool StripTenantPrefix(rocksdb::Slice* buf);

// DecodeTenantAndTablePrefix validates that the given key has a tenant and
// table prefix. On completion, buf holds the remainder of the key (with the
// prefix removed) and tbl stores the decoded descriptor ID of the table.
WARN_UNUSED_RESULT bool DecodeTenantAndTablePrefix(rocksdb::Slice* buf, uint64_t* tbl);

}  // namespace cockroach
