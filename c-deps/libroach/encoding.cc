// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include "encoding.h"
#include <rocksdb/slice.h>
#include "db.h"
#include "keys.h"

namespace cockroach {

void EncodeUint32(std::string* buf, uint32_t v) {
  const uint8_t tmp[sizeof(v)] = {
      uint8_t(v >> 24),
      uint8_t(v >> 16),
      uint8_t(v >> 8),
      uint8_t(v),
  };
  buf->append(reinterpret_cast<const char*>(tmp), sizeof(tmp));
}

void EncodeUint64(std::string* buf, uint64_t v) {
  const uint8_t tmp[sizeof(v)] = {
      uint8_t(v >> 56), uint8_t(v >> 48), uint8_t(v >> 40), uint8_t(v >> 32),
      uint8_t(v >> 24), uint8_t(v >> 16), uint8_t(v >> 8),  uint8_t(v),
  };
  buf->append(reinterpret_cast<const char*>(tmp), sizeof(tmp));
}

void EncodeUvarint64(std::string* buf, uint64_t v) {
  if (v <= kIntSmall) {
    const uint8_t tmp[1] = {
        uint8_t(v + kIntZero),
    };
    buf->append(reinterpret_cast<const char*>(tmp), sizeof(tmp));
  } else if (v <= 0xff) {
    const uint8_t tmp[2] = {
        uint8_t(kIntMax - 7),
        uint8_t(v),
    };
    buf->append(reinterpret_cast<const char*>(tmp), sizeof(tmp));
  } else if (v <= 0xffff) {
    const uint8_t tmp[3] = {
        uint8_t(kIntMax - 6),
        uint8_t(v >> 8),
        uint8_t(v),
    };
    buf->append(reinterpret_cast<const char*>(tmp), sizeof(tmp));
  } else if (v <= 0xffffff) {
    const uint8_t tmp[4] = {
        uint8_t(kIntMax - 5),
        uint8_t(v >> 16),
        uint8_t(v >> 8),
        uint8_t(v),
    };
    buf->append(reinterpret_cast<const char*>(tmp), sizeof(tmp));
  } else if (v <= 0xffffffff) {
    const uint8_t tmp[5] = {
        uint8_t(kIntMax - 4), uint8_t(v >> 24), uint8_t(v >> 16), uint8_t(v >> 8), uint8_t(v),
    };
    buf->append(reinterpret_cast<const char*>(tmp), sizeof(tmp));
  } else if (v <= 0xffffffffff) {
    const uint8_t tmp[6] = {
        uint8_t(kIntMax - 3), uint8_t(v >> 32), uint8_t(v >> 24),
        uint8_t(v >> 16),     uint8_t(v >> 8),  uint8_t(v),
    };
    buf->append(reinterpret_cast<const char*>(tmp), sizeof(tmp));
  } else if (v <= 0xffffffffffff) {
    const uint8_t tmp[7] = {
        uint8_t(kIntMax - 2), uint8_t(v >> 40), uint8_t(v >> 32), uint8_t(v >> 24),
        uint8_t(v >> 16),     uint8_t(v >> 8),  uint8_t(v),
    };
    buf->append(reinterpret_cast<const char*>(tmp), sizeof(tmp));
  } else if (v <= 0xffffffffffffff) {
    const uint8_t tmp[8] = {
        uint8_t(kIntMax - 1), uint8_t(v >> 48), uint8_t(v >> 40), uint8_t(v >> 32),
        uint8_t(v >> 24),     uint8_t(v >> 16), uint8_t(v >> 8),  uint8_t(v),
    };
    buf->append(reinterpret_cast<const char*>(tmp), sizeof(tmp));
  } else {
    const uint8_t tmp[9] = {
        uint8_t(kIntMax), uint8_t(v >> 56), uint8_t(v >> 48), uint8_t(v >> 40), uint8_t(v >> 32),
        uint8_t(v >> 24), uint8_t(v >> 16), uint8_t(v >> 8),  uint8_t(v),
    };
    buf->append(reinterpret_cast<const char*>(tmp), sizeof(tmp));
  }
}

bool DecodeUint32(rocksdb::Slice* buf, uint32_t* value) {
  const int N = sizeof(*value);
  if (buf->size() < N) {
    return false;
  }
  const uint8_t* b = reinterpret_cast<const uint8_t*>(buf->data());
  *value = (uint32_t(b[0]) << 24) | (uint32_t(b[1]) << 16) | (uint32_t(b[2]) << 8) | uint32_t(b[3]);
  buf->remove_prefix(N);
  return true;
}

bool DecodeUint64(rocksdb::Slice* buf, uint64_t* value) {
  const int N = sizeof(*value);
  if (buf->size() < N) {
    return false;
  }
  const uint8_t* b = reinterpret_cast<const uint8_t*>(buf->data());
  *value = (uint64_t(b[0]) << 56) | (uint64_t(b[1]) << 48) | (uint64_t(b[2]) << 40) |
           (uint64_t(b[3]) << 32) | (uint64_t(b[4]) << 24) | (uint64_t(b[5]) << 16) |
           (uint64_t(b[6]) << 8) | uint64_t(b[7]);
  buf->remove_prefix(N);
  return true;
}

bool DecodeUvarint64(rocksdb::Slice* buf, uint64_t* value) {
  if (buf->size() == 0) {
    return false;
  }
  int length = uint8_t((*buf)[0]) - kIntZero;
  buf->remove_prefix(1);  // skip length byte
  if (length <= kIntSmall) {
    *value = uint64_t(length);
    return true;
  }
  length -= kIntSmall;
  if (length < 0 || length > 8) {
    return false;
  } else if (buf->size() < length) {
    return false;
  }
  *value = 0;
  for (int i = 0; i < length; i++) {
    *value = (*value << 8) | uint8_t((*buf)[i]);
  }
  buf->remove_prefix(length);
  return true;
}

void EncodeTimestamp(std::string& s, int64_t wall_time, int32_t logical) {
  EncodeUint64(&s, uint64_t(wall_time));
  if (logical != 0) {
    EncodeUint32(&s, uint32_t(logical));
  }
}

std::string EncodeTimestamp(DBTimestamp ts) {
  std::string s;
  s.reserve(kMVCCVersionTimestampSize);
  EncodeTimestamp(s, ts.wall_time, ts.logical);
  return s;
}

bool EmptyTimestamp(DBTimestamp ts) { return ts.wall_time == 0 && ts.logical == 0; }

// MVCC keys are encoded as <key>\x00[<wall_time>[<logical>]]<#timestamp-bytes>. A
// custom RocksDB comparator (DBComparator) is used to maintain the desired
// ordering as these keys do not sort lexicographically correctly.
std::string EncodeKey(const rocksdb::Slice& key, int64_t wall_time, int32_t logical) {
  std::string s;
  const bool ts = wall_time != 0 || logical != 0;
  s.reserve(key.size() + 1 + (ts ? 1 + kMVCCVersionTimestampSize : 0));
  s.append(key.data(), key.size());
  if (ts) {
    // Add a NUL prefix to the timestamp data. See DBPrefixExtractor.Transform
    // for more details.
    s.push_back(0);
    EncodeTimestamp(s, wall_time, logical);
  }
  s.push_back(char(s.size() - key.size()));
  return s;
}

// MVCC keys are encoded as <key>\x00[<wall_time>[<logical>]]<#timestamp-bytes>. A
// custom RocksDB comparator (DBComparator) is used to maintain the desired
// ordering as these keys do not sort lexicographically correctly.
std::string EncodeKey(DBKey k) { return EncodeKey(ToSlice(k.key), k.wall_time, k.logical); }

WARN_UNUSED_RESULT bool DecodeTimestamp(rocksdb::Slice* timestamp, int64_t* wall_time,
                                        int32_t* logical) {
  uint64_t w;
  if (!DecodeUint64(timestamp, &w)) {
    return false;
  }
  *wall_time = int64_t(w);
  *logical = 0;
  if (timestamp->size() > 0) {
    // TODO(peter): Use varint decoding here.
    uint32_t l;
    if (!DecodeUint32(timestamp, &l)) {
      return false;
    }
    *logical = int32_t(l);
  }
  return true;
}

WARN_UNUSED_RESULT bool DecodeTimestamp(rocksdb::Slice buf,
                                        cockroach::util::hlc::Timestamp* timestamp) {
  int64_t wall_time;
  int32_t logical;
  if (!DecodeTimestamp(&buf, &wall_time, &logical)) {
    return false;
  }
  timestamp->set_wall_time(wall_time);
  timestamp->set_logical(logical);
  return true;
}

WARN_UNUSED_RESULT bool DecodeKey(rocksdb::Slice buf, rocksdb::Slice* key, int64_t* wall_time,
                                  int32_t* logical) {
  key->clear();

  rocksdb::Slice timestamp;
  if (!SplitKey(buf, key, &timestamp)) {
    return false;
  }
  if (timestamp.size() > 0) {
    timestamp.remove_prefix(1);  // The NUL prefix.
    if (!DecodeTimestamp(&timestamp, wall_time, logical)) {
      return false;
    }
  }
  return timestamp.empty();
}

WARN_UNUSED_RESULT bool DecodeRangeIDKey(rocksdb::Slice buf, int64_t* range_id,
                                         rocksdb::Slice* infix, rocksdb::Slice* suffix,
                                         rocksdb::Slice* detail) {
  if (!buf.starts_with(kLocalRangeIDPrefix)) {
    return false;
  }
  // Cut the prefix, the Range ID, and the infix specifier.
  buf.remove_prefix(kLocalRangeIDPrefix.size());
  uint64_t range_id_uint;
  if (!DecodeUvarint64(&buf, &range_id_uint)) {
    return false;
  }
  *range_id = int64_t(range_id_uint);
  if (buf.size() < kLocalSuffixLength + 1) {
    return false;
  }
  *infix = rocksdb::Slice(buf.data(), 1);
  buf.remove_prefix(1);
  *suffix = rocksdb::Slice(buf.data(), kLocalSuffixLength);
  buf.remove_prefix(kLocalSuffixLength);
  *detail = buf;
  return true;
}

rocksdb::Slice KeyPrefix(const rocksdb::Slice& src) {
  rocksdb::Slice key;
  rocksdb::Slice ts;
  if (!SplitKey(src, &key, &ts)) {
    return src;
  }
  // RocksDB requires that keys generated via Transform be comparable with
  // normal encoded MVCC keys. Encoded MVCC keys have a suffix indicating the
  // number of bytes of timestamp data. MVCC keys without a timestamp have a
  // suffix of 0. We're careful in EncodeKey to make sure that the user-key
  // always has a trailing 0. If there is no timestamp this falls out
  // naturally. If there is a timestamp we prepend a 0 to the encoded
  // timestamp data.
  assert(src.size() > key.size() && src[key.size()] == 0);
  return rocksdb::Slice(key.data(), key.size() + 1);
}

WARN_UNUSED_RESULT bool IsInt(rocksdb::Slice* buf) {
  if (buf->size() > 0) {
    return uint8_t((*buf)[0]) >= kIntMin && uint8_t((*buf)[0]) <= kIntMax;
  }

  return false;
}

WARN_UNUSED_RESULT bool StripTenantPrefix(rocksdb::Slice* buf) {
  if (buf->size() == 0) {
    return true;
  }
  // kTenantPrefix is guaranteed to be a single byte.
  if ((*buf)[0] != kTenantPrefix[0]) {
    return true;
  }
  buf->remove_prefix(1);
  uint64_t tid;
  return DecodeUvarint64(buf, &tid);
}

WARN_UNUSED_RESULT bool DecodeTenantAndTablePrefix(rocksdb::Slice* buf, uint64_t* tbl) {
  if (!StripTenantPrefix(buf)) {
    return false;
  }
  if (!IsInt(buf) || !DecodeUvarint64(buf, tbl)) {
    return false;
  }
  return true;
}
}  // namespace cockroach
