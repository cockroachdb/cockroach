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
// implied.  See the License for the specific language governing
// permissions and limitations under the License.

#include "encoding.h"
#include <rocksdb/slice.h>
#include "db.h"

namespace cockroach {

char* EncodeVarint32(char* dst, uint32_t v) {
  // Copied from RocksDB's coding.cc.
  // Operate on characters as unsigneds
  unsigned char* ptr = reinterpret_cast<unsigned char*>(dst);
  static const int B = 128;
  if (v < (1 << 7)) {
    *(ptr++) = v;
  } else if (v < (1 << 14)) {
    *(ptr++) = v | B;
    *(ptr++) = v >> 7;
  } else if (v < (1 << 21)) {
    *(ptr++) = v | B;
    *(ptr++) = (v >> 7) | B;
    *(ptr++) = v >> 14;
  } else if (v < (1 << 28)) {
    *(ptr++) = v | B;
    *(ptr++) = (v >> 7) | B;
    *(ptr++) = (v >> 14) | B;
    *(ptr++) = v >> 21;
  } else {
    *(ptr++) = v | B;
    *(ptr++) = (v >> 7) | B;
    *(ptr++) = (v >> 14) | B;
    *(ptr++) = (v >> 21) | B;
    *(ptr++) = v >> 28;
  }
  return reinterpret_cast<char*>(ptr);
}

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

// MVCC keys are encoded as <key>[<wall_time>[<logical>]]<#timestamp-bytes>. A
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

// MVCC keys are encoded as <key>[<wall_time>[<logical>]]<#timestamp-bytes>. A
// custom RocksDB comparator (DBComparator) is used to maintain the desired
// ordering as these keys do not sort lexicographically correctly.
std::string EncodeKey(DBKey k) { return EncodeKey(ToSlice(k.key), k.wall_time, k.logical); }

WARN_UNUSED_RESULT bool SplitKey(rocksdb::Slice buf, rocksdb::Slice* key,
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

}  // namespace cockroach
