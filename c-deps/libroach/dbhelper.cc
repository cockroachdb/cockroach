#include "dbhelper.h"
#include "fmt.h"

DBTimestamp ToDBTimestamp(const cockroach::util::hlc::LegacyTimestamp& timestamp) {
  return DBTimestamp{timestamp.wall_time(), timestamp.logical()};
}

DBTimestamp PrevTimestamp(DBTimestamp ts) {
  if (ts.logical > 0) {
    --ts.logical;
  } else if (ts.wall_time == 0) {
    fprintf(stderr, "no previous time for zero timestamp\n");
    abort();
  } else {
    --ts.wall_time;
    ts.logical = std::numeric_limits<int32_t>::max();
  }
  return ts;
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

std::string ToString(DBSlice s) { return std::string(s.data, s.len); }

std::string ToString(DBString s) { return std::string(s.data, s.len); }

rocksdb::Slice ToSlice(DBSlice s) { return rocksdb::Slice(s.data, s.len); }

rocksdb::Slice ToSlice(DBString s) { return rocksdb::Slice(s.data, s.len); }

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

WARN_UNUSED_RESULT bool DecodeHLCTimestamp(rocksdb::Slice buf,
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

DBSlice ToDBSlice(const rocksdb::Slice& s) {
  DBSlice result;
  result.data = const_cast<char*>(s.data());
  result.len = s.size();
  return result;
}

DBSlice ToDBSlice(const DBString& s) {
  DBSlice result;
  result.data = s.data;
  result.len = s.len;
  return result;
}

DBString ToDBString(const rocksdb::Slice& s) {
  DBString result;
  result.len = s.size();
  result.data = static_cast<char*>(malloc(result.len));
  memcpy(result.data, s.data(), s.size());
  return result;
}

DBKey ToDBKey(const rocksdb::Slice& s) {
  DBKey key;
  memset(&key, 0, sizeof(key));
  rocksdb::Slice tmp;
  if (DecodeKey(s, &tmp, &key.wall_time, &key.logical)) {
    key.key = ToDBSlice(tmp);
  }
  return key;
}

DBStatus ToDBStatus(const rocksdb::Status& status) {
  if (status.ok()) {
    return kSuccess;
  }
  return ToDBString(status.ToString());
}

DBStatus FmtStatus(const char* fmt_str, ...) {
  va_list ap;
  va_start(ap, fmt_str);
  std::string str;
  fmt::StringAppendV(&str, fmt_str, ap);
  va_end(ap);
  return ToDBString(str);
}
