#include "libroach.h"
#include "encoding.h"
#include <rocksdb/status.h>
#include "protos/util/hlc/timestamp.pb.h"
#include "protos/util/hlc/legacy_timestamp.pb.h"

// TODO(tschottdorf): consolidate with db.cc
#if defined(COMPILER_GCC) || defined(__clang__)
#define WARN_UNUSED_RESULT __attribute__((warn_unused_result))
#else
#define WARN_UNUSED_RESULT
#endif

const int kMVCCVersionTimestampSize = 12;

const DBTimestamp kZeroTimestamp = {0, 0};

inline bool operator==(const DBTimestamp& a, const DBTimestamp& b) {
  return a.wall_time == b.wall_time && a.logical == b.logical;
}

inline bool operator!=(const DBTimestamp& a, const DBTimestamp& b) { return !(a == b); }

inline bool operator<(const DBTimestamp& a, const DBTimestamp& b) {
  return a.wall_time < b.wall_time || (a.wall_time == b.wall_time && a.logical < b.logical);
}

inline bool operator>(const DBTimestamp& a, const DBTimestamp& b) { return b < a; }

inline bool operator<=(const DBTimestamp& a, const DBTimestamp& b) { return !(b < a); }

inline bool operator>=(const DBTimestamp& a, const DBTimestamp& b) { return b <= a; }

DBTimestamp ToDBTimestamp(const cockroach::util::hlc::LegacyTimestamp& timestamp);

DBTimestamp PrevTimestamp(DBTimestamp ts);

void EncodeTimestamp(std::string& s, int64_t wall_time, int32_t logical);

std::string EncodeTimestamp(DBTimestamp ts);

std::string ToString(DBSlice s);

std::string ToString(DBString s);

rocksdb::Slice ToSlice(DBSlice s);

rocksdb::Slice ToSlice(DBString s);

std::string EncodeKey(const rocksdb::Slice& key, int64_t wall_time, int32_t logical);

std::string EncodeKey(DBKey k);

bool SplitKey(rocksdb::Slice buf, rocksdb::Slice* key,
                                 rocksdb::Slice* timestamp);

bool DecodeTimestamp(rocksdb::Slice* timestamp, int64_t* wall_time,
                                        int32_t* logical);

bool DecodeHLCTimestamp(rocksdb::Slice buf,
                                           cockroach::util::hlc::Timestamp* timestamp);

WARN_UNUSED_RESULT inline bool DecodeKey(rocksdb::Slice buf, rocksdb::Slice* key,
                                         int64_t* wall_time, int32_t* logical) {
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

WARN_UNUSED_RESULT bool inline DecodeKey(rocksdb::Slice buf, rocksdb::Slice* key, DBTimestamp* ts) {
  return DecodeKey(buf, key, &ts->wall_time, &ts->logical);
}

rocksdb::Slice KeyPrefix(const rocksdb::Slice& src);

DBSlice ToDBSlice(const rocksdb::Slice& s);

DBSlice ToDBSlice(const DBString& s);

DBString ToDBString(const rocksdb::Slice& s);

DBKey ToDBKey(const rocksdb::Slice& s);

DBStatus ToDBStatus(const rocksdb::Status& status);

DBStatus FmtStatus(const char* fmt_str, ...);

const DBStatus kSuccess = {NULL, 0};
