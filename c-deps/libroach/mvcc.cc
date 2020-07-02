// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include "mvcc.h"
#include "comparator.h"
#include "encoding.h"
#include "keys.h"

using namespace cockroach;

namespace cockroach {

namespace {

bool IsValidSplitKey(const rocksdb::Slice& key) {
  if (key == kMeta2KeyMax) {
    // We do not allow splits at Meta2KeyMax. The reason for this is that range
    // decriptors are stored at RangeMetaKey(range.EndKey), so the new range
    // that ends at Meta2KeyMax would naturally store its decriptor at
    // RangeMetaKey(Meta2KeyMax) = Meta1KeyMax. However, Meta1KeyMax already
    // serves a different role of holding a second copy of the descriptor for
    // the range that spans the meta2/userspace boundary (see case 3a in
    // rangeAddressing). If we allowed splits at Meta2KeyMax, the two roles
    // would overlap. See #1206.
    return false;
  }
  for (auto span : kSortedNoSplitSpans) {
    // kSortedNoSplitSpans is both reverse sorted (largest to smallest) on the
    // span end key which allows us to early exit if our key to check is above
    // the end of the last no-split span.
    if (key.compare(span.second) >= 0) {
      return true;
    }
    if (key.compare(span.first) > 0) {
      return false;
    }
  }
  return true;
}

const int64_t kNanosecondPerSecond = 1e9;

inline int64_t age_factor(int64_t fromNS, int64_t toNS) {
  // Careful about implicit conversions here.
  // toNS/1e9 - fromNS/1e9 is not the same since
  // "1e9" is a double.
  return toNS / kNanosecondPerSecond - fromNS / kNanosecondPerSecond;
}

}  // namespace

// TODO(tschottdorf): it's unfortunate that this method duplicates the logic
// in (*MVCCStats).AgeTo. Passing now_nanos in is semantically tricky if there
// is a chance that we run into values ahead of now_nanos. Instead, now_nanos
// should be taken as a hint but determined by the max timestamp encountered.
//
// This implementation must match engine.ComputeStatsGo.
MVCCStatsResult MVCCComputeStatsInternal(::rocksdb::Iterator* const iter_rep, DBKey start,
                                         DBKey end, int64_t now_nanos) {
  MVCCStatsResult stats;
  memset(&stats, 0, sizeof(stats));

  iter_rep->Seek(EncodeKey(start));
  const std::string end_key = EncodeKey(end);

  cockroach::storage::enginepb::MVCCMetadata meta;
  std::string prev_key;
  bool first = false;
  // NB: making this uninitialized triggers compiler warnings
  // with `-Werror=maybe-uninitialized`. This warning seems like
  // a false positive (changing the above line to `first=true`
  // which results in equivalent code does not remove it either).
  // An assertion has been placed where the compiler would warn.
  int64_t accrue_gc_age_nanos = 0;

  for (; iter_rep->Valid() && kComparator.Compare(iter_rep->key(), end_key) < 0; iter_rep->Next()) {
    const rocksdb::Slice key = iter_rep->key();
    const rocksdb::Slice value = iter_rep->value();

    rocksdb::Slice decoded_key;
    int64_t wall_time = 0;
    int32_t logical = 0;
    if (!DecodeKey(key, &decoded_key, &wall_time, &logical)) {
      stats.status = FmtStatus("unable to decode key");
      return stats;
    }

    // Check for ignored keys.
    if (decoded_key.starts_with(kLocalRangeIDPrefix)) {
      // RangeID-local key.
      int64_t range_id = 0;
      rocksdb::Slice infix, suffix, detail;
      if (!DecodeRangeIDKey(decoded_key, &range_id, &infix, &suffix, &detail)) {
        stats.status = FmtStatus("unable to decode rangeID key");
        return stats;
      }

      if (infix.compare(kLocalRangeIDReplicatedInfix) == 0) {
        // Replicated RangeID-local key.
        if (suffix.compare(kLocalRangeAppliedStateSuffix) == 0) {
          // RangeAppliedState key. Ignore.
          continue;
        }
      }
    }

    const bool isSys = (rocksdb::Slice(decoded_key).compare(kLocalMax) < 0);
    const bool isValue = (wall_time != 0 || logical != 0);
    const bool implicitMeta = isValue && decoded_key != prev_key;
    prev_key.assign(decoded_key.data(), decoded_key.size());

    if (implicitMeta) {
      // No MVCCMetadata entry for this series of keys.
      meta.Clear();
      meta.set_key_bytes(kMVCCVersionTimestampSize);
      meta.set_val_bytes(value.size());
      meta.set_deleted(value.size() == 0);
      meta.mutable_timestamp()->set_wall_time(wall_time);
    }

    if (!isValue || implicitMeta) {
      const int64_t meta_key_size = decoded_key.size() + 1;
      const int64_t meta_val_size = implicitMeta ? 0 : value.size();
      const int64_t total_bytes = meta_key_size + meta_val_size;
      first = true;

      if (!implicitMeta && !meta.ParseFromArray(value.data(), value.size())) {
        stats.status = FmtStatus("unable to decode MVCCMetadata");
        return stats;
      }

      if (isSys) {
        stats.sys_bytes += total_bytes;
        stats.sys_count++;
        if (decoded_key.starts_with(kLocalRangeIDPrefix)) {
          // RangeID-local key.
          int64_t range_id = 0;
          rocksdb::Slice infix, suffix, detail;
          if (!DecodeRangeIDKey(decoded_key, &range_id, &infix, &suffix, &detail)) {
            stats.status = FmtStatus("unable to decode rangeID key");
            return stats;
          }
          if (infix.compare(kLocalRangeIDReplicatedInfix) == 0) {
            // Replicated RangeID-local key.
            if (suffix.compare(kLocalAbortSpanSuffix) == 0) {
              // Abort span key.
              stats.abort_span_bytes += total_bytes;
            }
          }
        }
      } else {
        if (!meta.deleted()) {
          stats.live_bytes += total_bytes;
          stats.live_count++;
        } else {
          stats.gc_bytes_age += total_bytes * age_factor(meta.timestamp().wall_time(), now_nanos);
        }
        stats.key_bytes += meta_key_size;
        stats.val_bytes += meta_val_size;
        stats.key_count++;
        if (meta.has_raw_bytes()) {
          stats.val_count++;
        }
      }
      if (!implicitMeta) {
        continue;
      }
    }

    const int64_t total_bytes = value.size() + kMVCCVersionTimestampSize;
    if (isSys) {
      stats.sys_bytes += total_bytes;
    } else {
      if (first) {
        first = false;
        if (!meta.deleted()) {
          stats.live_bytes += total_bytes;
        } else {
          stats.gc_bytes_age += total_bytes * age_factor(meta.timestamp().wall_time(), now_nanos);
        }
        if (meta.has_txn()) {
          stats.intent_bytes += total_bytes;
          stats.intent_count++;
          stats.intent_age += age_factor(meta.timestamp().wall_time(), now_nanos);
        }
        if (meta.key_bytes() != kMVCCVersionTimestampSize) {
          stats.status = FmtStatus("expected mvcc metadata key bytes to equal %d; got %d",
                                   kMVCCVersionTimestampSize, int(meta.key_bytes()));
          return stats;
        }
        if (meta.val_bytes() != value.size()) {
          stats.status = FmtStatus("expected mvcc metadata val bytes to equal %d; got %d",
                                   int(value.size()), int(meta.val_bytes()));
          return stats;
        }
        accrue_gc_age_nanos = meta.timestamp().wall_time();
      } else {
        bool is_tombstone = value.size() == 0;
        if (is_tombstone) {
          stats.gc_bytes_age += total_bytes * age_factor(wall_time, now_nanos);
        } else {
          assert(accrue_gc_age_nanos > 0);
          stats.gc_bytes_age += total_bytes * age_factor(accrue_gc_age_nanos, now_nanos);
        }
        accrue_gc_age_nanos = wall_time;
      }
      stats.key_bytes += kMVCCVersionTimestampSize;
      stats.val_bytes += value.size();
      stats.val_count++;
    }
  }

  stats.last_update_nanos = now_nanos;
  return stats;
}

}  // namespace cockroach

MVCCStatsResult MVCCComputeStats(DBIterator* iter, DBKey start, DBKey end, int64_t now_nanos) {
  return MVCCComputeStatsInternal(iter->rep.get(), start, end, now_nanos);
}

bool MVCCIsValidSplitKey(DBSlice key) { return IsValidSplitKey(ToSlice(key)); }

DBStatus MVCCFindSplitKey(DBIterator* iter, DBKey start, DBKey min_split, int64_t target_size,
                          DBString* split_key) {
  auto iter_rep = iter->rep.get();
  const std::string start_key = EncodeKey(start);
  iter_rep->Seek(start_key);
  const rocksdb::Slice min_split_key = ToSlice(min_split.key);

  int64_t size_so_far = 0;
  std::string best_split_key = start_key;
  int64_t best_split_diff = std::numeric_limits<int64_t>::max();
  std::string prev_key;

  for (; iter_rep->Valid(); iter_rep->Next()) {
    const rocksdb::Slice key = iter_rep->key();
    rocksdb::Slice decoded_key;
    int64_t wall_time = 0;
    int32_t logical = 0;
    if (!DecodeKey(key, &decoded_key, &wall_time, &logical)) {
      return FmtStatus("unable to decode key");
    }

    const bool valid = IsValidSplitKey(decoded_key) && decoded_key.compare(min_split_key) >= 0;
    int64_t diff = target_size - size_so_far;
    if (diff < 0) {
      diff = -diff;
    }
    if (valid && diff < best_split_diff) {
      best_split_key = decoded_key.ToString();
      best_split_diff = diff;
    }
    // If diff is increasing, that means we've passed the ideal split point and
    // should return the first key that we can. Note that best_split_key may
    // still be empty if we haven't reached min_split_key yet.
    if (diff > best_split_diff && !best_split_key.empty()) {
      break;
    }

    const bool is_value = (wall_time != 0 || logical != 0);
    if (is_value && decoded_key == prev_key) {
      size_so_far += kMVCCVersionTimestampSize + iter_rep->value().size();
    } else {
      size_so_far += decoded_key.size() + 1 + iter_rep->value().size();
      if (is_value) {
        size_so_far += kMVCCVersionTimestampSize;
      }
    }
    prev_key.assign(decoded_key.data(), decoded_key.size());
  }
  if (best_split_key == start_key) {
    return kSuccess;
  }
  *split_key = ToDBString(best_split_key);
  return kSuccess;
}

DBScanResults MVCCGet(DBIterator* iter, DBSlice key, DBTimestamp timestamp, DBTxn txn,
                      bool inconsistent, bool tombstones, bool fail_on_more_recent) {
  // Get is implemented as a scan where we retrieve a single key. We specify an
  // empty key for the end key which will ensure we don't retrieve a key
  // different than the start key. This is a bit of a hack.
  const DBSlice end = {0, 0};
  ScopedStats scoped_iter(iter);
  mvccForwardScanner scanner(iter, key, end, timestamp, 1 /* max_keys */, 0 /* target_bytes */, txn,
                             inconsistent, tombstones, fail_on_more_recent);
  return scanner.get();
}

DBScanResults MVCCScan(DBIterator* iter, DBSlice start, DBSlice end, DBTimestamp timestamp,
                       int64_t max_keys, int64_t target_bytes, DBTxn txn, bool inconsistent,
                       bool reverse, bool tombstones, bool fail_on_more_recent) {
  ScopedStats scoped_iter(iter);
  if (reverse) {
    mvccReverseScanner scanner(iter, end, start, timestamp, max_keys, target_bytes, txn,
                               inconsistent, tombstones, fail_on_more_recent);
    return scanner.scan();
  } else {
    mvccForwardScanner scanner(iter, start, end, timestamp, max_keys, target_bytes, txn, inconsistent, tombstones,
                               fail_on_more_recent);
    return scanner.scan();
  }
}
