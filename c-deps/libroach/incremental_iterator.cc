// Copyright 2019 The Cockroach Authors.
//
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with the Business
// Source License, use of this software will be governed by the Apache License,
// Version 2.0, included in the file licenses/APL.txt.

#include "incremental_iterator.h"
#include "comparator.h"
#include "encoding.h"
#include "protos/roachpb/errors.pb.h"

using namespace cockroach;

DBIncrementalIterator::DBIncrementalIterator(DBEngine* engine, DBIterOptions opts, DBKey start,
                                             DBKey end, DBString* write_intent)
    : engine(engine),
      opts(opts),
      valid(true),
      status(kSuccess),
      start(start),
      end(end),
      write_intent(write_intent) {

  start_time.set_wall_time(start.wall_time);
  start_time.set_logical(start.logical);
  end_time.set_wall_time(end.wall_time);
  end_time.set_logical(end.logical);

  DBIterOptions iter_opts = opts;
  if (!EmptyTimestamp(opts.min_timestamp_hint) || !EmptyTimestamp(opts.max_timestamp_hint)) {
    assert(!EmptyTimestamp(opts.max_timestamp_hint));
    DBIterOptions nontimebound_opts = DBIterOptions();
    nontimebound_opts.upper_bound = opts.upper_bound;
    iter_opts = nontimebound_opts;
    time_bound_iter.reset(DBNewIter(engine, opts));
  }
  iter.reset(DBNewIter(engine, iter_opts));
}

DBIncrementalIterator::~DBIncrementalIterator() {}

// legacyTimestampIsLess compares the timestamps t1 and t2, and returns a
// boolean indicating whether t1 is less than t2.
bool DBIncrementalIterator::legacyTimestampIsLess(const cockroach::util::hlc::LegacyTimestamp& t1,
                                                  const cockroach::util::hlc::LegacyTimestamp& t2) {
  return t1.wall_time() < t2.wall_time() ||
         (t1.wall_time() == t2.wall_time() && t1.logical() < t2.logical());
}

// extractKey extracts the key portion of the mvcc_key and put it in key. It
// returns a validity indicator.
WARN_UNUSED_RESULT bool DBIncrementalIterator::extractKey(rocksdb::Slice mvcc_key,
                                                          rocksdb::Slice* key) {
  rocksdb::Slice ts;
  if (!SplitKey(mvcc_key, key, &ts)) {
    valid = false;
    status = FmtStatus("failed to split mvcc key");
    return false;
  }
  return true;
}

// maybeSkipKeys checks if any keys can be skipped by using a time-bound
// iterator. If keys can be skipped, it will update the main iterator to point
// to the earliest version of the next candidate key.
// It is expected that TBI is at a key <= main iterator key when calling
// maybeSkipKeys().
void DBIncrementalIterator::maybeSkipKeys() {
  if (time_bound_iter == nullptr) {
    // We don't have a TBI, so we can't skip any keys.
    return;
  }

  rocksdb::Slice tbi_key;
  if (!extractKey(time_bound_iter->rep->key(), &tbi_key)) {
    return;
  }
  rocksdb::Slice iter_key;
  if (!extractKey(iter->rep->key(), &iter_key)) {
    return;
  }

  if (iter_key.compare(tbi_key) > 0) {
    // If the iterKey got ahead of the TBI key, advance the TBI Key.
    //
    // The case where iterKey == tbiKey, after this call, is the fast-path is
    // when the TBI and the main iterator are in lockstep. In this case, the
    // main iterator was referencing the next key that would be visited by the
    // TBI. This means that for the incremental iterator to perform a Next or
    // NextKey will require only 1 extra NextKey invocation while they remain in
    // lockstep. This could be common if most keys are modified or the
    // modifications are clustered in keyspace.
    //
    // NB: The Seek() below is expensive, so we aim to avoid it if both
    // iterators remain in lockstep as described above.
    auto state = DBIterNext(time_bound_iter.get(), true /* skip_current_key_versions */);
    if (!state.valid) {
      status = state.status;
      valid = false;
      return;
    }
    if (!extractKey(time_bound_iter->rep->key(), &tbi_key)) {
      return;
    }

    auto cmp = iter_key.compare(tbi_key);
    if (cmp > 0) {
      // If the tbiKey is still behind the iterKey, the TBI key may be seeing
      // phantom MVCCKey.Keys. These keys may not be seen by the main iterator
      // due to aborted transactions and keys which have been subsumed due to
      // range tombstones. In this case we can SeekGE() the TBI to the main iterator.
      DBKey seek_to = {};
      // NB: We don't ToDBKey as iter_key is already split.
      seek_to.key = ToDBSlice(iter_key);
      state = DBIterSeek(time_bound_iter.get(), seek_to);
      if (!state.valid) {
        status = state.status;
        valid = false;
        return;
      }
      if (!extractKey(time_bound_iter->rep->key(), &tbi_key)) {
        return;
      }
      cmp = iter_key.compare(tbi_key);
    }

    if (cmp < 0) {
      // In the case that the next MVCC key that the TBI observes is not the
      // same as the main iterator, we may be able to skip over a large group of
      // keys. The main iterator is seeked to the TBI in hopes that many keys
      // were skipped. Note that a Seek() is an order of magnitude more
      // expensive than a Next().
      DBKey seek_to = {};
      // NB: We don't ToDBKey as iter_key is already split.
      seek_to.key = ToDBSlice(tbi_key);
      state = DBIterSeek(iter.get(), seek_to);
      if (!state.valid) {
        status = state.status;
        valid = false;
        return;
      }
    }
  }
}

// advanceKey advances the main iterator until it is referencing a key within
// (start_time, end_time].
// It populates i.err with an error if either of the following was encountered:
// a) an inline value
// b) an intent with a timestamp within the incremental iterator's bounds
void DBIncrementalIterator::advanceKey() {
  for (;;) {
    maybeSkipKeys();
    if (!valid) {
      return;
    }

    rocksdb::Slice key;
    int64_t wall_time = 0;
    int32_t logical = 0;
    if (!DecodeKey(iter.get()->rep->key(), &key, &wall_time, &logical)) {
      status = ToDBString("unable to decode key");
      valid = false;
      return;
    }

    cockroach::storage::enginepb::MVCCMetadata meta;
    if (wall_time != 0 || logical != 0) {
      meta.mutable_timestamp()->set_wall_time(wall_time);
      meta.mutable_timestamp()->set_logical(logical);
    } else {
      const auto value = iter->rep->value();
      if (!meta.ParseFromArray(value.data(), value.size())) {
        status = ToDBString("failed to parse meta");
        valid = false;
        return;
      }
    }

    // Check for an inline value, as these are only used in non-user data.
    // They're not needed for backup, so they're not handled by this method.
    // If one shows up, throw an error so it's obvious something is wrong.
    if (meta.has_raw_bytes()) {
      valid = false;
      status = ToDBString("Inline values are unsupported by the IncrementalIterator");
      return;
    }

    if (meta.has_txn()) {
      if (legacyTimestampIsLess(start_time, meta.timestamp()) &&
          !legacyTimestampIsLess(end_time, meta.timestamp())) {
        cockroach::roachpb::WriteIntentError err;
        cockroach::roachpb::Intent* intent = err.add_intents();
        intent->mutable_single_key_span()->set_key(key.data(), key.size());
        intent->mutable_txn()->CopyFrom(meta.txn());

        status = ToDBString("WriteIntentError");
        valid = false;
        *write_intent = ToDBString(err.SerializeAsString());

        return;
      }
    }

    DBIterState state;
    if (legacyTimestampIsLess(end_time, meta.timestamp())) {
      state = DBIterNext(iter.get(), false);
    } else if (!legacyTimestampIsLess(start_time, meta.timestamp())) {
      state = DBIterNext(iter.get(), true);
    } else {
      // We have found a key within the time bounds, break.
      break;
    }

    if (!state.valid) {
      status = state.status;
      valid = false;
      return;
    }
  }
}

DBIterState DBIncrementalIterator::getState() {
  DBIterState state = {};
  state.valid = valid;
  state.status = status;

  if (state.valid) {
    rocksdb::Slice key;
    state.valid = DecodeKey(iter.get()->rep->key(), &key, &state.key.wall_time, &state.key.logical);
    if (state.valid) {
      state.key.key = ToDBSlice(key);
      state.value = ToDBSlice(iter.get()->rep->value());
    }
  }

  return state;
}

// seek advances the iterator to the first key in the engine which is >= the
// provided key. key should be a metadata key to ensure that the iterator has a
// chance to observe any intents on the key if they are there.
DBIterState DBIncrementalIterator::seek(DBKey key) {
  if (time_bound_iter != nullptr) {
    // Check which is the first key seen by the TBI.
    auto state = DBIterSeek(time_bound_iter.get(), key);
    if (!state.valid) {
      status = state.status;
      valid = false;
      return getState();
    }
    const rocksdb::Slice tbi_key(time_bound_iter->rep->key());
    // NB: iter_key needs to be constructed with ToSlice to ensure that an empty
    // rocksdb::Slice is properly created in the common case that key is an
    // empty key (the first key).
    const rocksdb::Slice iter_key(ToSlice(key.key));
    if (tbi_key.compare(iter_key) > 0) {
      // If the first key that the TBI sees is ahead of the given startKey, we
      // can seek directly to the first version of the key.
      key = ToDBKey(tbi_key);
    }
  }
  auto state = DBIterSeek(iter.get(), key);
  if (!state.valid) {
    status = state.status;
    valid = false;
    return getState();
  }
  advanceKey();
  return getState();
}

DBIterState DBIncrementalIterator::next(bool skip_current_key_versions) {
  auto state = DBIterNext(iter.get(), skip_current_key_versions);
  if (!state.valid) {
    status = state.status;
    valid = false;
    return getState();
  }
  advanceKey();
  return getState();
}

const rocksdb::Slice DBIncrementalIterator::key() { return iter.get()->rep->key(); }

const rocksdb::Slice DBIncrementalIterator::value() { return iter.get()->rep->value(); }
