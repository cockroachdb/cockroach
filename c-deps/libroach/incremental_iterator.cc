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
WARN_UNUSED_RESULT bool DBIncrementalIterator::extractKey(rocksdb::Slice mvcc_key, rocksdb::Slice *key) {
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
// to the earliest version of the next candidate key. Note: it is expected that
// the TBI does not reference a key greater than the main iterator.
void DBIncrementalIterator::maybeSkipKeys() {
  if (time_bound_iter == nullptr) {
    // We don't have a TBI, so we can't skip any keys.
    return;
  }

  rocksdb::Slice tbi_key;
  if(!extractKey(time_bound_iter->rep->key(), &tbi_key)) {
    return;
  }
  rocksdb::Slice iter_key;
  if(!extractKey(time_bound_iter->rep->key(), &iter_key)) {
    return;
  }

  if (iter_key.compare(tbi_key) > 0) {
    // If the iterKey got ahead of the TBI key, advance the TBI Key.
    auto state = DBIterNext(time_bound_iter.get(), true /* skip_current_key_versions */);
    if (!state.valid) {
        status = state.status;
        valid = false;
        return;
    }

    if(!extractKey(time_bound_iter->rep->key(), &tbi_key)) {
      return;
    }

    // The fast-path is when the TBI and the main iterator are in lockstep. In
    // this case, the main iterator was referencing the next key that would be
    // visited by the TBI. Now they should be referencing the same MVCC key, so
    // the if statement below will not be entered. This means that for the
    // incremental iterator to perform a Next or NextKey in this case, it will
    // require only 1 extra NextKey call to the TBI.
    if (iter_key.compare(tbi_key) < 0) {
      // In the case that the next MVCC key that the TBI observes is not the
      // same as the main iterator, we may be able to skip over a large group of
      // keys. The main iterator is seeked to the TBI in hopes that many keys
      // were skipped. Note that a Seek() is an order of magnitude more
      // expensive than a Next().
      state = DBIterSeek(iter.get(), ToDBKey(tbi_key));
      if (!state.valid) {
        status = state.status;
        valid = false;
        return;
      }
    }
  }
}

// advanceKey advances the main iterator until it is referencing a key whose
// version lies in (start_time, end_time]. It may return an error if it
// encounters an unexpected key such as a key with an inline value.
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

    cockroach::storage::engine::enginepb::MVCCMetadata meta;
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

DBIterState DBIncrementalIterator::seek(DBKey key) {
  if (time_bound_iter != nullptr) {
    auto state = DBIterSeek(time_bound_iter.get(), key);
    if (!state.valid) {
      status = state.status;
      valid = false;
      return getState();
    }
    const rocksdb::Slice tbi_key(time_bound_iter->rep->key());
    const rocksdb::Slice iter_key(key.key.data, key.key.len);
    if (tbi_key.compare(iter_key) > 0) {
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
