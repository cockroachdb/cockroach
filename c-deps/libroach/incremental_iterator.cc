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

  time_bound_iter.reset(nullptr);

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

// getKey returns the
std::string DBIncrementalIterator::getKey(rocksdb::Slice mvcc_key) {
  rocksdb::Slice key;
  rocksdb::Slice ts;
  if (!SplitKey(mvcc_key, &key, &ts)) {
    valid = false;
    status = FmtStatus("failed to split mvcc key");
  }
  return key.ToString();
}

// maybeSkipKeys checks if any keys can be skipped by using a time-bound
// iterator. If it can, the main iterator is advanced to the next key
// that should be considered. The TBI will point to a key with the same
// MVCC key.
//
// Pre: The TBI should be pointing to either the same key or the key prior
// to the same key as the main iterator. This is enforced by a combination
// of a) only updating the time-bound iterator from this method, and seeking
// the main iterator to match and b) only calling Next/NextKey on the main
// iterator once between invocations of this method.
//
// Post: If the main iterator was ahead of the TBI, the TBI will be advanced
// and the main iterator will point to the same MVCC key as the TBI, but
// possibly different timestamp.
void DBIncrementalIterator::maybeSkipKeys() {
  if (time_bound_iter != nullptr) {
    // If we have a time bound iterator, we should check if we should skip
    // keys.
    std::string tbi_key = getKey(time_bound_iter->rep->key());
    std::string iter_key = getKey(iter->rep->key());
    if (iter_key.compare(tbi_key) > 0) {
      // If the iterKey got ahead of the TBI key, advance the TBI Key.
      DBIterNext(time_bound_iter.get(), true /* skip_current_key_versions */);
      if (!valid) {
        return;
      }
      if (!time_bound_iter->rep->Valid()) {
        status = ToDBStatus(time_bound_iter->rep->status());
        valid = false;
        return;
      }

      tbi_key = getKey(time_bound_iter->rep->key());
      // The fast-path is when the TBI and the main iterator are in lockstep.
      // In this case, the main iterator was referencing the next key that
      // would be visited by the TBI. Now they should be referencing the same
      // MVCC key, so the if statement below will not be entered.
      // This means that for the incremental iterator to perform a Next or
      // NextKey in this case, it will require only 1 extra NextKey call to the
      // TBI.
      if (iter_key.compare(tbi_key) < 0) {
        // In the case that the next MVCC key that the TBI observes is not the
      	// same as the main iterator, we may be able to skip over a large group
      	// of keys. The main iterator is seeked to the TBI in hopes that many
      	// keys were skipped. Note that a Seek is an order of magnitude more
      	// expensive than a Next call.
        const auto seek_key = rocksdb::Slice(tbi_key);
        const auto iter_rep = iter->rep.get();
        iter_rep->Seek(seek_key);
        if (!iter_rep->status().ok()) {
          valid = false;
          status = ToDBStatus(iter_rep->status());
          return;
        }
      }
    }
  }
}

// advanceKey advances the main iterator until it is referencing a key whose
// version lies in (start_time, end_time]. It may return an error if it
// encounters an unexpected key such as a key with an inline value.
void DBIncrementalIterator::advanceKey() {
  for (;;) {
    if (!valid) {
      return;
    }

    if (!iter.get()->rep->Valid()) {
      status = ToDBStatus(iter.get()->rep->status());
      valid = false;
      return;
    }

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

    if (legacyTimestampIsLess(end_time, meta.timestamp())) {
      DBIterNext(iter.get(), false);
      continue;
    } else if (!legacyTimestampIsLess(start_time, meta.timestamp())) {
      DBIterNext(iter.get(), true);
      continue;
    }

    break;
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
    DBIterSeek(time_bound_iter.get(), key);
    if (!time_bound_iter->rep->Valid()) {
      status = ToDBStatus(time_bound_iter->rep->status());
      valid = false;
      return getState();
    }
    const rocksdb::Slice tbi_key(time_bound_iter->rep->key());
    const rocksdb::Slice iter_key(key.key.data, key.key.len);
    if (tbi_key.compare(iter_key) > 0) {
      key = ToDBKey(rocksdb::Slice(tbi_key));
    }
  }
  DBIterSeek(iter.get(), key);
  advanceKey();
  return getState();
}

DBIterState DBIncrementalIterator::next(bool skip_current_key_versions) {
  DBIterNext(iter.get(), skip_current_key_versions);
  advanceKey();
  return getState();
}

const rocksdb::Slice DBIncrementalIterator::key() { return iter.get()->rep->key(); }

const rocksdb::Slice DBIncrementalIterator::value() { return iter.get()->rep->value(); }
