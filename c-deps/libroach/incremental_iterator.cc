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

  sanity_iter.reset(NULL);

  start_time.set_wall_time(start.wall_time);
  start_time.set_logical(start.logical);
  end_time.set_wall_time(end.wall_time);
  end_time.set_logical(end.logical);

  // sanity_iter is only relevant if a time-bound iterator is required.
  //
  // It is necessary for correctness that sanity_iter be created before
  // iter. This is because the provided Reader may not be a consistent
  // snapshot, so the two could end up observing different information. The hack
  // around sanityCheckMetadataKey only works properly if all possible
  // discrepancies between the two iterators lead to intents and values falling
  // outside of the timestamp range **from iter's perspective**. This allows us
  // to simply ignore discrepancies that we notice in advance(). See #34819.
  if (!EmptyTimestamp(opts.min_timestamp_hint) || !EmptyTimestamp(opts.max_timestamp_hint)) {
    assert(!EmptyTimestamp(opts.max_timestamp_hint));
    DBIterOptions nontimebound_opts = DBIterOptions();
    nontimebound_opts.upper_bound = opts.upper_bound;
    sanity_iter.reset(DBNewIter(engine, nontimebound_opts));
  }
  iter.reset(DBNewIter(engine, opts));
}

DBIncrementalIterator::~DBIncrementalIterator() {}

// sanityCheckMetadataKey looks up the current `iter` key using a normal,
// non-time-bound iterator and returns the value if the normal iterator also
// sees that exact key. Otherwise, it returns false. It's used in the workaround
// in `advanceKey` for a time-bound iterator bug.
rocksdb::Slice DBIncrementalIterator::sanityCheckMetadataKey() {
  // If sanityIter is not set, it's because we're not using time-bound
  // iterator and we don't need the sanity check.
  if (sanity_iter == NULL) {
    valid = true;
    status = ToDBStatus(rocksdb::Status::OK());
    return iter.get()->rep->value();
  }

  auto sanity_iter_rep = sanity_iter->rep.get();
  sanity_iter_rep->Seek(iter->rep->key());

  if (!sanity_iter_rep->status().ok()) {
    valid = false;
    status = ToDBStatus(sanity_iter_rep->status());
    return NULL;
  } else if (!sanity_iter_rep->Valid()) {
    valid = false;
    status = ToDBStatus(rocksdb::Status::OK());
    return NULL;
  } else if (kComparator.Compare(sanity_iter_rep->key(), iter->rep->key()) != 0) {
    valid = false;
    status = ToDBStatus(rocksdb::Status::OK());
    return NULL;
  }

  valid = true;
  status = ToDBStatus(rocksdb::Status::OK());
  return sanity_iter.get()->rep->value();
}

// legacyTimestampIsLess compares the timestamps t1 and t2, and returns a
// boolean indicating whether t1 is less than t2.
bool DBIncrementalIterator::legacyTimestampIsLess(const cockroach::util::hlc::LegacyTimestamp& t1,
                                                  const cockroach::util::hlc::LegacyTimestamp& t2) {
  return t1.wall_time() < t2.wall_time() ||
         (t1.wall_time() == t2.wall_time() && t1.logical() < t2.logical());
}

// advanceKey finds the key and its appropriate version which lies in
// (start_time, end_time].
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
      // HACK(dan): Work around a known bug in the time-bound iterator.
      // For reasons described in #28358, a time-bound iterator will
      // sometimes see an unresolved intent where there is none. A normal
      // iterator doesn't have this problem, so we work around it by
      // double checking all non-value keys. If sanityCheckMetadataKey
      // returns false, then the intent was a phantom and we ignore it.
      // NB: this could return a older/newer intent than the one the time-bound
      // iterator saw; this is handled.
      auto value = sanityCheckMetadataKey();
      if (status.data != NULL) {
        return;
      } else if (!valid) {
        valid = true;
        DBIterNext(iter.get(), false);
        continue;
      }

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
