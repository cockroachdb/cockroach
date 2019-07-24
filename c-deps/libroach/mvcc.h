// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#pragma once

#include <algorithm>
#include "chunked_buffer.h"
#include "db.h"
#include "encoding.h"
#include "iterator.h"
#include "keys.h"
#include "protos/storage/engine/enginepb/mvcc.pb.h"
#include "status.h"
#include "timestamp.h"

namespace cockroach {

// kMaxItersBeforeSeek is the number of calls to iter->{Next,Prev}()
// to perform when looking for the next/prev key or a particular
// version before calling iter->Seek(). Note that mvccScanner makes
// this number adaptive. It starts with a value of kMaxItersPerSeek/2
// and increases the value every time a call to iter->{Next,Prev}()
// successfully finds the desired next key. It decrements the value
// whenever a call to iter->Seek() occurs. The adaptive
// iters-before-seek value is constrained to the range
// [1,kMaxItersBeforeSeek].
static const int kMaxItersBeforeSeek = 10;

// mvccScanner implements the MVCCGet, MVCCScan and MVCCReverseScan
// operations. Parameterizing the code on whether a forward or reverse
// scan is performed allows the different code paths to be compiled
// efficiently while still reusing the common code without difficulty.
//
// WARNING: Do not use iter_rep_->key() or iter_rep_->value()
// directly, use cur_raw_key_, cur_key_, and cur_value instead. In
// order to efficiently support reverse scans, we maintain a single
// entry buffer that allows "peeking" at the previous key. But the
// operation of "peeking" cause iter_rep_->{key,value}() to point to
// different data than what the scanner class considers the "current"
// key/value.
template <bool reverse> class mvccScanner {
 public:
  mvccScanner(DBIterator* iter, DBSlice start, DBSlice end, DBTimestamp timestamp, int64_t max_keys,
              DBTxn txn, bool inconsistent, bool tombstones, bool ignore_sequence)
      : iter_(iter),
        iter_rep_(iter->rep.get()),
        start_key_(ToSlice(start)),
        end_key_(ToSlice(end)),
        max_keys_(max_keys),
        timestamp_(timestamp),
        txn_id_(ToSlice(txn.id)),
        txn_epoch_(txn.epoch),
        txn_sequence_(txn.sequence),
        txn_max_timestamp_(txn.max_timestamp),
        inconsistent_(inconsistent),
        tombstones_(tombstones),
        ignore_sequence_(ignore_sequence),
        check_uncertainty_(timestamp < txn.max_timestamp),
        kvs_(new chunkedBuffer),
        intents_(new rocksdb::WriteBatch),
        peeked_(false),
        iters_before_seek_(kMaxItersBeforeSeek / 2) {
    memset(&results_, 0, sizeof(results_));
    results_.status = kSuccess;

    iter_->kvs.reset();
    iter_->intents.reset();
  }

  // The MVCC data is sorted by key and descending timestamp. If a key
  // has a write intent (i.e. an uncommitted transaction has written
  // to the key) a key with a zero timestamp, with an MVCCMetadata
  // value, will appear. We arrange for the keys to be sorted such
  // that the intent sorts first. For example:
  //
  //   A @ T3
  //   A @ T2
  //   A @ T1
  //   B <intent @ T2>
  //   B @ T2
  //
  // Here we have 2 keys, A and B. Key A has 3 versions, T3, T2 and
  // T1. Key B has 1 version, T1, and an intent. Scanning involves
  // looking for values at a particular timestamp. For example, let's
  // consider scanning this entire range at T2. We'll first seek to A,
  // discover the value @ T3. This value is newer than our read
  // timestamp so we'll iterate to find a value newer than our read
  // timestamp (the value @ T2). We then iterate to the next key and
  // discover the intent at B. What happens with the intent depends on
  // the mode we're reading in and the timestamp of the intent. In
  // this case, the intent is at our read timestamp. If we're
  // performing an inconsistent read we'll return the intent and read
  // at the instant of time just before the intent (for only that
  // key). If we're reading consistently, we'll either return the
  // intent along with an error or read the intent value if we're
  // reading transactionally and we own the intent.

  const DBScanResults& get() {
    if (!iterSeek(EncodeKey(start_key_, 0, 0))) {
      return results_;
    }
    getAndAdvance();
    return fillResults();
  }

  const DBScanResults& scan() {
    // TODO(peter): Remove this timing/debugging code.
    // auto pctx = rocksdb::get_perf_context();
    // pctx->Reset();
    // auto start_time = std::chrono::steady_clock::now();
    // auto elapsed = std::chrono::steady_clock::now() - start_time;
    // auto micros = std::chrono::duration_cast<std::chrono::microseconds>(elapsed).count();
    // printf("seek %d: %s\n", int(micros), pctx->ToString(true).c_str());

    if (reverse) {
      if (!iterSeekReverse(EncodeKey(start_key_, 0, 0))) {
        return results_;
      }
    } else {
      if (!iterSeek(EncodeKey(start_key_, 0, 0))) {
        return results_;
      }
    }

    while (getAndAdvance()) {
    }

    if (kvs_->Count() == max_keys_ && advanceKey()) {
      if (reverse) {
        // It is possible for cur_key_ to be pointing into mvccScanner.saved_buf_
        // instead of iter_rep_'s underlying storage if iterating in reverse (see
        // iterPeekPrev), so copy the key onto the DBIterator struct to ensure it
        // has a lifetime that outlives the DBScanResults.
        iter_->rev_resume_key.assign(cur_key_.data(), cur_key_.size());
        results_.resume_key = ToDBSlice(iter_->rev_resume_key);
      } else {
        results_.resume_key = ToDBSlice(cur_key_);
      }
    }

    return fillResults();
  }

 private:
  const DBScanResults& fillResults() {
    if (results_.status.len == 0) {
      if (kvs_->Count() > 0) {
        kvs_->GetChunks(&results_.data.bufs, &results_.data.len);
        results_.data.count = kvs_->Count();
      }
      if (intents_->Count() > 0) {
        results_.intents = ToDBSlice(intents_->Data());
      }
      iter_->kvs.reset(kvs_.release());
      iter_->intents.reset(intents_.release());
    }
    return results_;
  }

  bool getFromIntentHistory() {
    cockroach::storage::engine::enginepb::MVCCMetadata_SequencedIntent readIntent;
    readIntent.set_sequence(txn_sequence_);
    // Look for the intent with the sequence number less than or equal to the
    // read sequence. To do so, search using upper_bound, which returns an
    // iterator pointing to the first element in the range [first, last) that is
    // greater than value, or last if no such element is found. Then, return the
    // previous value.
    auto up = std::upper_bound(
        meta_.intent_history().begin(), meta_.intent_history().end(), readIntent,
        [](const cockroach::storage::engine::enginepb::MVCCMetadata_SequencedIntent& a,
           const cockroach::storage::engine::enginepb::MVCCMetadata_SequencedIntent& b) -> bool {
          return a.sequence() < b.sequence();
        });
    if (up == meta_.intent_history().begin()) {
      // It is possible that no intent exists such that the sequence is less
      // than the read sequence. In this case, we cannot read a value from the
      // intent history.
      return false;
    }
    const auto intent = *(up - 1);
    rocksdb::Slice value = intent.value();
    if (value.size() > 0 || tombstones_) {
      kvs_->Put(cur_raw_key_, value);
    }
    return true;
  }

  bool uncertaintyError(DBTimestamp ts) {
    results_.uncertainty_timestamp = ts;
    kvs_->Clear();
    intents_->Clear();
    return false;
  }

  bool setStatus(const DBStatus& status) {
    results_.status = status;
    return false;
  }

  bool getAndAdvance() {
    const bool is_value = cur_timestamp_ != kZeroTimestamp;

    if (is_value) {
      if (timestamp_ >= cur_timestamp_) {
        // 1. Fast path: there is no intent and our read timestamp is
        // newer than the most recent version's timestamp.
        return addAndAdvance(cur_value_);
      }

      if (check_uncertainty_) {
        // 2. Our txn's read timestamp is less than the max timestamp
        // seen by the txn. We need to check for clock uncertainty
        // errors.
        if (txn_max_timestamp_ >= cur_timestamp_) {
          return uncertaintyError(cur_timestamp_);
        }
        // Delegate to seekVersion to return a clock uncertainty error
        // if there are any more versions above txn_max_timestamp_.
        return seekVersion(txn_max_timestamp_, true);
      }

      // 3. Our txn's read timestamp is greater than or equal to the
      // max timestamp seen by the txn so clock uncertainty checks are
      // unnecessary. We need to seek to the desired version of the
      // value (i.e. one with a timestamp earlier than our read
      // timestamp).
      return seekVersion(timestamp_, false);
    }

    if (cur_value_.size() == 0) {
      return setStatus(FmtStatus("zero-length mvcc metadata"));
    }

    if (!meta_.ParseFromArray(cur_value_.data(), cur_value_.size())) {
      return setStatus(FmtStatus("unable to decode MVCCMetadata"));
    }

    if (meta_.has_raw_bytes()) {
      // 4. Emit immediately if the value is inline.
      return addAndAdvance(meta_.raw_bytes());
    }

    if (!meta_.has_txn()) {
      return setStatus(FmtStatus("intent without transaction"));
    }

    const bool own_intent = (meta_.txn().id() == txn_id_);
    const DBTimestamp meta_timestamp = ToDBTimestamp(meta_.timestamp());
    // meta_timestamp is the timestamp of an intent value, which we may or may
    // not end up ignoring, depending on factors codified below. If we do ignore
    // the intent then we want to read at a lower timestamp that's strictly
    // below the intent timestamp (to skip the intent), but also does not exceed
    // our read timestamp (to avoid erroneously picking up future committed
    // values); this timestamp is prev_timestamp.
    const DBTimestamp prev_timestamp =
        timestamp_ < meta_timestamp ? timestamp_ : PrevTimestamp(meta_timestamp);
    if (timestamp_ < meta_timestamp && !own_intent) {
      // 5. The key contains an intent, but we're reading before the
      // intent. Seek to the desired version. Note that if we own the
      // intent (i.e. we're reading transactionally) we want to read
      // the intent regardless of our read timestamp and fall into
      // case 8 below.
      return seekVersion(timestamp_, false);
    }

    if (inconsistent_) {
      // 6. The key contains an intent and we're doing an inconsistent
      // read at a timestamp newer than the intent. We ignore the
      // intent by insisting that the timestamp we're reading at is a
      // historical timestamp < the intent timestamp. However, we
      // return the intent separately; the caller may want to resolve
      // it.
      if (kvs_->Count() == max_keys_) {
        // We've already retrieved the desired number of keys and now
        // we're adding the resume key. We don't want to add the
        // intent here as the intents should only correspond to KVs
        // that lie before the resume key.
        return false;
      }
      intents_->Put(cur_raw_key_, cur_value_);
      return seekVersion(prev_timestamp, false);
    }

    if (!own_intent) {
      // 7. The key contains an intent which was not written by our
      // transaction and our read timestamp is newer than that of the
      // intent. Note that this will trigger an error on the Go
      // side. We continue scanning so that we can return all of the
      // intents in the scan range.
      intents_->Put(cur_raw_key_, cur_value_);
      return advanceKey();
    }

    if (txn_epoch_ == meta_.txn().epoch()) {
      if ((ignore_sequence_) || (txn_sequence_ >= meta_.txn().sequence())) {
        // 8. We're reading our own txn's intent at an equal or higher sequence.
        // Note that we read at the intent timestamp, not at our read timestamp
        // as the intent timestamp may have been pushed forward by another
        // transaction. Txn's always need to read their own writes.
        return seekVersion(meta_timestamp, false);
      } else {
        // 9. We're reading our own txn's intent at a lower sequence than is
        // currently present in the intent. This means the intent we're seeing
        // was written at a higher sequence than the read and that there may or
        // may not be earlier versions of the intent (with lower sequence
        // numbers) that we should read. If there exists a value in the intent
        // history that has a sequence number equal to or less than the read
        // sequence, read that value.
        const bool found = getFromIntentHistory();
        if (found) {
          return advanceKey();
        }
        // 10. If no value in the intent history has a sequence number equal to
        // or less than the read, we must ignore the intents laid down by the
        // transaction all together. We ignore the intent by insisting that the
        // timestamp we're reading at is a historical timestamp < the intent
        // timestamp.
        return seekVersion(prev_timestamp, false);
      }
    }

    if (txn_epoch_ < meta_.txn().epoch()) {
      // 11. We're reading our own txn's intent but the current txn has
      // an earlier epoch than the intent. Return an error so that the
      // earlier incarnation of our transaction aborts (presumably
      // this is some operation that was retried).
      return setStatus(FmtStatus("failed to read with epoch %u due to a write intent with epoch %u",
                                 txn_epoch_, meta_.txn().epoch()));
    }

    // 12. We're reading our own txn's intent but the current txn has a
    // later epoch than the intent. This can happen if the txn was
    // restarted and an earlier iteration wrote the value we're now
    // reading. In this case, we ignore the intent and read the
    // previous value as if the transaction were starting fresh.
    return seekVersion(prev_timestamp, false);
  }

  // nextKey advances the iterator to point to the next MVCC key
  // greater than cur_key_. Returns false if the iterator is exhausted
  // or an error occurs.
  bool nextKey() {
    key_buf_.assign(cur_key_.data(), cur_key_.size());

    for (int i = 0; i < iters_before_seek_; ++i) {
      if (!iterNext()) {
        return false;
      }
      if (cur_key_ != key_buf_) {
        iters_before_seek_ = std::max<int>(kMaxItersBeforeSeek, iters_before_seek_ + 1);
        return true;
      }
    }

    // We're pointed at a different version of the same key. Fall back
    // to seeking to the next key. We append 2 NULs to account for the
    // "next-key" and a trailing zero timestamp. See EncodeKey and
    // SplitKey for more details on the encoded key format.
    iters_before_seek_ = std::max<int>(1, iters_before_seek_ - 1);
    key_buf_.append("\0\0", 2);
    return iterSeek(key_buf_);
  }

  // backwardLatestVersion backs up the iterator to the latest version
  // for the specified key. The parameter i is used to maintain the
  // iteration count between the loop here and the caller (usually
  // prevKey). Returns false if an error occurred.
  bool backwardLatestVersion(const rocksdb::Slice& key, int i) {
    key_buf_.assign(key.data(), key.size());

    for (; i < iters_before_seek_; ++i) {
      rocksdb::Slice peeked_key;
      if (!iterPeekPrev(&peeked_key)) {
        return false;
      }
      if (peeked_key != key_buf_) {
        // The key changed which means the current key is the latest
        // version.
        iters_before_seek_ = std::max<int>(kMaxItersBeforeSeek, iters_before_seek_ + 1);
        return true;
      }
      if (!iterPrev()) {
        return false;
      }
    }

    iters_before_seek_ = std::max<int>(1, iters_before_seek_ - 1);
    key_buf_.append("\0", 1);
    return iterSeek(key_buf_);
  }

  // prevKey backs up the iterator to point to the prev MVCC key less
  // than the specified key. Returns false if the iterator is
  // exhausted or an error occurs.
  bool prevKey(const rocksdb::Slice& key) {
    key_buf_.assign(key.data(), key.size());

    for (int i = 0; i < iters_before_seek_; ++i) {
      rocksdb::Slice peeked_key;
      if (!iterPeekPrev(&peeked_key)) {
        return false;
      }
      if (peeked_key.empty()) {
        // `iterPeekPrev()` may return true even when it did not find a key. This
        // case is indicated by `peeked_key.empty()`. In that case there is not
        // going to be any prev key, so we are done.
        return false;
      }
      if (peeked_key != key_buf_) {
        return backwardLatestVersion(peeked_key, i + 1);
      }
      if (!iterPrev()) {
        return false;
      }
    }

    iters_before_seek_ = std::max<int>(1, iters_before_seek_ - 1);
    key_buf_.append("\0", 1);
    return iterSeekReverse(key_buf_);
  }

  // advanceKey advances the iterator to point to the next MVCC
  // key. Returns false if the iterator is exhausted or an error
  // occurs.
  bool advanceKey() {
    if (reverse) {
      return prevKey(cur_key_);
    } else {
      return nextKey();
    }
  }

  bool advanceKeyAtEnd() {
    if (reverse) {
      // Iterating to the next key might have caused the iterator to
      // reach the end of the key space. If that happens, back up to
      // the very last key.
      clearPeeked();
      iter_rep_->SeekToLast();
      if (!updateCurrent()) {
        return false;
      }
      return advanceKey();
    } else {
      // We've reached the end of the iterator and there is nothing
      // left to do.
      return false;
    }
  }

  bool advanceKeyAtNewKey(const rocksdb::Slice& key) {
    if (reverse) {
      // We've advanced to the next key but need to move back to the
      // previous key.
      return prevKey(key);
    } else {
      // We're already at the new key so there is nothing to do.
      return true;
    }
  }

  bool addAndAdvance(const rocksdb::Slice& value) {
    // Don't include deleted versions (value.size() == 0), unless we've been
    // instructed to include tombstones in the results.
    if (value.size() > 0 || tombstones_) {
      kvs_->Put(cur_raw_key_, value);
      if (kvs_->Count() == max_keys_) {
        return false;
      }
    }
    return advanceKey();
  }

  // seekVersion advances the iterator to point to an MVCC version for
  // the specified key that is earlier than <ts_wall_time,
  // ts_logical>. Returns false if the iterator is exhausted or an
  // error occurs. On success, advances the iterator to the next key.
  //
  // If the iterator is exhausted in the process or an error occurs,
  // return false, and true otherwise. If check_uncertainty is true,
  // then observing any version of the desired key with a timestamp
  // larger than our read timestamp results in an uncertainty error.
  //
  // TODO(peter): Passing check_uncertainty as a boolean is a bit
  // ungainly because it makes the subsequent comparison with
  // timestamp_ a bit subtle. Consider passing a
  // uncertainAboveTimestamp parameter. Or better, templatize this
  // method and pass a "check" functor.
  bool seekVersion(DBTimestamp desired_timestamp, bool check_uncertainty) {
    key_buf_.assign(cur_key_.data(), cur_key_.size());

    for (int i = 0; i < iters_before_seek_; ++i) {
      if (!iterNext()) {
        return advanceKeyAtEnd();
      }
      if (cur_key_ != key_buf_) {
        iters_before_seek_ = std::min<int>(kMaxItersBeforeSeek, iters_before_seek_ + 1);
        return advanceKeyAtNewKey(key_buf_);
      }
      if (desired_timestamp >= cur_timestamp_) {
        iters_before_seek_ = std::min<int>(kMaxItersBeforeSeek, iters_before_seek_ + 1);
        if (check_uncertainty && timestamp_ < cur_timestamp_) {
          return uncertaintyError(cur_timestamp_);
        }
        return addAndAdvance(cur_value_);
      }
    }

    iters_before_seek_ = std::max<int>(1, iters_before_seek_ - 1);
    if (!iterSeek(EncodeKey(key_buf_, desired_timestamp.wall_time, desired_timestamp.logical))) {
      return advanceKeyAtEnd();
    }
    if (cur_key_ != key_buf_) {
      return advanceKeyAtNewKey(key_buf_);
    }
    if (desired_timestamp >= cur_timestamp_) {
      if (check_uncertainty && timestamp_ < cur_timestamp_) {
        return uncertaintyError(cur_timestamp_);
      }
      return addAndAdvance(cur_value_);
    }
    return advanceKey();
  }

  bool updateCurrent() {
    if (!iter_rep_->Valid()) {
      return false;
    }
    cur_raw_key_ = iter_rep_->key();
    cur_value_ = iter_rep_->value();
    cur_timestamp_ = kZeroTimestamp;
    if (!DecodeKey(cur_raw_key_, &cur_key_, &cur_timestamp_)) {
      return setStatus(FmtStatus("failed to split mvcc key"));
    }
    return true;
  }

  // iterSeek positions the iterator at the first key that is greater
  // than or equal to key.
  bool iterSeek(const rocksdb::Slice& key) {
    clearPeeked();
    iter_rep_->Seek(key);
    return updateCurrent();
  }

  // iterSeekReverse positions the iterator at the last key that is
  // less than key.
  bool iterSeekReverse(const rocksdb::Slice& key) {
    clearPeeked();

    // `SeekForPrev` positions the iterator at the last key that is less than or
    // equal to `key` AND strictly less than `ReadOptions::iterate_upper_bound`.
    iter_rep_->SeekForPrev(key);
    if (iter_rep_->Valid() && key.compare(iter_rep_->key()) == 0) {
      iter_rep_->Prev();
    }
    if (!updateCurrent()) {
      return false;
    }
    if (cur_timestamp_ == kZeroTimestamp) {
      // We landed on an intent or inline value.
      return true;
    }

    // We landed on a versioned value, we need to back up to find the
    // latest version.
    return backwardLatestVersion(cur_key_, 0);
  }

  bool iterNext() {
    if (reverse && peeked_) {
      // If we had peeked at the previous entry, we need to advance
      // the iterator twice to get to the real next entry.
      peeked_ = false;
      iter_rep_->Next();
      if (!iter_rep_->Valid()) {
        return false;
      }
    }
    iter_rep_->Next();
    return updateCurrent();
  }

  bool iterPrev() {
    if (peeked_) {
      peeked_ = false;
      return updateCurrent();
    }
    iter_rep_->Prev();
    return updateCurrent();
  }

  // iterPeekPrev "peeks" at the previous key before the current
  // iterator position.
  bool iterPeekPrev(rocksdb::Slice* peeked_key) {
    if (!peeked_) {
      peeked_ = true;
      // We need to save a copy of the current iterator key and value
      // and adjust cur_raw_key_, cur_key and cur_value to point to
      // this saved data. We use a single buffer for this purpose:
      // saved_buf_.
      saved_buf_.resize(0);
      saved_buf_.reserve(cur_raw_key_.size() + cur_value_.size());
      saved_buf_.append(cur_raw_key_.data(), cur_raw_key_.size());
      saved_buf_.append(cur_value_.data(), cur_value_.size());
      cur_raw_key_ = rocksdb::Slice(saved_buf_.data(), cur_raw_key_.size());
      cur_value_ = rocksdb::Slice(saved_buf_.data() + cur_raw_key_.size(), cur_value_.size());
      rocksdb::Slice dummy_timestamp;
      if (!SplitKey(cur_raw_key_, &cur_key_, &dummy_timestamp)) {
        return setStatus(FmtStatus("failed to split mvcc key"));
      }

      // With the current iterator state saved we can move the
      // iterator to the previous entry.
      iter_rep_->Prev();
      if (!iter_rep_->Valid()) {
        // Peeking at the previous key should never leave the iterator
        // invalid. Instead, we seek back to the first key and set the
        // peeked_key to the empty key. Note that this prevents using
        // reverse scan to scan to the empty key.
        peeked_ = false;
        *peeked_key = rocksdb::Slice();
        iter_rep_->SeekToFirst();
        return updateCurrent();
      }
    }

    rocksdb::Slice dummy_timestamp;
    if (!SplitKey(iter_rep_->key(), peeked_key, &dummy_timestamp)) {
      return setStatus(FmtStatus("failed to split mvcc key"));
    }
    return true;
  }

  // clearPeeked clears the peeked flag. This should be called before
  // any iterator movement operations on iter_rep_.
  void clearPeeked() {
    if (reverse) {
      peeked_ = false;
    }
  }

 public:
  DBIterator* const iter_;
  rocksdb::Iterator* const iter_rep_;
  const rocksdb::Slice start_key_;
  const rocksdb::Slice end_key_;
  const int64_t max_keys_;
  const DBTimestamp timestamp_;
  const rocksdb::Slice txn_id_;
  const uint32_t txn_epoch_;
  const int32_t txn_sequence_;
  const DBTimestamp txn_max_timestamp_;
  const bool inconsistent_;
  const bool tombstones_;
  const bool ignore_sequence_;
  const bool check_uncertainty_;
  DBScanResults results_;
  std::unique_ptr<chunkedBuffer> kvs_;
  std::unique_ptr<rocksdb::WriteBatch> intents_;
  std::string key_buf_;
  std::string saved_buf_;
  bool peeked_;
  cockroach::storage::engine::enginepb::MVCCMetadata meta_;
  // cur_raw_key_ holds either iter_rep_->key() or the saved value of
  // iter_rep_->key() if we've peeked at the previous key (and peeked_
  // is true).
  rocksdb::Slice cur_raw_key_;
  // cur_key_ is the decoded MVCC key, separated from the timestamp
  // suffix.
  rocksdb::Slice cur_key_;
  // cur_value_ holds either iter_rep_->value() or the saved value of
  // iter_rep_->value() if we've peeked at the previous key (and
  // peeked_ is true).
  rocksdb::Slice cur_value_;
  // cur_timestamp_ is the timestamp for a decoded MVCC key.
  DBTimestamp cur_timestamp_;
  int iters_before_seek_;
};

typedef mvccScanner<false> mvccForwardScanner;
typedef mvccScanner<true> mvccReverseScanner;

}  // namespace cockroach
