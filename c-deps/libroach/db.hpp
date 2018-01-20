// Copyright 2018 The Cockroach Authors.
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

#include "protos/storage/engine/enginepb/mvcc.pb.h"

template <bool reverse> class mvccScanner {
 public:
  DBIterator* const iter_;
  rocksdb::Iterator* const iter_rep_;
  const rocksdb::Slice start_key_;
  const rocksdb::Slice end_key_;
  const int64_t max_keys_;
  const DBTimestamp timestamp_;
  const rocksdb::Slice txn_id_;
  const uint32_t txn_epoch_;
  const DBTimestamp txn_max_timestamp_;
  const bool consistent_;
  const bool check_uncertainty_;
  DBScanResults results_;
  std::unique_ptr<rocksdb::WriteBatch> kvs_;
  std::unique_ptr<rocksdb::WriteBatch> intents_;
  std::string key_buf_;
  std::string saved_buf_;
  bool peeked_;
  bool is_get_;
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

mvccScanner(DBIterator* iter, DBSlice start, DBSlice end, DBTimestamp timestamp, int64_t max_keys,
              DBTxn txn, bool consistent);
  const DBScanResults& get() ;
  const DBScanResults& scan() ;
    private:
  const DBScanResults& fillResults() ;
  bool uncertaintyError(DBTimestamp ts) ;
  bool setStatus(const DBStatus& status) ;
  bool getAndAdvance() ;
  bool nextKey() ;
  bool backwardLatestVersion(const rocksdb::Slice& key, int i) ;
  bool prevKey(const rocksdb::Slice& key) ;
  bool advanceKey() ;
  bool advanceKeyAtEnd() ;
  bool advanceKeyAtNewKey(const rocksdb::Slice& key) ;
  bool addAndAdvance(const rocksdb::Slice& value) ;
  bool seekVersion(DBTimestamp desired_timestamp, bool check_uncertainty) ;
  bool updateCurrent() ;
  bool iterSeek(const rocksdb::Slice& key) ;
  bool iterSeekReverse(const rocksdb::Slice& key) ;
  bool iterNext() ;
  bool iterPrev() ;
  bool iterPeekPrev(rocksdb::Slice* peeked_key) ;
  void clearPeeked() ;
};

typedef mvccScanner<false> mvccForwardScanner;
typedef mvccScanner<true> mvccReverseScanner;
