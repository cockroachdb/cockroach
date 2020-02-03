// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include "batch.h"
#include "comparator.h"
#include "db.h"
#include "defines.h"
#include "encoding.h"
#include "getter.h"
#include "iterator.h"
#include "status.h"

namespace cockroach {

namespace {

// ProcessDeltaKey performs the heavy lifting of processing the deltas for
// "key" contained in a batch and determining what the resulting value
// is. "delta" should have been seeked to "key", but may not be pointing to
// "key" if no updates existing for that key in the batch.
//
// Note that RocksDB WriteBatches append updates internally. WBWIIterator
// maintains an index for these updates on <key, seq-num>. Looping over the
// entries in WBWIIterator will return the keys in sorted order and, for each
// key, the updates as they were added to the batch.
//
// Upon return, the delta iterator will point to the next entry past key. The
// delta iterator may not be valid if the end of iteration was reached.
DBStatus ProcessDeltaKey(Getter* base, rocksdb::WBWIIterator* delta, rocksdb::Slice key,
                         DBString* value) {
  if (value->data != NULL) {
    free(value->data);
  }
  value->data = NULL;
  value->len = 0;

  int count = 0;
  for (; delta->Valid() && delta->Entry().key == key; ++count, delta->Next()) {
    rocksdb::WriteEntry entry = delta->Entry();
    switch (entry.type) {
    case rocksdb::kPutRecord:
      if (value->data != NULL) {
        free(value->data);
      }
      *value = ToDBString(entry.value);
      break;
    case rocksdb::kMergeRecord: {
      DBString existing;
      if (count == 0) {
        // If this is the first record for the key, then we need to
        // merge with the record in base.
        DBStatus status = base->Get(&existing);
        if (status.data != NULL) {
          if (value->data != NULL) {
            free(value->data);
            value->data = NULL;
            value->len = 0;
          }
          return status;
        }
      } else {
        // Merge with the value we've built up so far.
        existing = *value;
        value->data = NULL;
        value->len = 0;
      }
      if (existing.data != NULL) {
        DBStatus status = DBMergeOne(ToDBSlice(existing), ToDBSlice(entry.value), value);
        free(existing.data);
        if (status.data != NULL) {
          return status;
        }
      } else {
        *value = ToDBString(entry.value);
      }
      break;
    }
    case rocksdb::kSingleDeleteRecord:
      // Treating a SingleDelete operation as a standard Delete doesn't
      // quite mirror SingleDelete's expected implementation, but it
      // doesn't violate the operation's contract:
      // https://github.com/facebook/rocksdb/wiki/Single-Delete.
    case rocksdb::kDeleteRecord:
      if (value->data != NULL) {
        free(value->data);
      }
      // This mirrors the logic in DBGet(): a deleted entry is
      // indicated by a value with NULL data.
      value->data = NULL;
      value->len = 0;
      break;
    default:
      break;
    }
  }

  if (count > 0) {
    return kSuccess;
  }
  return base->Get(value);
}

// This was cribbed from RocksDB and modified to support merge
// records. A BaseDeltaIterator is an iterator which provides a merged
// view of a base iterator and a delta where the delta iterator is
// from a WriteBatchWithIndex.
//
// The prefix_same_as_start and upper_bound settings must also be applied to
// base_iterator for correct operation.
class BaseDeltaIterator : public rocksdb::Iterator {
 public:
  BaseDeltaIterator(rocksdb::Iterator* base_iterator, rocksdb::WBWIIterator* delta_iterator,
                    bool prefix_same_as_start, const rocksdb::Slice* upper_bound)
      : current_at_base_(true),
        equal_keys_(false),
        status_(rocksdb::Status::OK()),
        base_iterator_(base_iterator),
        delta_iterator_(delta_iterator),
        prefix_same_as_start_(prefix_same_as_start),
        upper_bound_(upper_bound) {
    merged_.data = NULL;
  }

  virtual ~BaseDeltaIterator() { ClearMerged(); }

  bool Valid() const override {
    return status_.ok() && (current_at_base_ ? BaseValid() : DeltaValid());
  }

  void SeekToFirst() override {
    base_iterator_->SeekToFirst();
    delta_iterator_->SeekToFirst();
    UpdateCurrent(false /* no prefix check */);
    MaybeSavePrefixStart();
  }

  void SeekToLast() override {
    prefix_start_key_.clear();
    base_iterator_->SeekToLast();
    delta_iterator_->SeekToLast();
    UpdateCurrent(false /* no prefix check */);
    MaybeSavePrefixStart();
  }

  void Seek(const rocksdb::Slice& k) override {
    if (prefix_same_as_start_) {
      prefix_start_key_ = KeyPrefix(k);
    }
    base_iterator_->Seek(k);
    delta_iterator_->Seek(k);
    UpdateCurrent(prefix_same_as_start_);

    // Similar to MaybeSavePrefixStart, but we can avoid computing the
    // prefix again.
    if (prefix_same_as_start_) {
      if (Valid()) {
        prefix_start_buf_ = prefix_start_key_.ToString();
        prefix_start_key_ = prefix_start_buf_;
      } else {
        prefix_start_key_.clear();
      }
    }
  }

  void Next() override {
    if (!Valid()) {
      status_ = rocksdb::Status::NotSupported("Next() on invalid iterator");
    }
    Advance();
  }

  void SeekForPrev(const rocksdb::Slice& k) override {
    status_ = rocksdb::Status::NotSupported("SeekForPrev() not supported");
  }

  void Prev() override { status_ = rocksdb::Status::NotSupported("Prev() not supported"); }

  rocksdb::Slice key() const override {
    return current_at_base_ ? base_iterator_->key() : delta_key_;
  }

  rocksdb::Slice value() const override {
    if (current_at_base_) {
      return base_iterator_->value();
    }
    return ToSlice(merged_);
  }

  rocksdb::Status status() const override {
    if (!status_.ok()) {
      return status_;
    }
    if (!base_iterator_->status().ok()) {
      return base_iterator_->status();
    }
    return delta_iterator_->status();
  }

 private:
  // -1 -- delta less advanced than base
  // 0 -- delta == base
  // 1 -- delta more advanced than base
  int Compare() const {
    assert(delta_iterator_->Valid() && base_iterator_->Valid());
    return kComparator.Compare(delta_iterator_->Entry().key, base_iterator_->key());
  }

  // Advance the iterator to the next key, advancing either the base
  // or delta iterators or both.
  void Advance() {
    if (equal_keys_) {
      assert(BaseValid() && DeltaValid());
      AdvanceBase();
      AdvanceDelta();
    } else {
      if (current_at_base_) {
        assert(BaseValid());
        AdvanceBase();
      } else {
        assert(DeltaValid());
        AdvanceDelta();
      }
    }
    UpdateCurrent(prefix_same_as_start_);
  }

  // Advance the delta iterator, clearing any cached (merged) value
  // the delta iterator was pointing at.
  void AdvanceDelta() {
    delta_iterator_->Next();
    ClearMerged();
  }

  // Process the current entry the delta iterator is pointing at. This
  // is needed to handle merge operations. Note that all of the
  // entries for a particular key are stored consecutively in the
  // write batch with the "earlier" entries appearing first. Returns
  // true if the current entry is deleted and false otherwise.
  bool ProcessDelta() WARN_UNUSED_RESULT {
    IteratorGetter base(equal_keys_ ? base_iterator_.get() : NULL);
    // The contents of WBWIIterator.Entry() are only valid until the
    // next mutation to the write batch. So keep a copy of the key
    // we're pointing at.
    delta_key_ = delta_iterator_->Entry().key.ToString();
    DBStatus status = ProcessDeltaKey(&base, delta_iterator_.get(), delta_key_, &merged_);
    if (status.data != NULL) {
      status_ = rocksdb::Status::Corruption("unable to merge records");
      free(status.data);
      return false;
    }

    // We advanced past the last entry for key and want to back up the
    // delta iterator, but we can only back up if the iterator is
    // valid.
    if (delta_iterator_->Valid()) {
      delta_iterator_->Prev();
    } else {
      delta_iterator_->SeekToLast();
    }

    return merged_.data == NULL;
  }

  // Advance the base iterator.
  void AdvanceBase() { base_iterator_->Next(); }

  // Save the prefix start key if prefix iteration is enabled. The
  // prefix start key is the prefix of the key that was seeked to. See
  // also Seek() where similar code is inlined.
  void MaybeSavePrefixStart() {
    if (prefix_same_as_start_) {
      if (Valid()) {
        prefix_start_buf_ = KeyPrefix(key()).ToString();
        prefix_start_key_ = prefix_start_buf_;
      } else {
        prefix_start_key_.clear();
      }
    }
  }

  // CheckPrefix checks the specified key against the prefix being
  // iterated over (if restricted), returning true if the key exceeds
  // the iteration boundaries.
  bool CheckPrefix(const rocksdb::Slice key) { return KeyPrefix(key) != prefix_start_key_; }

  bool BaseValid() const { return base_iterator_->Valid(); }

  bool DeltaValid() const { return delta_iterator_->Valid(); }

  // Update the state for the iterator. The check_prefix parameter
  // specifies whether iteration should stop if the next non-deleted
  // key has a prefix that differs from prefix_start_key_.
  //
  // UpdateCurrent is the work horse of the BaseDeltaIterator methods
  // and contains the logic for advancing either the base or delta
  // iterators or both, as well as overlaying the delta iterator state
  // on the base iterator.
  void UpdateCurrent(bool check_prefix) {
    ClearMerged();

    for (;;) {
      equal_keys_ = false;
      if (!BaseValid()) {
        // Base has finished.
        if (!DeltaValid()) {
          // Both base and delta have finished.
          return;
        }
        if (check_prefix && CheckPrefix(delta_iterator_->Entry().key)) {
          // The delta iterator key has a different prefix than the
          // one we're searching for. We set current_at_base_ to true
          // which will cause the iterator overall to be considered
          // not valid (since base currently isn't valid).
          current_at_base_ = true;
          return;
        }
        if (upper_bound_ != nullptr &&
            kComparator.Compare(delta_iterator_->Entry().key, *upper_bound_) >= 0) {
          // The delta iterator key is not less than the upper bound. As above,
          // we set current_at_base_ to true which will cause the iterator
          // overall to be considered not valid (since base currently isn't
          // valid).
          current_at_base_ = true;
          return;
        }
        if (!ProcessDelta()) {
          current_at_base_ = false;
          return;
        }
        // Delta is a deletion tombstone.
        AdvanceDelta();
        continue;
      }

      if (!DeltaValid()) {
        // Delta has finished.
        current_at_base_ = true;
        return;
      }

      // Delta and base are both valid. We need to compare keys to see
      // which to use.

      const int compare = Compare();
      if (compare > 0) {
        // Delta is greater than base (use base). The base iterator enforces
        // its own upper bound.
        current_at_base_ = true;
        return;
      }
      // Delta is less than or equal to base. If check_prefix is true,
      // for base to be valid it has to contain the prefix we were
      // searching for. It follows that delta contains the prefix
      // we're searching for. Similarly, if upper_bound is set, for
      // base to be valid it has to be less than the upper bound.
      // It follows that delta is less than upper_bound.
      if (compare == 0) {
        // Delta is equal to base.
        equal_keys_ = true;
      }
      if (!ProcessDelta()) {
        current_at_base_ = false;
        return;
      }

      // Delta is less than or equal to base and is a deletion
      // tombstone.
      AdvanceDelta();
      if (equal_keys_) {
        AdvanceBase();
      }
    }
  }

  // Clear the merged delta iterator value.
  void ClearMerged() const {
    if (merged_.data != NULL) {
      free(merged_.data);
      merged_.data = NULL;
      merged_.len = 0;
    }
  }

  // Is the iterator currently pointed at the base or delta iterator?
  // Also see equal_keys_ which indicates the base and delta iterator
  // keys are the same and both need to be advanced.
  bool current_at_base_;
  bool equal_keys_;
  mutable rocksdb::Status status_;
  // The merged delta value returned when we're pointed at the delta
  // iterator.
  mutable DBString merged_;
  // The base iterator, presumably obtained from a rocksdb::DB.
  std::unique_ptr<rocksdb::Iterator> base_iterator_;
  // The delta iterator obtained from a rocksdb::WriteBatchWithIndex.
  std::unique_ptr<rocksdb::WBWIIterator> delta_iterator_;
  // The key the delta iterator is currently pointed at. We can't use
  // delta_iterator_->Entry().key due to the handling of merge
  // operations.
  std::string delta_key_;
  // Is this a prefix iterator?
  const bool prefix_same_as_start_;
  // The key prefix that we're restricting iteration to. Only used if
  // prefix_same_as_start_ is true.
  std::string prefix_start_buf_;
  rocksdb::Slice prefix_start_key_;
  const rocksdb::Slice* upper_bound_;
};

class DBBatchInserter : public rocksdb::WriteBatch::Handler {
 public:
  DBBatchInserter(rocksdb::WriteBatchBase* batch) : batch_(batch) {}

  virtual rocksdb::Status PutCF(uint32_t column_family_id, const rocksdb::Slice& key,
                                const rocksdb::Slice& value) {
    if (column_family_id != 0) {
      return rocksdb::Status::InvalidArgument("DBBatchInserter: column families not supported");
    }
    return batch_->Put(key, value);
  }
  virtual rocksdb::Status DeleteCF(uint32_t column_family_id, const rocksdb::Slice& key) {
    if (column_family_id != 0) {
      return rocksdb::Status::InvalidArgument("DBBatchInserter: column families not supported");
    }
    return batch_->Delete(key);
  }
  virtual rocksdb::Status SingleDeleteCF(uint32_t column_family_id, const rocksdb::Slice& key) {
    if (column_family_id != 0) {
      return rocksdb::Status::InvalidArgument("DBBatchInserter: column families not supported");
    }
    return batch_->SingleDelete(key);
  }
  virtual rocksdb::Status MergeCF(uint32_t column_family_id, const rocksdb::Slice& key,
                                  const rocksdb::Slice& value) {
    if (column_family_id != 0) {
      return rocksdb::Status::InvalidArgument("DBBatchInserter: column families not supported");
    }
    return batch_->Merge(key, value);
  }
  virtual rocksdb::Status DeleteRangeCF(uint32_t column_family_id, const rocksdb::Slice& begin_key,
                                        const rocksdb::Slice& end_key) {
    if (column_family_id != 0) {
      return rocksdb::Status::InvalidArgument("DBBatchInserter: column families not supported");
    }
    return batch_->DeleteRange(begin_key, end_key);
  }
  virtual void LogData(const rocksdb::Slice& blob) { batch_->PutLogData(blob); }

 private:
  rocksdb::WriteBatchBase* const batch_;
};

}  // namespace

DBBatch::DBBatch(DBEngine* db)
    : DBEngine(db->rep, db->iters), updates(0), has_delete_range(false), batch(&kComparator) {}

DBBatch::~DBBatch() {}

DBStatus DBBatch::Put(DBKey key, DBSlice value) {
  ++updates;
  batch.Put(EncodeKey(key), ToSlice(value));
  return kSuccess;
}

DBStatus DBBatch::Merge(DBKey key, DBSlice value) {
  ++updates;
  batch.Merge(EncodeKey(key), ToSlice(value));
  return kSuccess;
}

DBStatus DBBatch::Get(DBKey key, DBString* value) {
  rocksdb::ReadOptions read_opts;
  DBGetter base(rep, read_opts, EncodeKey(key));
  if (updates == 0) {
    return base.Get(value);
  }
  if (has_delete_range) {
    // TODO(peter): We don't support iterators when the batch contains
    // delete range entries.
    return FmtStatus("cannot read from a batch containing delete range entries");
  }
  std::unique_ptr<rocksdb::WBWIIterator> iter(batch.NewIterator());
  iter->Seek(base.key);
  return ProcessDeltaKey(&base, iter.get(), base.key, value);
}

DBStatus DBBatch::Delete(DBKey key) {
  ++updates;
  batch.Delete(EncodeKey(key));
  return kSuccess;
}

DBStatus DBBatch::SingleDelete(DBKey key) {
  ++updates;
  batch.SingleDelete(EncodeKey(key));
  return kSuccess;
}

DBStatus DBBatch::DeleteRange(DBKey start, DBKey end) {
  ++updates;
  has_delete_range = true;
  batch.DeleteRange(EncodeKey(start), EncodeKey(end));
  return kSuccess;
}

DBStatus DBBatch::CommitBatch(bool sync) {
  if (updates == 0) {
    return kSuccess;
  }
  rocksdb::WriteOptions options;
  options.sync = sync;
  return ToDBStatus(rep->Write(options, batch.GetWriteBatch()));
}

DBStatus DBBatch::ApplyBatchRepr(DBSlice repr, bool sync) {
  if (sync) {
    return FmtStatus("unsupported");
  }
  // TODO(peter): It would be slightly more efficient to iterate over
  // repr directly instead of first converting it to a string.
  DBBatchInserter inserter(&batch);
  rocksdb::WriteBatch batch(ToString(repr));
  rocksdb::Status status = batch.Iterate(&inserter);
  if (!status.ok()) {
    return ToDBStatus(status);
  }
  updates += batch.Count();
  return kSuccess;
}

DBSlice DBBatch::BatchRepr() { return ToDBSlice(batch.GetWriteBatch()->Data()); }

DBIterator* DBBatch::NewIter(DBIterOptions iter_options) {
  if (has_delete_range) {
    // TODO(peter): We don't support iterators when the batch contains
    // delete range entries.
    return NULL;
  }
  DBIterator* iter = new DBIterator(iters, iter_options);
  rocksdb::Iterator* base = rep->NewIterator(iter->read_opts);
  rocksdb::WBWIIterator* delta = batch.NewIterator();
  iter->rep.reset(new BaseDeltaIterator(base, delta, iter_options.prefix, &iter->upper_bound));
  return iter;
}

DBStatus DBBatch::GetStats(DBStatsResult* stats) { return FmtStatus("unsupported"); }

DBStatus DBBatch::GetTickersAndHistograms(DBTickersAndHistogramsResult* stats) {
  return FmtStatus("unsupported");
}

DBString DBBatch::GetCompactionStats() { return ToDBString("unsupported"); }

DBStatus DBBatch::GetEnvStats(DBEnvStatsResult* stats) { return FmtStatus("unsupported"); }

DBStatus DBBatch::GetEncryptionRegistries(DBEncryptionRegistries* result) {
  return FmtStatus("unsupported");
}

DBStatus DBBatch::EnvWriteFile(DBSlice path, DBSlice contents) { return FmtStatus("unsupported"); }

DBStatus DBBatch::EnvOpenFile(DBSlice path, uint64_t bytes_per_sync, rocksdb::WritableFile** file) {
  return FmtStatus("unsupported");
}

DBStatus DBBatch::EnvReadFile(DBSlice path, DBSlice* contents) { return FmtStatus("unsupported"); }

DBStatus DBBatch::EnvCloseFile(rocksdb::WritableFile* file) { return FmtStatus("unsupported"); }

DBStatus DBBatch::EnvSyncFile(rocksdb::WritableFile* file) { return FmtStatus("unsupported"); }

DBStatus DBBatch::EnvAppendFile(rocksdb::WritableFile* file, DBSlice contents) {
  return FmtStatus("unsupported");
}

DBStatus DBBatch::EnvDeleteFile(DBSlice path) { return FmtStatus("unsupported"); }

DBStatus DBBatch::EnvDeleteDirAndFiles(DBSlice dir) { return FmtStatus("unsupported"); }

DBStatus DBBatch::EnvLinkFile(DBSlice oldname, DBSlice newname) { return FmtStatus("unsupported"); }

DBStatus DBBatch::EnvOpenReadableFile(DBSlice path, rocksdb::RandomAccessFile** file) {
  return FmtStatus("unsupported");
}
DBStatus DBBatch::EnvReadAtFile(rocksdb::RandomAccessFile* file, DBSlice buffer, int64_t offset,
                                int* n) {
  return FmtStatus("unsupported");
}
DBStatus DBBatch::EnvCloseReadableFile(rocksdb::RandomAccessFile* file) {
  return FmtStatus("unsupported");
}
DBStatus DBBatch::EnvOpenDirectory(DBSlice path, rocksdb::Directory** file) {
  return FmtStatus("unsupported");
}
DBStatus DBBatch::EnvSyncDirectory(rocksdb::Directory* file) { return FmtStatus("unsupported"); }
DBStatus DBBatch::EnvCloseDirectory(rocksdb::Directory* file) { return FmtStatus("unsupported"); }
DBStatus DBBatch::EnvRenameFile(DBSlice oldname, DBSlice newname) {
  return FmtStatus("unsupported");
}
DBStatus DBBatch::EnvCreateDir(DBSlice name) { return FmtStatus("unsupported"); }
DBStatus DBBatch::EnvDeleteDir(DBSlice name) { return FmtStatus("unsupported"); }
DBStatus DBBatch::EnvListDir(DBSlice name, std::vector<std::string>* result) { return FmtStatus("unsupported"); }
  
DBWriteOnlyBatch::DBWriteOnlyBatch(DBEngine* db) : DBEngine(db->rep, db->iters), updates(0) {}

DBWriteOnlyBatch::~DBWriteOnlyBatch() {}

DBStatus DBWriteOnlyBatch::Put(DBKey key, DBSlice value) {
  ++updates;
  batch.Put(EncodeKey(key), ToSlice(value));
  return kSuccess;
}

DBStatus DBWriteOnlyBatch::Merge(DBKey key, DBSlice value) {
  ++updates;
  batch.Merge(EncodeKey(key), ToSlice(value));
  return kSuccess;
}

DBStatus DBWriteOnlyBatch::Get(DBKey key, DBString* value) { return FmtStatus("unsupported"); }

DBStatus DBWriteOnlyBatch::Delete(DBKey key) {
  ++updates;
  batch.Delete(EncodeKey(key));
  return kSuccess;
}

DBStatus DBWriteOnlyBatch::SingleDelete(DBKey key) {
  ++updates;
  batch.SingleDelete(EncodeKey(key));
  return kSuccess;
}

DBStatus DBWriteOnlyBatch::DeleteRange(DBKey start, DBKey end) {
  ++updates;
  batch.DeleteRange(EncodeKey(start), EncodeKey(end));
  return kSuccess;
}

DBStatus DBWriteOnlyBatch::CommitBatch(bool sync) {
  if (updates == 0) {
    return kSuccess;
  }
  rocksdb::WriteOptions options;
  options.sync = sync;
  return ToDBStatus(rep->Write(options, &batch));
}

DBStatus DBWriteOnlyBatch::ApplyBatchRepr(DBSlice repr, bool sync) {
  if (sync) {
    return FmtStatus("unsupported");
  }
  // TODO(peter): It would be slightly more efficient to iterate over
  // repr directly instead of first converting it to a string.
  DBBatchInserter inserter(&batch);
  rocksdb::WriteBatch batch(ToString(repr));
  rocksdb::Status status = batch.Iterate(&inserter);
  if (!status.ok()) {
    return ToDBStatus(status);
  }
  updates += batch.Count();
  return kSuccess;
}

DBSlice DBWriteOnlyBatch::BatchRepr() { return ToDBSlice(batch.GetWriteBatch()->Data()); }

DBIterator* DBWriteOnlyBatch::NewIter(DBIterOptions) { return NULL; }

DBStatus DBWriteOnlyBatch::GetStats(DBStatsResult* stats) { return FmtStatus("unsupported"); }

DBStatus DBWriteOnlyBatch::GetTickersAndHistograms(DBTickersAndHistogramsResult* stats) {
  return FmtStatus("unsupported");
}

DBString DBWriteOnlyBatch::GetCompactionStats() { return ToDBString("unsupported"); }

DBStatus DBWriteOnlyBatch::GetEnvStats(DBEnvStatsResult* stats) { return FmtStatus("unsupported"); }

DBStatus DBWriteOnlyBatch::GetEncryptionRegistries(DBEncryptionRegistries* result) {
  return FmtStatus("unsupported");
}

DBStatus DBWriteOnlyBatch::EnvWriteFile(DBSlice path, DBSlice contents) {
  return FmtStatus("unsupported");
}

DBStatus DBWriteOnlyBatch::EnvOpenFile(DBSlice path, uint64_t bytes_per_sync, rocksdb::WritableFile** file) {
  return FmtStatus("unsupported");
}

DBStatus DBWriteOnlyBatch::EnvReadFile(DBSlice path, DBSlice* contents) {
  return FmtStatus("unsupported");
}

DBStatus DBWriteOnlyBatch::EnvCloseFile(rocksdb::WritableFile* file) {
  return FmtStatus("unsupported");
}

DBStatus DBWriteOnlyBatch::EnvSyncFile(rocksdb::WritableFile* file) {
  return FmtStatus("unsupported");
}

DBStatus DBWriteOnlyBatch::EnvAppendFile(rocksdb::WritableFile* file, DBSlice contents) {
  return FmtStatus("unsupported");
}

DBStatus DBWriteOnlyBatch::EnvDeleteFile(DBSlice path) { return FmtStatus("unsupported"); }

DBStatus DBWriteOnlyBatch::EnvDeleteDirAndFiles(DBSlice dir) { return FmtStatus("unsupported"); }

DBStatus DBWriteOnlyBatch::EnvLinkFile(DBSlice oldname, DBSlice newname) {
  return FmtStatus("unsupported");
}

DBStatus DBWriteOnlyBatch::EnvOpenReadableFile(DBSlice path, rocksdb::RandomAccessFile** file) {
  return FmtStatus("unsupported");
}
DBStatus DBWriteOnlyBatch::EnvReadAtFile(rocksdb::RandomAccessFile* file, DBSlice buffer,
                                         int64_t offset, int* n) {
  return FmtStatus("unsupported");
}
DBStatus DBWriteOnlyBatch::EnvCloseReadableFile(rocksdb::RandomAccessFile* file) {
  return FmtStatus("unsupported");
}
DBStatus DBWriteOnlyBatch::EnvOpenDirectory(DBSlice path, rocksdb::Directory** file) {
  return FmtStatus("unsupported");
}
DBStatus DBWriteOnlyBatch::EnvSyncDirectory(rocksdb::Directory* file) {
  return FmtStatus("unsupported");
}
DBStatus DBWriteOnlyBatch::EnvCloseDirectory(rocksdb::Directory* file) {
  return FmtStatus("unsupported");
}
DBStatus DBWriteOnlyBatch::EnvRenameFile(DBSlice oldname, DBSlice newname) {
  return FmtStatus("unsupported");
}
DBStatus DBWriteOnlyBatch::EnvCreateDir(DBSlice name) { return FmtStatus("unsupported"); }
DBStatus DBWriteOnlyBatch::EnvDeleteDir(DBSlice name) { return FmtStatus("unsupported"); }
DBStatus DBWriteOnlyBatch::EnvListDir(DBSlice name, std::vector<std::string>* result) {
  return FmtStatus("unsupported");
}

rocksdb::WriteBatch::Handler* GetDBBatchInserter(::rocksdb::WriteBatchBase* batch) {
  return new DBBatchInserter(batch);
}

}  // namespace cockroach
