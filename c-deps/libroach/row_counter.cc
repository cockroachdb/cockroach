// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include "row_counter.h"
#include <iostream>
#include "encoding.h"

using namespace cockroach;

int RowCounter::GetRowPrefixLength(rocksdb::Slice* key) {
  size_t n = key->size();

  if (!IsInt(key)) {
    // Not a table key, so the row prefix is the entire key.
    return n;
  }

  // The column ID length is encoded as a varint and we take advantage of the
  // fact that the column ID itself will be encoded in 0-9 bytes and thus the
  // length of the column ID data will fit in a single byte.
  rocksdb::Slice buf = rocksdb::Slice(*key);
  buf.remove_prefix(n - 1);

  if (!IsInt(&buf)) {
    // The last byte is not a valid column ID suffix.
    return 0;
  }

  uint64_t col_id_len;
  if (!DecodeUvarint64(&buf, &col_id_len)) {
    return 0;
  }

  if (col_id_len > uint64_t(n - 1)) {
    // The column ID length was impossible. colIDLen is the length of
    // the encoded column ID suffix. We add 1 to account for the byte
    // holding the length of the encoded column ID and if that total
    // (colIDLen+1) is greater than the key suffix (n == len(buf))
    // then we bail. Note that we don't consider this an error because
    // EnsureSafeSplitKey can be called on keys that look like table
    // keys but which do not have a column ID length suffix (e.g
    // by SystemConfig.ComputeSplitKey).
    return 0;
  }

  return n - int(col_id_len) - 1;
}

// EnsureSafeSplitKey transforms the SQL table key argumnet such that it is a
// valid split key (i.e. does not occur in the middle of a row).
void RowCounter::EnsureSafeSplitKey(rocksdb::Slice* key) {
  // The row prefix for a key is unique to keys in its row - no key without the
  // row prefix will be in the key's row. Therefore, we can be certain that
  // using the row prefix for a key as a split key is safe: it doesn't occur in
  // the middle of a row.
  int idx = GetRowPrefixLength(key);
  key->remove_suffix(key->size() - idx);
}

// Count examines each key passed to it and increments the running count when it
// sees a key that belongs to a new row.
bool RowCounter::Count(const rocksdb::Slice& key, cockroach::roachpb::BulkOpSummary* summary) {
  // EnsureSafeSplitKey is usually used to avoid splitting a row across ranges,
  // by returning the row's key prefix.
  // We reuse it here to count "rows" by counting when it changes.
  // Non-SQL keys are returned unchanged or may error -- we ignore them, since
  // non-SQL keys are obviously thus not SQL rows.

  rocksdb::Slice decoded_key;
  int64_t wall_time = 0;
  int32_t logical = 0;
  if (!DecodeKey(key, &decoded_key, &wall_time, &logical)) {
    return false;
  }

  size_t key_size = decoded_key.size();
  EnsureSafeSplitKey(&decoded_key);

  if (decoded_key.empty() || key_size == decoded_key.size()) {
    return true;
  }

  // no change key prefix => no new row.
  if (decoded_key == prev_key) {
    return true;
  }

  prev_key.assign(decoded_key.data(), decoded_key.size());

  uint64_t tbl;
  if (!DecodeTablePrefix(&decoded_key, &tbl)) {
    return false;
  }

  uint64_t index_id;
  if (!DecodeUvarint64(&decoded_key, &index_id)) {
    return false;
  } else if (index_id == 1) {
    summary->set_rows(summary->rows() + 1);
  } else {
    summary->set_index_entries(summary->index_entries() + 1);
  }

  return true;
}
