// Copyright 2014 The Cockroach Authors.
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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Peter Mattis (peter.mattis@gmail.com)

// Style settings: indent -kr -ci2 -cli2 -i2 -l80 -nut roachlib/merge.cc

#include <string>
#include <limits>
#include "data.pb.h"
#include "rocksdb/slice.h"
#include "roach_c.h"

namespace {

bool WillOverflow(int64_t a, int64_t b) {
  // Morally MinInt64 < a+b < MaxInt64, but without overflows.
  // First make sure that a <= b. If not, swap them.
  if (a > b) {
    std::swap(a, b);
  }
  // Now b is the larger of the numbers, and we compare sizes
  // in a way that can never over- or underflow.
  if (b > 0) {
    return a > (std::numeric_limits<int64_t>::max() - b);
  }
  return (std::numeric_limits<int64_t>::min() - b) > a;
}

bool MergeValues(proto::Value *left, const proto::Value &right) {
  if (left->has_bytes()) {
    if (right.has_bytes()) {
      *left->mutable_bytes() += right.bytes();
      return true;
    }
  } else if (left->has_integer()) {
    if (right.has_integer()) {
      if (WillOverflow(left->integer(), right.integer())) {
        return false;
      }
      left->set_integer(left->integer() + right.integer());
      return true;
    }
  } else {
    *left = right;
    return true;
  }
  return false;
}

// MergeResult serializes the result Value into a byte slice.
char* MergeResult(proto::Value* result, size_t *length) {
  // TODO(pmattis): Should recompute checksum here. Need a crc32
  // implementation and need to verify the checksumming is identical
  // to what is being done in Go. Worst case we can port the Go crc32
  // back to C/C++.
  result->clear_checksum();
  *length = result->ByteSize();
  char *value = static_cast<char*>(malloc(*length));
  if (!result->SerializeToArray(value, *length)) {
    return NULL;
  }
  return value;
}

}  // namespace

// MergeOne implements the merge operator on a single pair of values.
// update is merged with existing. This method is provided for
// invocation from Go code.
char* MergeOne(
    const char* existing, size_t existing_length,
    const char* update, size_t update_length,
    size_t* new_value_length, char** error_msg) {
  *new_value_length = 0;

  proto::Value result;
  if (!result.ParseFromArray(existing, existing_length)) {
    // Corrupted existing value.
    *error_msg = (char*)"corrupted existing value";
    return NULL;
  }

  proto::Value value;
  if (!value.ParseFromArray(update, update_length)) {
    // Corrupted update value.
    *error_msg = (char*)"corrupted update value";
    return NULL;
  }

  if (!MergeValues(&result, value)) {
    *error_msg = (char*)"incompatible merge values";
    return NULL;
  }

  char *new_value = MergeResult(&result, new_value_length);
  if (!new_value) {
    *error_msg = (char*)"serialization error";
    return NULL;
  }
  return new_value;
}

// MergeOperator implements the RocksDB custom merge operator for
// proto.Value objects. This method is called by RocksDB when merge
// operations are encountered, either when reading a value with a
// pending merge or when compacting merge operations.
char* MergeOperator(
    const char* key, size_t key_length,
    const char* existing_value,
    size_t existing_value_length,
    const char* const* operands_list,
    const size_t* operands_list_length,
    int num_operands, unsigned char* success,
    size_t* new_value_length)
{
  // TODO(pmattis): Taken from the old merger code, below are some
  // details about how errors returned by the merge operator are
  // handled. Need to test various error scenarios and decide on
  // desired behavior. Clear the key and it's gone. Corrupt it
  // properly and RocksDB might refuse to work with it at all until
  // you clear it manually, which may also not be what we want. The
  // problem with merges is that RocksDB won't really carry them out
  // while we have a chance to talk back to clients.
  //
  // If we indicate failure (*success = false), then the call to the
  // merger via rocksdb_merge will not return an error, but simply
  // remove or truncate the offending key (at least when the settings
  // specify that missing keys should be created; otherwise a
  // corruption error will be returned, but likely only after the next
  // read of the key). In effect, there is no propagation of error
  // information to the client.

  // TODO(pmattis): We should not be silent about errors. It's
  // currently irritating to get them logged in Go, but we should get
  // it fixed up so that such logging from C++ is straightforward.
  *success = false;
  *new_value_length = 0;

  proto::Value result;
  if (!result.ParseFromArray(existing_value, existing_value_length)) {
    // Corrupted existing value.
    return NULL;
  }

  for (int i = 0; i < num_operands; ++i) {
    proto::Value value;
    if (!value.ParseFromArray(operands_list[i], operands_list_length[i])) {
      // Corrupted operand.
      return NULL;
    }
    if (!MergeValues(&result, value)) {
      // Invalid merge operation.
      return NULL;
    }
  }

  char *new_value = MergeResult(&result, new_value_length);
  if (!new_value) {
    return NULL;
  }
  *success = true;
  return new_value;
}
