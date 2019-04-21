// Copyright 2019 The Cockroach Authors.
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

#include "env_sync_fault_injection.h"
#include "rocksdb/utilities/object_registry.h"

namespace rocksdb_utils {

// See comment above `SyncFaultInjectionEnv` class definition.
class SyncFaultInjectionWritableFile : public rocksdb::WritableFileWrapper {
 public:
  SyncFaultInjectionWritableFile(std::unique_ptr<rocksdb::WritableFile> target,
                                 int crash_failure_one_in,
                                 int sync_failure_one_in,
                                 bool crash_after_sync_failure);

  rocksdb::Status Append(const rocksdb::Slice& data) override;
  rocksdb::Status Sync() override;

 private:
  std::unique_ptr<rocksdb::WritableFile> target_;
  const int crash_failure_one_in_;
  const int sync_failure_one_in_;
  const bool crash_after_sync_failure_;
  // Countdown until crash if a sync failure already happened.
  int num_syncs_until_crash_;
  // Lock needed to handle concurrent writes and syncs.
  std::mutex mu_;
  // A buffer of written but unsynced data.
  std::string buffer_;

  // Some constants for use with `num_syncs_until_crash_`.
  const static int kNoCountdown = -1;
  const static int kStartCountdown = 10;
};

SyncFaultInjectionWritableFile::SyncFaultInjectionWritableFile(
    std::unique_ptr<rocksdb::WritableFile> target,
    int crash_failure_one_in,
    int sync_failure_one_in,
    bool crash_after_sync_failure) :
  rocksdb::WritableFileWrapper(target.get()),
  target_(std::move(target)),
  crash_failure_one_in_(crash_failure_one_in),
  sync_failure_one_in_(sync_failure_one_in),
  crash_after_sync_failure_(crash_after_sync_failure),
  num_syncs_until_crash_(kNoCountdown) {}

rocksdb::Status SyncFaultInjectionWritableFile::Append(
    const rocksdb::Slice& data) {
  std::unique_lock<std::mutex> lock(mu_);
  buffer_.append(data.data(), data.size());
  return rocksdb::Status::OK();
}

// We are using process crash to simulate system crash for tests and don't
// expect these tests to face actual system crashes. So for "syncing" it is
// sufficient to push data into page cache via the underlying `WritableFile`'s
// `Append()`. That should be enough for the file data to survive a process
// crash.
rocksdb::Status SyncFaultInjectionWritableFile::Sync() {
  std::unique_lock<std::mutex> lock(mu_);
  if (num_syncs_until_crash_ > kNoCountdown) {
    --num_syncs_until_crash_;
    if (num_syncs_until_crash_ == 0) {
      exit(0);
    }
  }

  if (crash_failure_one_in_ > 0 && random() % crash_failure_one_in_ == 0) {
    exit(0);
  } else if (sync_failure_one_in_ > 0 && random() % sync_failure_one_in_ == 0) {
    if (num_syncs_until_crash_ == kNoCountdown) {
      // This was the first failure. Start the countdown.
      num_syncs_until_crash_ = kStartCountdown;
    }
    buffer_.clear();
    return rocksdb::Status::OK();
  }
  std::string old_buffer;
  buffer_.swap(old_buffer);
  // It should be fine to buffer new writes while we're syncing old ones, so unlock.
  lock.unlock();
  return target_->Append(old_buffer);
}

SyncFaultInjectionEnv::SyncFaultInjectionEnv(
    Env* target,
    int crash_failure_one_in,
    int sync_failure_one_in,
    bool crash_after_sync_failure) :
  rocksdb::EnvWrapper(target),
  crash_failure_one_in_(crash_failure_one_in),
  sync_failure_one_in_(sync_failure_one_in),
  crash_after_sync_failure_(crash_after_sync_failure) {}

rocksdb::Status SyncFaultInjectionEnv::NewWritableFile(
    const std::string& filename,
    std::unique_ptr<rocksdb::WritableFile>* result,
    const rocksdb::EnvOptions& env_options) {
  std::unique_ptr<rocksdb::WritableFile> underlying_file;
  rocksdb::Status s = EnvWrapper::NewWritableFile(filename, &underlying_file, env_options);
  if (s.ok()) {
    result->reset(new SyncFaultInjectionWritableFile(
          std::move(underlying_file),
          crash_failure_one_in_,
          sync_failure_one_in_,
          crash_after_sync_failure_));
  }
  return s;
}

}  // rocksdb_utils
