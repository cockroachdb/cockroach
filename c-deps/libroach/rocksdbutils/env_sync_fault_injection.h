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

#pragma once

#include <mutex>
#include <string>

#include "rocksdb/env.h"

namespace rocksdb_utils {

// `SyncFaultInjectionEnv` creates files that buffer `Append()`s in process memory
// until `Sync()` is called. Such files enable us to simulate machine crashes by only
// crashing the process. This works since, unlike normal files whose writes survive
// process crash in page cache, these files' unsynced writes are dropped on the floor.
//
// Such files also enable us to simulate sync failure by dropping unsynced writes at
// the same time we inject a sync error. This is more comprehensive than the available
// fault injection tools I looked at (like libfiu and charybdefs), as those ones only
// inject errors without dropping unsynced writes.
class SyncFaultInjectionEnv : public rocksdb::EnvWrapper {
 public:
  // - `target`: A pointer to the underlying `Env`.
  // - `crash_failure_one_in`: During a sync operation, crash the process immediately
  //   with a probability of 1/n. All unsynced writes are lost since they are buffered
  //   in process memory.
  // - `sync_failure_one_in`: A sync operation will return failure with a probability
  //   of 1/n. All unsynced writes for the file are dropped to simulate the failure.
  // - `crash_after_sync_failure`: If set to true, the program will crash itself some
  //   time after the first simulated sync failure. It does not happen immediately to
  //   allow the system to get itself into a weird state in case it doesn't handle sync
  //   failures properly.
  SyncFaultInjectionEnv(
      Env* target,
      int crash_failure_one_in,
      int sync_failure_one_in,
      bool crash_after_sync_failure);

  rocksdb::Status NewWritableFile(const std::string& filename,
                                  std::unique_ptr<rocksdb::WritableFile>* result,
                                  const rocksdb::EnvOptions& env_options) override;

 private:
  const int crash_failure_one_in_;
  const int sync_failure_one_in_;
  const bool crash_after_sync_failure_;
};

}  // rocksdb_utils
