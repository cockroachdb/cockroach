// Copyright 2017 The Cockroach Authors.
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

#include "env_switching.h"

/*
 *
 * SwitchingEnv switches between a base_env (usually a Env::Default or MemEnv)
 * and an encrypted env.
 *
 *         -----------------------------
 *         | SwitchingEnv (EnvWrapper) |
 *         --- PLAIN - ENCRYPTED -------
 *               |        \
 *               |         \
 *               |    ------------------------------
 *               |    | encrypted_env (EnvWrapper) |
 *               |    ------------------------------
 *               |         /
 *               |        /
 *           ------------------
 *           | base_env (Env) |
 *           ------------------
 *
 * Any unimplemented methods are called on the base_env.
 */
class SwitchingEnv : public rocksdb::EnvWrapper {
 public:
  SwitchingEnv(rocksdb::Env* base_env, std::shared_ptr<rocksdb::Logger> logger)
      : rocksdb::EnvWrapper(base_env), logger(logger) {
    rocksdb::Info(logger, "initialized switching env");
  }

 private:
  std::shared_ptr<rocksdb::Logger> logger;
};

rocksdb::Env* NewSwitchingEnv(rocksdb::Env* base_env, std::shared_ptr<rocksdb::Logger> logger) {
  return new SwitchingEnv(base_env ? base_env : rocksdb::Env::Default(), logger);
}
