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

#include "getter.h"
#include "status.h"

namespace cockroach {

DBStatus IteratorGetter::Get(DBString* value) {
  if (base == NULL) {
    value->data = NULL;
    value->len = 0;
  } else {
    *value = ToDBString(base->value());
  }
  return kSuccess;
}

DBStatus DBGetter::Get(DBString* value) {
  std::string tmp;
  rocksdb::Status s = rep->Get(options, key, &tmp);
  if (!s.ok()) {
    if (s.IsNotFound()) {
      // This mirrors the logic in rocksdb_get(). It doesn't seem like
      // a good idea, but some code in engine_test.go depends on it.
      value->data = NULL;
      value->len = 0;
      return kSuccess;
    }
    return ToDBStatus(s);
  }
  *value = ToDBString(tmp);
  return kSuccess;
}

}  // namespace cockroach
