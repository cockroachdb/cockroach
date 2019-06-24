// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
