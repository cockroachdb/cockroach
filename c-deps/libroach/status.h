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

#include "db.h"
#include "fmt.h"

namespace cockroach {

const DBStatus kSuccess = {NULL, 0};

// ToDBStatus converts a rocksdb Status to a DBStatus.
inline DBStatus ToDBStatus(const rocksdb::Status& status) {
  if (status.ok()) {
    return kSuccess;
  }
  return ToDBString(status.ToString());
}

// FmtStatus formats the given arguments printf-style into a DBStatus.
__attribute__((__format__(GOOGLE_PRINTF_FORMAT, 1, 2))) inline DBStatus
FmtStatus(const char* fmt_str, ...) {
  va_list ap;
  va_start(ap, fmt_str);
  std::string str;
  fmt::StringAppendV(&str, fmt_str, ap);
  va_end(ap);
  return ToDBString(str);
}

}  // namespace cockroach
