// Copyright 2015 The Cockroach Authors.
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
// Author: Spencer Kimball (spencer.kimball@gmail.com)

#ifndef ROACHLIB_ENCODING_H
#define ROACHLIB_ENCODING_H

#include <stdint.h>

// DecodeBytes decodes the given key-encoded buf slice, returning true
// on a successful decode. The unencoded bytes are returned in
// *decoded.
bool DecodeBytes(const rocksdb::Slice& buf, std::string* decoded);

#endif // ROACHLIB_ENCODING_H

// local variables:
// mode: c++
// end:
