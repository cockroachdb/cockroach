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
// Author: Spencer Kimball (spencer.kimball@gmail.com)

#include "rocksdb/slice.h"

namespace {

const unsigned char kOrderedEncodingBinary     = 0x25;
const unsigned char kOrderedEncodingTerminator = 0x00;

}

bool DecodeBinary(const rocksdb::Slice& buf, std::string* decoded, std::string* remainder) {
  if (buf[0] != kOrderedEncodingBinary) {
    fprintf(stderr, "%s doesn't begin with binary encoding byte\n", buf.ToString().c_str());
    return false;
  }
  decoded->clear();
  int s = 6;
  int i = 1;
  if (buf[i] == kOrderedEncodingTerminator) {
    if (remainder != NULL) {
      rocksdb::Slice remSlice(buf);
      remSlice.remove_prefix(2);
      *remainder = remSlice.ToString();
    }
    return true;
  }

  int t = (buf[i] << 1) & 0xff;
  for (i = 2; buf[i] != kOrderedEncodingTerminator; i++) {
    if (s == 7) {
      decoded->push_back(t | (buf[i] & 0x7f));
      i++;
    } else {
      decoded->push_back(t | ((buf[i] & 0x7f) >> s));
    }

    t = (buf[i] << (8 - s)) & 0xff;

    if (buf[i] == kOrderedEncodingTerminator) {
      break;
    }

    if (s == 1) {
      s = 7;
    } else {
      s--;
    }
  }
  if (t != 0) {
    fprintf(stderr, "%s doesn't begin with binary encoding byte\n", buf.ToString().c_str());
    return false;
  }
  if (remainder != NULL) {
    rocksdb::Slice remSlice(buf);
    remSlice.remove_prefix(i+1);
    *remainder = remSlice.ToString();
  }
  return true;
}
