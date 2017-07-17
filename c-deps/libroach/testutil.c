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
//
// Author: Nikhil Benesch (nikhil.benesch@gmail.com)

#include <err.h>
#include <stdlib.h>
#include "testutil.h"

unsigned char*
make_bytes(size_t len) {
  long* buf = malloc(len);
  if (buf == NULL) {
    err(1, "out of memory");
  }
  for (size_t i = 0; i < len / sizeof(long); i++) {
    buf[i] = mrand48();
  }
  return (unsigned char *) buf;
}
