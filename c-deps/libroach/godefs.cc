// Copyright 2018 The Cockroach Authors.
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

#include "godefs.h"
#include <stdio.h>
#include <stdlib.h>

extern "C" {
static void __attribute__((noreturn)) die_missing_symbol(const char* name) {
  fprintf(stderr, "%s symbol missing; expected to be supplied by Go\n", name);
  abort();
}

// These are Go functions exported by storage/engine. We provide these stubs,
// which simply panic if called, to to allow intermediate build products to link
// successfully. Otherwise, when building ccl/storageccl/engineccl, Go will
// complain that these symbols are undefined. Because these stubs are marked
// "weak", they will be replaced by their proper implementation in
// storage/engine when the final cockroach binary is linked.
void __attribute__((weak)) rocksDBLog(char*, int) { die_missing_symbol(__func__); }
char* __attribute__((weak)) prettyPrintKey(DBKey) { die_missing_symbol(__func__); }
}  // extern "C"
