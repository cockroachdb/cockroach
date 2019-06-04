// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL.txt and at www.mariadb.com/bsl11.
//
// Change Date: 2022-10-01
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the Apache License, Version 2.0,
// included in the file licenses/APL.txt and at
// https://www.apache.org/licenses/LICENSE-2.0

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
bool __attribute__((weak)) rocksDBV(int, int) { die_missing_symbol(__func__); }
void __attribute__((weak)) rocksDBLog(int, char*, int) { die_missing_symbol(__func__); }
char* __attribute__((weak)) prettyPrintKey(DBKey) { die_missing_symbol(__func__); }
}  // extern "C"
