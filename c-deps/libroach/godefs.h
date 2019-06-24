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

#include <libroach.h>

extern "C" {
bool __attribute__((weak)) rocksDBV(int, int);
void __attribute__((weak)) rocksDBLog(int, char*, int);
char* __attribute__((weak)) prettyPrintKey(DBKey);
}  // extern "C"
