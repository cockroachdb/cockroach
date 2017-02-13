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
// implied. See the License for the specific language governing
// permissions and limitations under the License.
//
// Author: Peter Mattis (peter@cockroachlabs.com)

package rocksdb

import (
	"unsafe"

	// Link against the protobuf, rocksdb, and snappy libraries. This is
	// explicit because these Go libraries do not export any Go symbols.
	_ "github.com/cockroachdb/c-protobuf"
	_ "github.com/cockroachdb/c-rocksdb"
)

// #cgo CPPFLAGS: -I ../../../../vendor/github.com/cockroachdb/c-protobuf/internal/src -I ../../../../vendor/github.com/cockroachdb/c-rocksdb/internal/include
// #cgo CXXFLAGS: -std=c++11
// #cgo !strictld,darwin LDFLAGS: -Wl,-undefined -Wl,dynamic_lookup
// #cgo !strictld,!darwin LDFLAGS: -Wl,-unresolved-symbols=ignore-all
// #cgo linux LDFLAGS: -lrt
//
// #include "db.h"
import "C"

// Logger is a logging function to be set by the importing package. Its
// presence allows us to avoid depending on a logging package.
var Logger = func(string, ...interface{}) {}

//export rocksDBLog
func rocksDBLog(s *C.char, n C.int) {
	// Note that rocksdb logging is only enabled if log.V(3) is true
	// when RocksDB.Open() is called.
	Logger("%s", C.GoStringN(s, n))
}

// DBKeyPrinter may be set to a function to perform pretty-printing
// of rocksdb keys. It is normally set by an init function in the
// storage/engine package. The arguments are a DBKey decomposed into its
// primitive fields, because the compiler doesn't like passing C types
// across package boundaries.
var DBKeyPrinter func([]byte, int64, int32) string

//export prettyPrintKey
func prettyPrintKey(cKey C.DBKey) *C.char {
	if DBKeyPrinter == nil {
		return C.CString("pretty printer not initialized")
	}
	return C.CString(DBKeyPrinter(C.GoBytes(unsafe.Pointer(cKey.key.data), cKey.key.len),
		int64(cKey.wall_time), int32(cKey.logical)))
}
