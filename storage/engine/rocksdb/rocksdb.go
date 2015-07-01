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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Peter Mattis (peter@cockroachlabs.com)

package rocksdb

import (
	// Link against the protobuf, rocksdb, and snappy libaries. This is
	// explicit because these Go libraries do not export any Go symbols.
	_ "github.com/cockroachdb/c-protobuf"
	_ "github.com/cockroachdb/c-rocksdb"
	_ "github.com/cockroachdb/c-snappy"
)

// #cgo CPPFLAGS: -I ../../../../c-protobuf/internal/src -I ../../../../c-rocksdb/internal/include
// #cgo CXXFLAGS: -std=c++11
// #cgo darwin LDFLAGS: -Wl,-undefined -Wl,dynamic_lookup
// #cgo !darwin LDFLAGS: -Wl,-unresolved-symbols=ignore-all
// #cgo linux LDFLAGS: -lrt
import "C"

// Logger is a logging function to be set by the importing package. Its
// presence allows us to avoid depending on a logging package.
var Logger = func(string, ...interface{}) {}

//export rocksDBLog
func rocksDBLog(s *C.char, n C.int) {
	// Note that rocksdb logging is only enabled if log.V(1) is true
	// when RocksDB.Open() is called.
	Logger("%s", C.GoStringN(s, n))
}
