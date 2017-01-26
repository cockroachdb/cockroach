// Copyright 2017 The Cockroach Authors.
//
// Licensed under the Cockroach Community Licence (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/pkg/ccl/LICENSE

package rocksdbccl

import (
	// Import rocksdb for the cgo headers.
	_ "github.com/cockroachdb/cockroach/pkg/storage/engine/rocksdb"
)

// #cgo CPPFLAGS: -I ../../../../../vendor/github.com/cockroachdb/c-protobuf/internal/src -I ../../../../../vendor/github.com/cockroachdb/c-rocksdb/internal/include -I ../../../../../pkg/storage/engine/rocksdb
// #cgo CXXFLAGS: -std=c++11
// #cgo darwin LDFLAGS: -Wl,-undefined -Wl,dynamic_lookup
// #cgo !darwin LDFLAGS: -Wl,-unresolved-symbols=ignore-all
// #cgo linux LDFLAGS: -lrt
import "C"
