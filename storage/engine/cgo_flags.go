package engine

import (
	// Link against the protobuf and rocksdb libaries. This is
	// explicitly because these Go libraries do not export any Go
	// symbols.
	_ "github.com/cockroachdb/c-protobuf"
	_ "github.com/cockroachdb/c-rocksdb"
)

// #cgo CXXFLAGS: -std=c++11
// #cgo CPPFLAGS: -I ../../../c-protobuf/internal/src -I ../../../c-rocksdb/internal/include
// #cgo darwin LDFLAGS: -Wl,-undefined -Wl,dynamic_lookup
// #cgo !darwin LDFLAGS: -Wl,-unresolved-symbols=ignore-all
import "C"
