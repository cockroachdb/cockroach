// Package libprotoc uses the cgo compilation facilities to build the Protobuf
// compiler as a library.
package libprotoc

// #cgo CXXFLAGS: -std=c++11
// #cgo CPPFLAGS: -DHAVE_CONFIG_H -I../internal/src
// #cgo darwin LDFLAGS: -Wl,-undefined -Wl,dynamic_lookup
// #cgo !darwin LDFLAGS: -Wl,-unresolved-symbols=ignore-all
import "C"
