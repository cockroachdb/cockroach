// Package protobuf uses the cgo compilation facilities to build the
// Protobuf C++ library. Note that support for zlib is not compiled
// in.
package protobuf

// #cgo CXXFLAGS: -DLANG_CXX11 -std=c++11
// #cgo CPPFLAGS: -DHAVE_CONFIG_H -DHAVE_PTHREAD -Iinternal/src
import "C"
