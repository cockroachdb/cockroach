package main

import (
	"os"
	"unsafe"

	_ "github.com/cockroachdb/c-protobuf"
	_ "github.com/cockroachdb/c-protobuf/libprotoc"
)

// #cgo CXXFLAGS: -std=c++11
// #cgo CPPFLAGS: -I../.. -I../../internal/src
// #cgo darwin LDFLAGS: -Wl,-undefined -Wl,dynamic_lookup
// #cgo !darwin LDFLAGS: -Wl,-unresolved-symbols=ignore-all
//
// int cmain(int argc, char* argv[]);
import "C"

func main() {
	cargs := make([]*C.char, len(os.Args))
	for i, a := range os.Args {
		cargs[i] = C.CString(a)
	}
	C.cmain(C.int(len(os.Args)), (**C.char)(unsafe.Pointer(&cargs[0])))
}
