# Google Protobuf's

This is a go-gettable version of the Google C++ protobuf library for use in Go
code that needs to link against the C++ protobuf library but wants to integrate
with `go get` and `go build`. The sources track the 3.0.0 release; take a look
at `import.sh` to get the currently used version.

To use in your project you need to import the package and set appropriate cgo flag directives:

```
import _ "github.com/cockroachdb/c-protobuf"

// #cgo CXXFLAGS: -std=c++11
// #cgo CPPFLAGS: -I <relative-path>/c-protobuf/internal/src
// #cgo darwin LDFLAGS: -Wl,-undefined -Wl,dynamic_lookup
// #cgo !darwin LDFLAGS: -Wl,-unresolved-symbols=ignore-all
import "C"
```
