package protoparse

import (
	dpb "github.com/golang/protobuf/protoc-gen-go/descriptor"
	// link in packages that include the standard protos included with protoc
	_ "github.com/golang/protobuf/protoc-gen-go/plugin"
	_ "github.com/golang/protobuf/ptypes/any"
	_ "github.com/golang/protobuf/ptypes/duration"
	_ "github.com/golang/protobuf/ptypes/empty"
	_ "github.com/golang/protobuf/ptypes/struct"
	_ "github.com/golang/protobuf/ptypes/timestamp"
	_ "github.com/golang/protobuf/ptypes/wrappers"
	_ "google.golang.org/genproto/protobuf/api"
	_ "google.golang.org/genproto/protobuf/field_mask"
	_ "google.golang.org/genproto/protobuf/ptype"
	_ "google.golang.org/genproto/protobuf/source_context"

	"github.com/jhump/protoreflect/internal"
)

// All files that are included with protoc are also included with this package
// so that clients do not need to explicitly supply a copy of these protos (just
// like callers of protoc do not need to supply them).
var standardImports map[string]*dpb.FileDescriptorProto

func init() {
	standardFilenames := []string{
		"google/protobuf/any.proto",
		"google/protobuf/api.proto",
		"google/protobuf/compiler/plugin.proto",
		"google/protobuf/descriptor.proto",
		"google/protobuf/duration.proto",
		"google/protobuf/empty.proto",
		"google/protobuf/field_mask.proto",
		"google/protobuf/source_context.proto",
		"google/protobuf/struct.proto",
		"google/protobuf/timestamp.proto",
		"google/protobuf/type.proto",
		"google/protobuf/wrappers.proto",
	}

	standardImports = map[string]*dpb.FileDescriptorProto{}
	for _, fn := range standardFilenames {
		fd, err := internal.LoadFileDescriptor(fn)
		if err != nil {
			panic(err.Error())
		}
		standardImports[fn] = fd
	}
}
