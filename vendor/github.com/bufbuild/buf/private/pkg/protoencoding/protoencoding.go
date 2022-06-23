// Copyright 2020-2021 Buf Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package protoencoding

import (
	"github.com/bufbuild/buf/private/pkg/protodescriptor"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoregistry"
)

// Resolver is a Resolver.
//
// This is only needed in cases where extensions may be present.
type Resolver interface {
	protoregistry.ExtensionTypeResolver
	protoregistry.MessageTypeResolver
}

// NewResolver creates a New Resolver.
//
// If the input slice is empty, this returns nil
// The given FileDescriptors must be self-contained, that is they must contain all imports.
// This can NOT be guaranteed for FileDescriptorSets given over the wire, and can only be guaranteed from builds.
func NewResolver(fileDescriptors ...protodescriptor.FileDescriptor) (Resolver, error) {
	return newResolver(fileDescriptors...)
}

// Marshaler marshals Messages.
type Marshaler interface {
	Marshal(message proto.Message) ([]byte, error)
}

// NewWireMarshaler returns a new Marshaler for wire.
//
// See https://godoc.org/google.golang.org/protobuf/proto#MarshalOptions for a discussion on stability.
// This has the potential to be unstable over time.
func NewWireMarshaler() Marshaler {
	return newWireMarshaler()
}

// NewJSONMarshaler returns a new Marshaler for JSON.
//
// This has the potential to be unstable over time.
// resolver can be nil if unknown and are only needed for extensions.
func NewJSONMarshaler(resolver Resolver) Marshaler {
	return newJSONMarshaler(resolver, "", false)
}

// NewJSONMarshalerIndent returns a new Marshaler for JSON with indents.
//
// This has the potential to be unstable over time.
// resolver can be nil if unknown and are only needed for extensions.
func NewJSONMarshalerIndent(resolver Resolver) Marshaler {
	return newJSONMarshaler(resolver, "  ", false)
}

// NewJSONMarshalerUseProtoNames returns a new Marshaler for JSON using the proto names for keys.
//
// This has the potential to be unstable over time.
// resolver can be nil if unknown and are only needed for extensions.
func NewJSONMarshalerUseProtoNames(resolver Resolver) Marshaler {
	return newJSONMarshaler(resolver, "", true)
}

// Unmarshaler unmarshals Messages.
type Unmarshaler interface {
	Unmarshal(data []byte, message proto.Message) error
}

// NewWireUnmarshaler returns a new Unmarshaler for wire.
//
// resolver can be nil if unknown and are only needed for extensions.
func NewWireUnmarshaler(resolver Resolver) Unmarshaler {
	return newWireUnmarshaler(resolver)
}

// NewJSONUnmarshaler returns a new Unmarshaler for json.
//
// resolver can be nil if unknown and are only needed for extensions.
func NewJSONUnmarshaler(resolver Resolver) Unmarshaler {
	return newJSONUnmarshaler(resolver)
}
