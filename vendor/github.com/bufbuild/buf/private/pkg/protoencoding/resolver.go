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
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/dynamicpb"
)

func newResolver(fileDescriptors ...protodescriptor.FileDescriptor) (Resolver, error) {
	if len(fileDescriptors) == 0 {
		return nil, nil
	}
	// TODO: handle if resolvable
	files, err := protodesc.FileOptions{
		AllowUnresolvable: true,
	}.NewFiles(
		protodescriptor.FileDescriptorSetForFileDescriptors(fileDescriptors...),
	)
	if err != nil {
		return nil, err
	}
	types := &protoregistry.Types{}
	var rangeErr error
	files.RangeFiles(func(fileDescriptor protoreflect.FileDescriptor) bool {
		if err := addMessagesToTypes(types, fileDescriptor.Messages()); err != nil {
			rangeErr = err
			return false
		}
		if err := addExtensionsToTypes(types, fileDescriptor.Extensions()); err != nil {
			rangeErr = err
			return false
		}
		// There is no way to do register enum, and it is not used
		// https://github.com/golang/protobuf/issues/1065
		// https://godoc.org/google.golang.org/protobuf/types/dynamicpb does not have NewEnumType
		return true
	})
	if rangeErr != nil {
		return nil, rangeErr
	}
	return types, nil
}

func addMessagesToTypes(types *protoregistry.Types, messageDescriptors protoreflect.MessageDescriptors) error {
	messagesLen := messageDescriptors.Len()
	for i := 0; i < messagesLen; i++ {
		messageDescriptor := messageDescriptors.Get(i)
		if err := types.RegisterMessage(dynamicpb.NewMessageType(messageDescriptor)); err != nil {
			return err
		}
		if err := addMessagesToTypes(types, messageDescriptor.Messages()); err != nil {
			return err
		}
		if err := addExtensionsToTypes(types, messageDescriptor.Extensions()); err != nil {
			return err
		}
	}
	return nil
}

func addExtensionsToTypes(types *protoregistry.Types, extensionDescriptors protoreflect.ExtensionDescriptors) error {
	extensionsLen := extensionDescriptors.Len()
	for i := 0; i < extensionsLen; i++ {
		extensionDescriptor := extensionDescriptors.Get(i)
		if err := types.RegisterExtension(dynamicpb.NewExtensionType(extensionDescriptor)); err != nil {
			return err
		}
	}
	return nil
}
