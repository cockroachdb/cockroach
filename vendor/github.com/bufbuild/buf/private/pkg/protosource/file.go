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

package protosource

import (
	"fmt"

	"github.com/bufbuild/buf/private/pkg/protodescriptor"
	"google.golang.org/protobuf/types/descriptorpb"
)

type file struct {
	FileInfo
	descriptor
	optionExtensionDescriptor

	fileDescriptor protodescriptor.FileDescriptor
	syntax         Syntax
	fileImports    []FileImport
	messages       []Message
	enums          []Enum
	services       []Service
	extensions     []Field
	optimizeMode   FileOptionsOptimizeMode
}

func (f *file) Syntax() Syntax {
	return f.syntax
}

func (f *file) Package() string {
	return f.fileDescriptor.GetPackage()
}

func (f *file) FileImports() []FileImport {
	return f.fileImports
}

func (f *file) Messages() []Message {
	return f.messages
}

func (f *file) Enums() []Enum {
	return f.enums
}

func (f *file) Services() []Service {
	return f.services
}

func (f *file) Extensions() []Field {
	return f.extensions
}

func (f *file) CsharpNamespace() string {
	return f.fileDescriptor.GetOptions().GetCsharpNamespace()
}

func (f *file) GoPackage() string {
	return f.fileDescriptor.GetOptions().GetGoPackage()
}

func (f *file) JavaMultipleFiles() bool {
	return f.fileDescriptor.GetOptions().GetJavaMultipleFiles()
}

func (f *file) JavaOuterClassname() string {
	return f.fileDescriptor.GetOptions().GetJavaOuterClassname()
}

func (f *file) JavaPackage() string {
	return f.fileDescriptor.GetOptions().GetJavaPackage()
}

func (f *file) JavaStringCheckUtf8() bool {
	return f.fileDescriptor.GetOptions().GetJavaStringCheckUtf8()
}

func (f *file) ObjcClassPrefix() string {
	return f.fileDescriptor.GetOptions().GetObjcClassPrefix()
}

func (f *file) PhpClassPrefix() string {
	return f.fileDescriptor.GetOptions().GetPhpClassPrefix()
}

func (f *file) PhpNamespace() string {
	return f.fileDescriptor.GetOptions().GetPhpNamespace()
}

func (f *file) PhpMetadataNamespace() string {
	return f.fileDescriptor.GetOptions().GetPhpMetadataNamespace()
}

func (f *file) RubyPackage() string {
	return f.fileDescriptor.GetOptions().GetRubyPackage()
}

func (f *file) SwiftPrefix() string {
	return f.fileDescriptor.GetOptions().GetSwiftPrefix()
}

func (f *file) OptimizeFor() FileOptionsOptimizeMode {
	return f.optimizeMode
}

func (f *file) CcGenericServices() bool {
	return f.fileDescriptor.GetOptions().GetCcGenericServices()
}

func (f *file) JavaGenericServices() bool {
	return f.fileDescriptor.GetOptions().GetJavaGenericServices()
}

func (f *file) PyGenericServices() bool {
	return f.fileDescriptor.GetOptions().GetPyGenericServices()
}

func (f *file) PhpGenericServices() bool {
	return f.fileDescriptor.GetOptions().GetPhpGenericServices()
}

func (f *file) CcEnableArenas() bool {
	return f.fileDescriptor.GetOptions().GetCcEnableArenas()
}

func (f *file) PackageLocation() Location {
	return f.getLocationByPathKey(packagePathKey)
}

func (f *file) CsharpNamespaceLocation() Location {
	return f.getLocationByPathKey(csharpNamespacePathKey)
}

func (f *file) GoPackageLocation() Location {
	return f.getLocationByPathKey(goPackagePathKey)
}

func (f *file) JavaMultipleFilesLocation() Location {
	return f.getLocationByPathKey(javaMultipleFilesPathKey)
}

func (f *file) JavaOuterClassnameLocation() Location {
	return f.getLocationByPathKey(javaOuterClassnamePathKey)
}

func (f *file) JavaPackageLocation() Location {
	return f.getLocationByPathKey(javaPackagePathKey)
}

func (f *file) JavaStringCheckUtf8Location() Location {
	return f.getLocationByPathKey(javaStringCheckUtf8PathKey)
}

func (f *file) ObjcClassPrefixLocation() Location {
	return f.getLocationByPathKey(objcClassPrefixPathKey)
}

func (f *file) PhpClassPrefixLocation() Location {
	return f.getLocationByPathKey(phpClassPrefixPathKey)
}

func (f *file) PhpNamespaceLocation() Location {
	return f.getLocationByPathKey(phpNamespacePathKey)
}

func (f *file) PhpMetadataNamespaceLocation() Location {
	return f.getLocationByPathKey(phpMetadataNamespacePathKey)
}

func (f *file) RubyPackageLocation() Location {
	return f.getLocationByPathKey(rubyPackagePathKey)
}

func (f *file) SwiftPrefixLocation() Location {
	return f.getLocationByPathKey(swiftPrefixPathKey)
}

func (f *file) OptimizeForLocation() Location {
	return f.getLocationByPathKey(optimizeForPathKey)
}

func (f *file) CcGenericServicesLocation() Location {
	return f.getLocationByPathKey(ccGenericServicesPathKey)
}

func (f *file) JavaGenericServicesLocation() Location {
	return f.getLocationByPathKey(javaGenericServicesPathKey)
}

func (f *file) PyGenericServicesLocation() Location {
	return f.getLocationByPathKey(pyGenericServicesPathKey)
}

func (f *file) PhpGenericServicesLocation() Location {
	return f.getLocationByPathKey(phpGenericServicesPathKey)
}

func (f *file) CcEnableArenasLocation() Location {
	return f.getLocationByPathKey(ccEnableArenasPathKey)
}

func (f *file) SyntaxLocation() Location {
	return f.getLocationByPathKey(syntaxPathKey)
}

// does not validation of the fileDescriptorProto - this is assumed to be done elsewhere
// does no duplicate checking by name - could just have maps ie importToFileImport, enumNameToEnum, etc
func newFile(inputFile InputFile) (*file, error) {
	f := &file{
		FileInfo:       inputFile,
		fileDescriptor: inputFile.FileDescriptor(),
		optionExtensionDescriptor: newOptionExtensionDescriptor(
			inputFile.FileDescriptor().GetOptions(),
		),
	}
	descriptor := newDescriptor(
		f,
		newLocationStore(f.fileDescriptor.GetSourceCodeInfo().GetLocation()),
	)
	f.descriptor = descriptor

	if inputFile.IsSyntaxUnspecified() {
		f.syntax = SyntaxUnspecified
	} else {
		switch syntaxString := f.fileDescriptor.GetSyntax(); syntaxString {
		// if the syntax is "proto2", protoc and buf will not set the syntax
		// field even if it was explicitly set, this is why we have
		// IsSyntaxUnspecified
		case "", "proto2":
			f.syntax = SyntaxProto2
		case "proto3":
			f.syntax = SyntaxProto3
		default:
			return nil, fmt.Errorf("unknown syntax: %q", syntaxString)
		}
	}

	for dependencyIndex, dependency := range f.fileDescriptor.GetDependency() {
		fileImport, err := newFileImport(
			f.descriptor,
			dependency,
			getDependencyPath(dependencyIndex),
		)
		if err != nil {
			return nil, err
		}
		f.fileImports = append(f.fileImports, fileImport)
	}
	for _, dependencyIndex := range f.fileDescriptor.GetPublicDependency() {
		if int(dependencyIndex) < 0 || len(f.fileImports) <= int(dependencyIndex) {
			return nil, fmt.Errorf("got dependency index of %d but length of imports is %d", dependencyIndex, len(f.fileImports))
		}
		fileImport, ok := f.fileImports[dependencyIndex].(*fileImport)
		if !ok {
			return nil, fmt.Errorf("could not cast %T to a *fileImport", f.fileImports[dependencyIndex])
		}
		fileImport.setIsPublic()
	}
	for _, dependencyIndex := range f.fileDescriptor.GetWeakDependency() {
		if int(dependencyIndex) < 0 || len(f.fileImports) <= int(dependencyIndex) {
			return nil, fmt.Errorf("got dependency index of %d but length of imports is %d", dependencyIndex, len(f.fileImports))
		}
		fileImport, ok := f.fileImports[dependencyIndex].(*fileImport)
		if !ok {
			return nil, fmt.Errorf("could not cast %T to a *fileImport", f.fileImports[dependencyIndex])
		}
		fileImport.setIsWeak()
	}
	for _, dependencyIndex := range inputFile.UnusedDependencyIndexes() {
		if int(dependencyIndex) < 0 || len(f.fileImports) <= int(dependencyIndex) {
			return nil, fmt.Errorf("got dependency index of %d but length of imports is %d", dependencyIndex, len(f.fileImports))
		}
		fileImport, ok := f.fileImports[dependencyIndex].(*fileImport)
		if !ok {
			return nil, fmt.Errorf("could not cast %T to a *fileImport", f.fileImports[dependencyIndex])
		}
		fileImport.setIsUnused()
	}
	for enumIndex, enumDescriptorProto := range f.fileDescriptor.GetEnumType() {
		enum, err := f.populateEnum(
			enumDescriptorProto,
			enumIndex,
			nil,
			nil,
		)
		if err != nil {
			return nil, err
		}
		f.enums = append(f.enums, enum)
	}
	for messageIndex, descriptorProto := range f.fileDescriptor.GetMessageType() {
		message, err := f.populateMessage(
			descriptorProto,
			messageIndex,
			nil,
			nil,
			nil,
		)
		if err != nil {
			return nil, err
		}
		f.messages = append(f.messages, message)
	}
	for serviceIndex, serviceDescriptorProto := range f.fileDescriptor.GetService() {
		service, err := f.populateService(
			serviceDescriptorProto,
			serviceIndex,
		)
		if err != nil {
			return nil, err
		}
		f.services = append(f.services, service)
	}
	for extensionIndex, extensionDescriptorProto := range f.fileDescriptor.GetExtension() {
		extension, err := f.populateExtension(
			extensionDescriptorProto,
			extensionIndex,
		)
		if err != nil {
			return nil, err
		}
		f.extensions = append(f.extensions, extension)
	}
	optimizeMode, err := getFileOptionsOptimizeMode(f.fileDescriptor.GetOptions().GetOptimizeFor())
	if err != nil {
		return nil, err
	}
	f.optimizeMode = optimizeMode
	return f, nil
}

func (f *file) populateEnum(
	enumDescriptorProto *descriptorpb.EnumDescriptorProto,
	enumIndex int,
	// all message indexes leading to this enum
	nestedMessageIndexes []int,
	// all message names leading to this enum
	nestedMessageNames []string,
) (Enum, error) {
	enumNamedDescriptor, err := newNamedDescriptor(
		newLocationDescriptor(
			f.descriptor,
			getEnumPath(enumIndex, nestedMessageIndexes...),
		),
		enumDescriptorProto.GetName(),
		getEnumNamePath(enumIndex, nestedMessageIndexes...),
		nestedMessageNames,
	)
	if err != nil {
		return nil, err
	}
	enum := newEnum(
		enumNamedDescriptor,
		newOptionExtensionDescriptor(
			enumDescriptorProto.GetOptions(),
		),
		enumDescriptorProto.GetOptions().GetAllowAlias(),
		getEnumAllowAliasPath(enumIndex, nestedMessageIndexes...),
	)

	for enumValueIndex, enumValueDescriptorProto := range enumDescriptorProto.GetValue() {
		enumValueNamedDescriptor, err := newNamedDescriptor(
			newLocationDescriptor(
				f.descriptor,
				getEnumValuePath(enumIndex, enumValueIndex, nestedMessageIndexes...),
			),
			enumValueDescriptorProto.GetName(),
			getEnumValueNamePath(enumIndex, enumValueIndex, nestedMessageIndexes...),
			append(nestedMessageNames, enum.Name()),
		)
		if err != nil {
			return nil, err
		}
		enumValue := newEnumValue(
			enumValueNamedDescriptor,
			newOptionExtensionDescriptor(
				enumValueDescriptorProto.GetOptions(),
			),
			enum,
			int(enumValueDescriptorProto.GetNumber()),
			getEnumValueNumberPath(enumIndex, enumValueIndex, nestedMessageIndexes...),
		)
		enum.addValue(enumValue)
	}

	for reservedRangeIndex, reservedRangeDescriptorProto := range enumDescriptorProto.GetReservedRange() {
		reservedRangeLocationDescriptor := newLocationDescriptor(
			f.descriptor,
			getEnumReservedRangePath(enumIndex, reservedRangeIndex, nestedMessageIndexes...),
		)
		reservedEnumRange := newEnumRange(
			reservedRangeLocationDescriptor,
			enum,
			int(reservedRangeDescriptorProto.GetStart()),
			int(reservedRangeDescriptorProto.GetEnd()),
		)
		enum.addReservedEnumRange(reservedEnumRange)
	}
	for reservedNameIndex, reservedNameValue := range enumDescriptorProto.GetReservedName() {
		reservedNameLocationDescriptor := newLocationDescriptor(
			f.descriptor,
			getEnumReservedNamePath(enumIndex, reservedNameIndex, nestedMessageIndexes...),
		)
		reservedName, err := newReservedName(
			reservedNameLocationDescriptor,
			reservedNameValue,
		)
		if err != nil {
			return nil, err
		}
		enum.addReservedName(reservedName)
	}
	return enum, nil
}

func (f *file) populateMessage(
	descriptorProto *descriptorpb.DescriptorProto,
	// always stays the same on every recursive call
	topLevelMessageIndex int,
	// includes descriptorProto index
	nestedMessageIndexes []int,
	// does NOT include descriptorProto.GetName()
	nestedMessageNames []string,
	parent Message,
) (Message, error) {
	messageNamedDescriptor, err := newNamedDescriptor(
		newLocationDescriptor(
			f.descriptor,
			getMessagePath(topLevelMessageIndex, nestedMessageIndexes...),
		),
		descriptorProto.GetName(),
		getMessageNamePath(topLevelMessageIndex, nestedMessageIndexes...),
		nestedMessageNames,
	)
	if err != nil {
		return nil, err
	}
	message := newMessage(
		messageNamedDescriptor,
		newOptionExtensionDescriptor(
			descriptorProto.GetOptions(),
		),
		parent,
		descriptorProto.GetOptions().GetMapEntry(),
		descriptorProto.GetOptions().GetMessageSetWireFormat(),
		descriptorProto.GetOptions().GetNoStandardDescriptorAccessor(),
		getMessageMessageSetWireFormatPath(topLevelMessageIndex, nestedMessageIndexes...),
		getMessageNoStandardDescriptorAccessorPath(topLevelMessageIndex, nestedMessageIndexes...),
	)
	oneofIndexToOneof := make(map[int]*oneof)
	for oneofIndex, oneofDescriptorProto := range descriptorProto.GetOneofDecl() {
		oneofNamedDescriptor, err := newNamedDescriptor(
			newLocationDescriptor(
				f.descriptor,
				getMessageOneofPath(oneofIndex, topLevelMessageIndex, nestedMessageIndexes...),
			),
			oneofDescriptorProto.GetName(),
			getMessageOneofNamePath(oneofIndex, topLevelMessageIndex, nestedMessageIndexes...),
			append(nestedMessageNames, message.Name()),
		)
		if err != nil {
			return nil, err
		}
		oneof := newOneof(
			oneofNamedDescriptor,
			newOptionExtensionDescriptor(
				oneofDescriptorProto.GetOptions(),
			),
			message,
		)
		message.addOneof(oneof)
		oneofIndexToOneof[oneofIndex] = oneof
	}
	for fieldIndex, fieldDescriptorProto := range descriptorProto.GetField() {
		// TODO: not working for map entries
		fieldNamedDescriptor, err := newNamedDescriptor(
			newLocationDescriptor(
				f.descriptor,
				getMessageFieldPath(fieldIndex, topLevelMessageIndex, nestedMessageIndexes...),
			),
			fieldDescriptorProto.GetName(),
			getMessageFieldNamePath(fieldIndex, topLevelMessageIndex, nestedMessageIndexes...),
			append(nestedMessageNames, message.Name()),
		)
		if err != nil {
			return nil, err
		}
		var packed *bool
		if fieldDescriptorProto.Options != nil {
			packed = fieldDescriptorProto.GetOptions().Packed
		}
		label, err := getFieldDescriptorProtoLabel(fieldDescriptorProto.GetLabel())
		if err != nil {
			return nil, err
		}
		typ, err := getFieldDescriptorProtoType(fieldDescriptorProto.GetType())
		if err != nil {
			return nil, err
		}
		jsType, err := getFieldOptionsJSType(fieldDescriptorProto.GetOptions().GetJstype())
		if err != nil {
			return nil, err
		}
		cType, err := getFieldOptionsCType(fieldDescriptorProto.GetOptions().GetCtype())
		if err != nil {
			return nil, err
		}
		var oneof *oneof
		var ok bool
		if fieldDescriptorProto.OneofIndex != nil {
			oneofIndex := int(*fieldDescriptorProto.OneofIndex)
			oneof, ok = oneofIndexToOneof[oneofIndex]
			if !ok {
				return nil, fmt.Errorf("no oneof for index %d", oneofIndex)
			}
		}
		field := newField(
			fieldNamedDescriptor,
			newOptionExtensionDescriptor(
				fieldDescriptorProto.GetOptions(),
			),
			message,
			int(fieldDescriptorProto.GetNumber()),
			label,
			typ,
			fieldDescriptorProto.GetTypeName(),
			fieldDescriptorProto.GetExtendee(),
			oneof,
			fieldDescriptorProto.GetProto3Optional(),
			fieldDescriptorProto.GetJsonName(),
			jsType,
			cType,
			packed,
			getMessageFieldNumberPath(fieldIndex, topLevelMessageIndex, nestedMessageIndexes...),
			getMessageFieldTypePath(fieldIndex, topLevelMessageIndex, nestedMessageIndexes...),
			getMessageFieldTypeNamePath(fieldIndex, topLevelMessageIndex, nestedMessageIndexes...),
			getMessageFieldJSONNamePath(fieldIndex, topLevelMessageIndex, nestedMessageIndexes...),
			getMessageFieldJSTypePath(fieldIndex, topLevelMessageIndex, nestedMessageIndexes...),
			getMessageFieldCTypePath(fieldIndex, topLevelMessageIndex, nestedMessageIndexes...),
			getMessageFieldPackedPath(fieldIndex, topLevelMessageIndex, nestedMessageIndexes...),
			getMessageFieldExtendeePath(fieldIndex, topLevelMessageIndex, nestedMessageIndexes...),
		)
		message.addField(field)
		if oneof != nil {
			oneof.addField(field)
		}
	}
	for fieldIndex, fieldDescriptorProto := range descriptorProto.GetExtension() {
		fieldNamedDescriptor, err := newNamedDescriptor(
			newLocationDescriptor(
				f.descriptor,
				getMessageExtensionPath(fieldIndex, topLevelMessageIndex, nestedMessageIndexes...),
			),
			fieldDescriptorProto.GetName(),
			getMessageExtensionNamePath(fieldIndex, topLevelMessageIndex, nestedMessageIndexes...),
			append(nestedMessageNames, message.Name()),
		)
		if err != nil {
			return nil, err
		}
		var packed *bool
		if fieldDescriptorProto.Options != nil {
			packed = fieldDescriptorProto.GetOptions().Packed
		}
		label, err := getFieldDescriptorProtoLabel(fieldDescriptorProto.GetLabel())
		if err != nil {
			return nil, err
		}
		typ, err := getFieldDescriptorProtoType(fieldDescriptorProto.GetType())
		if err != nil {
			return nil, err
		}
		jsType, err := getFieldOptionsJSType(fieldDescriptorProto.GetOptions().GetJstype())
		if err != nil {
			return nil, err
		}
		cType, err := getFieldOptionsCType(fieldDescriptorProto.GetOptions().GetCtype())
		if err != nil {
			return nil, err
		}
		var oneof *oneof
		var ok bool
		if fieldDescriptorProto.OneofIndex != nil {
			oneofIndex := int(*fieldDescriptorProto.OneofIndex)
			oneof, ok = oneofIndexToOneof[oneofIndex]
			if !ok {
				return nil, fmt.Errorf("no oneof for index %d", oneofIndex)
			}
		}
		field := newField(
			fieldNamedDescriptor,
			newOptionExtensionDescriptor(
				fieldDescriptorProto.GetOptions(),
			),
			message,
			int(fieldDescriptorProto.GetNumber()),
			label,
			typ,
			fieldDescriptorProto.GetTypeName(),
			fieldDescriptorProto.GetExtendee(),
			oneof,
			fieldDescriptorProto.GetProto3Optional(),
			fieldDescriptorProto.GetJsonName(),
			jsType,
			cType,
			packed,
			getMessageExtensionNumberPath(fieldIndex, topLevelMessageIndex, nestedMessageIndexes...),
			getMessageExtensionTypePath(fieldIndex, topLevelMessageIndex, nestedMessageIndexes...),
			getMessageExtensionTypeNamePath(fieldIndex, topLevelMessageIndex, nestedMessageIndexes...),
			getMessageExtensionJSONNamePath(fieldIndex, topLevelMessageIndex, nestedMessageIndexes...),
			getMessageExtensionJSTypePath(fieldIndex, topLevelMessageIndex, nestedMessageIndexes...),
			getMessageExtensionCTypePath(fieldIndex, topLevelMessageIndex, nestedMessageIndexes...),
			getMessageExtensionPackedPath(fieldIndex, topLevelMessageIndex, nestedMessageIndexes...),
			getMessageExtensionExtendeePath(fieldIndex, topLevelMessageIndex, nestedMessageIndexes...),
		)
		if err != nil {
			return nil, err
		}
		message.addExtension(field)
		if oneof != nil {
			oneof.addField(field)
		}
	}
	for reservedRangeIndex, reservedRangeDescriptorProto := range descriptorProto.GetReservedRange() {
		reservedRangeLocationDescriptor := newLocationDescriptor(
			f.descriptor,
			getMessageReservedRangePath(reservedRangeIndex, topLevelMessageIndex, nestedMessageIndexes...),
		)
		reservedMessageRange := newMessageRange(
			reservedRangeLocationDescriptor,
			message,
			int(reservedRangeDescriptorProto.GetStart()),
			int(reservedRangeDescriptorProto.GetEnd()),
		)
		message.addReservedMessageRange(reservedMessageRange)
	}
	for reservedNameIndex, reservedNameValue := range descriptorProto.GetReservedName() {
		reservedNameLocationDescriptor := newLocationDescriptor(
			f.descriptor,
			getMessageReservedNamePath(reservedNameIndex, topLevelMessageIndex, nestedMessageIndexes...),
		)
		reservedName, err := newReservedName(
			reservedNameLocationDescriptor,
			reservedNameValue,
		)
		if err != nil {
			return nil, err
		}
		message.addReservedName(reservedName)
	}
	for extensionRangeIndex, extensionRangeDescriptorProto := range descriptorProto.GetExtensionRange() {
		extensionRangeLocationDescriptor := newLocationDescriptor(
			f.descriptor,
			getMessageExtensionRangePath(extensionRangeIndex, topLevelMessageIndex, nestedMessageIndexes...),
		)
		extensionMessageRange := newMessageRange(
			extensionRangeLocationDescriptor,
			message,
			int(extensionRangeDescriptorProto.GetStart()),
			int(extensionRangeDescriptorProto.GetEnd()),
		)
		message.addExtensionMessageRange(extensionMessageRange)
	}
	for enumIndex, enumDescriptorProto := range descriptorProto.GetEnumType() {
		nestedEnum, err := f.populateEnum(
			enumDescriptorProto,
			enumIndex,
			// this is all of the message indexes including this one
			// TODO we should refactor get.*Path messages to be more consistent
			append([]int{topLevelMessageIndex}, nestedMessageIndexes...),
			append(nestedMessageNames, message.Name()),
		)
		if err != nil {
			return nil, err
		}
		message.addNestedEnum(nestedEnum)
	}
	for nestedMessageIndex, nestedMessageDescriptorProto := range descriptorProto.GetNestedType() {
		nestedMessage, err := f.populateMessage(
			nestedMessageDescriptorProto,
			topLevelMessageIndex,
			append(nestedMessageIndexes, nestedMessageIndex),
			append(nestedMessageNames, message.Name()),
			message,
		)
		if err != nil {
			return nil, err
		}
		message.addNestedMessage(nestedMessage)
	}
	return message, nil
}

func (f *file) populateService(
	serviceDescriptorProto *descriptorpb.ServiceDescriptorProto,
	serviceIndex int,
) (Service, error) {
	serviceNamedDescriptor, err := newNamedDescriptor(
		newLocationDescriptor(
			f.descriptor,
			getServicePath(serviceIndex),
		),
		serviceDescriptorProto.GetName(),
		getServiceNamePath(serviceIndex),
		nil,
	)
	if err != nil {
		return nil, err
	}
	service := newService(
		serviceNamedDescriptor,
		newOptionExtensionDescriptor(
			serviceDescriptorProto.GetOptions(),
		),
	)
	for methodIndex, methodDescriptorProto := range serviceDescriptorProto.GetMethod() {
		methodNamedDescriptor, err := newNamedDescriptor(
			newLocationDescriptor(
				f.descriptor,
				getMethodPath(serviceIndex, methodIndex),
			),
			methodDescriptorProto.GetName(),
			getMethodNamePath(serviceIndex, methodIndex),
			[]string{service.Name()},
		)
		if err != nil {
			return nil, err
		}
		idempotencyLevel, err := getMethodOptionsIdempotencyLevel(methodDescriptorProto.GetOptions().GetIdempotencyLevel())
		if err != nil {
			return nil, err
		}
		method, err := newMethod(
			methodNamedDescriptor,
			newOptionExtensionDescriptor(
				methodDescriptorProto.GetOptions(),
			),
			service,
			methodDescriptorProto.GetInputType(),
			methodDescriptorProto.GetOutputType(),
			methodDescriptorProto.GetClientStreaming(),
			methodDescriptorProto.GetServerStreaming(),
			getMethodInputTypePath(serviceIndex, methodIndex),
			getMethodOutputTypePath(serviceIndex, methodIndex),
			idempotencyLevel,
			getMethodIdempotencyLevelPath(serviceIndex, methodIndex),
		)
		if err != nil {
			return nil, err
		}
		service.addMethod(method)
	}
	return service, nil
}

func (f *file) populateExtension(
	fieldDescriptorProto *descriptorpb.FieldDescriptorProto,
	fieldIndex int,
) (Field, error) {
	fieldNamedDescriptor, err := newNamedDescriptor(
		newLocationDescriptor(
			f.descriptor,
			getFileExtensionPath(fieldIndex),
		),
		fieldDescriptorProto.GetName(),
		getFileExtensionNamePath(fieldIndex),
		nil,
	)
	if err != nil {
		return nil, err
	}
	var packed *bool
	if fieldDescriptorProto.Options != nil {
		packed = fieldDescriptorProto.GetOptions().Packed
	}
	label, err := getFieldDescriptorProtoLabel(fieldDescriptorProto.GetLabel())
	if err != nil {
		return nil, err
	}
	typ, err := getFieldDescriptorProtoType(fieldDescriptorProto.GetType())
	if err != nil {
		return nil, err
	}
	jsType, err := getFieldOptionsJSType(fieldDescriptorProto.GetOptions().GetJstype())
	if err != nil {
		return nil, err
	}
	cType, err := getFieldOptionsCType(fieldDescriptorProto.GetOptions().GetCtype())
	if err != nil {
		return nil, err
	}
	return newField(
		fieldNamedDescriptor,
		newOptionExtensionDescriptor(
			fieldDescriptorProto.GetOptions(),
		),
		nil,
		int(fieldDescriptorProto.GetNumber()),
		label,
		typ,
		fieldDescriptorProto.GetTypeName(),
		fieldDescriptorProto.GetExtendee(),
		nil,
		fieldDescriptorProto.GetProto3Optional(),
		fieldDescriptorProto.GetJsonName(),
		jsType,
		cType,
		packed,
		getFileExtensionNumberPath(fieldIndex),
		getFileExtensionTypePath(fieldIndex),
		getFileExtensionTypeNamePath(fieldIndex),
		getFileExtensionJSONNamePath(fieldIndex),
		getFileExtensionJSTypePath(fieldIndex),
		getFileExtensionCTypePath(fieldIndex),
		getFileExtensionPackedPath(fieldIndex),
		getFileExtensionExtendeePath(fieldIndex),
	), nil
}
