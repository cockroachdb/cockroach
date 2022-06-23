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

// Package protosource defines minimal interfaces for Protobuf descriptor types.
//
// This is done so that the backing package can be swapped out easily.
//
// All values that return SourceLocation can be nil.
//
// Testing is currently implicitly done through the bufcheck packages, however
// if this were to be split out into a separate library, it would need a separate
// testing suite.
package protosource

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/bufbuild/buf/private/pkg/normalpath"
	"github.com/bufbuild/buf/private/pkg/protodescriptor"
	"google.golang.org/protobuf/reflect/protoreflect"
)

const (
	// SyntaxUnspecified represents no syntax being specified.
	//
	// This is functionally equivalent to SyntaxProto2.
	SyntaxUnspecified Syntax = iota + 1
	// SyntaxProto2 represents the proto2 syntax.
	SyntaxProto2
	// SyntaxProto3 represents the proto3 syntax.
	SyntaxProto3
)

// Syntax is the syntax of a file.
type Syntax int

// String returns the string representation of s
func (s Syntax) String() string {
	switch s {
	case SyntaxUnspecified:
		return "unspecified"
	case SyntaxProto2:
		return "proto2"
	case SyntaxProto3:
		return "proto3"
	default:
		return strconv.Itoa(int(s))
	}
}

// Descriptor is the base interface for a descriptor type.
type Descriptor interface {
	// File returns the associated File.
	//
	// Always non-nil.
	File() File
}

// LocationDescriptor is the base interface for a descriptor type with a location.
type LocationDescriptor interface {
	Descriptor

	// Location returns the location of the entire descriptor.
	//
	// Can return nil, although will generally not be nil.
	Location() Location
}

// NamedDescriptor is the base interface for a named descriptor type.
type NamedDescriptor interface {
	LocationDescriptor

	// FullName returns the fully-qualified name, i.e. some.pkg.Nested.Message.FooEnum.ENUM_VALUE.
	//
	// Always non-empty.
	FullName() string
	// NestedName returns the full nested name without the package, i.e. Nested.Message.FooEnum
	// or Nested.Message.FooEnum.ENUM_VALUE.
	//
	// Always non-empty.
	NestedName() string
	// Name returns the short name, or the name of a value or field, i.e. FooEnum or ENUM_VALUE.
	//
	// Always non-empty.
	Name() string
	// NameLocation returns the location of the name of the descriptor.
	//
	// If the backing descriptor does not have name-level resolution, this will
	// attempt to return a location of the entire descriptor.
	//
	// If the backing descriptor has comments for the entire descriptor, these
	// will be added to the named location.
	//
	// Can return nil.
	NameLocation() Location
}

// ContainerDescriptor contains Enums and Messages.
type ContainerDescriptor interface {
	Enums() []Enum
	Messages() []Message
}

// OptionExtensionDescriptor contains option extensions.
type OptionExtensionDescriptor interface {
	// OptionExtension returns the value for an options extension field.
	//
	// Returns false if the extension is not set.
	// Panics if the ExtensionType does not extend the message.
	//
	// TODO: handle the panic by figuring out if ExtensionType extends the
	// message before passing to HasExtension or GetExtension.
	//
	// See https://pkg.go.dev/google.golang.org/protobuf/proto#HasExtension
	// See https://pkg.go.dev/google.golang.org/protobuf/proto#GetExtension
	OptionExtension(extensionType protoreflect.ExtensionType) (interface{}, bool)
}

// Location defines source code info location information.
//
// May be extended in the future to include comments.
//
// Note that unlike SourceCodeInfo_Location, these are not zero-indexed.
type Location interface {
	StartLine() int
	StartColumn() int
	EndLine() int
	EndColumn() int
	LeadingComments() string
	TrailingComments() string
	// NOT a copy. Do not modify.
	LeadingDetachedComments() []string
}

// ModuleIdentity is a module identity.
type ModuleIdentity interface {
	Remote() string
	Owner() string
	Repository() string
}

// FileInfo contains Protobuf file info.
type FileInfo interface {
	// Path is the path of the file relative to the root it is contained within.
	// This will be normalized, validated and never empty,
	// This will be unique within a given Image.
	Path() string
	// ExternalPath returns the path that identifies this file externally.
	//
	// This will be unnormalized.
	// Never empty. Falls back to Path if there is not an external path.
	//
	// Example:
	//	 Assume we had the input path /foo/bar which is a local directory.

	//   Path: one/one.proto
	//   RootDirPath: proto
	//   ExternalPath: /foo/bar/proto/one/one.proto
	ExternalPath() string
	// ModuleIdentity is the module that this file came from.
	//
	// Note this *can* be nil if we did not build from a named module.
	// All code must assume this can be nil.
	// Note that nil checking should work since the backing type is always a pointer.
	ModuleIdentity() ModuleIdentity
	// Commit is the commit for the module that this file came from.
	//
	// This will only be set if ModuleIdentity is set, but may not be set
	// even if ModuleIdentity is set, that is commit is optional information
	// even if we know what module this file came from.
	Commit() string
}

// File is a file descriptor.
type File interface {
	Descriptor
	FileInfo

	// Top-level only.
	ContainerDescriptor
	OptionExtensionDescriptor

	Syntax() Syntax
	Package() string
	FileImports() []FileImport
	Services() []Service
	Extensions() []Field

	CsharpNamespace() string
	GoPackage() string
	JavaMultipleFiles() bool
	JavaOuterClassname() string
	JavaPackage() string
	JavaStringCheckUtf8() bool
	ObjcClassPrefix() string
	PhpClassPrefix() string
	PhpNamespace() string
	PhpMetadataNamespace() string
	RubyPackage() string
	SwiftPrefix() string

	OptimizeFor() FileOptionsOptimizeMode
	CcGenericServices() bool
	JavaGenericServices() bool
	PyGenericServices() bool
	PhpGenericServices() bool
	CcEnableArenas() bool

	SyntaxLocation() Location
	PackageLocation() Location
	CsharpNamespaceLocation() Location
	GoPackageLocation() Location
	JavaMultipleFilesLocation() Location
	JavaOuterClassnameLocation() Location
	JavaPackageLocation() Location
	JavaStringCheckUtf8Location() Location
	ObjcClassPrefixLocation() Location
	PhpClassPrefixLocation() Location
	PhpNamespaceLocation() Location
	PhpMetadataNamespaceLocation() Location
	RubyPackageLocation() Location
	SwiftPrefixLocation() Location

	OptimizeForLocation() Location
	CcGenericServicesLocation() Location
	JavaGenericServicesLocation() Location
	PyGenericServicesLocation() Location
	PhpGenericServicesLocation() Location
	CcEnableArenasLocation() Location
}

// FileImport is a file import descriptor.
type FileImport interface {
	LocationDescriptor

	Import() string
	IsPublic() bool
	IsWeak() bool
	IsUnused() bool
}

// TagRange is a tag range from start to end.
type TagRange interface {
	LocationDescriptor

	// Start is the start of the range.
	Start() int
	// End is the end of the range.
	// Inclusive.
	End() int
	// Max says that the End is the max.
	Max() bool
}

// ReservedName is a reserved name for an enum or message.
type ReservedName interface {
	LocationDescriptor

	Value() string
}

// ReservedDescriptor has reserved ranges and names.
type ReservedDescriptor interface {
	ReservedTagRanges() []TagRange
	ReservedNames() []ReservedName
}

// EnumRange is a TagRange for Enums.
type EnumRange interface {
	TagRange

	Enum() Enum
}

// MessageRange is a TagRange for Messages.
type MessageRange interface {
	TagRange

	Message() Message
}

// Enum is an enum descriptor.
type Enum interface {
	NamedDescriptor
	ReservedDescriptor
	OptionExtensionDescriptor

	Values() []EnumValue
	ReservedEnumRanges() []EnumRange

	AllowAlias() bool
	AllowAliasLocation() Location
}

// EnumValue is an enum value descriptor.
type EnumValue interface {
	NamedDescriptor
	OptionExtensionDescriptor

	Enum() Enum
	Number() int

	NumberLocation() Location
}

// Message is a message descriptor.
type Message interface {
	NamedDescriptor
	// Only those directly nested under this message.
	ContainerDescriptor
	ReservedDescriptor
	OptionExtensionDescriptor

	// Includes fields in oneofs.
	Fields() []Field
	Extensions() []Field
	Oneofs() []Oneof
	ExtensionMessageRanges() []MessageRange
	ReservedMessageRanges() []MessageRange

	// Will return nil if this is a top-level message
	Parent() Message
	IsMapEntry() bool

	MessageSetWireFormat() bool
	NoStandardDescriptorAccessor() bool
	MessageSetWireFormatLocation() Location
	NoStandardDescriptorAccessorLocation() Location
}

// Field is a field descriptor.
type Field interface {
	NamedDescriptor
	OptionExtensionDescriptor

	// May be nil if this is attached to a file.
	Message() Message
	Number() int
	Label() FieldDescriptorProtoLabel
	Type() FieldDescriptorProtoType
	TypeName() string
	// may be nil
	Oneof() Oneof
	Proto3Optional() bool
	JSONName() string
	JSType() FieldOptionsJSType
	CType() FieldOptionsCType
	// Set vs unset matters for packed
	// See the comments on descriptor.proto
	Packed() *bool
	// Empty string unless the field is part of an extension
	Extendee() string

	NumberLocation() Location
	TypeLocation() Location
	TypeNameLocation() Location
	JSONNameLocation() Location
	JSTypeLocation() Location
	CTypeLocation() Location
	PackedLocation() Location
	ExtendeeLocation() Location
}

// Oneof is a oneof descriptor.
type Oneof interface {
	NamedDescriptor
	OptionExtensionDescriptor

	Message() Message
	Fields() []Field
}

// Service is a service descriptor.
type Service interface {
	NamedDescriptor
	OptionExtensionDescriptor

	Methods() []Method
}

// Method is a method descriptor.
type Method interface {
	NamedDescriptor
	OptionExtensionDescriptor

	Service() Service
	InputTypeName() string
	OutputTypeName() string
	ClientStreaming() bool
	ServerStreaming() bool
	InputTypeLocation() Location
	OutputTypeLocation() Location

	IdempotencyLevel() MethodOptionsIdempotencyLevel
	IdempotencyLevelLocation() Location
}

// InputFile is an input file for NewFile.
type InputFile interface {
	FileInfo
	// FileDescriptor is the backing FileDescriptor for this File.
	//
	// This will never be nil.
	// The value Path() is equal to FileDescriptor().GetName() .
	FileDescriptor() protodescriptor.FileDescriptor
	// IsSyntaxUnspecified will be true if the syntax was not explicitly specified.
	IsSyntaxUnspecified() bool
	// UnusedDependencyIndexes returns the indexes of the unused dependencies within
	// FileDescriptor.GetDependency().
	//
	// All indexes will be valid.
	// Will return nil if empty.
	UnusedDependencyIndexes() []int32
}

// NewFile returns a new File.
func NewFile(inputFile InputFile) (File, error) {
	return newFile(inputFile)
}

// NewFilesUnstable converts the input Files into Files.
//
// This may be done concurrently and the returned Files may not be in the same
// order as the input FileDescriptors. If ordering matters, use NewFile.
func NewFilesUnstable(ctx context.Context, inputFiles ...InputFile) ([]File, error) {
	return newFilesUnstable(ctx, inputFiles...)
}

// SortFiles sorts the Files by FilePath.
func SortFiles(files []File) {
	sort.Slice(files, func(i int, j int) bool { return files[i].Path() < files[j].Path() })
}

// FilePathToFile maps the Files to a map from Path() to File.
//
// Returns error if file paths are not unique.
func FilePathToFile(files ...File) (map[string]File, error) {
	filePathToFile := make(map[string]File, len(files))
	for _, file := range files {
		filePath := file.Path()
		if _, ok := filePathToFile[filePath]; ok {
			return nil, fmt.Errorf("duplicate filePath: %q", filePath)
		}
		filePathToFile[filePath] = file
	}
	return filePathToFile, nil
}

// DirPathToFiles maps the Files to a map from directory
// to the slice of Files in that directory.
//
// Returns error if file paths are not unique.
// Directories are normalized.
//
// Files will be sorted by FilePath.
func DirPathToFiles(files ...File) (map[string][]File, error) {
	return mapFiles(files, func(file File) string { return normalpath.Dir(file.Path()) })
}

// PackageToFiles maps the Files to a map from Protobuf package
// to the slice of Files in that package.
//
// Returns error if file paths are not unique.
//
// Files will be sorted by Path.
func PackageToFiles(files ...File) (map[string][]File, error) {
	// works for no package since "" is a valid map key
	return mapFiles(files, File.Package)
}

// ForEachEnum calls f on each Enum in the given ContainerDescriptor, including nested Enums.
//
// Returns error and stops iterating if f returns error
// Never returns error unless f returns error.
func ForEachEnum(f func(Enum) error, containerDescriptor ContainerDescriptor) error {
	for _, enum := range containerDescriptor.Enums() {
		if err := f(enum); err != nil {
			return err
		}
	}
	for _, message := range containerDescriptor.Messages() {
		if err := ForEachEnum(f, message); err != nil {
			return err
		}
	}
	return nil
}

// ForEachMessage calls f on each Message in the given ContainerDescriptor, including nested Messages.
//
// Returns error and stops iterating if f returns error
// Never returns error unless f returns error.
func ForEachMessage(f func(Message) error, containerDescriptor ContainerDescriptor) error {
	for _, message := range containerDescriptor.Messages() {
		if err := f(message); err != nil {
			return err
		}
		if err := ForEachMessage(f, message); err != nil {
			return err
		}
	}
	return nil
}

// NestedNameToEnum maps the Enums in the ContainerDescriptor to a map from
// nested name to Enum.
//
// Returns error if Enums do not have unique nested names within the ContainerDescriptor,
// which should generally never happen for properly-formed ContainerDescriptors.
func NestedNameToEnum(containerDescriptor ContainerDescriptor) (map[string]Enum, error) {
	nestedNameToEnum := make(map[string]Enum)
	if err := ForEachEnum(
		func(enum Enum) error {
			nestedName := enum.NestedName()
			if _, ok := nestedNameToEnum[nestedName]; ok {
				return fmt.Errorf("duplicate enum: %q", nestedName)
			}
			nestedNameToEnum[nestedName] = enum
			return nil
		},
		containerDescriptor,
	); err != nil {
		return nil, err
	}
	return nestedNameToEnum, nil
}

// FullNameToEnum maps the Enums in the Files to a map from full name to enum.
//
// Returns error if the Enums do not have unique full names within the Files,
// which should generally never happen for properly-formed Files.
func FullNameToEnum(files ...File) (map[string]Enum, error) {
	fullNameToEnum := make(map[string]Enum)
	for _, file := range files {
		if err := ForEachEnum(
			func(enum Enum) error {
				fullName := enum.FullName()
				if _, ok := fullNameToEnum[fullName]; ok {
					return fmt.Errorf("duplicate enum: %q", fullName)
				}
				fullNameToEnum[fullName] = enum
				return nil
			},
			file,
		); err != nil {
			return nil, err
		}
	}
	return fullNameToEnum, nil
}

// PackageToNestedNameToEnum maps the Enums in the Files to a map from
// package to nested name to Enum.
//
// Returns error if the Enums do not have unique nested names within the packages,
// which should generally never happen for properly-formed Files.
func PackageToNestedNameToEnum(files ...File) (map[string]map[string]Enum, error) {
	packageToNestedNameToEnum := make(map[string]map[string]Enum)
	for _, file := range files {
		if err := ForEachEnum(
			func(enum Enum) error {
				pkg := enum.File().Package()
				nestedName := enum.NestedName()
				nestedNameToEnum, ok := packageToNestedNameToEnum[pkg]
				if !ok {
					nestedNameToEnum = make(map[string]Enum)
					packageToNestedNameToEnum[pkg] = nestedNameToEnum
				}
				if _, ok := nestedNameToEnum[nestedName]; ok {
					return fmt.Errorf("duplicate enum in package %q: %q", pkg, nestedName)
				}
				nestedNameToEnum[nestedName] = enum
				return nil
			},
			file,
		); err != nil {
			return nil, err
		}
	}
	return packageToNestedNameToEnum, nil
}

// NameToEnumValue maps the EnumValues in the Enum to a map from name to EnumValue.
//
// Returns error if the EnumValues do not have unique names within the Enum,
// which should generally never happen for properly-formed Enums.
func NameToEnumValue(enum Enum) (map[string]EnumValue, error) {
	nameToEnumValue := make(map[string]EnumValue)
	for _, enumValue := range enum.Values() {
		name := enumValue.Name()
		if _, ok := nameToEnumValue[name]; ok {
			return nil, fmt.Errorf("duplicate enum value name for enum %q: %q", enum.NestedName(), name)
		}
		nameToEnumValue[name] = enumValue
	}
	return nameToEnumValue, nil
}

// NumberToNameToEnumValue maps the EnumValues in the Enum to a map from number to name to EnumValue.
//
// Duplicates by number may occur if allow_alias = true.
//
// Returns error if the EnumValues do not have unique names within the Enum for a given number,
// which should generally never happen for properly-formed Enums.
func NumberToNameToEnumValue(enum Enum) (map[int]map[string]EnumValue, error) {
	numberToNameToEnumValue := make(map[int]map[string]EnumValue)
	for _, enumValue := range enum.Values() {
		number := enumValue.Number()
		nameToEnumValue, ok := numberToNameToEnumValue[number]
		if !ok {
			nameToEnumValue = make(map[string]EnumValue)
			numberToNameToEnumValue[number] = nameToEnumValue
		}
		name := enumValue.Name()
		if _, ok := nameToEnumValue[name]; ok {
			return nil, fmt.Errorf("duplicate enum value name for enum %q: %q", enum.NestedName(), name)
		}
		nameToEnumValue[name] = enumValue
	}
	return numberToNameToEnumValue, nil
}

// NestedNameToMessage maps the Messages in the ContainerDescriptor to a map from
// nested name to Message.
//
// Returns error if Messages do not have unique nested names within the ContainerDescriptor,
// which should generally never happen for properly-formed files.
func NestedNameToMessage(containerDescriptor ContainerDescriptor) (map[string]Message, error) {
	nestedNameToMessage := make(map[string]Message)
	if err := ForEachMessage(
		func(message Message) error {
			nestedName := message.NestedName()
			if _, ok := nestedNameToMessage[nestedName]; ok {
				return fmt.Errorf("duplicate message: %q", nestedName)
			}
			nestedNameToMessage[nestedName] = message
			return nil
		},
		containerDescriptor,
	); err != nil {
		return nil, err
	}
	return nestedNameToMessage, nil
}

// FullNameToMessage maps the Messages in the Files to a map from full name to message.
//
// Returns error if the Messages do not have unique full names within the Files,
// which should generally never happen for properly-formed Files.
func FullNameToMessage(files ...File) (map[string]Message, error) {
	fullNameToMessage := make(map[string]Message)
	for _, file := range files {
		if err := ForEachMessage(
			func(message Message) error {
				fullName := message.FullName()
				if _, ok := fullNameToMessage[fullName]; ok {
					return fmt.Errorf("duplicate message: %q", fullName)
				}
				fullNameToMessage[fullName] = message
				return nil
			},
			file,
		); err != nil {
			return nil, err
		}
	}
	return fullNameToMessage, nil
}

// PackageToNestedNameToMessage maps the Messages in the Files to a map from
// package to nested name to Message.
//
// Returns error if the Messages do not have unique nested names within the packages,
// which should generally never happen for properly-formed Files.
func PackageToNestedNameToMessage(files ...File) (map[string]map[string]Message, error) {
	packageToNestedNameToMessage := make(map[string]map[string]Message)
	for _, file := range files {
		if err := ForEachMessage(
			func(message Message) error {
				pkg := message.File().Package()
				nestedName := message.NestedName()
				nestedNameToMessage, ok := packageToNestedNameToMessage[pkg]
				if !ok {
					nestedNameToMessage = make(map[string]Message)
					packageToNestedNameToMessage[pkg] = nestedNameToMessage
				}
				if _, ok := nestedNameToMessage[nestedName]; ok {
					return fmt.Errorf("duplicate message in package %q: %q", pkg, nestedName)
				}
				nestedNameToMessage[nestedName] = message
				return nil
			},
			file,
		); err != nil {
			return nil, err
		}
	}
	return packageToNestedNameToMessage, nil
}

// NumberToMessageField maps the Fields in the Message to a map from number to Field.
//
// TODO: is this right?
// Includes extensions.
//
// Returns error if the Fields do not have unique numbers within the Message,
// which should generally never happen for properly-formed Messages.
func NumberToMessageField(message Message) (map[int]Field, error) {
	numberToMessageField := make(map[int]Field)
	for _, messageField := range message.Fields() {
		number := messageField.Number()
		if _, ok := numberToMessageField[number]; ok {
			return nil, fmt.Errorf("duplicate message field: %d", number)
		}
		numberToMessageField[number] = messageField
	}
	for _, messageField := range message.Extensions() {
		if messageField.Extendee() != "."+message.FullName() {
			// TODO: ideally we want this field to be returned when
			// the Extendee message is passed into some function,
			// need to investigate what index is necessary for that.
			continue
		}
		number := messageField.Number()
		if _, ok := numberToMessageField[number]; ok {
			return nil, fmt.Errorf("duplicate message field: %d", number)
		}
		numberToMessageField[number] = messageField
	}
	return numberToMessageField, nil
}

// NumberToMessageFieldForLabel maps the Fields with the given label in the message
// to a map from number to Field.
//
// TODO: is this right?
// Includes extensions.
//
// Returns error if the Fields do not have unique numbers within the Message,
// which should generally never happen for properly-formed Messages.
func NumberToMessageFieldForLabel(message Message, label FieldDescriptorProtoLabel) (map[int]Field, error) {
	numberToField, err := NumberToMessageField(message)
	if err != nil {
		return nil, err
	}
	for number, field := range numberToField {
		if field.Label() != label {
			delete(numberToField, number)
		}
	}
	return numberToField, nil
}

// NameToMessageOneof maps the Oneofs in the Message to a map from name to Oneof.
//
// Returns error if the Oneofs do not have unique names within the Message,
// which should generally never happen for properly-formed Messages.
func NameToMessageOneof(message Message) (map[string]Oneof, error) {
	nameToMessageOneof := make(map[string]Oneof)
	for _, messageOneof := range message.Oneofs() {
		name := messageOneof.Name()
		if _, ok := nameToMessageOneof[name]; ok {
			return nil, fmt.Errorf("duplicate message oneof: %q", name)
		}
		nameToMessageOneof[name] = messageOneof
	}
	return nameToMessageOneof, nil
}

// NameToService maps the Services in the File to a map from name to Service.
//
// Returns error if Services do not have unique names within the File, which should
// generally never happen for properly-formed Files.
func NameToService(file File) (map[string]Service, error) {
	nameToService := make(map[string]Service)
	for _, service := range file.Services() {
		name := service.Name()
		if _, ok := nameToService[name]; ok {
			return nil, fmt.Errorf("duplicate service: %q", name)
		}
		nameToService[name] = service
	}
	return nameToService, nil
}

// FullNameToService maps the Services in the Files to a map from full name to Service.
//
// Returns error if Services do not have unique full names within the Files, which should
// generally never happen for properly-formed Files.
func FullNameToService(files ...File) (map[string]Service, error) {
	fullNameToService := make(map[string]Service)
	for _, file := range files {
		for _, service := range file.Services() {
			fullName := service.FullName()
			if _, ok := fullNameToService[fullName]; ok {
				return nil, fmt.Errorf("duplicate service: %q", fullName)
			}
			fullNameToService[fullName] = service
		}
	}
	return fullNameToService, nil
}

// PackageToNameToService maps the Services in the Files to a map from
// package to name to Service.
//
// Returns error if the Services do not have unique names within the packages,
// which should generally never happen for properly-formed Files.
func PackageToNameToService(files ...File) (map[string]map[string]Service, error) {
	packageToNameToService := make(map[string]map[string]Service)
	for _, file := range files {
		for _, service := range file.Services() {
			pkg := service.File().Package()
			name := service.Name()
			nameToService, ok := packageToNameToService[pkg]
			if !ok {
				nameToService = make(map[string]Service)
				packageToNameToService[pkg] = nameToService
			}
			if _, ok := nameToService[name]; ok {
				return nil, fmt.Errorf("duplicate service in package %q: %q", pkg, name)
			}
			nameToService[name] = service
		}
	}
	return packageToNameToService, nil
}

// NameToMethod maps the Methods in the Service to a map from name to Method.
//
// Returns error if Methods do not have unique names within the Service, which should
// generally never happen for properly-formed Services.
func NameToMethod(service Service) (map[string]Method, error) {
	nameToMethod := make(map[string]Method)
	for _, method := range service.Methods() {
		name := method.Name()
		if _, ok := nameToMethod[name]; ok {
			return nil, fmt.Errorf("duplicate method: %q", name)
		}
		nameToMethod[name] = method
	}
	return nameToMethod, nil
}

// FullNameToMethod maps the Methods in the Files to a map from full name to Method.
//
// Returns error if Methods do not have unique full names within the Files, which should
// generally never happen for properly-formed Files.
func FullNameToMethod(files ...File) (map[string]Method, error) {
	fullNameToMethod := make(map[string]Method)
	for _, file := range files {
		for _, service := range file.Services() {
			for _, method := range service.Methods() {
				fullName := method.FullName()
				if _, ok := fullNameToMethod[fullName]; ok {
					return nil, fmt.Errorf("duplicate method: %q", fullName)
				}
				fullNameToMethod[fullName] = method
			}
		}
	}
	return fullNameToMethod, nil
}

// StringToReservedTagRange maps the ReservedTagRanges in the ReservedDescriptor to a map
// from string string to reserved TagRange.
//
// Ignores duplicates.
func StringToReservedTagRange(reservedDescriptor ReservedDescriptor) map[string]TagRange {
	stringToReservedTagRange := make(map[string]TagRange)
	for _, reservedTagRange := range reservedDescriptor.ReservedTagRanges() {
		stringToReservedTagRange[TagRangeString(reservedTagRange)] = reservedTagRange
	}
	return stringToReservedTagRange
}

// ValueToReservedName maps the ReservedNames in the ReservedDescriptor to a map
// from string value to ReservedName.
//
// Ignores duplicates.
func ValueToReservedName(reservedDescriptor ReservedDescriptor) map[string]ReservedName {
	valueToReservedName := make(map[string]ReservedName)
	for _, reservedName := range reservedDescriptor.ReservedNames() {
		valueToReservedName[reservedName.Value()] = reservedName
	}
	return valueToReservedName
}

// StringToExtensionMessageRange maps the ExtensionMessageRanges in the Message to a map
// from string string to ExtensionMessageRange.
//
// Ignores duplicates.
func StringToExtensionMessageRange(message Message) map[string]MessageRange {
	stringToExtensionMessageRange := make(map[string]MessageRange)
	for _, extensionMessageRange := range message.ExtensionMessageRanges() {
		stringToExtensionMessageRange[TagRangeString(extensionMessageRange)] = extensionMessageRange
	}
	return stringToExtensionMessageRange
}

// NumberInReservedRanges returns true if the number is in one of the Ranges.
func NumberInReservedRanges(number int, reservedRanges ...TagRange) bool {
	for _, reservedRange := range reservedRanges {
		start := reservedRange.Start()
		end := reservedRange.End()
		if number >= start && number <= end {
			return true
		}
	}
	return false
}

// NameInReservedNames returns true if the name is in one of the ReservedNames.
func NameInReservedNames(name string, reservedNames ...ReservedName) bool {
	for _, reservedName := range reservedNames {
		if name == reservedName.Value() {
			return true
		}
	}
	return false
}

// EnumIsSubset checks if subsetEnum is a subset of supersetEnum.
func EnumIsSubset(supersetEnum Enum, subsetEnum Enum) (bool, error) {
	supersetNameToEnumValue, err := NameToEnumValue(supersetEnum)
	if err != nil {
		return false, err
	}
	subsetNameToEnumValue, err := NameToEnumValue(subsetEnum)
	if err != nil {
		return false, err
	}
	for subsetName, subsetEnumValue := range subsetNameToEnumValue {
		supersetEnumValue, ok := supersetNameToEnumValue[subsetName]
		if !ok {
			// The enum value does not exist by name, this is not a superset.
			return false, nil
		}
		if subsetEnumValue.Number() != supersetEnumValue.Number() {
			// The enum values are not equal, this is not a superset.
			return false, nil
		}
	}
	// All enum values by name exist in the superset and have the same number,
	// subsetEnum is a subset of supersetEnum.
	return true, nil
}

// TagRangeString returns the string representation of the range.
func TagRangeString(tagRange TagRange) string {
	start := tagRange.Start()
	end := tagRange.End()
	if start == end {
		return fmt.Sprintf("[%d]", start)
	}
	if tagRange.Max() {
		return fmt.Sprintf("[%d,max]", start)
	}
	return fmt.Sprintf("[%d,%d]", start, end)
}

// FreeMessageRangeString returns the string representation of the free ranges for the message.
func FreeMessageRangeString(message Message) string {
	freeRanges := FreeMessageRanges(message)
	if len(freeRanges) == 0 {
		return ""
	}
	suffixes := make([]string, len(freeRanges))
	for i, freeRange := range freeRanges {
		suffixes[i] = freeMessageRangeStringSuffix(freeRange)
	}
	return fmt.Sprintf(
		"%- 35s free: %s",
		freeRanges[0].Message().FullName(),
		strings.Join(suffixes, " "),
	)
}

// FreeMessageRanges returns the free message ranges for the given message.
//
// Not recursive.
func FreeMessageRanges(message Message) []MessageRange {
	used := append(
		message.ReservedMessageRanges(),
		message.ExtensionMessageRanges()...,
	)
	for _, field := range message.Fields() {
		used = append(
			used,
			newFreeMessageRange(message, field.Number(), field.Number()),
		)
	}
	sort.Slice(used, func(i, j int) bool {
		return used[i].Start() < used[j].Start()
	})
	// now compute the inverse (unused ranges)
	unused := make([]MessageRange, 0, len(used)+1)
	last := 0
	for _, r := range used {
		if r.Start() <= last+1 {
			last = r.End()
			continue
		}
		unused = append(
			unused,
			newFreeMessageRange(message, last+1, r.Start()-1),
		)
		last = r.End()
	}
	if last < messageRangeInclusiveMax {
		unused = append(
			unused,
			newFreeMessageRange(message, last+1, messageRangeInclusiveMax),
		)
	}
	return unused
}

func freeMessageRangeStringSuffix(freeRange MessageRange) string {
	start := freeRange.Start()
	end := freeRange.End()
	if start == end {
		return fmt.Sprintf("%d", start)
	}
	if freeRange.Max() {
		return fmt.Sprintf("%d-INF", start)
	}
	return fmt.Sprintf("%d-%d", start, end)
}

func mapFiles(files []File, getKey func(File) string) (map[string][]File, error) {
	keyToFilePathToFile := make(map[string]map[string]File)
	for _, file := range files {
		if err := addUniqueFileToMap(keyToFilePathToFile, getKey(file), file); err != nil {
			return nil, err
		}
	}
	return mapToSortedFiles(keyToFilePathToFile), nil
}

func addUniqueFileToMap(keyToFilePathToFile map[string]map[string]File, key string, file File) error {
	filePathToFile, ok := keyToFilePathToFile[key]
	if !ok {
		filePathToFile = make(map[string]File)
		keyToFilePathToFile[key] = filePathToFile
	}
	if _, ok := filePathToFile[file.Path()]; ok {
		return fmt.Errorf("duplicate file: %s", file.Path())
	}
	filePathToFile[file.Path()] = file
	return nil
}

func mapToSortedFiles(keyToFileMap map[string]map[string]File) map[string][]File {
	keyToSortedFiles := make(map[string][]File, len(keyToFileMap))
	for key, fileMap := range keyToFileMap {
		files := make([]File, 0, len(fileMap))
		for _, file := range fileMap {
			files = append(files, file)
		}
		SortFiles(files)
		keyToSortedFiles[key] = files
	}
	return keyToSortedFiles
}
