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

type field struct {
	namedDescriptor
	optionExtensionDescriptor

	message  Message
	number   int
	label    FieldDescriptorProtoLabel
	typ      FieldDescriptorProtoType
	typeName string
	// if the field is an extension, this is the type being extended
	extendee string
	// this has to be the pointer to the private struct or you have the bug where the
	// interface is nil but value == nil is false
	oneof          *oneof
	proto3Optional bool
	jsonName       string
	jsType         FieldOptionsJSType
	cType          FieldOptionsCType
	packed         *bool
	numberPath     []int32
	typePath       []int32
	typeNamePath   []int32
	jsonNamePath   []int32
	jsTypePath     []int32
	cTypePath      []int32
	packedPath     []int32
	extendeePath   []int32
}

func newField(
	namedDescriptor namedDescriptor,
	optionExtensionDescriptor optionExtensionDescriptor,
	message Message,
	number int,
	label FieldDescriptorProtoLabel,
	typ FieldDescriptorProtoType,
	typeName string,
	extendee string,
	oneof *oneof,
	proto3Optional bool,
	jsonName string,
	jsType FieldOptionsJSType,
	cType FieldOptionsCType,
	packed *bool,
	numberPath []int32,
	typePath []int32,
	typeNamePath []int32,
	jsonNamePath []int32,
	jsTypePath []int32,
	cTypePath []int32,
	packedPath []int32,
	extendeePath []int32,
) *field {
	return &field{
		namedDescriptor:           namedDescriptor,
		optionExtensionDescriptor: optionExtensionDescriptor,
		message:                   message,
		number:                    number,
		label:                     label,
		typ:                       typ,
		typeName:                  typeName,
		extendee:                  extendee,
		oneof:                     oneof,
		proto3Optional:            proto3Optional,
		jsonName:                  jsonName,
		jsType:                    jsType,
		cType:                     cType,
		packed:                    packed,
		numberPath:                numberPath,
		typePath:                  typePath,
		typeNamePath:              typeNamePath,
		jsonNamePath:              jsonNamePath,
		jsTypePath:                jsTypePath,
		cTypePath:                 cTypePath,
		packedPath:                packedPath,
		extendeePath:              extendeePath,
	}
}

func (f *field) Message() Message {
	return f.message
}

func (f *field) Number() int {
	return f.number
}

func (f *field) Label() FieldDescriptorProtoLabel {
	return f.label
}

func (f *field) Type() FieldDescriptorProtoType {
	return f.typ
}

func (f *field) TypeName() string {
	return f.typeName
}

func (f *field) Extendee() string {
	return f.extendee
}

func (f *field) Oneof() Oneof {
	// this has to be done or you have the bug where the interface is nil
	// but value == nil is false
	if f.oneof == nil {
		return nil
	}
	return f.oneof
}

func (f *field) Proto3Optional() bool {
	return f.proto3Optional
}

func (f *field) JSONName() string {
	return f.jsonName
}

func (f *field) JSType() FieldOptionsJSType {
	return f.jsType
}

func (f *field) CType() FieldOptionsCType {
	return f.cType
}

func (f *field) Packed() *bool {
	return f.packed
}

func (f *field) NumberLocation() Location {
	return f.getLocation(f.numberPath)
}

func (f *field) TypeLocation() Location {
	return f.getLocation(f.typePath)
}

func (f *field) TypeNameLocation() Location {
	return f.getLocation(f.typeNamePath)
}

func (f *field) JSONNameLocation() Location {
	return f.getLocation(f.jsonNamePath)
}

func (f *field) JSTypeLocation() Location {
	return f.getLocation(f.jsTypePath)
}

func (f *field) CTypeLocation() Location {
	return f.getLocation(f.cTypePath)
}

func (f *field) PackedLocation() Location {
	return f.getLocation(f.packedPath)
}

func (f *field) ExtendeeLocation() Location {
	return f.getLocation(f.extendeePath)
}
