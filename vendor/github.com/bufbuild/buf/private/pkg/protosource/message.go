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

type message struct {
	namedDescriptor
	optionExtensionDescriptor

	fields                           []Field
	extensions                       []Field
	nestedMessages                   []Message
	nestedEnums                      []Enum
	oneofs                           []Oneof
	reservedMessageRanges            []MessageRange
	reservedNames                    []ReservedName
	extensionMessageRanges           []MessageRange
	parent                           Message
	isMapEntry                       bool
	messageSetWireFormat             bool
	noStandardDescriptorAccessor     bool
	messageSetWireFormatPath         []int32
	noStandardDescriptorAccessorPath []int32
}

func newMessage(
	namedDescriptor namedDescriptor,
	optionExtensionDescriptor optionExtensionDescriptor,
	parent Message,
	isMapEntry bool,
	messageSetWireFormat bool,
	noStandardDescriptorAccessor bool,
	messageSetWireFormatPath []int32,
	noStandardDescriptorAccessorPath []int32,
) *message {
	return &message{
		namedDescriptor:                  namedDescriptor,
		optionExtensionDescriptor:        optionExtensionDescriptor,
		isMapEntry:                       isMapEntry,
		messageSetWireFormat:             messageSetWireFormat,
		noStandardDescriptorAccessor:     noStandardDescriptorAccessor,
		messageSetWireFormatPath:         messageSetWireFormatPath,
		noStandardDescriptorAccessorPath: noStandardDescriptorAccessorPath,
	}
}

func (m *message) Fields() []Field {
	return m.fields
}

func (m *message) Extensions() []Field {
	return m.extensions
}

func (m *message) Messages() []Message {
	return m.nestedMessages
}

func (m *message) Enums() []Enum {
	return m.nestedEnums
}

func (m *message) Oneofs() []Oneof {
	return m.oneofs
}

func (m *message) ReservedMessageRanges() []MessageRange {
	return m.reservedMessageRanges
}

func (m *message) ReservedTagRanges() []TagRange {
	tagRanges := make([]TagRange, len(m.reservedMessageRanges))
	for i, reservedMessageRange := range m.reservedMessageRanges {
		tagRanges[i] = reservedMessageRange
	}
	return tagRanges
}

func (m *message) ReservedNames() []ReservedName {
	return m.reservedNames
}

func (m *message) ExtensionMessageRanges() []MessageRange {
	return m.extensionMessageRanges
}

func (m *message) Parent() Message {
	return m.parent
}

func (m *message) IsMapEntry() bool {
	return m.isMapEntry
}

func (m *message) MessageSetWireFormat() bool {
	return m.messageSetWireFormat
}

func (m *message) NoStandardDescriptorAccessor() bool {
	return m.noStandardDescriptorAccessor
}

func (m *message) MessageSetWireFormatLocation() Location {
	return m.getLocation(m.messageSetWireFormatPath)
}

func (m *message) NoStandardDescriptorAccessorLocation() Location {
	return m.getLocation(m.noStandardDescriptorAccessorPath)
}

func (m *message) addField(field Field) {
	m.fields = append(m.fields, field)
}

func (m *message) addExtension(extension Field) {
	m.extensions = append(m.extensions, extension)
}

func (m *message) addNestedMessage(nestedMessage Message) {
	m.nestedMessages = append(m.nestedMessages, nestedMessage)
}

func (m *message) addNestedEnum(nestedEnum Enum) {
	m.nestedEnums = append(m.nestedEnums, nestedEnum)
}

func (m *message) addOneof(oneof Oneof) {
	m.oneofs = append(m.oneofs, oneof)
}

func (m *message) addReservedMessageRange(reservedMessageRange MessageRange) {
	m.reservedMessageRanges = append(m.reservedMessageRanges, reservedMessageRange)
}

func (m *message) addReservedName(reservedName ReservedName) {
	m.reservedNames = append(m.reservedNames, reservedName)
}

func (m *message) addExtensionMessageRange(extensionMessageRange MessageRange) {
	m.extensionMessageRanges = append(m.extensionMessageRanges, extensionMessageRange)
}
