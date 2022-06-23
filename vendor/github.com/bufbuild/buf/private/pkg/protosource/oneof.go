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

type oneof struct {
	namedDescriptor
	optionExtensionDescriptor

	message Message
	fields  []Field
}

func newOneof(
	namedDescriptor namedDescriptor,
	optionExtensionDescriptor optionExtensionDescriptor,
	message Message,
) *oneof {
	return &oneof{
		namedDescriptor:           namedDescriptor,
		optionExtensionDescriptor: optionExtensionDescriptor,
		message:                   message,
	}
}

func (o *oneof) Message() Message {
	return o.message
}

func (o *oneof) Fields() []Field {
	return o.fields
}

func (o *oneof) addField(field Field) {
	o.fields = append(o.fields, field)
}
