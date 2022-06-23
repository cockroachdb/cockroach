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

type enumValue struct {
	namedDescriptor
	optionExtensionDescriptor

	enum       Enum
	number     int
	numberPath []int32
}

func newEnumValue(
	namedDescriptor namedDescriptor,
	optionExtensionDescriptor optionExtensionDescriptor,
	enum Enum,
	number int,
	numberPath []int32,
) *enumValue {
	return &enumValue{
		namedDescriptor:           namedDescriptor,
		optionExtensionDescriptor: optionExtensionDescriptor,
		enum:                      enum,
		number:                    number,
		numberPath:                numberPath,
	}
}

func (e *enumValue) Enum() Enum {
	return e.enum
}

func (e *enumValue) Number() int {
	return e.number
}

func (e *enumValue) NumberLocation() Location {
	return e.getLocation(e.numberPath)
}
