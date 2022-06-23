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

const (
	enumRangeInclusiveMax = 2147483647
)

type enumRange struct {
	locationDescriptor

	enum  Enum
	start int
	end   int
}

func newEnumRange(
	locationDescriptor locationDescriptor,
	enum Enum,
	start int,
	// end is inclusive for enums
	end int,
) *enumRange {
	return &enumRange{
		locationDescriptor: locationDescriptor,
		enum:               enum,
		start:              start,
		end:                end,
	}
}

func (r *enumRange) Enum() Enum {
	return r.enum
}

func (r *enumRange) Start() int {
	return r.start
}

func (r *enumRange) End() int {
	return r.end
}

func (r *enumRange) Max() bool {
	return r.end == enumRangeInclusiveMax
}
