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
	messageRangeInclusiveMax = 536870911
)

type messageRange struct {
	locationDescriptor

	message Message
	start   int
	end     int
}

func newMessageRange(
	locationDescriptor locationDescriptor,
	message Message,
	start int,
	end int,
) *messageRange {
	return &messageRange{
		locationDescriptor: locationDescriptor,
		message:            message,
		start:              start,
		// end is exclusive for messages
		end: end - 1,
	}
}

func newFreeMessageRange(message Message, start int, endInclusive int) MessageRange {
	return newMessageRange(
		newLocationDescriptor(
			newDescriptor(
				message.File(),
				nil,
			),
			nil,
		),
		message,
		start,
		// we expect exclusive for newMessageRange
		endInclusive+1,
	)
}

func (r *messageRange) Message() Message {
	return r.message
}

func (r *messageRange) Start() int {
	return r.start
}

func (r *messageRange) End() int {
	return r.end
}

func (r *messageRange) Max() bool {
	return r.end == messageRangeInclusiveMax
}
