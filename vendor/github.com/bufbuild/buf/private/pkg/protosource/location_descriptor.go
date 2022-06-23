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

type locationDescriptor struct {
	descriptor

	path []int32
}

func newLocationDescriptor(
	descriptor descriptor,
	path []int32,
) locationDescriptor {
	return locationDescriptor{
		descriptor: descriptor,
		path:       path,
	}
}

func (l *locationDescriptor) Location() Location {
	return l.getLocation(l.path)
}
