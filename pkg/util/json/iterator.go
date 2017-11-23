// Copyright 2017 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package json

// ObjectKeyIterator is an iterator to access the keys of an object.
type ObjectKeyIterator struct {
	src jsonObject
	idx int
}

// Next returns true and the next key in the iterator if one exists,
// and false otherwise.
func (it *ObjectKeyIterator) Next() (bool, string) {
	it.idx++
	if it.idx >= len(it.src) {
		return false, ""
	}
	return true, *it.src[it.idx].k.AsText()
}
