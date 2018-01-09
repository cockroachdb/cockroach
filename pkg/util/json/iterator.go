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

// ObjectIterator is an iterator to access the key value pair of an object in
// sorted order based on key.
type ObjectIterator struct {
	src jsonObject
	idx int
}

func newObjectIterator(src jsonObject) *ObjectIterator {
	return &ObjectIterator{
		src: src,
		idx: -1,
	}
}

// Next updates the cursor and returns whether the next pair exists.
func (it *ObjectIterator) Next() bool {
	if it.idx >= len(it.src)-1 {
		return false
	}
	it.idx++
	return true
}

// Key returns key of the current pair.
func (it *ObjectIterator) Key() string {
	return string(it.src[it.idx].k)
}

// Value returns value of the current pair
func (it *ObjectIterator) Value() JSON {
	return it.src[it.idx].v
}
