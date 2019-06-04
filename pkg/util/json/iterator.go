// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL.txt and at www.mariadb.com/bsl11.
//
// Change Date: 2022-10-01
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the Apache License, Version 2.0,
// included in the file licenses/APL.txt and at
// https://www.apache.org/licenses/LICENSE-2.0

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
