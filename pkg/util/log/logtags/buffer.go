// Copyright 2018 The Cockroach Authors.
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

package logtags

import (
	"fmt"
	"strings"
)

const staticSlots = 4

// Buffer is an immutable list of Tags.
type Buffer struct {
	tags     []Tag
	prealloc [staticSlots]Tag
}

// SingleTagBuffer returns a Buffer with a single tag.
func SingleTagBuffer(key string, value interface{}) *Buffer {
	b := &Buffer{}
	b.init(1, 1)
	b.tags[0] = Tag{key: key, value: value}
	return b
}

// Get returns the tags, as a slice. This slice must not be modified.
func (b *Buffer) Get() []Tag {
	return b.tags
}

// Add returns a new buffer with one more tag. If the tag has the same key as an
// earlier tag, that tag is overwritten.
// The receiver can be nil.
func (b *Buffer) Add(key string, value interface{}) *Buffer {
	if b == nil {
		return SingleTagBuffer(key, value)
	}
	res := &Buffer{}
	res.init(len(b.tags), len(b.tags)+1)
	copy(res.tags, b.tags)
	res.addOrReplace(key, value)
	return res
}

// Merge returns a new buffer which contains tags from the receiver, followed by
// the tags from <other>.
//
// If both buffers have the same tag, the tag will appear only one time, with
// the value it has in <other>. It can appear either in the place of the tag in
// <b> or the tag in <other> (depending on which is deemed more efficient).
//
// The method can return <b> or <other> if the result is identical with one of
// them.
//
// The receiver can be nil.
func (b *Buffer) Merge(other *Buffer) *Buffer {
	if b == nil || len(b.tags) == 0 {
		return other
	}
	if len(other.tags) == 0 {
		return b
	}

	// Check for a common case where b's tags are a subsequence of the
	// other tags. In practice this happens when we start with an annotated
	// context and we annotate it again at a lower level (with more specific
	// information). Frequent examples seen in practice are when b has a node ID
	// tag and other has both a node and a store ID tag; and when b has a node ID
	// and store ID tag and other has node, store, and replica tags.
	if diff := len(other.tags) - len(b.tags); diff >= 0 {
		i, j := 0, 0
		for i < len(b.tags) && j-i <= diff {
			if b.tags[i].key == other.tags[j].key {
				i++
			}
			j++
		}
		if i == len(b.tags) {
			return other
		}
	}

	// Another common case is when we aren't adding any new tags or values; find
	// and ignore the longest prefix of redundant tags.
	i := 0
	for ; i < len(other.tags); i++ {
		idx := b.find(other.tags[i].key)
		if idx == -1 || b.tags[idx].value != other.tags[i].value {
			break
		}
	}
	if i == len(other.tags) {
		return b
	}

	res := &Buffer{}
	res.init(len(b.tags), len(b.tags)+len(other.tags)-i)
	copy(res.tags, b.tags)

	for ; i < len(other.tags); i++ {
		res.addOrReplace(other.tags[i].key, other.tags[i].value)
	}
	return res
}

// String returns a string representation of the tags. Intended for debugging;
// note that this is not exactly the same with the representation that
// ultimately appears in logs (which is "prettier").
func (b *Buffer) String() string {
	var buf strings.Builder
	for i, t := range b.Get() {
		if i > 0 {
			buf.WriteString(",")
		}
		fmt.Fprintf(&buf, "%s=%s", t.Key(), t.Value())
	}
	return buf.String()
}

func (b *Buffer) init(length, maxLenHint int) {
	if length <= staticSlots {
		// Even if maxLenHint is larger that staticSlots, we still want to try to
		// avoid the allocation (especially since tags frequently get deduplicated).
		b.tags = b.prealloc[:length]
	} else {
		b.tags = make([]Tag, length, maxLenHint)
	}
}

func (b *Buffer) addOrReplace(key string, value interface{}) {
	for i := range b.tags {
		if b.tags[i].key == key {
			b.tags[i].value = value
			return
		}
	}
	b.tags = append(b.tags, Tag{key: key, value: value})
}

// find returns the position of the tag with the given key, or -1 if
// it is not found.
func (b *Buffer) find(key string) int {
	for i := range b.tags {
		if b.tags[i].key == key {
			return i
		}
	}
	return -1
}
