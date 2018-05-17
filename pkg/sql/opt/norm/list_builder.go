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

package norm

import (
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
)

// listBuilder is a helper class that efficiently builds memo lists by always
// reusing a "scratch" slice to hold temporary results. The reusable slice
// is stored in the Factory. Usage:
//
//   lb := listBuilder{f: f}
//   lb.addItem(item)
//   lb.addItems(items)
//   res := lb.buildList()
//
// The listBuilder should always be constructed on the stack so that it's safe
// to recurse on factory construction methods without danger of trying to reuse
// a scratch slice that's already in use further up the stack.
type listBuilder struct {
	f     *Factory
	items []memo.GroupID
}

// addItem appends a single memo group ID to the list under construction.
func (b *listBuilder) addItem(item memo.GroupID) {
	b.ensureItems()
	b.items = append(b.items, item)
}

// addItems appends a list of memo group IDs to the list under construction.
func (b *listBuilder) addItems(items []memo.GroupID) {
	b.ensureItems()
	b.items = append(b.items, items...)
}

// setLength sets the length of the list to the given value. This is useful for
// truncating the list.
func (b *listBuilder) setLength(len int) {
	b.items = b.items[:len]
}

// buildList constructs a memo list from the list of appended items, and returns
// the scratch list to the factory. The state of the list builder is reset.
func (b *listBuilder) buildList() memo.ListID {
	listID := b.f.InternList(b.items)

	// Save the list in the factory for possible future reuse.
	b.f.scratchItems = b.items
	b.items = nil

	return listID
}

func (b *listBuilder) ensureItems() {
	// Try to reuse scratch list stored in factory.
	if b.items == nil {
		b.items = b.f.scratchItems
		b.items = b.items[:0]

		// Set the factory scratch list to nil so that recursive calls won't try
		// to use it when it's already in use.
		b.f.scratchItems = nil
	}
}
