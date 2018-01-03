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

package idalloc

import "github.com/cockroachdb/cockroach/pkg/roachpb"

// SetIDKey sets the key which the allocator increments.
func (ia *Allocator) SetIDKey(idKey roachpb.Key) {
	ia.idKey.Store(idKey)
}

// IDs returns the channel that the allocator uses to buffer available ids.
func (ia *Allocator) IDs() <-chan uint32 {
	return ia.ids
}
