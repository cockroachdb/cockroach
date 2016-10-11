// Copyright 2016 The Cockroach Authors.
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
//
// Author: Peter Mattis (peter@cockroachlabs.com)

package engine

import "github.com/cockroachdb/cockroach/util/bufalloc"

// AllocIterKeyValue returns iter.Key() and iter.Value() with the underlying
// storage allocated from the passed ChunkAllocator.
func AllocIterKeyValue(
	a bufalloc.ByteAllocator, iter Iterator,
) (bufalloc.ByteAllocator, MVCCKey, []byte) {
	key := iter.unsafeKey()
	a, key.Key = a.Copy(key.Key, 0)
	value := iter.unsafeValue()
	a, value = a.Copy(value, 0)
	return a, key, value
}
