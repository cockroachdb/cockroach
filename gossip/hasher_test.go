// Copyright 2014 The Cockroach Authors.
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
// implied.  See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.

package gossip

import (
	"testing"
)

// TestHashing verifies similar keys hash to different values for an
// arbitrary number of hash functions.
func TestHashing(t *testing.T) {
	h := newHasher()
	keys := []string{"test1", "test2", "1", "2"}

	seen := map[uint32]bool{} // set of all hash values seen
	for _, key := range keys {
		h.hashKey(key)
		for i := uint32(0); i < uint32(10); i++ {
			val := h.getHash(i)
			if _, ok := seen[val]; ok {
				t.Error("already hashed to value", val)
			}
			seen[val] = true
		}
	}
}
