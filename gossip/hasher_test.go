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

// Verify similar keys hash to different values for arbitrary numbers
// of hash functions.
func TestHashing(t *testing.T) {
	h := NewHasher()
	keys := []string{"test1", "test2", "1", "2"}

	seen := make(map[uint32]bool) // set of all hash values seen
	for _, key := range keys {
		if err := h.HashKey(key); err != nil {
			t.Error("failed to hash", err)
		}

		for i := 0; i < 10; i++ {
			val := h.GetHash(i)
			if _, ok := seen[val]; ok {
				t.Error("already hashed to value", val)
			}
			seen[val] = true
		}
	}
}
