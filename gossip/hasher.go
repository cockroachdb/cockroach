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
	"fmt"
	"hash"
	"io"

	"github.com/spaolacci/murmur3"
)

// hasher utilizes a 64 bit variant of MurmurHash3 to support
// the use of the Kirsch and MitzenMacher method to determine k
// hashed values using this formula:
//
// H[i](x) = hash[0:4] + i*hash[4:8]
//
// http://www.eecs.harvard.edu/~kirsch/pubs/bbbf/rsa.pdf
type hasher struct {
	mmh3 hash.Hash64
	// hashed is true if a key has been hashed.
	hashed bool
	// h1 is the first 4 bytes of the key hash.
	h1 uint32
	// h2 is the last 4 bytes of the key hash.
	h2 uint32
}

// newHasher allocates and returns a new hasher.
func newHasher() *hasher {
	return &hasher{mmh3: murmur3.New64()}
}

// HashKey writes the given key string to the hasher.
func (h *hasher) hashKey(key string) {
	h.mmh3.Reset() // clear current hash state
	if _, err := io.WriteString(h.mmh3, key); err != nil {
		panic(fmt.Sprintf("unable to write string to hasher: %s", key))
	}
	h.hashed = true
	sum := h.mmh3.Sum64()
	h.h1 = uint32(sum & 0xffffffff)
	h.h2 = uint32((sum >> 32) & 0xffffffff)
}

// getHash returns the hash value at the given offset.
func (h *hasher) getHash(i uint32) uint32 {
	if !h.hashed {
		panic("hasher must be initialized first with a call to HashKey(key)")
	}
	return h.h1 + i*h.h2
}
