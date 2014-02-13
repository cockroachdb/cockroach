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
	"github.com/spaolacci/murmur3"
	"hash"
	"io"
)

// To make running k hash functions performant, use Kirsch and
// Mitzenmacher method to determine k hashed values using formula:
//
// H[i](x) = hash[0:4] + i*hash[4:8]
//
// http://www.eecs.harvard.edu/~kirsch/pubs/bbbf/rsa.pdf
//
// We use the 64 bit variant of MurmurHash3 for the 8 bytes of hashed
// values.
type Hasher struct {
	mmh3   hash.Hash64
	hashed bool   // true if we've hashed a key
	h1     uint32 // first 4 bytes of key hash
	h2     uint32 // last 4 bytes of key hash
}

func NewHasher() *Hasher {
	return &Hasher{murmur3.New64(), false, 0, 0}
}

func (h *Hasher) HashKey(key string) error {
	h.mmh3.Reset() // clear current hash state
	if _, err := io.WriteString(h.mmh3, key); err != nil {
		return err
	}
	h.hashed = true
	sum := h.mmh3.Sum64()
	h.h1 = uint32(sum & 0xffffffff)
	h.h2 = uint32((sum >> 32) & 0xffffffff)
	return nil
}

func (h *Hasher) GetHash(i int) uint32 {
	if !h.hashed {
		panic("hasher must be initialized first with a call to HashKey(key)")
	}
	return h.h1 + uint32(i)*h.h2
}
