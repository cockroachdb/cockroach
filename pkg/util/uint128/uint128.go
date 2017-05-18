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
//
// Author: Tristan Ohlson (tsohlson@gmail.com)

package uint128

import (
	"encoding/binary"
	"fmt"
)

// Uint128 is a big-endian 128 bit unsigned integer which wraps two
// uint64s.
type Uint128 struct {
	hi, lo uint64
}

// FromBytes parses the byte slice as a big-endian unsigned integer.
func FromBytes(b []byte) (Uint128, error) {
	if len(b) != 16 {
		return Uint128{}, fmt.Errorf("expected 16 bytes got %d", len(b))
	}

	hi := binary.BigEndian.Uint64(b[:8])
	lo := binary.BigEndian.Uint64(b[8:])

	return Uint128{hi, lo}, nil
}

// GetBytes returns a big-endian byte representation.
func (u Uint128) GetBytes() []byte {
	buf := make([]byte, 16)
	binary.BigEndian.PutUint64(buf[:8], u.hi)
	binary.BigEndian.PutUint64(buf[8:], u.lo)
	return buf
}

// Add returns a new Uint128 incremented by n.
func (u Uint128) Add(n uint64) Uint128 {
	if u.lo > u.lo+n {
		u.hi++
	}
	u.lo += n
	return u
}

// Sub returns a new Uint128 decremented by n.
func (u Uint128) Sub(n uint64) Uint128 {
	if n > u.lo {
		u.hi--
	}
	u.lo -= n
	return u
}
