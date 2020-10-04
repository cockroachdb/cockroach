// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package util

import (
	"hash/crc32"

	"github.com/cockroachdb/errors"
)

// CRC32 computes the Castagnoli CRC32 of the given data.
func CRC32(data []byte) uint32 {
	hash := crc32.New(crc32.MakeTable(crc32.Castagnoli))
	if _, err := hash.Write(data); err != nil {
		panic(errors.Wrap(err, `"It never returns an error." -- https://golang.org/pkg/hash`))
	}
	return hash.Sum32()
}

// Magic FNV Base constant as suitable for a FNV-64 hash.
const fnvBase = uint64(14695981039346656037)
const fnvPrime = 1099511628211

func FNV64Init() uint64 {
	return fnvBase
}

func FNV64AddToHash(s0 uint64, c int32) uint64 {
	s0 *= fnvPrime
	s0 ^= uint64(c)
	return s0
}

func FNV64ToByteArray(s0 uint64) []byte {
	b := make([]byte, 16)
	const hex = "0123456789abcdef"
	for i := 0; i < 16; i++ {
		b[i] = hex[s0&0xf]
		s0 = s0 >> 4
	}
	return b
}
