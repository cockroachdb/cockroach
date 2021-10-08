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

// FNV64 encapsulates the hash state.
type FNV64 struct {
	sum uint64
}

// MakeFNV64 initializes a new FNV64 hash state.
func MakeFNV64() FNV64 {
	return FNV64{sum: fnvBase}
}

// IsInitialized returns true if the hash struct was initialized, which happens
// automatically when created through MakeFNV64 above.
func (f *FNV64) IsInitialized() bool {
	return f.sum != 0
}

// Add modifies the underlying FNV64 state by accumulating the given integer
// hash to the existing state.
func (f *FNV64) Add(c uint64) {
	f.sum *= fnvPrime
	f.sum ^= c
}

// Sum returns the hash value accumulated till now.
func (f *FNV64) Sum() uint64 {
	return f.sum
}
