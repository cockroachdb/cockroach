// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package internal

// T is a Template type. The methods in the interface make up its contract.
//
//lint:ignore U1001 unused
type T interface {
	ID() uint64
	Key() []byte
	EndKey() []byte
	String() string
	// Used for testing only.
	New() T
	SetID(uint64)
	SetKey([]byte)
	SetEndKey([]byte)
}
