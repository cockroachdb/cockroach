// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package internal

// T is a Template type. The methods in the interface make up its contract.
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
