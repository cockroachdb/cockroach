// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package concurrency

// WIP: ignore this file! Nothing in it is used yet.

// LockType ...
type LockType uint32

const (
	_ LockType = 1 << iota
	ReadBounded
	ReadUnbounded
	Upgrade
	Write
)

// Compatable ...
func (t1 LockType) Compatable(t2 LockType) bool {
	return false
}

// LockDurability ...
type LockDurability uint32

const (
	_ LockType = 1 << iota
	Replicated
	Unreplicated
)

// LockState ...
type LockState uint64
