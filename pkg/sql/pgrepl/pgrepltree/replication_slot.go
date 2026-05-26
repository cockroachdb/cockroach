// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package pgrepltree

type SlotKind int

const (
	PhysicalReplication SlotKind = iota + 1
	LogicalReplication
)
