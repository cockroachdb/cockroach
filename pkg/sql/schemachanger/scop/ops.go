// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package scop

// Op represents an action to be taken on a single descriptor.
type Op interface {
	Type() Type
}

// Type represents the type of operation for an Op. Ops can be grouped into the
// same Stage only if they share a type.
type Type int

//go:generate stringer -type=Type

const (
	_ Type = iota
	// MutationType represents metadata changes.
	MutationType
	// BackfillType represents index backfills.
	BackfillType
	// ValidationType represents constraint and unique index validations
	// performed using internal queries.
	ValidationType

	maxType int = iota - 1
)

// IsValid is true if the Type has a valid value.
func (t Type) IsValid() bool {
	return t > 0 && t <= Type(maxType)
}

type baseOp struct{}
