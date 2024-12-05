// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package scop describes ops within a schema change.
package scop

import "github.com/cockroachdb/redact"

// Op represents an action to be taken on a single descriptor.
type Op interface {
	Type() Type
}

// Type represents the type of operation for an Op. Ops can be grouped into the
// same Stage only if they share a type.
type Type int

var _ redact.SafeValue = Type(0)

// SafeValue implements the redact.SafeValue interface.
func (t Type) SafeValue() {}

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
