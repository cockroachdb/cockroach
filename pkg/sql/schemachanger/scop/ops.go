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

import "github.com/cockroachdb/errors"

// Op represents an action to be taken on a single descriptor.
type Op interface {
	Type() Type
}

// Ops represents a slice of operations where all operations have the
// same type.
type Ops interface {
	Type() Type
	Slice() []Op
}

// ExtendOps takes an existing set of ops and additional ops of the same
// kind and constructs a new ops with the added ops happening last.
func ExtendOps(existing Ops, added ...Op) (_ Ops, err error) {
	return makeOps(append(existing.Slice(), added...)...)
}

// MakeOps takes a slice of ops, ensures they are all of one kind, and then
// returns an implementation of Ops corresponding to that type. The set of ops
// must all be the same type, otherwise MakeOps will panic.
func MakeOps(ops ...Op) Ops {
	ret, err := makeOps(ops...)
	if err != nil {
		panic(err)
	}
	return ret
}

func makeOps(ops ...Op) (Ops, error) {
	// The type of stage doesn't matter for nil ops.
	// TODO: (fqazi) Inside planning we need to plan for *multiple* state
	// transitions now that we can optimize out edges. Once that  is done this
	// temporary workaround can be fully dropped.
	if len(ops) == 0 {
		return nil, nil
	}
	var typ Type
	for i, op := range ops {
		if i == 0 {
			typ = op.Type()
			continue
		}
		if op.Type() != typ {
			return nil, errors.Errorf(
				"slice contains ops of type %s and %s", op.Type().String(), op)
		}
	}
	switch typ {
	case MutationType:
		return mutationOps(ops), nil
	case BackfillType:
		return backfillOps(ops), nil
	case ValidationType:
		return validationOps(ops), nil
	default:
		return nil, errors.Errorf("unknown op type %s", typ.String())
	}
}

// Type represents the type of operation for an Op. Ops can be grouped into the
// the same Stage only if they share a type.
type Type int

//go:generate stringer -type=Type

const (
	_ Type = iota
	// MutationType represents descriptor changes.
	MutationType
	// BackfillType represents index backfills.
	BackfillType
	// ValidationType represents constraint and unique index validations
	// performed using internal queries.
	ValidationType
)

type mutationOps []Op

func (m mutationOps) Type() Type  { return MutationType }
func (m mutationOps) Slice() []Op { return m }

type backfillOps []Op

func (b backfillOps) Type() Type  { return BackfillType }
func (b backfillOps) Slice() []Op { return b }

type validationOps []Op

func (v validationOps) Type() Type  { return ValidationType }
func (v validationOps) Slice() []Op { return v }

var _ Ops = (mutationOps)(nil)
var _ Ops = (backfillOps)(nil)
var _ Ops = (validationOps)(nil)

type baseOp struct{}
