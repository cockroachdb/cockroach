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

// MakeOps takes a slice of ops, ensures they are all of one kind, and then
// returns an implementation of Ops corresponding to that type. The set of ops
// must all be the same type, otherwise MakeOps will panic.
func MakeOps(ops ...Op) Ops {
	var typ Type
	for i, op := range ops {
		if i == 0 {
			typ = op.Type()
			continue
		}
		if op.Type() != typ {
			panic(errors.Errorf(
				"slice contains ops of type %s and %s", op.Type().String(), op))
		}
	}
	switch typ {
	case MutationType:
		return mutationOps(ops)
	case BackfillType:
		return backfillOps(ops)
	case ValidationType:
		return validationOps(ops)
	default:
		panic(errors.Errorf("unknown op type %s", typ.String()))
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

// A Phase represents the context in which an op is executed within a schema
// change. Different phases require different dependencies for the execution of
// the ops to be plumbed in.
//
// Today, we support the phases corresponding to async schema changes initiated
// and partially executed in the user transaction. This will change as we
// transition to transactional schema changes.
type Phase int

const (
	// StatementPhase refers to execution of ops occurring during statement
	// execution during the user transaction.
	StatementPhase Phase = iota
	// PreCommitPhase refers to execution of ops occurring during the user
	// transaction immediately before commit.
	PreCommitPhase
	// PostCommitPhase refers to execution of ops occurring after the user
	// transaction has committed (i.e., in the async schema change job).
	PostCommitPhase
)
