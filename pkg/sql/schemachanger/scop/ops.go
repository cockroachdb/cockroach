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

type Type int

const (
	_ Type = iota
	DescriptorMutationType
	BackfillType
	ValidationType
)

type MutationOps []MutationOp

func (m MutationOps) Len() int      { return len(m) }
func (m MutationOps) At(idx int) Op { return m[idx] }

type BackfillOps []BackfillOp

func (b BackfillOps) Len() int      { return len(b) }
func (b BackfillOps) At(idx int) Op { return b[idx] }

type ValidationOps []ValidationOp

func (v ValidationOps) Len() int      { return len(v) }
func (v ValidationOps) At(idx int) Op { return v[idx] }

var _ Ops = (MutationOps)(nil)
var _ Ops = (BackfillOps)(nil)
var _ Ops = (ValidationOps)(nil)

// Op represents an action to be taken on a single descriptor.
type Op interface {
	op()
	Type() Type
}

type Ops interface {
	Len() int
	At(idx int) Op
}

type baseOp struct{}

func (baseOp) op() {}
