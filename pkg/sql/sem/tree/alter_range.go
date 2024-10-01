// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tree

import "github.com/cockroachdb/errors"

// RelocateRange represents an `ALTER RANGE .. RELOCATE ..`
// statement.
type RelocateRange struct {
	Rows            *Select
	ToStoreID       Expr
	FromStoreID     Expr
	SubjectReplicas RelocateSubject
}

// RelocateSubject indicates what replicas of a range should be relocated.
type RelocateSubject int8

const (
	// RelocateLease indicates that leases should be relocated.
	RelocateLease RelocateSubject = iota
	// RelocateVoters indicates what voter replicas should be relocated.
	RelocateVoters
	// RelocateNonVoters indicates that non-voter replicas should be relocated.
	RelocateNonVoters
)

// Format implementsthe NodeFormatter interface.
func (n *RelocateSubject) Format(ctx *FmtCtx) {
	ctx.WriteString(n.String())
}

func (n RelocateSubject) String() string {
	switch n {
	case RelocateLease:
		return "LEASE"
	case RelocateVoters:
		return "VOTERS"
	case RelocateNonVoters:
		return "NONVOTERS"
	default:
		panic(errors.AssertionFailedf("programming error: unhandled case %d", int(n)))
	}
}

// Format implements the NodeFormatter interface.
func (n *RelocateRange) Format(ctx *FmtCtx) {
	ctx.WriteString("ALTER RANGE RELOCATE ")
	ctx.FormatNode(&n.SubjectReplicas)
	// When relocating leases, the origin store is implicit.
	if n.SubjectReplicas != RelocateLease {
		ctx.WriteString(" FROM ")
		ctx.FormatNode(n.FromStoreID)
	}
	ctx.WriteString(" TO ")
	ctx.FormatNode(n.ToStoreID)
	ctx.WriteString(" FOR ")
	ctx.FormatNode(n.Rows)
}
