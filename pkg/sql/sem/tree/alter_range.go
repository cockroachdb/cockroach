// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tree

import (
	"strconv"

	"github.com/cockroachdb/errors"
)

// RelocateRange represents an `ALTER RANGE .. RELOCATE ..`
// statement.
type RelocateRange struct {
	Rows            *Select
	ToStoreID       int64
	FromStoreID     int64
	SubjectReplicas RelocateSubject
}

// RelocateSubject indicates what replicas of a range should be relocated.
type RelocateSubject int

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
		ctx.WriteString(strconv.FormatInt(n.FromStoreID, 10))
	}
	ctx.WriteString(" TO ")
	ctx.WriteString(strconv.FormatInt(n.ToStoreID, 10))
	ctx.WriteString(" FOR ")
	ctx.FormatNode(n.Rows)
}
