// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package scmutationexec

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/errors"
)

// NewImmediateVisitor creates a new scop.ImmediateMutationVisitor.
func NewImmediateVisitor(
	s ImmediateMutationStateUpdater, clock Clock, dr DescriptorReader,
) scop.ImmediateMutationVisitor {
	return &immediateVisitor{
		ImmediateMutationStateUpdater: s,
		clock:                         clock,
		descriptorReader:              dr,
	}
}

type immediateVisitor struct {
	ImmediateMutationStateUpdater
	clock            Clock
	descriptorReader DescriptorReader
}

var _ scop.ImmediateMutationVisitor = (*immediateVisitor)(nil)

// NewDeferredVisitor creates a new scop.DeferredMutationVisitor.
func NewDeferredVisitor(s DeferredMutationStateUpdater) scop.DeferredMutationVisitor {
	return &deferredVisitor{
		DeferredMutationStateUpdater: s,
	}
}

var _ scop.DeferredMutationVisitor = (*deferredVisitor)(nil)

type deferredVisitor struct {
	DeferredMutationStateUpdater
}

func (i *immediateVisitor) NotImplemented(_ context.Context, _ scop.NotImplemented) error {
	return errors.AssertionFailedf("not implemented operation was hit unexpectedly.")
}

func (i *immediateVisitor) NotImplementedForPublicObjects(
	ctx context.Context, op scop.NotImplementedForPublicObjects,
) error {
	desc := i.MaybeGetCheckedOutDescriptor(op.DescID)
	if desc == nil || desc.Dropped() {
		return nil
	}
	return errors.AssertionFailedf("not implemented operation was hit "+
		"unexpectedly, no dropped descriptor was found. %v", op.ElementType)
}
