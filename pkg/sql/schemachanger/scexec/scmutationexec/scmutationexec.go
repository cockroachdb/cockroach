// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package scmutationexec

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
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
	return nil
}
