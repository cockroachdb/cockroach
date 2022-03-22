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

// NewMutationVisitor creates a new scop.MutationVisitor.
func NewMutationVisitor(
	s MutationVisitorStateUpdater, nr NameResolver, sd SyntheticDescriptors, clock Clock,
) scop.MutationVisitor {
	return &visitor{
		nr:    nr,
		sd:    sd,
		s:     s,
		clock: clock,
	}
}

var _ scop.MutationVisitor = (*visitor)(nil)

type visitor struct {
	clock Clock
	nr    NameResolver
	sd    SyntheticDescriptors
	s     MutationVisitorStateUpdater
}

func (m *visitor) NotImplemented(_ context.Context, _ scop.NotImplemented) error {
	return nil
}
