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
	cr CatalogReader, s MutationVisitorStateUpdater, clock Clock,
) scop.MutationVisitor {
	return &visitor{
		cr:    cr,
		s:     s,
		clock: clock,
	}
}

type visitor struct {
	clock Clock
	cr    CatalogReader
	s     MutationVisitorStateUpdater
}

var _ scop.MutationVisitor = (*visitor)(nil)

func (m *visitor) NotImplemented(_ context.Context, op scop.NotImplemented) error {
	return nil
}
