// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package scmutationexec

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
)

func (i *immediateVisitor) DiscardZoneConfig(ctx context.Context, op scop.DiscardZoneConfig) error {
	i.ImmediateMutationStateUpdater.DeleteZoneConfig(op.DescID)
	return nil
}

func (i *immediateVisitor) DiscardTableZoneConfig(
	ctx context.Context, op scop.DiscardTableZoneConfig,
) error {
	i.ImmediateMutationStateUpdater.DeleteZoneConfig(op.TableID)
	return nil
}

func (i *immediateVisitor) DiscardSubzoneConfig(
	ctx context.Context, op scop.DiscardSubzoneConfig,
) error {
	i.ImmediateMutationStateUpdater.DeleteSubzoneConfig(op.TableID, op.Subzone, op.SubzoneSpans)
	return nil
}
