// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package scmutationexec

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
)

func (i *immediateVisitor) DiscardZoneConfig(ctx context.Context, op scop.DiscardZoneConfig) error {
	i.ImmediateMutationStateUpdater.DeleteZoneConfig(op.DescID)
	return nil
}

func (i *immediateVisitor) DiscardTableZoneConfig(
	ctx context.Context, op scop.DiscardTableZoneConfig,
) error {
	zc := op.ZoneConfig
	if zc.IsSubzonePlaceholder() && len(zc.Subzones) == 0 {
		i.ImmediateMutationStateUpdater.DeleteZoneConfig(op.TableID)
	} else {
		i.ImmediateMutationStateUpdater.UpdateZoneConfig(op.TableID,
			protoutil.Clone(&op.ZoneConfig).(*zonepb.ZoneConfig))
	}
	return nil
}

func (i *immediateVisitor) DiscardSubzoneConfig(
	ctx context.Context, op scop.DiscardSubzoneConfig,
) error {
	i.ImmediateMutationStateUpdater.DeleteSubzoneConfig(op.TableID, op.Subzone, op.SubzoneSpans)
	return nil
}
