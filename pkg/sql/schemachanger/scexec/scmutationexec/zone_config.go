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
	zc := op.ZoneConfig
	if zc.IsSubzonePlaceholder() && len(zc.Subzones) == 0 {
		i.ImmediateMutationStateUpdater.DeleteZoneConfig(op.DescID)
	} else {
		i.ImmediateMutationStateUpdater.UpdateZoneConfig(op.DescID, zc)
	}
	return nil
}
