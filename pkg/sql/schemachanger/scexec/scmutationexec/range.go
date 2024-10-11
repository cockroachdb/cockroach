// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package scmutationexec

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
)

func (i *immediateVisitor) AddNamedRangeZoneConfig(
	ctx context.Context, op scop.AddNamedRangeZoneConfig,
) error {
	i.ImmediateMutationStateUpdater.UpdateZoneConfig(op.RangeID, op.ZoneConfig)
	return nil
}
