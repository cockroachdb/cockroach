// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package scmutationexec

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
)

func (i *immediateVisitor) AddNamedRangeZoneConfig(
	ctx context.Context, op scop.AddNamedRangeZoneConfig,
) error {
	id, ok := zonepb.NamedZones[op.RangeName]
	if !ok {
		return errors.AssertionFailedf("unknown named zone: %s", op.RangeName)
	}
	i.ImmediateMutationStateUpdater.UpdateZoneConfig(catid.DescID(id),
		protoutil.Clone(&op.ZoneConfig).(*zonepb.ZoneConfig))
	return nil
}

func (i *immediateVisitor) DiscardNamedRangeZoneConfig(
	ctx context.Context, op scop.DiscardNamedRangeZoneConfig,
) error {
	id, ok := zonepb.NamedZones[op.RangeName]
	if !ok {
		return errors.AssertionFailedf("unknown named zone: %s", op.RangeName)
	}
	i.ImmediateMutationStateUpdater.DeleteZoneConfig(catid.DescID(id))
	return nil
}
