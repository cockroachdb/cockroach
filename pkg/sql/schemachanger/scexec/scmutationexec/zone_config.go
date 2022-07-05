// Copyright 2022 The Cockroach Authors.
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

	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
)

func (m *visitor) AddSubZoneConfig(ctx context.Context, op scop.AddSubZoneConfig) error {
	tbl, err := m.checkOutTable(ctx, op.DescriptorID)
	if err != nil {
		return err
	}
	return m.s.AddSubZoneConfig(ctx, tbl, &zonepb.Subzone{
		IndexID:       uint32(op.IndexID),
		PartitionName: op.PartitionName,
	})
}

func (m *visitor) RemoveSubZoneConfig(ctx context.Context, op scop.RemoveSubZoneConfig) error {
	tbl, err := m.checkOutTable(ctx, op.DescriptorID)
	if err != nil {
		return err
	}
	return m.s.RemoveSubZoneConfig(ctx, tbl, &zonepb.Subzone{
		IndexID:       uint32(op.IndexID),
		PartitionName: op.PartitionName,
	})
}
