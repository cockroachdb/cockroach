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

func (i *immediateVisitor) AddTableZoneConfig(
	ctx context.Context, op scop.AddTableZoneConfig,
) error {
	i.ImmediateMutationStateUpdater.UpdateZoneConfig(op.TableID, protoutil.Clone(&op.ZoneConfig).(*zonepb.ZoneConfig))
	return nil
}

func (i *immediateVisitor) SetTableSchemaLocked(
	ctx context.Context, op scop.SetTableSchemaLocked,
) error {
	tbl, err := i.checkOutTable(ctx, op.TableID)
	if err != nil {
		return err
	}
	tbl.SchemaLocked = op.Locked
	return nil
}
