// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package scmutationexec

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/storageparam/tablestorageparam"
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

func (i *immediateVisitor) SetTableStorageParam(
	ctx context.Context, op scop.SetTableStorageParam,
) error {
	tbl, err := i.checkOutTable(ctx, op.Param.TableID)
	if err != nil {
		return err
	}
	setter := tablestorageparam.NewSetter(tbl, false /* isNewObject */)
	return setter.SetToStringValue(ctx, op.Param.Name, op.Param.Value)
}

func (i *immediateVisitor) ResetTableStorageParam(
	ctx context.Context, op scop.ResetTableStorageParam,
) error {
	tbl, err := i.checkOutTable(ctx, op.Param.TableID)
	if err != nil {
		return err
	}
	setter := tablestorageparam.NewSetter(tbl, false /* isNewObject */)
	return setter.ResetToZeroValue(ctx, op.Param.Name)
}

func (i *immediateVisitor) UpsertRowLevelTTL(ctx context.Context, op scop.UpsertRowLevelTTL) error {
	tbl, err := i.checkOutTable(ctx, op.TableID)
	if err != nil {
		return err
	}

	if op.RowLevelTTL == (catpb.RowLevelTTL{}) {
		tbl.RowLevelTTL = nil
		return nil
	}

	// Make a copy of the RowLevelTTL so we can take its address.
	ttl := op.RowLevelTTL
	tbl.RowLevelTTL = &ttl
	return nil
}

func (d *deferredVisitor) UpdateTTLScheduleMetadata(
	ctx context.Context, op scop.UpdateTTLScheduleMetadata,
) error {
	return d.DeferredMutationStateUpdater.UpdateTTLScheduleMetadata(ctx, op.TableID, op.NewName)
}

func (d *deferredVisitor) UpdateTTLScheduleCron(
	ctx context.Context, op scop.UpdateTTLScheduleCron,
) error {
	return d.DeferredMutationStateUpdater.UpdateTTLScheduleCron(ctx, op.ScheduleID, op.NewCronExpr)
}

func (d *deferredVisitor) CreateRowLevelTTLSchedule(
	ctx context.Context, op scop.CreateRowLevelTTLSchedule,
) error {
	return d.DeferredMutationStateUpdater.CreateRowLevelTTLSchedule(ctx, op.TableID)
}

func (i *immediateVisitor) SetTableLocalityGlobal(
	ctx context.Context, op scop.SetTableLocalityGlobal,
) error {
	tbl, err := i.checkOutTable(ctx, op.TableID)
	if err != nil {
		return err
	}
	tbl.SetTableLocalityGlobal()
	return nil
}

func (i *immediateVisitor) SetTableLocalityPrimaryRegion(
	ctx context.Context, op scop.SetTableLocalityPrimaryRegion,
) error {
	tbl, err := i.checkOutTable(ctx, op.TableID)
	if err != nil {
		return err
	}
	tbl.SetTableLocalityRegionalByTable("")
	return nil
}

func (i *immediateVisitor) SetTableLocalitySecondaryRegion(
	ctx context.Context, op scop.SetTableLocalitySecondaryRegion,
) error {
	tbl, err := i.checkOutTable(ctx, op.TableID)
	if err != nil {
		return err
	}
	tbl.SetTableLocalityRegionalByTable(tree.Name(op.RegionName))
	return nil
}

func (i *immediateVisitor) SetTableLocalityRegionalByRow(
	ctx context.Context, op scop.SetTableLocalityRegionalByRow,
) error {
	tbl, err := i.checkOutTable(ctx, op.TableID)
	if err != nil {
		return err
	}
	tbl.SetTableLocalityRegionalByRow(tree.Name(op.As))
	tbl.PartitionAllBy = true
	return nil
}

func (i *immediateVisitor) UnsetTableLocality(
	ctx context.Context, op scop.UnsetTableLocality,
) error {
	tbl, err := i.checkOutTable(ctx, op.TableID)
	if err != nil {
		return err
	}
	// We should not reset TableLocality itself, specially for RBR tables. Otherwise,
	// GC won't cleanup partitions correctly during drop table.
	tbl.PartitionAllBy = false
	tbl.RBRUsingConstraint = descpb.ConstraintID(0)
	return nil
}
