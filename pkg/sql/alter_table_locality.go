// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/dbdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/errors"
)

type alterTableSetLocalityNode struct {
	n         tree.AlterTableLocality
	tableDesc *tabledesc.Mutable
}

// AlterTableLocality transforms a tree.AlterTableLocality into a plan node.
func (p *planner) AlterTableLocality(
	ctx context.Context, n *tree.AlterTableLocality,
) (planNode, error) {
	if err := checkSchemaChangeEnabled(
		ctx,
		p.ExecCfg(),
		"ALTER TABLE",
	); err != nil {
		return nil, err
	}

	tableDesc, err := p.ResolveMutableTableDescriptorEx(
		ctx, n.Name, !n.IfExists, tree.ResolveRequireTableDesc,
	)
	if err != nil {
		return nil, err
	}
	if tableDesc == nil {
		return newZeroNode(nil /* columns */), nil
	}

	// This check for CREATE privilege is kept for backwards compatibility.
	if err := p.CheckPrivilege(ctx, tableDesc, privilege.CREATE); err != nil {
		return nil, pgerror.Newf(pgcode.InsufficientPrivilege,
			"must be owner of table %s or have CREATE privilege on table %s",
			tree.Name(tableDesc.GetName()), tree.Name(tableDesc.GetName()))
	}

	return &alterTableSetLocalityNode{
		n:         *n,
		tableDesc: tableDesc,
	}, nil
}

func (n *alterTableSetLocalityNode) Next(runParams) (bool, error) { return false, nil }
func (n *alterTableSetLocalityNode) Values() tree.Datums          { return tree.Datums{} }
func (n *alterTableSetLocalityNode) Close(context.Context)        {}

func (n *alterTableSetLocalityNode) alterTableLocalityGlobalToRegionalByTable(
	params runParams, dbDesc *dbdesc.Immutable, tableName tree.TableName,
) error {
	const operation string = "alter table locality GLOBAL to REGIONAL BY TABLE"
	if err := assertIsMultiRegionDatabase(dbDesc, operation); err != nil {
		return err
	}
	if !n.tableDesc.IsLocalityGlobal() {
		return errors.AssertionFailedf(
			"invalid call %q on incorrect table locality. %v",
			operation,
			n.tableDesc.LocalityConfig,
		)
	}

	n.tableDesc.LocalityConfig = generateNewLocalityConfigForLocalityRegionalByTable(
		descpb.RegionName(n.n.Locality.TableRegion),
	)

	// Finalize the alter by writing a new table descriptor and updating the zone configuration.
	if err := n.validateAndWriteNewTableLocalityAndZoneConfig(
		params,
		dbDesc,
		tableName,
	); err != nil {
		return err
	}

	return nil
}

func (n *alterTableSetLocalityNode) alterTableLocalityRegionalByTableToGlobal(
	params runParams, dbDesc *dbdesc.Immutable, tableName tree.TableName,
) error {
	const operation string = "alter table locality REGIONAL BY TABLE to GLOBAL"
	if err := assertIsMultiRegionDatabase(dbDesc, operation); err != nil {
		return err
	}
	if !n.tableDesc.IsLocalityRegionalByTable() {
		return errors.AssertionFailedf(
			"invalid call %q on incorrect table locality. %v",
			operation,
			n.tableDesc.LocalityConfig,
		)
	}

	n.tableDesc.LocalityConfig = generateNewLocalityConfigForLocalityGlobal()

	// Finalize the alter by writing a new table descriptor and updating the zone configuration.
	if err := n.validateAndWriteNewTableLocalityAndZoneConfig(
		params,
		dbDesc,
		tableName,
	); err != nil {
		return err
	}

	return nil
}

func (n *alterTableSetLocalityNode) alterTableLocalityRegionalByTableToRegionalByTable(
	params runParams, dbDesc *dbdesc.Immutable, tableName tree.TableName,
) error {
	const operation string = "alter table locality REGIONAL BY TABLE to REGIONAL BY TABLE"
	if err := assertIsMultiRegionDatabase(dbDesc, operation); err != nil {
		return err
	}
	if !n.tableDesc.IsLocalityRegionalByTable() {
		return errors.AssertionFailedf(
			"invalid call %q on incorrect table locality. %v",
			operation,
			n.tableDesc.LocalityConfig,
		)
	}

	n.tableDesc.LocalityConfig = generateNewLocalityConfigForLocalityRegionalByTable(
		descpb.RegionName(n.n.Locality.TableRegion),
	)

	// Finalize the alter by writing a new table descriptor and updating the zone configuration.
	if err := n.validateAndWriteNewTableLocalityAndZoneConfig(
		params,
		dbDesc,
		tableName,
	); err != nil {
		return err
	}

	return nil
}

func (n *alterTableSetLocalityNode) startExec(params runParams) error {
	// Ensure that the database is multi-region enabled.
	dbDesc, err := catalogkv.MustGetDatabaseDescByID(
		params.ctx,
		params.extendedEvalCtx.Txn,
		params.extendedEvalCtx.EvalContext.Codec,
		n.tableDesc.GetParentID(),
	)
	if err != nil {
		return err
	}
	if !dbDesc.IsMultiRegion() {
		return pgerror.Newf(
			pgcode.InvalidTableDefinition,
			"cannot alter a table's LOCALITY if its database is not multi-region enabled",
		)
	}

	// Get the fully resolved table name for use in the calls below.
	resolvedSchema, err := params.p.Descriptors().GetImmutableSchemaByID(
		params.ctx,
		params.p.txn,
		n.tableDesc.GetParentSchemaID(),
		tree.SchemaLookupFlags{})
	if err != nil {
		return err
	}

	tableName := tree.MakeTableNameWithSchema(
		tree.Name(dbDesc.Name),
		tree.Name(resolvedSchema.Name),
		tree.Name(n.tableDesc.GetName()),
	)

	newLocality := n.n.Locality
	existingLocality := n.tableDesc.LocalityConfig

	// If the table's LocalityConfig is nil, it means that the table is REGIONAL BY
	// TABLE in the PRIMARY REGION. Construct a dummy LocalityConfig to mimic that.
	if existingLocality == nil {
		existingLocality = &descpb.TableDescriptor_LocalityConfig{
			Locality: &descpb.TableDescriptor_LocalityConfig_RegionalByTable_{
				RegionalByTable: &descpb.TableDescriptor_LocalityConfig_RegionalByTable{},
			},
		}
	}

	// Look at the existing locality, and implement any changes required to move to
	// the new locality.
	switch existingLocality.Locality.(type) {
	case *descpb.TableDescriptor_LocalityConfig_Global_:
		switch newLocality.LocalityLevel {
		case tree.LocalityLevelGlobal:
			// GLOBAL to GLOBAL - no op.
			return nil
		case tree.LocalityLevelRow:
			// GLOBAL to REGIONAL BY ROW
			return unimplemented.New("alter table locality to REGIONAL BY ROW", "implementation pending")
		case tree.LocalityLevelTable:
			if err = n.alterTableLocalityGlobalToRegionalByTable(params, dbDesc, tableName); err != nil {
				return err
			}
		default:
			return errors.AssertionFailedf("unknown table locality: %v", newLocality)
		}
	case *descpb.TableDescriptor_LocalityConfig_RegionalByTable_:
		switch newLocality.LocalityLevel {
		case tree.LocalityLevelGlobal:
			err = n.alterTableLocalityRegionalByTableToGlobal(params, dbDesc, tableName)
			if err != nil {
				return err
			}
		case tree.LocalityLevelRow:
			return unimplemented.New("alter table locality to REGIONAL BY ROW", "implementation pending")
		case tree.LocalityLevelTable:
			err = n.alterTableLocalityRegionalByTableToRegionalByTable(params, dbDesc, tableName)
			if err != nil {
				return err
			}
		default:
			return errors.AssertionFailedf("unknown table locality: %v", newLocality)
		}
	case *descpb.TableDescriptor_LocalityConfig_RegionalByRow_:
		switch newLocality.LocalityLevel {
		case tree.LocalityLevelGlobal:
			return unimplemented.New("alter table locality from REGIONAL BY ROW", "implementation pending")
		case tree.LocalityLevelRow:
			// Altering to same table locality pattern. We're done.
			return unimplemented.New("alter table locality from REGIONAL BY ROW", "implementation pending")
		case tree.LocalityLevelTable:
			return unimplemented.New("alter table locality from REGIONAL BY ROW", "implementation pending")
		default:
			return errors.AssertionFailedf("unknown table locality: %v", newLocality)
		}
	default:
		return errors.AssertionFailedf("unknown table locality: %v", existingLocality)
	}

	// Record this table alteration in the event log. This is an auditable log
	// event and is recorded in the same transaction as the table descriptor
	// update.
	return params.p.logEvent(params.ctx,
		n.tableDesc.ID,
		&eventpb.AlterTable{
			TableName: n.n.Name.String(),
		})
}

func generateNewLocalityConfigForLocalityGlobal() *descpb.TableDescriptor_LocalityConfig {
	return &descpb.TableDescriptor_LocalityConfig{
		Locality: &descpb.TableDescriptor_LocalityConfig_Global_{
			Global: &descpb.TableDescriptor_LocalityConfig_Global{},
		},
	}
}

func generateNewLocalityConfigForLocalityRegionalByTable(
	newRegionName descpb.RegionName,
) *descpb.TableDescriptor_LocalityConfig {
	lc := &descpb.TableDescriptor_LocalityConfig{}
	var newRegion *descpb.RegionName = nil

	// If we haven't been provided a new region, we're altering to the PRIMARY REGION.
	alterToPrimaryRegion := newRegionName == ""
	// If we're altering to the primary region, leave the newRegion as nil.
	if !alterToPrimaryRegion {
		newRegion = &newRegionName
	}

	// Setup the Locality and Region fields of the LocalityConfig. In cases where we're
	// altering to the PRIMARY REGION, the Region will be set to nil so that SHOW CREATE
	// TABLE will show "REGIONAL BY TABLE IN PRIMARY REGION".
	lc.Locality =
		&descpb.TableDescriptor_LocalityConfig_RegionalByTable_{
			RegionalByTable: &descpb.TableDescriptor_LocalityConfig_RegionalByTable{
				Region: newRegion,
			},
		}

	return lc
}

// validateAndWriteNewTableLocalityAndZoneConfig validates the newly updated LocalityConfig
// in a table descriptor, writes that table descriptor, and writes a new zone configuration
// for the given table.
func (n *alterTableSetLocalityNode) validateAndWriteNewTableLocalityAndZoneConfig(
	params runParams, dbDesc *dbdesc.Immutable, tableName tree.TableName,
) error {
	// Validate the new locality before updating the table descriptor.
	if err := tabledesc.ValidateTableLocalityConfig(
		tableName.String(),
		n.tableDesc.LocalityConfig,
		dbDesc,
	); err != nil {
		return err
	}

	// Write out the table descriptor update.
	if err := params.p.writeSchemaChange(
		params.ctx,
		n.tableDesc,
		descpb.InvalidMutationID,
		tree.AsStringWithFQNames(&n.n, params.Ann()),
	); err != nil {
		return err
	}

	// Update the zone configuration.
	if err := params.p.applyZoneConfigFromTableLocalityConfig(
		params.ctx,
		tableName,
		n.tableDesc.TableDesc(),
		*dbDesc.RegionConfig,
	); err != nil {
		return err
	}

	return nil
}
