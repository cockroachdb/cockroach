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
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/multiregion"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemaexpr"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/errors"
)

type alterTableSetLocalityNode struct {
	n         tree.AlterTableLocality
	tableDesc *tabledesc.Mutable
	dbDesc    catalog.DatabaseDescriptor
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

	_, tableDesc, err := p.ResolveMutableTableDescriptorEx(
		ctx, n.Name, !n.IfExists, tree.ResolveRequireTableDesc,
	)
	if err != nil {
		return nil, err
	}
	if tableDesc == nil {
		return newZeroNode(nil /* columns */), nil
	}

	if err := p.checkPrivilegesForMultiRegionOp(ctx, tableDesc); err != nil {
		return nil, err
	}

	// Ensure that the database is multi-region enabled.
	_, dbDesc, err := p.Descriptors().GetImmutableDatabaseByID(
		ctx,
		p.txn,
		tableDesc.GetParentID(),
		tree.DatabaseLookupFlags{
			AvoidLeased: true,
			Required:    true,
		},
	)
	if err != nil {
		return nil, err
	}

	if !dbDesc.IsMultiRegion() {
		return nil, errors.WithHint(pgerror.Newf(
			pgcode.InvalidTableDefinition,
			"cannot alter a table's LOCALITY if its database is not multi-region enabled",
		),
			"database must first be multi-region enabled using ALTER DATABASE ... SET PRIMARY REGION <region>",
		)
	}

	return &alterTableSetLocalityNode{
		n:         *n,
		tableDesc: tableDesc,
		dbDesc:    dbDesc,
	}, nil
}

func (n *alterTableSetLocalityNode) Next(runParams) (bool, error) { return false, nil }
func (n *alterTableSetLocalityNode) Values() tree.Datums          { return tree.Datums{} }
func (n *alterTableSetLocalityNode) Close(context.Context)        {}

func (n *alterTableSetLocalityNode) alterTableLocalityGlobalToRegionalByTable(
	params runParams,
) error {
	if !n.tableDesc.IsLocalityGlobal() {
		f := params.p.EvalContext().FmtCtx(tree.FmtSimple)
		if err := multiregion.FormatTableLocalityConfig(n.tableDesc.LocalityConfig, f); err != nil {
			// While we're in an error path and generally it's bad to return a
			// different error in an error path, we will only get an error here if the
			// locality is corrupted, in which case, it's probably the right error
			// to return.
			return err
		}
		return errors.AssertionFailedf(
			"invalid call %q on incorrect table locality %s",
			"alter table locality GLOBAL to REGIONAL BY TABLE",
			f.String(),
		)
	}

	_, dbDesc, err := params.p.Descriptors().GetImmutableDatabaseByID(
		params.ctx, params.p.txn, n.tableDesc.ParentID,
		tree.DatabaseLookupFlags{Required: true})
	if err != nil {
		return err
	}

	regionEnumID, err := dbDesc.MultiRegionEnumID()
	if err != nil {
		return err
	}

	if err := params.p.alterTableDescLocalityToRegionalByTable(
		params.ctx, n.n.Locality.TableRegion, n.tableDesc, regionEnumID,
	); err != nil {
		return err
	}

	// Finalize the alter by writing a new table descriptor and updating the zone
	// configuration.
	if err := n.writeNewTableLocalityAndZoneConfig(
		params,
		n.dbDesc,
	); err != nil {
		return err
	}

	return nil
}

func (n *alterTableSetLocalityNode) alterTableLocalityToGlobal(params runParams) error {
	regionEnumID, err := n.dbDesc.MultiRegionEnumID()
	if err != nil {
		return err
	}
	err = params.p.alterTableDescLocalityToGlobal(params.ctx, n.tableDesc, regionEnumID)
	if err != nil {
		return err
	}

	// Finalize the alter by writing a new table descriptor and updating the zone
	// configuration.
	if err := n.writeNewTableLocalityAndZoneConfig(
		params,
		n.dbDesc,
	); err != nil {
		return err
	}

	return nil
}

func (n *alterTableSetLocalityNode) alterTableLocalityRegionalByTableToRegionalByTable(
	params runParams,
) error {
	const operation string = "alter table locality REGIONAL BY TABLE to REGIONAL BY TABLE"
	if !n.tableDesc.IsLocalityRegionalByTable() {
		return errors.AssertionFailedf(
			"invalid call %q on incorrect table locality. %v",
			operation,
			n.tableDesc.LocalityConfig,
		)
	}

	_, dbDesc, err := params.p.Descriptors().GetImmutableDatabaseByID(
		params.ctx, params.p.txn, n.tableDesc.ParentID,
		tree.DatabaseLookupFlags{Required: true})
	if err != nil {
		return err
	}

	regionEnumID, err := dbDesc.MultiRegionEnumID()
	if err != nil {
		return err
	}

	if err := params.p.alterTableDescLocalityToRegionalByTable(
		params.ctx, n.n.Locality.TableRegion, n.tableDesc, regionEnumID,
	); err != nil {
		return err
	}

	// Finalize the alter by writing a new table descriptor and updating the zone configuration.
	if err := n.writeNewTableLocalityAndZoneConfig(
		params,
		n.dbDesc,
	); err != nil {
		return err
	}

	return nil
}

func (n *alterTableSetLocalityNode) alterTableLocalityToRegionalByRow(
	params runParams, newLocality *tree.Locality,
) error {
	mayNeedImplicitCRDBRegionCol := false
	var mutationIdxAllowedInSameTxn *int
	var newColumnName *tree.Name

	// Check if the region column exists already - if so, use it.
	// Otherwise, if we have no name was specified, implicitly create the
	// crdb_region column.
	partColName := newLocality.RegionalByRowColumn

	primaryIndexColIdxStart := 0
	if n.tableDesc.IsLocalityRegionalByRow() {
		as := n.tableDesc.LocalityConfig.GetRegionalByRow().As

		// If the REGIONAL BY ROW (AS <col>) is exactly the same, do nothing.
		defaultColumnSpecified := as == nil && partColName == tree.RegionalByRowRegionNotSpecifiedName
		sameAsColumnSpecified := as != nil && *as == string(partColName)
		if defaultColumnSpecified || sameAsColumnSpecified {
			return nil
		}

		// Otherwise, signal that we have to omit the implicit partitioning columns
		// when modifying the primary key.
		primaryIndexColIdxStart = int(n.tableDesc.PrimaryIndex.Partitioning.NumImplicitColumns)
	}

	for _, idx := range n.tableDesc.AllIndexes() {
		if idx.IsSharded() {
			return pgerror.Newf(
				pgcode.FeatureNotSupported,
				"cannot convert %s to REGIONAL BY ROW as the table contains hash sharded indexes",
				tree.Name(n.tableDesc.GetName()),
			)
		}
	}

	if newLocality.RegionalByRowColumn == tree.RegionalByRowRegionNotSpecifiedName {
		partColName = tree.RegionalByRowRegionDefaultColName
		mayNeedImplicitCRDBRegionCol = true
	}

	partCol, err := n.tableDesc.FindColumnWithName(partColName)
	createDefaultRegionCol := mayNeedImplicitCRDBRegionCol && sqlerrors.IsUndefinedColumnError(err)
	if err != nil && !createDefaultRegionCol {
		return err
	}

	enumTypeID, err := n.dbDesc.MultiRegionEnumID()
	if err != nil {
		return err
	}
	enumOID := typedesc.TypeIDToOID(enumTypeID)

	var newColumnID *descpb.ColumnID
	var newColumnDefaultExpr *string

	if !createDefaultRegionCol {
		// If the column is not public, we cannot use it yet.
		if !partCol.Public() {
			return colinfo.NewUndefinedColumnError(string(partColName))
		}

		// If we already have a column with the given name, check it is compatible to be made
		// a PRIMARY KEY.
		if partCol.GetType().Oid() != typedesc.TypeIDToOID(enumTypeID) {
			return pgerror.Newf(
				pgcode.InvalidTableDefinition,
				"cannot use column %s for REGIONAL BY ROW table as it does not have the %s type",
				partColName,
				tree.RegionEnum,
			)
		}

		// Check whether the given row is NOT NULL.
		if partCol.IsNullable() {
			return errors.WithHintf(
				pgerror.Newf(
					pgcode.InvalidTableDefinition,
					"cannot use column %s for REGIONAL BY ROW table as it may contain NULL values",
					partColName,
				),
				"Add the NOT NULL constraint first using ALTER TABLE %s ALTER COLUMN %s SET NOT NULL.",
				tree.Name(n.tableDesc.Name),
				partColName,
			)
		}
	} else {
		// No crdb_region column is found so we are implicitly creating it.
		// We insert the column definition before altering the primary key.

		primaryRegion, err := n.dbDesc.PrimaryRegionName()
		if err != nil {
			return err
		}
		// No crdb_region column is found so we are implicitly creating it.
		// We insert the column definition before altering the primary key.
		//
		// Note we initially set the default expression to be primary_region,
		// so that it is backfilled this way. When the backfill is complete,
		// we will change this to use gateway_region.
		defaultColDef := &tree.AlterTableAddColumn{
			ColumnDef: regionalByRowDefaultColDef(
				enumOID,
				regionalByRowRegionDefaultExpr(enumOID, tree.Name(primaryRegion)),
				maybeRegionalByRowOnUpdateExpr(params.EvalContext(), enumOID),
			),
		}
		tn, err := params.p.getQualifiedTableName(params.ctx, n.tableDesc)
		if err != nil {
			return err
		}
		if err := params.p.addColumnImpl(
			params,
			&alterTableNode{
				tableDesc: n.tableDesc,
				n: &tree.AlterTable{
					Cmds: []tree.AlterTableCmd{defaultColDef},
				},
			},
			tn,
			n.tableDesc,
			defaultColDef,
		); err != nil {
			return err
		}

		// Allow add column mutation to be on the same mutation ID in AlterPrimaryKey.
		mutationIdx := len(n.tableDesc.Mutations) - 1
		mutationIdxAllowedInSameTxn = &mutationIdx
		newColumnName = &partColName

		version := params.ExecCfg().Settings.Version.ActiveVersion(params.ctx)
		if err := n.tableDesc.AllocateIDs(params.ctx, version); err != nil {
			return err
		}

		// On the AlterPrimaryKeyMutation, sanitize and form the correct default
		// expression to replace the crdb_region column with when the mutation
		// is finalized.
		// NOTE: this is important, as the schema changer is NOT database aware.
		// The primary_region default helps us also have a material value.
		// This can be removed when the default_expr can serialize user defined
		// functions.
		col := n.tableDesc.Mutations[mutationIdx].GetColumn()
		finalDefaultExpr, err := schemaexpr.SanitizeVarFreeExpr(
			params.ctx,
			regionalByRowGatewayRegionDefaultExpr(enumOID),
			col.Type,
			"REGIONAL BY ROW DEFAULT",
			params.p.SemaCtx(),
			tree.VolatilityVolatile,
		)
		if err != nil {
			return err
		}
		s := tree.Serialize(finalDefaultExpr)
		newColumnDefaultExpr = &s
		newColumnID = &col.ID
	}
	return n.alterTableLocalityFromOrToRegionalByRow(
		params,
		tabledesc.LocalityConfigRegionalByRow(newLocality.RegionalByRowColumn),
		mutationIdxAllowedInSameTxn,
		newColumnName,
		newColumnID,
		newColumnDefaultExpr,
		n.tableDesc.PrimaryIndex.KeyColumnNames[primaryIndexColIdxStart:],
		n.tableDesc.PrimaryIndex.KeyColumnDirections[primaryIndexColIdxStart:],
	)
}

// alterTableLocalityFromOrToRegionalByRow processes a change for any ALTER TABLE
// SET LOCALITY where the before OR after state is REGIONAL BY ROW.
func (n *alterTableSetLocalityNode) alterTableLocalityFromOrToRegionalByRow(
	params runParams,
	newLocalityConfig catpb.LocalityConfig,
	mutationIdxAllowedInSameTxn *int,
	newColumnName *tree.Name,
	newColumnID *descpb.ColumnID,
	newColumnDefaultExpr *string,
	pkColumnNames []string,
	pkColumnDirections []descpb.IndexDescriptor_Direction,
) error {
	// Preserve the same PK columns - implicit partitioning will be added in
	// AlterPrimaryKey.
	cols := make([]tree.IndexElem, len(pkColumnNames))
	for i, col := range pkColumnNames {
		cols[i] = tree.IndexElem{
			Column: tree.Name(col),
		}
		switch dir := pkColumnDirections[i]; dir {
		case descpb.IndexDescriptor_ASC:
			cols[i].Direction = tree.Ascending
		case descpb.IndexDescriptor_DESC:
			cols[i].Direction = tree.Descending
		default:
			return errors.AssertionFailedf("unknown direction: %v", dir)
		}
	}

	// We re-use ALTER PRIMARY KEY to do the the work for us.
	//
	// Altering to REGIONAL BY ROW is effectively a PRIMARY KEY swap where we
	// add the implicit partitioning to the PK, with all indexes underneath
	// being re-written to point to the correct PRIMARY KEY and also being
	// implicitly partitioned. The AlterPrimaryKey will also set the relevant
	// zone configurations on the newly re-created indexes and table itself.
	if err := params.p.AlterPrimaryKey(
		params.ctx,
		n.tableDesc,
		tree.AlterTableAlterPrimaryKey{
			Name:    tree.Name(n.tableDesc.PrimaryIndex.Name),
			Columns: cols,
		},
		&alterPrimaryKeyLocalitySwap{
			localityConfigSwap: descpb.PrimaryKeySwap_LocalityConfigSwap{
				OldLocalityConfig:                 *n.tableDesc.LocalityConfig,
				NewLocalityConfig:                 newLocalityConfig,
				NewRegionalByRowColumnID:          newColumnID,
				NewRegionalByRowColumnDefaultExpr: newColumnDefaultExpr,
			},
			mutationIdxAllowedInSameTxn: mutationIdxAllowedInSameTxn,
			newColumnName:               newColumnName,
		},
	); err != nil {
		return err
	}

	return params.p.writeSchemaChange(
		params.ctx,
		n.tableDesc,
		n.tableDesc.ClusterVersion.NextMutationID,
		tree.AsStringWithFQNames(&n.n, params.Ann()),
	)
}

func (n *alterTableSetLocalityNode) startExec(params runParams) error {
	newLocality := n.n.Locality
	existingLocality := n.tableDesc.LocalityConfig

	existingLocalityTelemetryName, err := existingLocality.TelemetryName()
	if err != nil {
		return err
	}
	telemetry.Inc(
		sqltelemetry.AlterTableLocalityCounter(
			existingLocalityTelemetryName,
			newLocality.TelemetryName(),
		),
	)

	// We should check index zone configs if moving to REGIONAL BY ROW.
	if err := params.p.validateZoneConfigForMultiRegionTableWasNotModifiedByUser(
		params.ctx,
		n.dbDesc,
		n.tableDesc,
	); err != nil {
		return err
	}

	// Look at the existing locality, and implement any changes required to move to
	// the new locality.
	switch existingLocality.Locality.(type) {
	case *catpb.LocalityConfig_Global_:
		switch newLocality.LocalityLevel {
		case tree.LocalityLevelGlobal:
			if err := n.alterTableLocalityToGlobal(params); err != nil {
				return err
			}
		case tree.LocalityLevelRow:
			if err := n.alterTableLocalityToRegionalByRow(
				params,
				newLocality,
			); err != nil {
				return err
			}
		case tree.LocalityLevelTable:
			if err := n.alterTableLocalityGlobalToRegionalByTable(params); err != nil {
				return err
			}
		default:
			return errors.AssertionFailedf("unknown table locality: %v", newLocality)
		}
	case *catpb.LocalityConfig_RegionalByTable_:
		switch newLocality.LocalityLevel {
		case tree.LocalityLevelGlobal:
			if err := n.alterTableLocalityToGlobal(params); err != nil {
				return err
			}
		case tree.LocalityLevelRow:
			if err := n.alterTableLocalityToRegionalByRow(
				params,
				newLocality,
			); err != nil {
				return err
			}
		case tree.LocalityLevelTable:
			if err := n.alterTableLocalityRegionalByTableToRegionalByTable(params); err != nil {
				return err
			}
		default:
			return errors.AssertionFailedf("unknown table locality: %v", newLocality)
		}
	case *catpb.LocalityConfig_RegionalByRow_:
		explicitColStart := n.tableDesc.PrimaryIndex.Partitioning.NumImplicitColumns
		switch newLocality.LocalityLevel {
		case tree.LocalityLevelGlobal:
			return n.alterTableLocalityFromOrToRegionalByRow(
				params,
				tabledesc.LocalityConfigGlobal(),
				nil, /* mutationIdxAllowedInSameTxn */
				nil, /* newColumnName */
				nil, /*	newColumnID */
				nil, /*	newColumnDefaultExpr */
				n.tableDesc.PrimaryIndex.KeyColumnNames[explicitColStart:],
				n.tableDesc.PrimaryIndex.KeyColumnDirections[explicitColStart:],
			)
		case tree.LocalityLevelRow:
			if err := n.alterTableLocalityToRegionalByRow(
				params,
				newLocality,
			); err != nil {
				return err
			}
		case tree.LocalityLevelTable:
			return n.alterTableLocalityFromOrToRegionalByRow(
				params,
				tabledesc.LocalityConfigRegionalByTable(n.n.Locality.TableRegion),
				nil, /* mutationIdxAllowedInSameTxn */
				nil, /* newColumnName */
				nil, /*	newColumnID */
				nil, /*	newColumnDefaultExpr */
				n.tableDesc.PrimaryIndex.KeyColumnNames[explicitColStart:],
				n.tableDesc.PrimaryIndex.KeyColumnDirections[explicitColStart:],
			)
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

// writeNewTableLocalityAndZoneConfig writes the table descriptor with the newly
// updated LocalityConfig and writes a new zone configuration for the table.
func (n *alterTableSetLocalityNode) writeNewTableLocalityAndZoneConfig(
	params runParams, dbDesc catalog.DatabaseDescriptor,
) error {
	// Write out the table descriptor update.
	if err := params.p.writeSchemaChange(
		params.ctx,
		n.tableDesc,
		descpb.InvalidMutationID,
		tree.AsStringWithFQNames(&n.n, params.Ann()),
	); err != nil {
		return err
	}

	regionConfig, err := SynthesizeRegionConfig(params.ctx, params.p.txn, dbDesc.GetID(), params.p.Descriptors())
	if err != nil {
		return err
	}
	// Update the zone configuration.
	if err := ApplyZoneConfigForMultiRegionTable(
		params.ctx,
		params.p.txn,
		params.p.ExecCfg(),
		regionConfig,
		n.tableDesc,
		ApplyZoneConfigForMultiRegionTableOptionTableAndIndexes,
	); err != nil {
		return err
	}

	return nil
}

// alterTableDescLocalityToRegionalByTable changes the locality of the given tableDesc
// to Regional By Table homed in the specified region. It also handles the
// dependency with the multi-region enum, if one exists.
func (p *planner) alterTableDescLocalityToRegionalByTable(
	ctx context.Context, region tree.Name, tableDesc *tabledesc.Mutable, regionEnumID descpb.ID,
) error {
	if tableDesc.GetMultiRegionEnumDependencyIfExists() {
		typesDependedOn := []descpb.ID{regionEnumID}
		if err := p.removeTypeBackReferences(ctx, typesDependedOn, tableDesc.GetID(),
			fmt.Sprintf("remove back ref on mr-enum %d for table %d", regionEnumID, tableDesc.GetID()),
		); err != nil {
			return err
		}
	}
	tableDesc.SetTableLocalityRegionalByTable(region)
	if tableDesc.GetMultiRegionEnumDependencyIfExists() {
		return p.addTypeBackReference(
			ctx, regionEnumID, tableDesc.ID,
			fmt.Sprintf("add back ref on mr-enum %d for table %d", regionEnumID, tableDesc.GetID()),
		)
	}
	return nil
}

// alterTableDescLocalityToGlobal changes the locality of the given tableDesc to
// global. It also removes the dependency on the multi-region enum, if it
// existed before the locality switch.
func (p *planner) alterTableDescLocalityToGlobal(
	ctx context.Context, tableDesc *tabledesc.Mutable, regionEnumID descpb.ID,
) error {
	if tableDesc.GetMultiRegionEnumDependencyIfExists() {
		typesDependedOn := []descpb.ID{regionEnumID}
		if err := p.removeTypeBackReferences(ctx, typesDependedOn, tableDesc.GetID(),
			fmt.Sprintf("remove back ref no mr-enum %d for table %d", regionEnumID, tableDesc.GetID()),
		); err != nil {
			return err
		}
	}
	tableDesc.SetTableLocalityGlobal()
	return nil
}

// setNewLocalityConfig sets the locality config of the given table descriptor to
// the provided config. It also removes the dependency on the multi-region enum,
// if it existed before the locality switch.
func setNewLocalityConfig(
	ctx context.Context,
	desc *tabledesc.Mutable,
	txn *kv.Txn,
	b *kv.Batch,
	config catpb.LocalityConfig,
	kvTrace bool,
	descsCol *descs.Collection,
) error {
	getMultiRegionTypeDesc := func() (*typedesc.Mutable, error) {
		_, dbDesc, err := descsCol.GetImmutableDatabaseByID(
			ctx, txn, desc.GetParentID(), tree.DatabaseLookupFlags{Required: true})
		if err != nil {
			return nil, err
		}

		regionEnumID, err := dbDesc.MultiRegionEnumID()
		if err != nil {
			return nil, err
		}
		return descsCol.GetMutableTypeVersionByID(ctx, txn, regionEnumID)
	}
	// If there was a dependency before on the multi-region enum before the
	// new locality is set, we must unlink the dependency.
	if desc.GetMultiRegionEnumDependencyIfExists() {
		typ, err := getMultiRegionTypeDesc()
		if err != nil {
			return err
		}
		typ.RemoveReferencingDescriptorID(desc.GetID())
		if err := descsCol.WriteDescToBatch(ctx, kvTrace, typ, b); err != nil {
			return err
		}
	}
	desc.LocalityConfig = &config
	// If there is a dependency after the new locality is set, we must add it.
	if desc.GetMultiRegionEnumDependencyIfExists() {
		typ, err := getMultiRegionTypeDesc()
		if err != nil {
			return err
		}
		typ.AddReferencingDescriptorID(desc.GetID())
		if err := descsCol.WriteDescToBatch(ctx, kvTrace, typ, b); err != nil {
			return err
		}
	}
	return nil
}
