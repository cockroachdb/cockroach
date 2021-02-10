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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/dbdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/errors"
)

type alterTableSetLocalityNode struct {
	n         tree.AlterTableLocality
	tableDesc *tabledesc.Mutable
	dbDesc    *dbdesc.Immutable
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

	// Ensure that the database is multi-region enabled.
	dbDesc, err := p.Descriptors().GetImmutableDatabaseByID(
		ctx,
		p.txn,
		tableDesc.GetParentID(),
		tree.DatabaseLookupFlags{},
	)
	if err != nil {
		return nil, err
	}

	if !dbDesc.IsMultiRegion() {
		return nil, pgerror.Newf(
			pgcode.InvalidTableDefinition,
			"cannot alter a table's LOCALITY if its database is not multi-region enabled",
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
		f := tree.NewFmtCtx(tree.FmtSimple)
		if err := tabledesc.FormatTableLocalityConfig(n.tableDesc.LocalityConfig, f); err != nil {
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

	n.tableDesc.SetTableLocalityRegionalByTable(n.n.Locality.TableRegion)

	// Finalize the alter by writing a new table descriptor and updating the zone
	// configuration.
	if err := n.validateAndWriteNewTableLocalityAndZoneConfig(
		params,
		n.dbDesc,
	); err != nil {
		return err
	}

	return nil
}

func (n *alterTableSetLocalityNode) alterTableLocalityRegionalByTableToGlobal(
	params runParams,
) error {
	const operation string = "alter table locality REGIONAL BY TABLE to GLOBAL"
	if !n.tableDesc.IsLocalityRegionalByTable() {
		return errors.AssertionFailedf(
			"invalid call %q on incorrect table locality. %v",
			operation,
			n.tableDesc.LocalityConfig,
		)
	}

	n.tableDesc.SetTableLocalityGlobal()

	// Finalize the alter by writing a new table descriptor and updating the zone
	// configuration.
	if err := n.validateAndWriteNewTableLocalityAndZoneConfig(
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

	n.tableDesc.SetTableLocalityRegionalByTable(n.n.Locality.TableRegion)

	// Finalize the alter by writing a new table descriptor and updating the zone configuration.
	if err := n.validateAndWriteNewTableLocalityAndZoneConfig(
		params,
		n.dbDesc,
	); err != nil {
		return err
	}

	return nil
}

func (n *alterTableSetLocalityNode) alterTableLocalityNonRegionalByRowToRegionalByRow(
	params runParams, newLocality *tree.Locality,
) error {
	enumTypeID, err := n.dbDesc.MultiRegionEnumID()
	if err != nil {
		return err
	}
	enumOID := typedesc.TypeIDToOID(enumTypeID)

	mayNeedImplicitCRDBRegionCol := false
	var mutationIdxAllowedInSameTxn *int
	var newColumnName *tree.Name

	// Check if the region column exists already - if so, use it.
	// Otherwise, if we have no name was specified, implicitly create the
	// crdb_region column.
	partColName := newLocality.RegionalByRowColumn
	if newLocality.RegionalByRowColumn == tree.RegionalByRowRegionNotSpecifiedName {
		partColName = tree.RegionalByRowRegionDefaultColName
		mayNeedImplicitCRDBRegionCol = true
	}

	partCol, err := n.tableDesc.FindColumnWithName(partColName)
	createDefaultRegionCol := mayNeedImplicitCRDBRegionCol && sqlerrors.IsUndefinedColumnError(err)
	if err != nil && !createDefaultRegionCol {
		return err
	}

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
					tree.RegionEnum,
				),
				"Add the NOT NULL constraint first using ALTER TABLE %s ALTER COLUMN %s SET NOT NULL.",
				tree.Name(n.tableDesc.Name),
				partColName,
			)
		}
	} else {
		// No crdb_region column is found so we are implicitly creating it.
		// We insert the column definition before altering the primary key.
		defaultColDef := &tree.AlterTableAddColumn{
			ColumnDef: regionalByRowDefaultColDef(enumOID),
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
			params.SessionData(),
		); err != nil {
			return err
		}

		// Allow add column mutation to be on the same mutation ID in AlterPrimaryKey.
		mutationIdx := len(n.tableDesc.GetMutations()) - 1
		mutationIdxAllowedInSameTxn = &mutationIdx
		newColumnName = &partColName

		if err := n.tableDesc.AllocateIDs(params.ctx); err != nil {
			return err
		}
	}
	return n.alterTableLocalityRegionalByRowChange(
		params,
		tabledesc.LocalityConfigRegionalByRow(newLocality.RegionalByRowColumn),
		mutationIdxAllowedInSameTxn,
		newColumnName,
		n.tableDesc.PrimaryIndex.ColumnNames,
		n.tableDesc.PrimaryIndex.ColumnDirections,
	)
}

// alterTableLocalityRegionalByRowChange processes a change for ALTER TABLE ...
// LOCALITY REGIONAL BY ROW, based on the new locality and PK columns.
func (n *alterTableSetLocalityNode) alterTableLocalityRegionalByRowChange(
	params runParams,
	newLocalityConfig descpb.TableDescriptor_LocalityConfig,
	mutationIdxAllowedInSameTxn *int,
	newColumnName *tree.Name,
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
	// zone configurations appropriate stages of the newly re-created indexes
	// on the table itself.
	if err := params.p.AlterPrimaryKey(
		params.ctx,
		n.tableDesc,
		&tree.AlterTableAlterPrimaryKey{
			Name:    tree.Name(n.tableDesc.PrimaryIndex.Name),
			Columns: cols,
		},
		&alterPrimaryKeyLocalitySwap{
			localityConfigSwap: descpb.PrimaryKeySwap_LocalityConfigSwap{
				OldLocalityConfig: *n.tableDesc.LocalityConfig,
				NewLocalityConfig: newLocalityConfig,
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

	// Look at the existing locality, and implement any changes required to move to
	// the new locality.
	switch existingLocality.Locality.(type) {
	case *descpb.TableDescriptor_LocalityConfig_Global_:
		switch newLocality.LocalityLevel {
		case tree.LocalityLevelGlobal:
			return nil
		case tree.LocalityLevelRow:
			if err := n.alterTableLocalityNonRegionalByRowToRegionalByRow(
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
	case *descpb.TableDescriptor_LocalityConfig_RegionalByTable_:
		switch newLocality.LocalityLevel {
		case tree.LocalityLevelGlobal:
			if err := n.alterTableLocalityRegionalByTableToGlobal(params); err != nil {
				return err
			}
		case tree.LocalityLevelRow:
			if err := n.alterTableLocalityNonRegionalByRowToRegionalByRow(
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
	case *descpb.TableDescriptor_LocalityConfig_RegionalByRow_:
		explicitColStart := n.tableDesc.PrimaryIndex.Partitioning.NumImplicitColumns
		switch newLocality.LocalityLevel {
		case tree.LocalityLevelGlobal:
			return n.alterTableLocalityRegionalByRowChange(
				params,
				tabledesc.LocalityConfigGlobal(),
				nil, /* mutationIdxAllowedInSameTxn */
				nil, /* newColumnName */
				n.tableDesc.PrimaryIndex.ColumnNames[explicitColStart:],
				n.tableDesc.PrimaryIndex.ColumnDirections[explicitColStart:],
			)
		case tree.LocalityLevelRow:
			return unimplemented.NewWithIssue(59632, "implementation pending")
		case tree.LocalityLevelTable:
			return n.alterTableLocalityRegionalByRowChange(
				params,
				tabledesc.LocalityConfigRegionalByTable(n.n.Locality.TableRegion),
				nil, /* mutationIdxAllowedInSameTxn */
				nil, /* newColumnName */
				n.tableDesc.PrimaryIndex.ColumnNames[explicitColStart:],
				n.tableDesc.PrimaryIndex.ColumnDirections[explicitColStart:],
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

// validateAndWriteNewTableLocalityAndZoneConfig validates the newly updated
// LocalityConfig in a table descriptor, writes that table descriptor, and
// writes a new zone configuration for the given table.
func (n *alterTableSetLocalityNode) validateAndWriteNewTableLocalityAndZoneConfig(
	params runParams, dbDesc *dbdesc.Immutable,
) error {
	// Validate the new locality before updating the table descriptor.
	dg := catalogkv.NewOneLevelUncachedDescGetter(params.p.txn, params.EvalContext().Codec)
	if err := n.tableDesc.ValidateTableLocalityConfig(
		params.ctx,
		dg,
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
	if err := applyZoneConfigForMultiRegionTable(
		params.ctx,
		params.p.txn,
		params.p.ExecCfg(),
		*dbDesc.RegionConfig,
		n.tableDesc,
		applyZoneConfigForMultiRegionTableOptionTableAndIndexes,
	); err != nil {
		return err
	}

	return nil
}
