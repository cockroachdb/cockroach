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

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/dbdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/multiregion"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgnotice"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/roleoption"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/errors"
)

type alterDatabaseOwnerNode struct {
	n    *tree.AlterDatabaseOwner
	desc *dbdesc.Mutable
}

// AlterDatabaseOwner transforms a tree.AlterDatabaseOwner into a plan node.
func (p *planner) AlterDatabaseOwner(
	ctx context.Context, n *tree.AlterDatabaseOwner,
) (planNode, error) {
	if err := checkSchemaChangeEnabled(
		ctx,
		p.ExecCfg(),
		"ALTER DATABASE",
	); err != nil {
		return nil, err
	}

	dbDesc, err := p.Descriptors().GetMutableDatabaseByName(ctx, p.txn, string(n.Name),
		tree.DatabaseLookupFlags{Required: true})
	if err != nil {
		return nil, err
	}

	return &alterDatabaseOwnerNode{n: n, desc: dbDesc}, nil
}

func (n *alterDatabaseOwnerNode) startExec(params runParams) error {
	newOwner := n.n.Owner
	oldOwner := n.desc.GetPrivileges().Owner()

	if err := params.p.checkCanAlterDatabaseAndSetNewOwner(params.ctx, n.desc, newOwner); err != nil {
		return err
	}

	// If the owner we want to set to is the current owner, do a no-op.
	if newOwner == oldOwner {
		return nil
	}

	if err := params.p.writeNonDropDatabaseChange(
		params.ctx,
		n.desc,
		tree.AsStringWithFQNames(n.n, params.Ann()),
	); err != nil {
		return err
	}

	return nil
}

// checkCanAlterDatabaseAndSetNewOwner handles privilege checking and setting new owner.
// Called in ALTER DATABASE and REASSIGN OWNED BY.
func (p *planner) checkCanAlterDatabaseAndSetNewOwner(
	ctx context.Context, desc catalog.MutableDescriptor, newOwner security.SQLUsername,
) error {
	if err := p.checkCanAlterToNewOwner(ctx, desc, newOwner); err != nil {
		return err
	}

	// To alter the owner, the user also has to have CREATEDB privilege.
	if err := p.CheckRoleOption(ctx, roleoption.CREATEDB); err != nil {
		return err
	}

	privs := desc.GetPrivileges()
	privs.SetOwner(newOwner)

	// Log Alter Database Owner event. This is an auditable log event and is
	// recorded in the same transaction as the table descriptor update.
	return p.logEvent(ctx,
		desc.GetID(),
		&eventpb.AlterDatabaseOwner{
			DatabaseName: desc.GetName(),
			Owner:        newOwner.Normalized(),
		})
}

func (n *alterDatabaseOwnerNode) Next(runParams) (bool, error) { return false, nil }
func (n *alterDatabaseOwnerNode) Values() tree.Datums          { return tree.Datums{} }
func (n *alterDatabaseOwnerNode) Close(context.Context)        {}

type alterDatabaseAddRegionNode struct {
	n    *tree.AlterDatabaseAddRegion
	desc *dbdesc.Mutable
}

// AlterDatabaseAddRegion transforms a tree.AlterDatabaseAddRegion into a plan node.
func (p *planner) AlterDatabaseAddRegion(
	ctx context.Context, n *tree.AlterDatabaseAddRegion,
) (planNode, error) {
	if err := checkSchemaChangeEnabled(
		ctx,
		p.ExecCfg(),
		"ALTER DATABASE",
	); err != nil {
		return nil, err
	}

	dbDesc, err := p.Descriptors().GetMutableDatabaseByName(ctx, p.txn, string(n.Name),
		tree.DatabaseLookupFlags{Required: true},
	)
	if err != nil {
		return nil, err
	}

	// If we get to this point and the database is not a multi-region database, it means that
	// the database doesn't yet have a primary region. Since we need a primary region before
	// we can add a region, return an error here.
	if !dbDesc.IsMultiRegion() {
		return nil, errors.WithHintf(
			pgerror.Newf(pgcode.InvalidDatabaseDefinition, "cannot add region %s to database %s",
				n.Region.String(),
				n.Name.String(),
			),
			"you must add a PRIMARY REGION first using ALTER DATABASE %s PRIMARY REGION %s",
			n.Name.String(),
			n.Region.String(),
		)
	}

	if err := p.checkPrivilegesForMultiRegionOp(ctx, dbDesc); err != nil {
		return nil, err
	}

	if err := p.checkNoRegionalByRowChangeUnderway(
		ctx,
		dbDesc,
	); err != nil {
		return nil, err
	}

	// Adding a region also involves repartitioning all REGIONAL BY ROW tables
	// underneath the hood, so we must ensure the user has the requisite
	// privileges.
	if err := p.checkPrivilegesForRepartitioningRegionalByRowTables(
		ctx,
		dbDesc,
	); err != nil {
		return nil, err
	}

	return &alterDatabaseAddRegionNode{n: n, desc: dbDesc}, nil
}

// GetMultiRegionEnumAddValuePlacementCCL is the public hook point for the
// CCL-licensed code to determine the placement for a new region inside
// a region enum.
var GetMultiRegionEnumAddValuePlacementCCL = func(
	execCfg *ExecutorConfig, typeDesc *typedesc.Mutable, region tree.Name,
) (tree.AlterTypeAddValue, error) {
	return tree.AlterTypeAddValue{}, sqlerrors.NewCCLRequiredError(
		errors.New("adding regions to a multi-region database requires a CCL binary"),
	)
}

func (n *alterDatabaseAddRegionNode) startExec(params runParams) error {
	if err := params.p.validateZoneConfigForMultiRegionDatabaseWasNotModifiedByUser(
		params.ctx,
		n.desc,
	); err != nil {
		return err
	}

	telemetry.Inc(sqltelemetry.AlterDatabaseAddRegionCounter)

	if err := params.p.checkRegionIsCurrentlyActive(
		params.ctx,
		descpb.RegionName(n.n.Region),
	); err != nil {
		return err
	}

	// Get the type descriptor for the multi-region enum.
	typeDesc, err := params.p.Descriptors().GetMutableTypeVersionByID(
		params.ctx,
		params.p.txn,
		n.desc.RegionConfig.RegionEnumID,
	)
	if err != nil {
		return err
	}

	placement, err := GetMultiRegionEnumAddValuePlacementCCL(
		params.p.ExecCfg(),
		typeDesc,
		n.n.Region,
	)
	if err != nil {
		return err
	}

	// Add the new region value to the enum. This function adds the value to the enum and
	// persists the new value to the supplied type descriptor.
	jobDesc := fmt.Sprintf("Adding new region value %q to %q", tree.EnumValue(n.n.Region), tree.RegionEnum)
	if err := params.p.addEnumValue(
		params.ctx,
		typeDesc,
		&placement,
		jobDesc,
	); err != nil {
		if pgerror.GetPGCode(err) == pgcode.DuplicateObject {
			if n.n.IfNotExists {
				params.p.BufferClientNotice(
					params.ctx,
					pgnotice.Newf("region %q already exists; skipping", n.n.Region),
				)
				return nil
			}
			return pgerror.Newf(
				pgcode.DuplicateObject,
				"region %q already added to database",
				n.n.Region,
			)
		}
		return err
	}

	// Validate the type descriptor after the changes. We have to do this explicitly here, because
	// we're using an internal call to addEnumValue above which doesn't perform validation.
	if err := validateDescriptor(params.ctx, params.p, typeDesc); err != nil {
		return err
	}

	// Log Alter Database Add Region event. This is an auditable log event and is
	// recorded in the same transaction as the database descriptor, type
	// descriptor, and zone configuration updates.
	return params.p.logEvent(params.ctx,
		n.desc.GetID(),
		&eventpb.AlterDatabaseAddRegion{
			DatabaseName: n.desc.GetName(),
			RegionName:   n.n.Region.String(),
		})
}

func (n *alterDatabaseAddRegionNode) Next(runParams) (bool, error) { return false, nil }
func (n *alterDatabaseAddRegionNode) Values() tree.Datums          { return tree.Datums{} }
func (n *alterDatabaseAddRegionNode) Close(context.Context)        {}

type alterDatabaseDropRegionNode struct {
	n                     *tree.AlterDatabaseDropRegion
	desc                  *dbdesc.Mutable
	removingPrimaryRegion bool
	toDrop                []*typedesc.Mutable
}

var allowDropFinalRegion = settings.RegisterBoolSetting(
	"sql.multiregion.drop_primary_region.enabled",
	"allows dropping the PRIMARY REGION of a database if it is the last region",
	true,
).WithPublic()

// AlterDatabaseDropRegion transforms a tree.AlterDatabaseDropRegion into a plan node.
func (p *planner) AlterDatabaseDropRegion(
	ctx context.Context, n *tree.AlterDatabaseDropRegion,
) (planNode, error) {
	if err := checkSchemaChangeEnabled(
		ctx,
		p.ExecCfg(),
		"ALTER DATABASE",
	); err != nil {
		return nil, err
	}

	dbDesc, err := p.Descriptors().GetMutableDatabaseByName(ctx, p.txn, string(n.Name),
		tree.DatabaseLookupFlags{Required: true})
	if err != nil {
		return nil, err
	}

	if !dbDesc.IsMultiRegion() {
		if n.IfExists {
			p.BufferClientNotice(
				ctx,
				pgnotice.Newf("region %q is not defined on the database; skipping", n.Region),
			)
			return &alterDatabaseDropRegionNode{}, nil
		}
		return nil, pgerror.New(pgcode.InvalidDatabaseDefinition, "database has no regions to drop")
	}

	if err := p.checkPrivilegesForMultiRegionOp(ctx, dbDesc); err != nil {
		return nil, err
	}

	if err := p.checkNoRegionalByRowChangeUnderway(
		ctx,
		dbDesc,
	); err != nil {
		return nil, err
	}

	// Dropping a region also involves repartitioning all REGIONAL BY ROW tables
	// underneath the hood, so we must ensure the user has the requisite
	// privileges.
	if err := p.checkPrivilegesForRepartitioningRegionalByRowTables(
		ctx,
		dbDesc,
	); err != nil {
		return nil, err
	}

	if err := p.validateZoneConfigForMultiRegionDatabaseWasNotModifiedByUser(
		ctx,
		dbDesc,
	); err != nil {
		return nil, err
	}

	removingPrimaryRegion := false
	var toDrop []*typedesc.Mutable

	if dbDesc.RegionConfig.PrimaryRegion == descpb.RegionName(n.Region) {
		removingPrimaryRegion = true

		typeID, err := dbDesc.MultiRegionEnumID()
		if err != nil {
			return nil, err
		}
		typeDesc, err := p.Descriptors().GetMutableTypeVersionByID(ctx, p.txn, typeID)
		if err != nil {
			return nil, err
		}

		// Ensure that there's only 1 region on the multi-region enum (the primary
		// region) as this can only be dropped once all other regions have been
		// dropped. This check must account for regions that are transitioning,
		// as transitions aren't guaranteed to succeed.
		regions, err := typeDesc.RegionNamesIncludingTransitioning()
		if err != nil {
			return nil, err
		}
		if len(regions) != 1 {
			return nil, errors.WithHintf(
				pgerror.Newf(
					pgcode.InvalidDatabaseDefinition,
					"cannot drop region %q",
					dbDesc.RegionConfig.PrimaryRegion,
				),
				"You must designate another region as the primary region using "+
					"ALTER DATABASE %s PRIMARY REGION <region name> or remove all other regions before "+
					"attempting to drop region %q", dbDesc.GetName(), n.Region,
			)
		}

		if allowDrop := allowDropFinalRegion.Get(&p.execCfg.Settings.SV); !allowDrop {
			return nil, pgerror.Newf(
				pgcode.InvalidDatabaseDefinition,
				"databases in this cluster must have at least 1 region",
				n.Region,
				DefaultPrimaryRegionClusterSettingName,
			)
		}

		// When the last region is removed from the database, we also clean up
		// detritus multi-region type descriptor. This includes both the
		// type descriptor and its array counterpart.
		toDrop = append(toDrop, typeDesc)
		arrayTypeDesc, err := p.Descriptors().GetMutableTypeVersionByID(ctx, p.txn, typeDesc.ArrayTypeID)
		if err != nil {
			return nil, err
		}
		toDrop = append(toDrop, arrayTypeDesc)
		for _, desc := range toDrop {
			// canDropTypeDesc ensures that there are no references to tables on the
			// type descriptor. This is what we expect when dropping the final
			// (primary) region from a database, as REGIONAL BY ROW tables and
			// REGIONAL BY TABLE tables (homed explicitly in the final region) will
			// store a reference on the type descriptor. It is sufficient to simply
			// check for stored references and not go through the validation that
			// happens in the type_schema changer in this scenario.
			if err := p.canDropTypeDesc(ctx, desc, tree.DropRestrict); err != nil {
				return nil, errors.Wrapf(
					err, "error removing primary region from database %s", dbDesc.Name)
			}
		}
	}

	// Ensure survivability goal and number of regions after the drop jive.
	regionConfig, err := SynthesizeRegionConfig(ctx, p.txn, dbDesc.ID, p.Descriptors())
	if err != nil {
		return nil, err
	}
	if err := multiregion.CanDropRegion(regionConfig); err != nil {
		return nil, err
	}

	return &alterDatabaseDropRegionNode{
		n,
		dbDesc,
		removingPrimaryRegion,
		toDrop,
	}, nil
}

// checkPrivilegesForMultiRegionOp ensures the current user has the required
// privileges to perform a multi-region operation of the given (table|database)
// descriptor. A multi-region operation can be altering the table's locality,
// performing a region add/drop that implicitly repartitions the given table,
// changing the survivability goal on the database etc.
// The user must:
// - either be part of an admin role.
// - or be an owner of the table.
// - or have the CREATE privilege on the table.
// privilege on the table descriptor.
func (p *planner) checkPrivilegesForMultiRegionOp(
	ctx context.Context, desc catalog.Descriptor,
) error {
	hasAdminRole, err := p.HasAdminRole(ctx)
	if err != nil {
		return err
	}
	if !hasAdminRole {
		// TODO(arul): It's worth noting CREATE isn't a thing on tables in postgres,
		// so this will require some changes when (if) we move our privilege system
		// to be more in line with postgres.
		err := p.CheckPrivilege(ctx, desc, privilege.CREATE)
		// Wrap an insufficient privileges error a bit better to reflect the lack
		// of ownership as well.
		if pgerror.GetPGCode(err) == pgcode.InsufficientPrivilege {
			return pgerror.Newf(pgcode.InsufficientPrivilege,
				"user %s must be owner of %s or have %s privilege on %s %s",
				p.SessionData().User(),
				desc.GetName(),
				privilege.CREATE,
				desc.DescriptorType(),
				desc.GetName(),
			)
		}
		return err
	}
	return nil
}

// checkPrivilegesForRepartitioningRegionalByRowTables returns an error if the
// user does not have sufficient privileges to repartition any of the region by
// row tables inside the given database.
func (p *planner) checkPrivilegesForRepartitioningRegionalByRowTables(
	ctx context.Context, dbDesc catalog.DatabaseDescriptor,
) error {
	return p.forEachMutableTableInDatabase(ctx, dbDesc,
		func(ctx context.Context, scName string, tbDesc *tabledesc.Mutable) error {
			if tbDesc.IsLocalityRegionalByRow() {
				err := p.checkPrivilegesForMultiRegionOp(ctx, tbDesc)
				// Return a better error message here.
				if pgerror.GetPGCode(err) == pgcode.InsufficientPrivilege {
					return errors.Wrapf(err,
						"cannot repartition regional by row table",
					)
				}
				if err != nil {
					return err
				}
			}
			return nil
		})
}

// removeLocalityConfigFromAllTablesInDB removes the locality config from all
// tables under the supplied database.
func removeLocalityConfigFromAllTablesInDB(
	ctx context.Context, p *planner, desc catalog.DatabaseDescriptor,
) error {
	if !desc.IsMultiRegion() {
		return errors.AssertionFailedf(
			"cannot remove locality configs from tables in non multi-region database with ID %d",
			desc.GetID(),
		)
	}
	b := p.Txn().NewBatch()
	if err := p.forEachMutableTableInDatabase(
		ctx,
		desc,
		func(ctx context.Context, scName string, tbDesc *tabledesc.Mutable) error {
			// The user must either be an admin or have the requisite privileges.
			if err := p.checkPrivilegesForMultiRegionOp(ctx, tbDesc); err != nil {
				return err
			}

			switch t := tbDesc.LocalityConfig.Locality.(type) {
			case *descpb.TableDescriptor_LocalityConfig_Global_:
				if err := ApplyZoneConfigForMultiRegionTable(
					ctx,
					p.txn,
					p.ExecCfg(),
					multiregion.RegionConfig{}, // pass dummy config as it is not used.
					tbDesc,
					applyZoneConfigForMultiRegionTableOptionRemoveGlobalZoneConfig,
				); err != nil {
					return err
				}
			case *descpb.TableDescriptor_LocalityConfig_RegionalByTable_:
				if t.RegionalByTable.Region != nil {
					// This should error during the type descriptor changes.
					return errors.AssertionFailedf(
						"unexpected REGIONAL BY TABLE IN <region> on table %s during DROP REGION",
						tbDesc.Name,
					)
				}
			case *descpb.TableDescriptor_LocalityConfig_RegionalByRow_:
				// This should error during the type descriptor changes.
				return errors.AssertionFailedf(
					"unexpected REGIONAL BY ROW on table %s during DROP REGION",
					tbDesc.Name,
				)
			default:
				return errors.AssertionFailedf(
					"unexpected locality %T on table %s during DROP REGION",
					t,
					tbDesc.Name,
				)
			}
			tbDesc.LocalityConfig = nil
			return p.writeSchemaChangeToBatch(ctx, tbDesc, b)
		},
	); err != nil {
		return err
	}
	return p.Txn().Run(ctx, b)
}

func (n *alterDatabaseDropRegionNode) startExec(params runParams) error {
	if n.n == nil {
		return nil
	}
	typeDesc, err := params.p.Descriptors().GetMutableTypeVersionByID(
		params.ctx,
		params.p.txn,
		n.desc.RegionConfig.RegionEnumID,
	)
	if err != nil {
		return err
	}

	if n.removingPrimaryRegion {
		telemetry.Inc(sqltelemetry.AlterDatabaseDropPrimaryRegionCounter)
		for _, desc := range n.toDrop {
			jobDesc := fmt.Sprintf("drop multi-region enum with ID %d", desc.ID)
			err := params.p.dropTypeImpl(params.ctx, desc, jobDesc, true /* queueJob */)
			if err != nil {
				return err
			}
		}

		err = removeLocalityConfigFromAllTablesInDB(params.ctx, params.p, n.desc)
		if err != nil {
			return errors.Wrap(err, "error removing locality configs from tables")
		}

		n.desc.UnsetMultiRegionConfig()
		if err := discardMultiRegionFieldsForDatabaseZoneConfig(
			params.ctx,
			n.desc.ID,
			params.p.txn,
			params.p.execCfg,
		); err != nil {
			return err
		}
	} else {
		telemetry.Inc(sqltelemetry.AlterDatabaseDropRegionCounter)
		// dropEnumValue tries to remove the region value from the multi-region type
		// descriptor. Among other things, it validates that the region is not in
		// use by any tables. A region is considered "in use" if either a REGIONAL BY
		// TABLE table is explicitly homed in that region or a row in a REGIONAL BY
		// ROW table is homed in that region. The type schema changer is responsible
		// for all the requisite validation.
		if err := params.p.dropEnumValue(params.ctx, typeDesc, tree.EnumValue(n.n.Region)); err != nil {
			if pgerror.GetPGCode(err) == pgcode.UndefinedObject {
				if n.n.IfExists {
					params.p.BufferClientNotice(
						params.ctx,
						pgnotice.Newf("region %q is not defined on the database; skipping", n.n.Region),
					)
					return nil
				}
				return pgerror.Newf(
					pgcode.UndefinedObject,
					"region %q has not been added to the database",
					n.n.Region,
				)
			}
			return err
		}
	}

	if err := params.p.writeNonDropDatabaseChange(
		params.ctx,
		n.desc,
		tree.AsStringWithFQNames(n.n, params.Ann()),
	); err != nil {
		return err
	}

	// Log Alter Database Drop Region event. This is an auditable log event and is
	// recorded in the same transaction as the table descriptor update.
	return params.p.logEvent(params.ctx,
		n.desc.GetID(),
		&eventpb.AlterDatabaseDropRegion{
			DatabaseName: n.desc.GetName(),
			RegionName:   n.n.Region.String(),
		})
}

func (n *alterDatabaseDropRegionNode) Next(runParams) (bool, error) { return false, nil }
func (n *alterDatabaseDropRegionNode) Values() tree.Datums          { return tree.Datums{} }
func (n *alterDatabaseDropRegionNode) Close(context.Context)        {}

type alterDatabasePrimaryRegionNode struct {
	n    *tree.AlterDatabasePrimaryRegion
	desc *dbdesc.Mutable
}

// AlterDatabasePrimaryRegion transforms a tree.AlterDatabasePrimaryRegion into a plan node.
func (p *planner) AlterDatabasePrimaryRegion(
	ctx context.Context, n *tree.AlterDatabasePrimaryRegion,
) (planNode, error) {
	if err := checkSchemaChangeEnabled(
		ctx,
		p.ExecCfg(),
		"ALTER DATABASE",
	); err != nil {
		return nil, err
	}

	dbDesc, err := p.Descriptors().GetMutableDatabaseByName(ctx, p.txn, string(n.Name),
		tree.DatabaseLookupFlags{Required: true},
	)
	if err != nil {
		return nil, err
	}

	if err := p.checkPrivilegesForMultiRegionOp(ctx, dbDesc); err != nil {
		return nil, err
	}

	return &alterDatabasePrimaryRegionNode{n: n, desc: dbDesc}, nil
}

// switchPrimaryRegion performs the work in ALTER DATABASE ... PRIMARY REGION for the case
// where the database is already a multi-region database.
func (n *alterDatabasePrimaryRegionNode) switchPrimaryRegion(params runParams) error {
	telemetry.Inc(sqltelemetry.SwitchPrimaryRegionCounter)
	// First check if the new primary region has been added to the database. If not, return
	// an error, as it must be added before it can be used as a primary region.
	prevRegionConfig, err := SynthesizeRegionConfig(params.ctx, params.p.txn, n.desc.ID, params.p.Descriptors())
	if err != nil {
		return err
	}
	found := false
	for _, r := range prevRegionConfig.Regions() {
		if r == descpb.RegionName(n.n.PrimaryRegion) {
			found = true
			break
		}
	}

	if !found {
		return errors.WithHintf(
			pgerror.Newf(pgcode.InvalidName,
				"region %s has not been added to the database",
				n.n.PrimaryRegion.String(),
			),
			"you must add the region to the database before setting it as primary region, using "+
				"ALTER DATABASE %s ADD REGION %s",
			n.n.Name.String(),
			n.n.PrimaryRegion.String(),
		)
	}

	// Get the type descriptor for the multi-region enum.
	typeDesc, err := params.p.Descriptors().GetMutableTypeVersionByID(
		params.ctx,
		params.p.txn,
		n.desc.RegionConfig.RegionEnumID)
	if err != nil {
		return err
	}

	// To update the primary region we need to modify the database descriptor,
	// update the multi-region enum, and write a new zone configuration.
	n.desc.RegionConfig.PrimaryRegion = descpb.RegionName(n.n.PrimaryRegion)
	if err := params.p.writeNonDropDatabaseChange(
		params.ctx,
		n.desc,
		tree.AsStringWithFQNames(n.n, params.Ann()),
	); err != nil {
		return err
	}

	// Update the primary region in the type descriptor, and write it back out.
	typeDesc.RegionConfig.PrimaryRegion = descpb.RegionName(n.n.PrimaryRegion)
	if err := params.p.writeTypeDesc(params.ctx, typeDesc); err != nil {
		return err
	}

	updatedRegionConfig, err := SynthesizeRegionConfig(
		params.ctx, params.p.txn, n.desc.ID, params.p.Descriptors(),
	)
	if err != nil {
		return err
	}

	// Update the database's zone configuration.
	if err := ApplyZoneConfigFromDatabaseRegionConfig(
		params.ctx,
		n.desc.ID,
		updatedRegionConfig,
		params.p.txn,
		params.p.execCfg,
	); err != nil {
		return err
	}

	return nil
}

// addDefaultLocalityConfigToAllTables adds a default locality config to all
// tables inside the supplied database. The default locality config indicates
// that the table is a REGIONAL BY TABLE table homed in the primary region of
// the database.
func addDefaultLocalityConfigToAllTables(
	ctx context.Context, p *planner, dbDesc catalog.DatabaseDescriptor, regionEnumID descpb.ID,
) error {
	if !dbDesc.IsMultiRegion() {
		return errors.AssertionFailedf(
			"cannot add locality config to tables in non multi-region database with ID %d",
			dbDesc.GetID(),
		)
	}
	b := p.Txn().NewBatch()
	if err := p.forEachMutableTableInDatabase(
		ctx,
		dbDesc,
		func(ctx context.Context, scName string, tbDesc *tabledesc.Mutable) error {
			if err := p.checkPrivilegesForMultiRegionOp(ctx, tbDesc); err != nil {
				return err
			}

			if err := checkCanConvertTableToMultiRegion(dbDesc, tbDesc); err != nil {
				return err
			}

			if tbDesc.MaterializedView() {
				if err := p.alterTableDescLocalityToGlobal(
					ctx, tbDesc, regionEnumID,
				); err != nil {
					return err
				}
			} else {
				if err := p.alterTableDescLocalityToRegionalByTable(
					ctx, tree.PrimaryRegionNotSpecifiedName, tbDesc, regionEnumID,
				); err != nil {
					return err
				}
			}
			if err := p.writeSchemaChangeToBatch(ctx, tbDesc, b); err != nil {
				return err
			}
			return nil
		},
	); err != nil {
		return err
	}
	return p.Txn().Run(ctx, b)
}

// checkCanConvertTableToMultiRegion checks whether a given table can be converted
// to a multi-region table.
func checkCanConvertTableToMultiRegion(
	dbDesc catalog.DatabaseDescriptor, tableDesc catalog.TableDescriptor,
) error {
	if tableDesc.GetPrimaryIndex().GetPartitioning().NumColumns() > 0 {
		return errors.WithDetailf(
			pgerror.Newf(
				pgcode.ObjectNotInPrerequisiteState,
				"cannot convert database %s to a multi-region database",
				dbDesc.GetName(),
			),
			"cannot convert table %s to a multi-region table as it is partitioned",
			tableDesc.GetName(),
		)
	}
	for _, idx := range tableDesc.AllIndexes() {
		if idx.GetPartitioning().NumColumns() > 0 {
			return errors.WithDetailf(
				pgerror.Newf(
					pgcode.ObjectNotInPrerequisiteState,
					"cannot convert database %s to a multi-region database",
					dbDesc.GetName(),
				),
				"cannot convert table %s to a multi-region table as it has index/constraint %s with partitioning",
				tableDesc.GetName(),
				idx.GetName(),
			)
		}
	}
	return nil
}

// setInitialPrimaryRegion sets the primary region in cases where the database
// isn't already a multi-region database.
func (n *alterDatabasePrimaryRegionNode) setInitialPrimaryRegion(params runParams) error {
	telemetry.Inc(sqltelemetry.SetInitialPrimaryRegionCounter)
	// Create the region config structure to be added to the database descriptor.
	regionConfig, err := params.p.maybeInitializeMultiRegionMetadata(
		params.ctx,
		tree.SurvivalGoalDefault,
		n.n.PrimaryRegion,
		[]tree.Name{n.n.PrimaryRegion},
	)
	if err != nil {
		return err
	}

	// Check we are writing valid zone configurations.
	if err := params.p.validateAllMultiRegionZoneConfigsInDatabase(
		params.ctx,
		n.desc,
		&zoneConfigForMultiRegionValidatorSetInitialRegion{},
	); err != nil {
		return err
	}

	// Set the region config on the database descriptor.
	if err := n.desc.SetInitialMultiRegionConfig(regionConfig); err != nil {
		return err
	}

	if err := addDefaultLocalityConfigToAllTables(
		params.ctx,
		params.p,
		n.desc,
		regionConfig.RegionEnumID(),
	); err != nil {
		return err
	}

	// Write the modified database descriptor.
	if err := params.p.writeNonDropDatabaseChange(
		params.ctx,
		n.desc,
		tree.AsStringWithFQNames(n.n, params.Ann()),
	); err != nil {
		return err
	}

	// Initialize that multi-region database by creating the multi-region enum
	// and the database-level zone configuration.
	err = params.p.maybeInitializeMultiRegionDatabase(params.ctx, n.desc, regionConfig)
	if err != nil {
		// Hijack the error if an object called `crdb_internal_regions` already
		// exists in the database and return a suitable hint.
		if pgerror.GetPGCode(err) == pgcode.DuplicateObject {
			return errors.WithHint(
				errors.WithDetail(err,
					"multi-region databases employ an internal enum called crdb_internal_region to "+
						"manage regions which conflicts with the existing object"),
				`object "crdb_internal_regions" must be renamed or dropped before adding the primary region`)
		}
		return err
	}
	return nil
}

func (n *alterDatabasePrimaryRegionNode) startExec(params runParams) error {
	// Block adding a primary region to the system database. This ensures that the system
	// database can never be made into a multi-region database.
	if n.desc.GetID() == keys.SystemDatabaseID {
		return pgerror.Newf(
			pgcode.FeatureNotSupported,
			"adding a primary region to the system database is not supported",
		)
	}

	// There are two paths to consider here: either this is the first setting of
	// the primary region, OR we're updating the primary region. In the case where
	// this is the first setting of the primary region, the call will turn the
	// database into a "multi-region" database. This requires creating a
	// RegionConfig structure in the database descriptor, creating a multi-region
	// enum, and setting up the database-level zone configuration. The second case
	// is simpler, as the multi-region infrastructure is already setup. In this
	// case we just need to update the database and type descriptor, and the zone
	// config.
	if !n.desc.IsMultiRegion() {
		// No need for zone configuration validation here, as #59719 will block
		// getting into this state if there are zone configurations applied at the
		// database level.
		err := n.setInitialPrimaryRegion(params)
		if err != nil {
			return err
		}
	} else {
		if err := params.p.validateZoneConfigForMultiRegionDatabaseWasNotModifiedByUser(
			params.ctx,
			n.desc,
		); err != nil {
			return err
		}

		err := n.switchPrimaryRegion(params)
		if err != nil {
			return err
		}
	}

	// Log Alter Database Primary Region event. This is an auditable log event and
	// is recorded in the same transaction as the database descriptor, and zone
	// configuration updates.
	return params.p.logEvent(params.ctx,
		n.desc.GetID(),
		&eventpb.AlterDatabasePrimaryRegion{
			DatabaseName:      n.desc.GetName(),
			PrimaryRegionName: n.n.PrimaryRegion.String(),
		})
}

func (n *alterDatabasePrimaryRegionNode) Next(runParams) (bool, error) { return false, nil }
func (n *alterDatabasePrimaryRegionNode) Values() tree.Datums          { return tree.Datums{} }
func (n *alterDatabasePrimaryRegionNode) Close(context.Context)        {}
func (n *alterDatabasePrimaryRegionNode) ReadingOwnWrites()            {}

type alterDatabaseSurvivalGoalNode struct {
	n    *tree.AlterDatabaseSurvivalGoal
	desc *dbdesc.Mutable
}

// AlterDatabaseSurvivalGoal transforms a tree.AlterDatabaseSurvivalGoal into a plan node.
func (p *planner) AlterDatabaseSurvivalGoal(
	ctx context.Context, n *tree.AlterDatabaseSurvivalGoal,
) (planNode, error) {
	if err := checkSchemaChangeEnabled(
		ctx,
		p.ExecCfg(),
		"ALTER DATABASE",
	); err != nil {
		return nil, err
	}

	dbDesc, err := p.Descriptors().GetMutableDatabaseByName(ctx, p.txn, string(n.Name),
		tree.DatabaseLookupFlags{Required: true},
	)
	if err != nil {
		return nil, err
	}
	if err := p.checkPrivilegesForMultiRegionOp(ctx, dbDesc); err != nil {
		return nil, err
	}

	return &alterDatabaseSurvivalGoalNode{n: n, desc: dbDesc}, nil
}

func (n *alterDatabaseSurvivalGoalNode) startExec(params runParams) error {
	// If the database is not a multi-region database, the survival goal cannot be changed.
	if !n.desc.IsMultiRegion() {
		return errors.WithHintf(
			pgerror.New(pgcode.InvalidName,
				"database must have associated regions before a survival goal can be set",
			),
			"you must first add a primary region to the database using "+
				"ALTER DATABASE %s PRIMARY REGION <region_name>",
			n.n.Name.String(),
		)
	}

	if err := params.p.validateZoneConfigForMultiRegionDatabaseWasNotModifiedByUser(
		params.ctx,
		n.desc,
	); err != nil {
		return err
	}

	telemetry.Inc(
		sqltelemetry.AlterDatabaseSurvivalGoalCounter(
			n.n.SurvivalGoal.TelemetryName(),
		),
	)

	// Update the survival goal in the database descriptor
	survivalGoal, err := TranslateSurvivalGoal(n.n.SurvivalGoal)
	if err != nil {
		return err
	}
	n.desc.RegionConfig.SurvivalGoal = survivalGoal

	if err := params.p.writeNonDropDatabaseChange(
		params.ctx,
		n.desc,
		tree.AsStringWithFQNames(n.n, params.Ann()),
	); err != nil {
		return err
	}

	regionConfig, err := SynthesizeRegionConfig(params.ctx, params.p.txn, n.desc.ID, params.p.Descriptors())
	if err != nil {
		return err
	}

	if err := multiregion.ValidateRegionConfig(regionConfig); err != nil {
		return err
	}

	// Update the database's zone configuration.
	if err := ApplyZoneConfigFromDatabaseRegionConfig(
		params.ctx,
		n.desc.ID,
		regionConfig,
		params.p.txn,
		params.p.execCfg,
	); err != nil {
		return err
	}

	// Update all REGIONAL BY TABLE tables' zone configurations. This is required as replica
	// placement for REGIONAL BY TABLE tables is dependant on the survival goal.
	if err := params.p.updateZoneConfigsForAllTables(params.ctx, n.desc); err != nil {
		return err
	}

	// Log Alter Database Survival Goal event. This is an auditable log event and
	// is recorded in the same transaction as the database descriptor, and zone
	// configuration updates.
	return params.p.logEvent(params.ctx,
		n.desc.GetID(),
		&eventpb.AlterDatabaseSurvivalGoal{
			DatabaseName: n.desc.GetName(),
			SurvivalGoal: survivalGoal.String(),
		},
	)
}

func (n *alterDatabaseSurvivalGoalNode) Next(runParams) (bool, error) { return false, nil }
func (n *alterDatabaseSurvivalGoalNode) Values() tree.Datums          { return tree.Datums{} }
func (n *alterDatabaseSurvivalGoalNode) Close(context.Context)        {}
