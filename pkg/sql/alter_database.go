// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/dbdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/multiregion"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/zone"
	"github.com/cockroachdb/cockroach/pkg/sql/decodeusername"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgnotice"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/regionliveness"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlclustersettings"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/util/intsets"
	"github.com/cockroachdb/cockroach/pkg/util/iterutil"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/cockroach/pkg/util/log/logpb"
	"github.com/cockroachdb/errors"
	yaml "gopkg.in/yaml.v2"
)

type alterDatabaseOwnerNode struct {
	zeroInputPlanNode
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

	dbDesc, err := p.Descriptors().MutableByName(p.txn).Database(ctx, string(n.Name))
	if err != nil {
		return nil, err
	}

	return &alterDatabaseOwnerNode{n: n, desc: dbDesc}, nil
}

func (n *alterDatabaseOwnerNode) startExec(params runParams) error {
	newOwner, err := decodeusername.FromRoleSpec(
		params.p.SessionData(), username.PurposeValidation, n.n.Owner,
	)
	if err != nil {
		return err
	}
	oldOwner := n.desc.GetPrivileges().Owner()

	if err := params.p.checkCanAlterToNewOwner(params.ctx, n.desc, newOwner); err != nil {
		return err
	}

	// To alter the owner, the user also has to have CREATEDB privilege.
	if err := params.p.CheckGlobalPrivilegeOrRoleOption(params.ctx, privilege.CREATEDB); err != nil {
		return err
	}

	if err := params.p.setNewDatabaseOwner(params.ctx, n.desc, newOwner); err != nil {
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

// setNewDatabaseOwner handles setting a new database owner.
// Called in ALTER DATABASE and REASSIGN OWNED BY.
func (p *planner) setNewDatabaseOwner(
	ctx context.Context, desc catalog.MutableDescriptor, newOwner username.SQLUsername,
) error {
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
	zeroInputPlanNode
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

	dbDesc, err := p.Descriptors().MutableByName(p.txn).Database(ctx, string(n.Name))
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

// validateExistingZoneCfg validates existing zone configs for a given descriptor
// ID.
func (p *planner) validateExistingZoneCfg(ctx context.Context, id descpb.ID) error {
	zc, err := p.Descriptors().GetZoneConfig(ctx, p.txn, id)
	if err != nil || zc == nil {
		return err
	}
	return zc.ZoneConfigProto().Validate()
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
		catpb.RegionName(n.n.Region),
		n.desc.ID == keys.SystemDatabaseID,
	); err != nil {
		return err
	}

	// Validate if the existing zone config of this descriptor is sane,
	// since otherwise, any modifications for multi-region could fail during
	// the job phase.
	if err := params.p.validateExistingZoneCfg(params.ctx, n.desc.ID); err != nil {
		return err
	}

	// Get the type descriptor for the multi-region enum.
	typeDesc, err := params.p.Descriptors().MutableByID(params.p.txn).Type(params.ctx, n.desc.RegionConfig.RegionEnumID)
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

	// Once more than one region exists on the system database, we will
	// force it into region survival mode.
	if n.desc.GetID() == keys.SystemDatabaseID {
		if err := params.p.setSystemDatabaseSurvival(params.ctx); err != nil {
			return err
		}

		params.p.BufferClientNotice(
			params.ctx,
			pgnotice.Newf("Rolling restart is recommended after adding a region to system database in order to propogate region information."),
		)
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
	zeroInputPlanNode
	n                     *tree.AlterDatabaseDropRegion
	desc                  *dbdesc.Mutable
	removingPrimaryRegion bool
	toDrop                []*typedesc.Mutable
}

var allowDropFinalRegion = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	"sql.multiregion.drop_primary_region.enabled",
	"allows dropping the PRIMARY REGION of a database if it is the last region",
	true,
	settings.WithPublic)

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

	dbDesc, err := p.Descriptors().MutableByName(p.txn).Database(ctx, string(n.Name))
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

	// Ensure the secondary region is not being dropped
	if dbDesc.RegionConfig.SecondaryRegion == catpb.RegionName(n.Region) {
		return nil, errors.WithHintf(
			pgerror.Newf(
				pgcode.InvalidDatabaseDefinition,
				"cannot drop region %q",
				dbDesc.RegionConfig.SecondaryRegion,
			),
			"You must designate another region as the secondary region using "+
				"ALTER DATABASE %s SECONDARY REGION <region name> or remove the secondary region "+
				"using ALTER DATABASE %s DROP SECONDARY REGION in order to drop region %q",
			dbDesc.GetName(),
			dbDesc.GetName(),
			n.Region,
		)
	}

	removingPrimaryRegion := false
	var toDrop []*typedesc.Mutable
	if dbDesc.RegionConfig.PrimaryRegion == catpb.RegionName(n.Region) {
		removingPrimaryRegion = true

		typeID, err := dbDesc.MultiRegionEnumID()
		if err != nil {
			return nil, err
		}
		typeDesc, err := p.Descriptors().MutableByID(p.txn).Type(ctx, typeID)
		if err != nil {
			return nil, err
		}
		regionEnumDesc := typeDesc.AsRegionEnumTypeDescriptor()
		if regionEnumDesc == nil {
			return nil, errors.AssertionFailedf(
				"expected region enum type, not %s for type %q (%d)",
				typeDesc.GetKind(), typeDesc.GetName(), typeDesc.GetID())
		}

		// Ensure that there's only 1 region on the multi-region enum (the primary
		// region) as this can only be dropped once all other regions have been
		// dropped. This check must account for regions that are transitioning,
		// as transitions aren't guaranteed to succeed.
		var regions catpb.RegionNames
		_ = regionEnumDesc.ForEachRegion(func(name catpb.RegionName, _ descpb.TypeDescriptor_EnumMember_Direction) error {
			regions = append(regions, name)
			return nil
		})
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

		isSystemDatabase := dbDesc.ID == keys.SystemDatabaseID
		if allowDrop := allowDropFinalRegion.Get(&p.execCfg.Settings.SV); !allowDrop ||
			// The system database, once it's been made multi-region, must not be
			// allowed to go back.
			isSystemDatabase {
			return nil, pgerror.Newf(
				pgcode.InvalidDatabaseDefinition,
				"databases in this cluster must have at least 1 region",
				n.Region,
				sqlclustersettings.DefaultPrimaryRegionClusterSettingName,
			)
		}

		// When the last region is removed from the database, we also clean up
		// detritus multi-region type descriptor. This includes both the
		// type descriptor and its array counterpart.
		toDrop = append(toDrop, typeDesc)
		arrayTypeDesc, err := p.Descriptors().MutableByID(p.txn).Type(ctx, typeDesc.ArrayTypeID)
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

	// If this is the system database, we can only drop a region if it is
	// not a part of any other database.
	if isSystemDatabase := dbDesc.ID == keys.SystemDatabaseID; isSystemDatabase {
		if err := p.checkCanDropSystemDatabaseRegion(ctx, n.Region); err != nil {
			return nil, err
		}
		tablesToClean := []string{"sqlliveness", "lease", "sql_instances"}
		for _, t := range tablesToClean {
			livenessQuery := fmt.Sprintf(
				`SELECT count(*) > 0 FROM system.%s WHERE crdb_region = '%s' AND 
          crdb_internal.sql_liveness_is_alive(session_id)`, t, n.Region)
			row, err := p.QueryRowEx(ctx, "check-session-liveness-for-region",
				sessiondata.NodeUserSessionDataOverride, livenessQuery)
			if err != nil {
				return nil, err
			}
			// Block dropping n.Region if any associated session is active.
			if tree.MustBeDBool(row[0]) {
				return nil, errors.WithHintf(
					pgerror.Newf(
						pgcode.InvalidDatabaseDefinition,
						"cannot drop region %q",
						n.Region,
					),
					"You must not have any active sessions that are in this region. "+
						"Ensure that there no nodes that still belong to region %q", n.Region,
				)
			}
		}
		// For the region_liveness table, we can just safely remove the reference
		// (if any) of the dropping region from the table.
		if _, err := p.ExecEx(ctx, "remove-region-liveness-ref",
			sessiondata.NodeUserSessionDataOverride, `DELETE FROM system.region_liveness
			        WHERE crdb_region = $1`, n.Region); err != nil {
			return nil, err
		}
		if err := regionliveness.CleanupSystemTableForRegion(ctx, p.execCfg.Codec, n.Region.String(), p.txn); err != nil {
			return nil, err
		}
	}

	// Ensure survivability goal and number of regions after the drop jive.
	regionConfig, err := SynthesizeRegionConfig(ctx, p.txn, dbDesc.ID, p.Descriptors())
	if err != nil {
		return nil, err
	}
	if err := multiregion.CanDropRegion(catpb.RegionName(n.Region), regionConfig, dbDesc.GetID() == keys.SystemDatabaseID); err != nil {
		return nil, err
	}

	return &alterDatabaseDropRegionNode{
		n:                     n,
		desc:                  dbDesc,
		removingPrimaryRegion: removingPrimaryRegion,
		toDrop:                toDrop,
	}, nil
}

// checkCanDropSystemDatabaseRegion checks if region is in use in any database
// other than the system database. If it is, an error is returned.
func (p *planner) checkCanDropSystemDatabaseRegion(ctx context.Context, region tree.Name) error {
	dbs, err := p.Descriptors().GetAllDatabases(ctx, p.txn)
	if err != nil {
		return err
	}
	var typeIDsToFetchSet catalog.DescriptorIDSet
	if err := dbs.ForEachDescriptor(func(desc catalog.Descriptor) error {
		db, ok := desc.(catalog.DatabaseDescriptor)
		if !ok {
			return errors.WithDetailf(
				errors.AssertionFailedf(
					"got unexpected non-database %T while iterating databases",
					desc,
				),
				"unexpected descriptor: %v", desc)
		}
		if !db.IsMultiRegion() ||
			// We expect that this region is part of the system database.
			db.GetID() == keys.SystemDatabaseID {
			return nil
		}
		typeID, err := db.MultiRegionEnumID()
		if err != nil {
			return errors.NewAssertionErrorWithWrappedErrf(
				err, "expected multi-region enum ID for %s (%d)",
				db.GetName(), db.GetID())
		}
		typeIDsToFetchSet.Add(typeID)
		return nil
	}); err != nil {
		return err
	}
	typeIDsToFetch := typeIDsToFetchSet.Ordered()
	dbTypes, err := p.Descriptors().ByIDWithoutLeased(p.txn).Get().Descs(ctx, typeIDsToFetch)
	if err != nil {
		return errors.Wrapf(err, "failed to fetch multi-region enums while attempting to drop"+
			" system database region %s", &region)
	}

	enumHasMember := func(haystack catalog.EnumTypeDescriptor, needle string) bool {
		for i, n := 0, haystack.NumEnumMembers(); i < n; i++ {
			if haystack.GetMemberLogicalRepresentation(i) == needle {
				return true
			}
		}
		return false
	}
	var usedBy intsets.Fast
	for i, desc := range dbTypes {
		typDesc, ok := desc.(catalog.EnumTypeDescriptor)
		if !ok {
			return errors.AssertionFailedf(
				"found non-enum type with ID %d", typeIDsToFetch[i],
			)
		}
		if enumHasMember(typDesc, string(region)) {
			usedBy.Add(i)
		}
	}
	if !usedBy.Empty() {
		var databases tree.NameList
		usedBy.ForEach(func(i int) {
			databases = append(databases,
				tree.Name(dbs.LookupDescriptor(dbTypes[i].GetParentID()).GetName()))
		})
		sort.Slice(databases, func(i, j int) bool {
			return databases[i] < databases[j]
		})
		return errors.WithHintf(
			pgerror.Newf(pgcode.DependentObjectsStillExist,
				"cannot drop region %v from the system database while that region is still in use",
				&region,
			), "region is in use by databases: %v", &databases)
	}
	return nil
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
//
// For the system database, the conditions are more stringent. The user must
// be the node user, and this must be a secondary tenant.
func (p *planner) checkPrivilegesForMultiRegionOp(
	ctx context.Context, desc catalog.Descriptor,
) error {

	// Unless MultiRegionSystemDatabaseEnabled is true, only secondary tenants may
	// have their system database set up for multi-region operations. Even then,
	// ensure that only the node user may configure the system database.
	//
	// TODO(rafi): For now, we also allow root to perform the various operations
	// to enable testing. When MultiRegionSystemDatabaseEnabled leaves preview,
	// we may want to allow any admin user to perform these operations as well.
	if desc.GetID() == keys.SystemDatabaseID {
		if multiRegionSystemDatabase := sqlclustersettings.MultiRegionSystemDatabaseEnabled.Get(&p.execCfg.Settings.SV); !multiRegionSystemDatabase &&
			p.execCfg.Codec.ForSystemTenant() {
			return pgerror.Newf(
				pgcode.FeatureNotSupported,
				"modifying the regions of system database is not supported",
			)
		}
		if u := p.SessionData().User(); !u.IsNodeUser() && !u.IsRootUser() {
			return pgerror.Newf(
				pgcode.InsufficientPrivilege,
				"user %s may not modify the system database",
				u,
			)
		}
	}

	hasAdminRole, err := p.HasAdminRole(ctx)
	if err != nil {
		return err
	}
	if !hasAdminRole {
		// TODO(arul): It's worth noting CREATE isn't a thing on tables in postgres,
		// so this will require some changes when (if) we move our privilege system
		// to be more in line with postgres.
		hasPriv, err := p.HasPrivilege(ctx, desc, privilege.CREATE, p.User())
		if err != nil {
			return err
		}
		// Wrap an insufficient privileges error a bit better to reflect the lack
		// of ownership as well.
		if !hasPriv {
			return pgerror.Newf(pgcode.InsufficientPrivilege,
				"user %s must be owner of %s or have %s privilege on %s %s",
				p.SessionData().User(),
				desc.GetName(),
				privilege.CREATE,
				desc.DescriptorType(),
				desc.GetName(),
			)
		}
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
			case *catpb.LocalityConfig_Global_:
				if err := ApplyZoneConfigForMultiRegionTable(
					ctx,
					p.InternalSQLTxn(),
					p.ExecCfg(),
					p.extendedEvalCtx.Tracing.KVTracingEnabled(),
					multiregion.RegionConfig{}, // pass dummy config as it is not used.
					tbDesc,
					applyZoneConfigForMultiRegionTableOptionRemoveGlobalZoneConfig,
				); err != nil {
					return err
				}
			case *catpb.LocalityConfig_RegionalByTable_:
				if t.RegionalByTable.Region != nil {
					// This should error during the type descriptor changes.
					return errors.AssertionFailedf(
						"unexpected REGIONAL BY TABLE IN <region> on table %s during DROP REGION",
						tbDesc.Name,
					)
				}
			case *catpb.LocalityConfig_RegionalByRow_:
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

	// Validate if the existing zone config of this descriptor is sane,
	// since otherwise, any modifications for multi-region could fail during
	// the job phase.
	if err := params.p.validateExistingZoneCfg(params.ctx, n.desc.ID); err != nil {
		return err
	}

	typeDesc, err := params.p.Descriptors().MutableByID(params.p.txn).Type(params.ctx, n.desc.RegionConfig.RegionEnumID)
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
			params.p.InternalSQLTxn(),
			params.p.execCfg,
			params.extendedEvalCtx.Tracing.KVTracingEnabled(),
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
	zeroInputPlanNode
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

	dbDesc, err := p.Descriptors().MutableByName(p.txn).Database(ctx, string(n.Name))
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
	if !prevRegionConfig.Regions().Contains(catpb.RegionName(n.n.PrimaryRegion)) &&
		!prevRegionConfig.AddingRegions().Contains(catpb.RegionName(n.n.PrimaryRegion)) {
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

	if prevRegionConfig.HasSecondaryRegion() && catpb.RegionName(n.n.PrimaryRegion) == prevRegionConfig.SecondaryRegion() {
		return errors.WithHintf(
			pgerror.Newf(pgcode.InvalidDatabaseDefinition,
				"region %s is currently the secondary region",
				n.n.PrimaryRegion.String(),
			),
			"you must drop the secondary region using ALTER DATABASE %s DROP SECONDARY REGION",
			n.n.Name.String(),
		)
	}

	// Get the type descriptor for the multi-region enum.
	typeDesc, err := params.p.Descriptors().MutableByID(params.p.txn).Type(params.ctx, n.desc.RegionConfig.RegionEnumID)
	if err != nil {
		return err
	}

	oldPrimaryRegion := n.desc.RegionConfig.PrimaryRegion

	// To update the primary region we need to modify the database descriptor,
	// update the multi-region enum, and write a new zone configuration.
	n.desc.RegionConfig.PrimaryRegion = catpb.RegionName(n.n.PrimaryRegion)
	if err := params.p.writeNonDropDatabaseChange(
		params.ctx,
		n.desc,
		tree.AsStringWithFQNames(n.n, params.Ann()),
	); err != nil {
		return err
	}

	// Update the primary region in the type descriptor, and write it back out.
	typeDesc.RegionConfig.PrimaryRegion = catpb.RegionName(n.n.PrimaryRegion)
	if err := params.p.writeTypeDesc(params.ctx, typeDesc); err != nil {
		return err
	}

	updatedRegionConfig, err := SynthesizeRegionConfig(
		params.ctx, params.p.txn, n.desc.ID, params.p.Descriptors(),
	)
	if err != nil {
		return err
	}

	// Validate the final zone config at the end of the transaction, since
	// we will not be validating localities right now.
	*params.extendedEvalCtx.validateDbZoneConfig = true

	// Update the database's zone configuration.
	if err := ApplyZoneConfigFromDatabaseRegionConfig(
		params.ctx,
		n.desc.ID,
		updatedRegionConfig,
		params.p.InternalSQLTxn(),
		params.p.execCfg,
		false, /*validateLocalities*/
		params.extendedEvalCtx.Tracing.KVTracingEnabled(),
	); err != nil {
		return err
	}

	isNewPrimaryRegionMemberOfASuperRegion, superRegionOfNewPrimaryRegion := multiregion.IsMemberOfSuperRegion(catpb.RegionName(n.n.PrimaryRegion), updatedRegionConfig)
	isOldPrimaryRegionMemberOfASuperRegion, superRegionOfOldPrimaryRegion := multiregion.IsMemberOfSuperRegion(oldPrimaryRegion, updatedRegionConfig)

	if (isNewPrimaryRegionMemberOfASuperRegion || isOldPrimaryRegionMemberOfASuperRegion) &&
		superRegionOfNewPrimaryRegion != superRegionOfOldPrimaryRegion {
		newSuperRegionMsg := ""
		if isNewPrimaryRegionMemberOfASuperRegion {
			newSuperRegionMsg = fmt.Sprintf("\nthe new primary region %s is a member of super region %s", n.n.PrimaryRegion, superRegionOfNewPrimaryRegion)
		}

		oldSuperRegionMsg := ""
		if isOldPrimaryRegionMemberOfASuperRegion {
			oldSuperRegionMsg = fmt.Sprintf("\nthe old primary region %s is a member of super region %s", oldPrimaryRegion, superRegionOfOldPrimaryRegion)
		}

		if !params.p.SessionData().OverrideAlterPrimaryRegionInSuperRegion {
			return errors.WithTelemetry(
				errors.WithHint(errors.Newf("moving the primary region into or "+
					"out of a super region could have undesirable data placement "+
					"implications as all regional tables in the primary region will now moved.\n%s%s", newSuperRegionMsg, oldSuperRegionMsg),
					"You can enable this operation by running `SET alter_primary_region_super_region_override = 'on'`."),
				"sql.schema.alter_primary_region_in_super_region",
			)
		}
	}

	if isNewPrimaryRegionMemberOfASuperRegion {
		params.p.BufferClientNotice(params.ctx, pgnotice.Newf("all REGIONAL BY TABLE tables without a region explicitly set are now part of the super region %s", superRegionOfNewPrimaryRegion))
	}

	if isOldPrimaryRegionMemberOfASuperRegion {
		params.p.BufferClientNotice(params.ctx, pgnotice.Newf("all REGIONAL BY TABLE tables without a region explicitly set are no longer part of the super region %s", superRegionOfOldPrimaryRegion))
	}

	// If the old or new primary region is a member of a super region, we also
	// have to update regional tables that are part of the default region.
	opts := WithOnlyGlobalTables
	if isNewPrimaryRegionMemberOfASuperRegion || isOldPrimaryRegionMemberOfASuperRegion {
		opts = WithOnlyRegionalTablesAndGlobalTables
	}

	// Update all GLOBAL tables' zone configurations. This is required as if
	// LOCALITY GLOBAL is used with PLACEMENT RESTRICTED, the global tables' zone
	// configs must be explicitly rebuilt so as to move their primary region.
	// If there are super regions defined on the database, we may have to update
	// the zone config for regional tables.
	if updatedRegionConfig.IsPlacementRestricted() || isNewPrimaryRegionMemberOfASuperRegion || isOldPrimaryRegionMemberOfASuperRegion {
		if err := params.p.refreshZoneConfigsForTables(
			params.ctx,
			n.desc,
			opts,
		); err != nil {
			return err
		}
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
	if tableDesc.GetPrimaryIndex().PartitioningColumnCount() > 0 {
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
		if idx.PartitioningColumnCount() > 0 {
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
		tree.DataPlacementUnspecified,
		tree.SecondaryRegionNotSpecifiedName,
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

	// If this is the system database, we need to do some additional magic to
	// reconfigure the various tables and set the correct localities.
	if n.desc.GetID() == keys.SystemDatabaseID {
		if err := params.p.optimizeSystemDatabase(params.ctx); err != nil {
			return err
		}
	}
	return nil
}

func (n *alterDatabasePrimaryRegionNode) startExec(params runParams) error {

	// Validate if the existing zone config of this descriptor is sane,
	// since otherwise, any modifications for multi-region could fail during
	// the job phase.
	if err := params.p.validateExistingZoneCfg(params.ctx, n.desc.ID); err != nil {
		return err
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
	zeroInputPlanNode
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

	dbDesc, err := p.Descriptors().MutableByName(p.txn).Database(ctx, string(n.Name))
	if err != nil {
		return nil, err
	}
	if err := p.checkPrivilegesForMultiRegionOp(ctx, dbDesc); err != nil {
		return nil, err
	}

	return &alterDatabaseSurvivalGoalNode{n: n, desc: dbDesc}, nil
}

func (n *alterDatabaseSurvivalGoalNode) startExec(params runParams) error {
	return params.p.alterDatabaseSurvivalGoal(
		params.ctx, n.desc, n.n.SurvivalGoal, tree.AsStringWithFQNames(n.n, params.Ann()),
	)
}

// alterDatabaseSurvivalGoal modifies a multi-region database's survival goal,
// it could potentially cause a change to the system database's survival goal.
func (p *planner) alterDatabaseSurvivalGoal(
	ctx context.Context, db *dbdesc.Mutable, targetSurvivalGoal tree.SurvivalGoal, jobDesc string,
) error {
	// If the database is not a multi-region database, the survival goal cannot be changed.
	if !db.IsMultiRegion() {
		return errors.WithHintf(
			pgerror.New(pgcode.InvalidName,
				"database must have associated regions before a survival goal can be set",
			),
			"you must first add a primary region to the database using "+
				"ALTER DATABASE %s PRIMARY REGION <region_name>",
			db.GetName(),
		)
	}

	if targetSurvivalGoal == tree.SurvivalGoalRegionFailure &&
		db.RegionConfig.Placement == descpb.DataPlacement_RESTRICTED {
		return errors.WithDetailf(
			pgerror.New(pgcode.InvalidParameterValue,
				"a region-survivable database cannot also have a restricted placement policy"),
			"PLACEMENT RESTRICTED may only be used with SURVIVE ZONE FAILURE",
		)
	}

	if targetSurvivalGoal == tree.SurvivalGoalRegionFailure {
		superRegions, err := p.getSuperRegionsForDatabase(ctx, db)
		if err != nil {
			return err
		}
		for _, sr := range superRegions {
			if err := multiregion.CanSatisfySurvivalGoal(descpb.SurvivalGoal_REGION_FAILURE, len(sr.Regions)); err != nil {
				return errors.Wrapf(err, "super region %s only has %d region(s)", sr.SuperRegionName, len(sr.Regions))
			}
		}
	}

	if err := p.validateZoneConfigForMultiRegionDatabaseWasNotModifiedByUser(
		ctx,
		db,
	); err != nil {
		return err
	}

	telemetry.Inc(
		sqltelemetry.AlterDatabaseSurvivalGoalCounter(
			targetSurvivalGoal.TelemetryName(),
		),
	)

	// Update the survival goal in the database descriptor
	survivalGoal, err := TranslateSurvivalGoal(targetSurvivalGoal)
	if err != nil {
		return err
	}
	db.RegionConfig.SurvivalGoal = survivalGoal

	if err := p.writeNonDropDatabaseChange(ctx, db, jobDesc); err != nil {
		return err
	}

	regionConfig, err := SynthesizeRegionConfig(ctx, p.txn, db.ID, p.Descriptors())
	if err != nil {
		return err
	}

	// Validate the final zone config at the end of the transaction, since
	// we will not be validating localities right now.
	*p.extendedEvalCtx.validateDbZoneConfig = true
	// Update the database's zone configuration.
	if err := ApplyZoneConfigFromDatabaseRegionConfig(
		ctx,
		db.ID,
		regionConfig,
		p.InternalSQLTxn(),
		p.execCfg,
		false, /* validateLocalities */
		p.extendedEvalCtx.Tracing.KVTracingEnabled(),
	); err != nil {
		return err
	}

	// Update all REGIONAL BY TABLE tables' zone configurations. This is required as replica
	// placement for REGIONAL BY TABLE tables is dependant on the survival goal.
	if err := p.refreshZoneConfigsForTables(ctx, db); err != nil {
		return err
	}

	// Log Alter Database Survival Goal event. This is an auditable log event and
	// is recorded in the same transaction as the database descriptor, and zone
	// configuration updates.
	return p.logEvent(ctx,
		db.GetID(),
		&eventpb.AlterDatabaseSurvivalGoal{
			DatabaseName: db.GetName(),
			SurvivalGoal: survivalGoal.String(),
		},
	)
}

func (n *alterDatabaseSurvivalGoalNode) Next(runParams) (bool, error) { return false, nil }
func (n *alterDatabaseSurvivalGoalNode) Values() tree.Datums          { return tree.Datums{} }
func (n *alterDatabaseSurvivalGoalNode) Close(context.Context)        {}

type alterDatabasePlacementNode struct {
	zeroInputPlanNode
	n    *tree.AlterDatabasePlacement
	desc *dbdesc.Mutable
}

// AlterDatabasePlacement transforms a tree.AlterDatabasePlacement into a plan node.
func (p *planner) AlterDatabasePlacement(
	ctx context.Context, n *tree.AlterDatabasePlacement,
) (planNode, error) {
	if err := checkSchemaChangeEnabled(
		ctx,
		p.ExecCfg(),
		"ALTER DATABASE",
	); err != nil {
		return nil, err
	}

	if !p.EvalContext().SessionData().PlacementEnabled {
		return nil, errors.WithHint(pgerror.New(
			pgcode.ExperimentalFeature,
			"ALTER DATABASE PLACEMENT requires that the session setting "+
				"enable_multiregion_placement_policy is enabled",
		),
			"to enable, enable the session setting or the cluster "+
				"setting sql.defaults.multiregion_placement_policy.enabled",
		)
	}

	dbDesc, err := p.Descriptors().MutableByName(p.txn).Database(ctx, string(n.Name))
	if err != nil {
		return nil, err
	}
	if err := p.checkPrivilegesForMultiRegionOp(ctx, dbDesc); err != nil {
		return nil, err
	}

	return &alterDatabasePlacementNode{n: n, desc: dbDesc}, nil
}

func (n *alterDatabasePlacementNode) startExec(params runParams) error {
	// If the database is not a multi-region database, the survival goal cannot be changed.
	if !n.desc.IsMultiRegion() {
		return errors.WithHintf(
			pgerror.New(pgcode.InvalidName,
				"database must have associated regions before a placement policy can be set",
			),
			"you must first add a primary region to the database using "+
				"ALTER DATABASE %s PRIMARY REGION <region_name>",
			n.n.Name.String(),
		)
	}

	if n.n.Placement == tree.DataPlacementRestricted &&
		n.desc.RegionConfig.SurvivalGoal == descpb.SurvivalGoal_REGION_FAILURE {
		return errors.WithDetailf(
			pgerror.New(pgcode.InvalidParameterValue,
				"a region-survivable database cannot also have a restricted placement policy"),
			"PLACEMENT RESTRICTED may only be used with SURVIVE ZONE FAILURE",
		)
	}

	if err := params.p.validateZoneConfigForMultiRegionDatabaseWasNotModifiedByUser(
		params.ctx,
		n.desc,
	); err != nil {
		return err
	}

	telemetry.Inc(
		sqltelemetry.AlterDatabasePlacementCounter(
			n.n.Placement.TelemetryName(),
		),
	)

	// Update the placement strategy in the database descriptor
	newPlacement, err := TranslateDataPlacement(n.n.Placement)
	if err != nil {
		return err
	}
	n.desc.SetPlacement(newPlacement)

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

	// Validate the final zone config at the end of the transaction, since
	// we will not be validating localities right now.
	*params.extendedEvalCtx.validateDbZoneConfig = true
	// Update the database's zone configuration.
	if err := ApplyZoneConfigFromDatabaseRegionConfig(
		params.ctx,
		n.desc.ID,
		regionConfig,
		params.p.InternalSQLTxn(),
		params.p.execCfg,
		false, /* validateLocalities */
		params.extendedEvalCtx.Tracing.KVTracingEnabled(),
	); err != nil {
		return err
	}

	// Update all GLOBAL tables' zone configurations. This is required because
	// GLOBAL table's can inherit the database's zone configuration under the
	// DEFAULT placement policy. However, under the RESTRICTED placement policy,
	// non-voters are removed from the database. But global tables need non-voters
	// to behave properly, and as such, need a bespoke zone configuration.
	// Regardless of the transition direction (DEFAULT -> RESTRICTED, RESTRICTED
	// -> DEFAULT), we need to refresh the zone configuration of all GLOBAL
	// table's inside the database to either carry a bespoke configuration or go
	// back to inheriting it from the database.
	if err := params.p.refreshZoneConfigsForTables(
		params.ctx,
		n.desc,
		WithOnlyGlobalTables,
	); err != nil {
		return err
	}

	// Log Alter Placement Goal event. This is an auditable log event and
	// is recorded in the same transaction as the database descriptor, and zone
	// configuration updates.
	return params.p.logEvent(params.ctx,
		n.desc.GetID(),
		&eventpb.AlterDatabasePlacement{
			DatabaseName: n.desc.GetName(),
			Placement:    newPlacement.String(),
		},
	)
}

func (n *alterDatabasePlacementNode) Next(runParams) (bool, error) { return false, nil }
func (n *alterDatabasePlacementNode) Values() tree.Datums          { return tree.Datums{} }
func (n *alterDatabasePlacementNode) Close(context.Context)        {}

type alterDatabaseAddSuperRegion struct {
	zeroInputPlanNode
	n    *tree.AlterDatabaseAddSuperRegion
	desc *dbdesc.Mutable
}

func (p *planner) AlterDatabaseAddSuperRegion(
	ctx context.Context, n *tree.AlterDatabaseAddSuperRegion,
) (planNode, error) {
	if err := p.isSuperRegionEnabled(); err != nil {
		return nil, err
	}

	if err := checkSchemaChangeEnabled(
		ctx,
		p.ExecCfg(),
		"ALTER DATABASE",
	); err != nil {
		return nil, err
	}

	// Validate no duplicate regions exist.
	existingRegionNames := make(map[tree.Name]struct{})
	for _, region := range n.Regions {
		if _, found := existingRegionNames[region]; found {
			return nil, pgerror.Newf(pgcode.DuplicateObject,
				"duplicate region %s found in super region %s", region, n.SuperRegionName)
		}
		existingRegionNames[region] = struct{}{}
	}

	dbDesc, err := p.Descriptors().MutableByName(p.txn).Database(ctx, string(n.DatabaseName))
	if err != nil {
		return nil, err
	}
	if err := p.checkPrivilegesForMultiRegionOp(ctx, dbDesc); err != nil {
		return nil, err
	}

	return &alterDatabaseAddSuperRegion{n: n, desc: dbDesc}, nil
}

func (n *alterDatabaseAddSuperRegion) startExec(params runParams) error {
	// If the database is not a multi-region database, a super region cannot
	// be added.
	if !n.desc.IsMultiRegion() {
		return errors.WithHintf(
			pgerror.New(pgcode.InvalidName,
				"database must be multi-region to support super regions",
			),
			"you must first add a primary region to the database using "+
				"ALTER DATABASE %s PRIMARY REGION <region_name>",
			n.n.DatabaseName.String(),
		)
	}

	// Check if the primary and secondary regions are members of this super region.
	regionConfig, err := SynthesizeRegionConfig(
		params.ctx, params.p.txn, n.desc.ID, params.p.Descriptors(),
	)
	if err != nil {
		return err
	}

	if regionConfig.HasSecondaryRegion() {
		primaryRegion := regionConfig.PrimaryRegionString()
		regions := nameToRegionName(n.n.Regions)
		err := validateSecondaryRegion(catpb.RegionName(primaryRegion), regionConfig.SecondaryRegion(), regions)
		if err != nil {
			return err
		}
	}

	typeID, err := n.desc.MultiRegionEnumID()
	if err != nil {
		return err
	}
	typeDesc, err := params.p.Descriptors().MutableByID(params.p.txn).Type(params.ctx, typeID)
	if err != nil {
		return err
	}
	return params.p.addSuperRegion(params.ctx, n.desc, typeDesc, n.n.Regions, n.n.SuperRegionName, tree.AsStringWithFQNames(n.n, params.Ann()))
}

// addSuperRegion adds the super region in sorted order based on the
// name of super region.
func addSuperRegion(r *descpb.TypeDescriptor_RegionConfig, superRegion descpb.SuperRegion) {
	idx := sort.Search(len(r.SuperRegions), func(i int) bool {
		return !(r.SuperRegions[i].SuperRegionName < superRegion.SuperRegionName)
	})
	if idx == len(r.SuperRegions) {
		// Not found but should be inserted at the end.
		r.SuperRegions = append(r.SuperRegions, superRegion)
	} else {
		// New element to be inserted at idx.
		r.SuperRegions = append(r.SuperRegions, descpb.SuperRegion{})
		copy(r.SuperRegions[idx+1:], r.SuperRegions[idx:])
		r.SuperRegions[idx] = superRegion
	}
}

func (n *alterDatabaseAddSuperRegion) Next(runParams) (bool, error) { return false, nil }
func (n *alterDatabaseAddSuperRegion) Values() tree.Datums          { return tree.Datums{} }
func (n *alterDatabaseAddSuperRegion) Close(context.Context)        {}

type alterDatabaseDropSuperRegion struct {
	zeroInputPlanNode
	n    *tree.AlterDatabaseDropSuperRegion
	desc *dbdesc.Mutable
}

func (p *planner) AlterDatabaseDropSuperRegion(
	ctx context.Context, n *tree.AlterDatabaseDropSuperRegion,
) (planNode, error) {
	if err := p.isSuperRegionEnabled(); err != nil {
		return nil, err
	}

	if err := checkSchemaChangeEnabled(
		ctx,
		p.ExecCfg(),
		"ALTER DATABASE",
	); err != nil {
		return nil, err
	}

	dbDesc, err := p.Descriptors().MutableByName(p.txn).Database(ctx, string(n.DatabaseName))
	if err != nil {
		return nil, err
	}
	if err := p.checkPrivilegesForMultiRegionOp(ctx, dbDesc); err != nil {
		return nil, err
	}

	return &alterDatabaseDropSuperRegion{n: n, desc: dbDesc}, nil
}

func (n *alterDatabaseDropSuperRegion) startExec(params runParams) error {
	// If the database is not a multi-region database, there should not be any
	// super regions.
	if !n.desc.IsMultiRegion() {
		return errors.WithHintf(
			pgerror.New(pgcode.InvalidName,
				"database must be multi-region to support super regions",
			),
			"you must first add a primary region to the database using "+
				"ALTER DATABASE %s PRIMARY REGION <region_name>",
			n.n.DatabaseName.String(),
		)
	}

	typeID, err := n.desc.MultiRegionEnumID()
	if err != nil {
		return err
	}
	typeDesc, err := params.p.Descriptors().MutableByID(params.p.txn).Type(params.ctx, typeID)
	if err != nil {
		return err
	}

	dropped := removeSuperRegion(typeDesc.RegionConfig, n.n.SuperRegionName)

	if !dropped {
		return errors.Newf("super region %s not found", n.n.SuperRegionName)
	}

	if err := params.p.writeTypeSchemaChange(params.ctx, typeDesc, tree.AsStringWithFQNames(n.n, params.Ann())); err != nil {
		return err
	}

	// Update all regional and regional by row tables.
	if err := params.p.refreshZoneConfigsForTables(
		params.ctx,
		n.desc,
	); err != nil {
		return err
	}

	return nil
}

func removeSuperRegion(
	r *descpb.TypeDescriptor_RegionConfig, superRegionName tree.Name,
) (dropped bool) {
	for i, superRegion := range r.SuperRegions {
		if superRegion.SuperRegionName == string(superRegionName) {
			r.SuperRegions = append(r.SuperRegions[:i], r.SuperRegions[i+1:]...)
			dropped = true
			break
		}
	}

	return dropped
}

func (n *alterDatabaseDropSuperRegion) Next(runParams) (bool, error) { return false, nil }
func (n *alterDatabaseDropSuperRegion) Values() tree.Datums          { return tree.Datums{} }
func (n *alterDatabaseDropSuperRegion) Close(context.Context)        {}

func (p *planner) isSuperRegionEnabled() error {
	if !p.SessionData().EnableSuperRegions {
		return errors.WithTelemetry(
			pgerror.WithCandidateCode(
				errors.WithHint(errors.New("super regions are only supported experimentally"),
					"You can enable super regions by running `SET enable_super_regions = 'on'`."),
				pgcode.ExperimentalFeature),
			"sql.schema.super_regions_disabled",
		)
	}

	return nil
}

func (p *planner) getSuperRegionsForDatabase(
	ctx context.Context, desc catalog.DatabaseDescriptor,
) ([]descpb.SuperRegion, error) {
	typeID, err := desc.MultiRegionEnumID()
	if err != nil {
		return nil, err
	}
	typeDesc, err := p.Descriptors().MutableByID(p.txn).Type(ctx, typeID)
	if err != nil {
		return nil, err
	}

	return typeDesc.RegionConfig.SuperRegions, nil
}

type alterDatabaseAlterSuperRegion struct {
	zeroInputPlanNode
	n    *tree.AlterDatabaseAlterSuperRegion
	desc *dbdesc.Mutable
}

func (n *alterDatabaseAlterSuperRegion) Next(runParams) (bool, error) { return false, nil }
func (n *alterDatabaseAlterSuperRegion) Values() tree.Datums          { return tree.Datums{} }
func (n *alterDatabaseAlterSuperRegion) Close(context.Context)        {}

func (p *planner) AlterDatabaseAlterSuperRegion(
	ctx context.Context, n *tree.AlterDatabaseAlterSuperRegion,
) (planNode, error) {
	if err := p.isSuperRegionEnabled(); err != nil {
		return nil, err
	}

	if err := checkSchemaChangeEnabled(
		ctx,
		p.ExecCfg(),
		"ALTER DATABASE",
	); err != nil {
		return nil, err
	}

	dbDesc, err := p.Descriptors().MutableByName(p.txn).Database(ctx, string(n.DatabaseName))
	if err != nil {
		return nil, err
	}
	if err := p.checkPrivilegesForMultiRegionOp(ctx, dbDesc); err != nil {
		return nil, err
	}

	return &alterDatabaseAlterSuperRegion{n: n, desc: dbDesc}, nil
}

func (n *alterDatabaseAlterSuperRegion) startExec(params runParams) error {
	// If the database is not a multi-region database, a super region cannot
	// be added.
	if !n.desc.IsMultiRegion() {
		return errors.WithHintf(
			pgerror.New(pgcode.InvalidName,
				"database must be multi-region to support super regions",
			),
			"you must first add a primary region to the database using "+
				"ALTER DATABASE %s PRIMARY REGION <region_name>",
			n.n.DatabaseName.String(),
		)
	}

	typeID, err := n.desc.MultiRegionEnumID()
	if err != nil {
		return err
	}
	typeDesc, err := params.p.Descriptors().MutableByID(params.p.txn).Type(params.ctx, typeID)
	if err != nil {
		return err
	}

	// Check that the secondary region isn't being dropped from this super region.
	regionConfig, err := SynthesizeRegionConfig(
		params.ctx, params.p.txn, n.desc.ID, params.p.Descriptors(),
	)
	if err != nil {
		return err
	}

	if regionConfig.HasSecondaryRegion() {
		regions := nameToRegionName(n.n.Regions)
		err = validateSecondaryRegion(regionConfig.PrimaryRegion(), typeDesc.RegionConfig.SecondaryRegion, regions)
		if err != nil {
			return err
		}
	}

	// Remove the old super region.
	dropped := removeSuperRegion(typeDesc.RegionConfig, n.n.SuperRegionName)
	if !dropped {
		return errors.WithHint(pgerror.Newf(pgcode.UndefinedObject,
			"super region %q of database %q does not exist", n.n.SuperRegionName, n.n.DatabaseName),
			"super region must be added before it can be altered")
	}

	// Validate that adding the super region with the new regions is valid.
	return params.p.addSuperRegion(params.ctx, n.desc, typeDesc, n.n.Regions, n.n.SuperRegionName, tree.AsStringWithFQNames(n.n, params.Ann()))

}

func (p *planner) addSuperRegion(
	ctx context.Context,
	desc *dbdesc.Mutable,
	typeDesc *typedesc.Mutable,
	regionList []tree.Name,
	superRegionName tree.Name,
	op string,
) error {
	regionsDesc := typeDesc.AsRegionEnumTypeDescriptor()
	if regionsDesc == nil {
		return errors.AssertionFailedf("expected region enum type, not %s for type %q (%d)",
			typeDesc.GetKind(), typeDesc.GetName(), typeDesc.GetID())
	}

	var regionNames catpb.RegionNames
	_ = regionsDesc.ForEachPublicRegion(func(name catpb.RegionName) error {
		regionNames = append(regionNames, name)
		return nil
	})

	regionsInDatabase := make(map[catpb.RegionName]struct{})
	for _, regionName := range regionNames {
		regionsInDatabase[regionName] = struct{}{}
	}

	regionSet := make(map[catpb.RegionName]struct{})
	regions := make([]catpb.RegionName, len(regionList))

	// Check that the region is part of the database.
	// And create a slice of the regions in the super region.
	for i, region := range regionList {
		_, found := regionsInDatabase[catpb.RegionName(region)]
		if !found {
			return errors.Newf("region %s not part of database", region)
		}

		regionSet[catpb.RegionName(region)] = struct{}{}
		regions[i] = catpb.RegionName(region)
	}

	if err := multiregion.CanSatisfySurvivalGoal(desc.RegionConfig.SurvivalGoal, len(regionList)); err != nil {
		return errors.Wrapf(err, "super region %s only has %d region(s)", superRegionName, len(regionList))
	}

	sort.Slice(regions, func(i, j int) bool {
		return regions[i] < regions[j]
	})

	// Ensure that the super region name is not already used and that
	// the super regions don't overlap.
	if err := regionsDesc.ForEachSuperRegion(func(superRegion string) error {
		if superRegion == string(superRegionName) {
			return errors.Newf("super region %s already exists", superRegion)
		}
		return regionsDesc.ForEachRegionInSuperRegion(superRegion, func(region catpb.RegionName) error {
			if _, found := regionSet[region]; found {
				return errors.Newf("region %s is already part of super region %s", region, superRegion)
			}
			return nil
		})
	}); err != nil {
		return err
	}

	addSuperRegion(typeDesc.RegionConfig, descpb.SuperRegion{
		SuperRegionName: string(superRegionName),
		Regions:         regions,
	})

	if err := p.writeTypeSchemaChange(ctx, typeDesc, op); err != nil {
		return err
	}

	// Update all regional and regional by row tables.
	return p.refreshZoneConfigsForTables(
		ctx,
		desc,
	)
}

type alterDatabaseSecondaryRegion struct {
	zeroInputPlanNode
	n    *tree.AlterDatabaseSecondaryRegion
	desc *dbdesc.Mutable
}

func (p *planner) AlterDatabaseSecondaryRegion(
	ctx context.Context, n *tree.AlterDatabaseSecondaryRegion,
) (planNode, error) {
	if err := checkSchemaChangeEnabled(
		ctx,
		p.ExecCfg(),
		"ALTER DATABASE",
	); err != nil {
		return nil, err
	}

	dbDesc, err := p.Descriptors().MutableByName(p.txn).Database(ctx, string(n.DatabaseName))
	if err != nil {
		return nil, err
	}
	if err := p.checkPrivilegesForMultiRegionOp(ctx, dbDesc); err != nil {
		return nil, err
	}

	return &alterDatabaseSecondaryRegion{n: n, desc: dbDesc}, nil
}

func (n *alterDatabaseSecondaryRegion) startExec(params runParams) error {
	if !n.desc.IsMultiRegion() {
		return errors.WithHintf(
			pgerror.New(pgcode.InvalidDatabaseDefinition,
				"database must be multi-region to support a secondary region",
			),
			"you must first add a primary region to the database using "+
				"ALTER DATABASE %s PRIMARY REGION <region_name>",
			n.n.DatabaseName.String(),
		)
	}

	// Validate if the existing zone config of this descriptor is sane,
	// since otherwise, any modifications for multi-region could fail during
	// the job phase.
	if err := params.p.validateExistingZoneCfg(params.ctx, n.desc.ID); err != nil {
		return err
	}

	// Verify that the secondary region is part of the region list.
	prevRegionConfig, err := SynthesizeRegionConfig(params.ctx, params.p.txn, n.desc.ID, params.p.Descriptors())
	if err != nil {
		return err
	}

	// If we're setting the secondary region to the current secondary region,
	// there is nothing to be done.
	if prevRegionConfig.SecondaryRegion() == catpb.RegionName(n.n.SecondaryRegion) {
		return nil
	}

	if !prevRegionConfig.Regions().Contains(catpb.RegionName(n.n.SecondaryRegion)) &&
		!prevRegionConfig.AddingRegions().Contains(catpb.RegionName(n.n.SecondaryRegion)) {
		return errors.WithHintf(
			pgerror.Newf(pgcode.InvalidDatabaseDefinition,
				"region %s has not been added to the database",
				n.n.SecondaryRegion.String(),
			),
			"you must add the region to the database before setting it as secondary region, using "+
				"ALTER DATABASE %s ADD REGION %s",
			n.n.DatabaseName.String(),
			n.n.SecondaryRegion.String(),
		)
	}

	// The secondary region cannot be the current primary region.
	if prevRegionConfig.PrimaryRegion() == catpb.RegionName(n.n.SecondaryRegion) {
		return pgerror.New(pgcode.InvalidDatabaseDefinition,
			"the secondary region cannot be the same as the current primary region",
		)
	}

	regions, ok := prevRegionConfig.GetSuperRegionRegionsForRegion(prevRegionConfig.PrimaryRegion())
	if !ok && prevRegionConfig.IsMemberOfExplicitSuperRegion(catpb.RegionName(n.n.SecondaryRegion)) {
		return pgerror.New(pgcode.InvalidDatabaseDefinition,
			"the secondary region can not be in a super region, unless the primary is also "+
				"within a super region",
		)
	}
	err = validateSecondaryRegion(prevRegionConfig.PrimaryRegion(), catpb.RegionName(n.n.SecondaryRegion), regions)
	if err != nil {
		return err
	}

	// Get the type descriptor for the multi-region enum.
	typeDesc, err := params.p.Descriptors().MutableByID(params.p.txn).Type(params.ctx, n.desc.RegionConfig.RegionEnumID)
	if err != nil {
		return err
	}

	// To update the secondary region we need to modify the database descriptor,
	// update the multi-region enum, and write a new zone configuration.
	n.desc.RegionConfig.SecondaryRegion = catpb.RegionName(n.n.SecondaryRegion)
	if err := params.p.writeNonDropDatabaseChange(
		params.ctx,
		n.desc,
		tree.AsStringWithFQNames(n.n, params.Ann()),
	); err != nil {
		return err
	}

	// Update the secondary region in the type descriptor, and write it back out.
	typeDesc.RegionConfig.SecondaryRegion = catpb.RegionName(n.n.SecondaryRegion)
	if err := params.p.writeTypeDesc(params.ctx, typeDesc); err != nil {
		return err
	}

	updatedRegionConfig, err := SynthesizeRegionConfig(
		params.ctx, params.p.txn, n.desc.ID, params.p.Descriptors(),
	)
	if err != nil {
		return err
	}

	// Validate the final zone config at the end of the transaction, since
	// we will not be validating localities right now.
	*params.extendedEvalCtx.validateDbZoneConfig = true
	// Update the database's zone configuration.
	if err := ApplyZoneConfigFromDatabaseRegionConfig(
		params.ctx,
		n.desc.ID,
		updatedRegionConfig,
		params.p.InternalSQLTxn(),
		params.p.execCfg,
		false, /* validateLocalities */
		params.extendedEvalCtx.Tracing.KVTracingEnabled(),
	); err != nil {
		return err
	}

	// Update all regional and regional by row tables.
	return params.p.refreshZoneConfigsForTables(
		params.ctx,
		n.desc,
	)
}

func (n *alterDatabaseSecondaryRegion) Next(runParams) (bool, error) { return false, nil }
func (n *alterDatabaseSecondaryRegion) Values() tree.Datums          { return tree.Datums{} }
func (n *alterDatabaseSecondaryRegion) Close(context.Context)        {}

type alterDatabaseDropSecondaryRegion struct {
	zeroInputPlanNode
	n    *tree.AlterDatabaseDropSecondaryRegion
	desc *dbdesc.Mutable
}

func (p *planner) AlterDatabaseDropSecondaryRegion(
	ctx context.Context, n *tree.AlterDatabaseDropSecondaryRegion,
) (planNode, error) {

	if err := checkSchemaChangeEnabled(
		ctx,
		p.ExecCfg(),
		"ALTER DATABASE",
	); err != nil {
		return nil, err
	}

	dbDesc, err := p.Descriptors().MutableByName(p.txn).Database(ctx, string(n.DatabaseName))
	if err != nil {
		return nil, err
	}

	return &alterDatabaseDropSecondaryRegion{n: n, desc: dbDesc}, nil

}

func (n *alterDatabaseDropSecondaryRegion) startExec(params runParams) error {
	if !n.desc.IsMultiRegion() {
		return errors.WithHintf(
			pgerror.New(pgcode.InvalidDatabaseDefinition,
				"database must be multi-region to support a secondary region",
			),
			"you must first add a primary region to the database using "+
				"ALTER DATABASE %s PRIMARY REGION <region_name>",
			n.n.DatabaseName.String(),
		)
	}

	if n.desc.RegionConfig.SecondaryRegion == "" {
		if n.n.IfExists {
			params.p.BufferClientNotice(
				params.ctx,
				pgnotice.Newf("No secondary region is defined on the database; skipping"),
			)
			return nil
		}
		return pgerror.Newf(pgcode.UndefinedParameter,
			"database %s doesn't have a secondary region defined", n.desc.GetName(),
		)
	}

	typeID, err := n.desc.MultiRegionEnumID()
	if err != nil {
		return err
	}
	typeDesc, err := params.p.Descriptors().MutableByID(params.p.txn).Type(params.ctx, typeID)
	if err != nil {
		return err
	}

	n.desc.RegionConfig.SecondaryRegion = ""
	if err := params.p.writeNonDropDatabaseChange(
		params.ctx,
		n.desc,
		tree.AsStringWithFQNames(n.n, params.Ann()),
	); err != nil {
		return err
	}

	// Update the secondary region in the type descriptor, and write it back out.
	typeDesc.RegionConfig.SecondaryRegion = ""
	if err := params.p.writeTypeDesc(params.ctx, typeDesc); err != nil {
		return err
	}

	updatedRegionConfig, err := SynthesizeRegionConfig(
		params.ctx, params.p.txn, n.desc.ID, params.p.Descriptors(),
	)
	if err != nil {
		return err
	}

	// Validate the final zone config at the end of the transaction, since
	// we will not be validating localities right now.
	*params.extendedEvalCtx.validateDbZoneConfig = true
	// Update the database's zone configuration.
	if err := ApplyZoneConfigFromDatabaseRegionConfig(
		params.ctx,
		n.desc.ID,
		updatedRegionConfig,
		params.p.InternalSQLTxn(),
		params.p.execCfg,
		false, /* validateLocalities */
		params.extendedEvalCtx.Tracing.KVTracingEnabled(),
	); err != nil {
		return err
	}

	// Update all regional and regional by row tables.
	if err := params.p.refreshZoneConfigsForTables(
		params.ctx,
		n.desc,
	); err != nil {
		return err
	}

	return nil
}

// validateSecondaryRegion ensures the primary region and the secondary region
// are both inside or both outside a super region. The primary region and secondary
// region should also be within the same super region.
func validateSecondaryRegion(
	primaryRegion catpb.RegionName, secondaryRegion catpb.RegionName, regions catpb.RegionNames,
) error {

	secondaryInCurrSuper, primaryInCurrSuper := false, false
	for _, region := range regions {
		if region == secondaryRegion {
			secondaryInCurrSuper = true
		}
		if region == primaryRegion {
			primaryInCurrSuper = true
		}
	}

	if primaryInCurrSuper == secondaryInCurrSuper {
		return nil
	}

	return pgerror.New(pgcode.InvalidDatabaseDefinition,
		"the secondary region must be in the same super region as the current primary region",
	)
}

// nameToRegionName returns a slice of region names.
func nameToRegionName(regions tree.NameList) []catpb.RegionName {
	regionList := make([]catpb.RegionName, len(regions))
	for i, region := range regions {
		regionList[i] = catpb.RegionName(region)
	}

	return regionList
}

func (n *alterDatabaseDropSecondaryRegion) Next(runParams) (bool, error) { return false, nil }
func (n *alterDatabaseDropSecondaryRegion) Values() tree.Datums          { return tree.Datums{} }
func (n *alterDatabaseDropSecondaryRegion) Close(context.Context)        {}

type alterDatabaseSetZoneConfigExtensionNode struct {
	zeroInputPlanNode
	n          *tree.AlterDatabaseSetZoneConfigExtension
	desc       *dbdesc.Mutable
	yamlConfig tree.TypedExpr
	options    map[tree.Name]zone.OptionValue
}

// AlterDatabaseSetZoneConfigExtension transforms a
// tree.AlterDatabaseSetZoneConfigExtension into a plan node.
func (p *planner) AlterDatabaseSetZoneConfigExtension(
	ctx context.Context, n *tree.AlterDatabaseSetZoneConfigExtension,
) (planNode, error) {

	if err := checkSchemaChangeEnabled(
		ctx,
		p.ExecCfg(),
		"ALTER DATABASE",
	); err != nil {
		return nil, err
	}

	dbDesc, err := p.Descriptors().MutableByName(p.txn).Database(ctx, string(n.DatabaseName))
	if err != nil {
		return nil, err
	}

	if err := p.checkPrivilegesForMultiRegionOp(ctx, dbDesc); err != nil {
		return nil, err
	}

	yamlConfig, err := p.getUpdatedZoneConfigYamlConfig(ctx, n.YAMLConfig)
	if err != nil {
		return nil, err
	}

	options, err := p.getUpdatedZoneConfigOptions(ctx, n.Options, "database")
	if err != nil {
		return nil, err
	}

	if n.SetDefault {
		return nil, pgerror.Newf(pgcode.InvalidParameterValue,
			"unsupported zone config parameter: SET DEFAULT")
	}

	return &alterDatabaseSetZoneConfigExtensionNode{
		n:          n,
		desc:       dbDesc,
		yamlConfig: yamlConfig,
		options:    options,
	}, nil
}

func (n *alterDatabaseSetZoneConfigExtensionNode) startExec(params runParams) error {
	// Verify that the database is a multi-region database.
	if !n.desc.IsMultiRegion() {
		return errors.WithHintf(
			pgerror.New(pgcode.InvalidName,
				"database must have associated regions before a zone config extension can be set",
			),
			"you must first add a primary region to the database using "+
				"ALTER DATABASE %s PRIMARY REGION <region_name>",
			n.n.DatabaseName.String(),
		)
	}

	yamlConfig, deleteZone, err := evaluateYAMLConfig(n.yamlConfig, params)
	if err != nil {
		return err
	}

	// There's no need to inherit zone options from parents for zone config
	// extension, because it's not allowed to give a nil value for a zone
	// config when setting the extension.
	optionsStr, _, setters, err := evaluateZoneOptions(n.options, params)
	if err != nil {
		return err
	}

	// Get the type descriptor for the multi-region enum.
	typeID, err := n.desc.MultiRegionEnumID()
	if err != nil {
		return err
	}
	typeDesc, err := params.p.Descriptors().MutableByID(params.p.txn).Type(params.ctx, typeID)
	if err != nil {
		return err
	}
	regionsDesc := typeDesc.AsRegionEnumTypeDescriptor()
	if regionsDesc == nil {
		return errors.AssertionFailedf("expected region enum type, not %s for type %q (%d)",
			typeDesc.GetKind(), typeDesc.GetName(), typeDesc.GetID())
	}

	// Verify that the region is present in the database, if necessary.
	if n.n.LocalityLevel == tree.LocalityLevelTable && n.n.RegionName != "" {
		var found bool
		_ = regionsDesc.ForEachPublicRegion(func(name catpb.RegionName) error {
			if name == catpb.RegionName(n.n.RegionName) {
				found = true
				return iterutil.StopIteration()
			}
			return nil
		})
		if !found {
			return pgerror.Newf(
				pgcode.UndefinedObject,
				"region %q has not been added to the database",
				n.n.RegionName,
			)
		}
	}

	currentZone := zonepb.NewZoneConfig()
	if currentZoneConfigWithRaw, err := params.p.Descriptors().GetZoneConfig(
		params.ctx, params.p.Txn(), n.desc.ID,
	); err != nil {
		return err
	} else if currentZoneConfigWithRaw != nil {
		currentZone = currentZoneConfigWithRaw.ZoneConfigProto()
	}

	if deleteZone {
		switch n.n.LocalityLevel {
		case tree.LocalityLevelGlobal:
			typeDesc.RegionConfig.ZoneConfigExtensions.Global = nil
		case tree.LocalityLevelTable:
			if n.n.RegionName != "" {
				delete(typeDesc.RegionConfig.ZoneConfigExtensions.RegionalIn, catpb.RegionName(n.n.RegionName))
			} else {
				typeDesc.RegionConfig.ZoneConfigExtensions.Regional = nil
			}
		default:
			return errors.AssertionFailedf("unexpected locality level %v", n.n.LocalityLevel)
		}
	} else {
		// Validate the user input.
		if len(yamlConfig) == 0 || yamlConfig[len(yamlConfig)-1] != '\n' {
			// YAML values must always end with a newline character. If there is none,
			// for UX convenience add one.
			yamlConfig += "\n"
		}

		// Load settings from YAML. If there was no YAML (e.g. because the
		// query specified CONFIGURE ZONE USING), the YAML string will be
		// empty, in which case the unmarshaling will be a no-op. This is
		// innocuous.
		newZone := zonepb.NewZoneConfig()
		if err := yaml.UnmarshalStrict([]byte(yamlConfig), newZone); err != nil {
			return pgerror.Wrap(err, pgcode.CheckViolation, "could not parse zone config")
		}

		// Load settings from var = val assignments. If there were no such
		// settings, (e.g. because the query specified CONFIGURE ZONE = or
		// USING DEFAULT), the setter slice will be empty and this will be
		// a no-op. This is innocuous.
		for _, setter := range setters {
			// A setter may fail with an error-via-panic. Catch those.
			if err := func() (err error) {
				defer func() {
					if p := recover(); p != nil {
						if errP, ok := p.(error); ok {
							// Catch and return the error.
							err = errP
						} else {
							// Nothing we know about, let it continue as a panic.
							panic(p)
						}
					}
				}()

				setter(newZone)
				return nil
			}(); err != nil {
				return err
			}
		}

		// Validate that there are no conflicts in the zone setup.
		if err := zonepb.ValidateNoRepeatKeysInZone(newZone); err != nil {
			return err
		}

		if err := validateZoneAttrsAndLocalities(
			params.ctx, params.p.InternalSQLTxn().Regions(), params.p.ExecCfg(), currentZone, newZone,
		); err != nil {
			return err
		}

		switch n.n.LocalityLevel {
		case tree.LocalityLevelGlobal:
			typeDesc.RegionConfig.ZoneConfigExtensions.Global = newZone
		case tree.LocalityLevelTable:
			if n.n.RegionName != "" {
				if typeDesc.RegionConfig.ZoneConfigExtensions.RegionalIn == nil {
					typeDesc.RegionConfig.ZoneConfigExtensions.RegionalIn = make(map[catpb.RegionName]zonepb.ZoneConfig)
				}
				typeDesc.RegionConfig.ZoneConfigExtensions.RegionalIn[catpb.RegionName(n.n.RegionName)] = *newZone
			} else {
				typeDesc.RegionConfig.ZoneConfigExtensions.Regional = newZone
			}
		default:
			return errors.AssertionFailedf("unexpected locality level %v", n.n.LocalityLevel)
		}
	}

	if err := params.p.writeTypeSchemaChange(
		params.ctx, typeDesc, tree.AsStringWithFQNames(n.n, params.Ann()),
	); err != nil {
		return err
	}

	updatedRegionConfig, err := SynthesizeRegionConfig(params.ctx, params.p.txn, n.desc.ID, params.p.Descriptors())
	if err != nil {
		return err
	}

	// We need to validate that the zone config extension won't violate the original
	// region config before we truly write the update. Since the zone config
	// extension can affect the final zone config of the database AND
	// tables in it, we now follow these steps:
	// 1. Generate and validate the newly created zone config for the database.
	// 2. For each table under this database, validate the newly derived zone
	// config. After the validation passed for all tables, we are now sure that the
	// new zone config is sound and update the zone config for each table.
	// 3. Update the zone config for the database.

	// Validate if the zone config extension is compatible with the database.
	dbZoneConfig, err := generateAndValidateZoneConfigForMultiRegionDatabase(
		params.ctx, params.p.InternalSQLTxn().Regions(), params.ExecCfg(), updatedRegionConfig, currentZone, true, /* validateLocalities */
	)
	if err != nil {
		return err
	}

	// Validate and update all tables' zone configurations.
	if err := params.p.refreshZoneConfigsForTablesWithValidation(
		params.ctx,
		n.desc,
	); err != nil {
		return err
	}

	// Apply the zone config extension to the database.
	if err := applyZoneConfigForMultiRegionDatabase(
		params.ctx,
		n.desc.ID,
		dbZoneConfig,
		params.p.InternalSQLTxn(),
		params.p.execCfg,
		params.extendedEvalCtx.Tracing.KVTracingEnabled(),
	); err != nil {
		return err
	}

	// Record that the change has occurred for auditing.
	eventDetails := eventpb.CommonZoneConfigDetails{
		Target:  tree.AsStringWithFQNames(n.n, params.Ann()),
		Config:  strings.TrimSpace(yamlConfig),
		Options: optionsStr,
	}
	var info logpb.EventPayload
	if deleteZone {
		info = &eventpb.RemoveZoneConfig{CommonZoneConfigDetails: eventDetails}
	} else {
		info = &eventpb.AlterDatabaseSetZoneConfigExtension{CommonZoneConfigDetails: eventDetails}
	}
	if err := params.p.logEvent(params.ctx, n.desc.ID, info); err != nil {
		return err
	}

	return nil
}

func (n *alterDatabaseSetZoneConfigExtensionNode) Next(runParams) (bool, error) { return false, nil }
func (n *alterDatabaseSetZoneConfigExtensionNode) Values() tree.Datums          { return tree.Datums{} }
func (n *alterDatabaseSetZoneConfigExtensionNode) Close(context.Context)        {}
