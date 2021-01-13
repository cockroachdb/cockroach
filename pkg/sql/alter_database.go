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
	"sort"

	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/dbdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/roleoption"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
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

	_, dbDesc, err := p.Descriptors().GetMutableDatabaseByName(ctx, p.txn, n.Name.String(),
		tree.DatabaseLookupFlags{Required: true})
	if err != nil {
		return nil, err
	}

	return &alterDatabaseOwnerNode{n: n, desc: dbDesc}, nil
}

func (n *alterDatabaseOwnerNode) startExec(params runParams) error {
	privs := n.desc.GetPrivileges()

	// If the owner we want to set to is the current owner, do a no-op.
	newOwner := n.n.Owner
	if newOwner == privs.Owner() {
		return nil
	}
	if err := params.p.checkCanAlterDatabaseAndSetNewOwner(params.ctx, n.desc, newOwner); err != nil {
		return err
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

	// Log Alter Database Owner event. This is an auditable log event and is recorded
	// in the same transaction as the table descriptor update.
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

	_, dbDesc, err := p.Descriptors().GetMutableDatabaseByName(ctx, p.txn, n.Name.String(),
		tree.DatabaseLookupFlags{Required: true},
	)
	if err != nil {
		return nil, err
	}

	return &alterDatabaseAddRegionNode{n: n, desc: dbDesc}, nil
}

func (n *alterDatabaseAddRegionNode) startExec(params runParams) error {
	// To add a region, the user has to have CREATEDB privileges, or be an admin user.
	if err := params.p.CheckRoleOption(params.ctx, roleoption.CREATEDB); err != nil {
		return err
	}

	// If we get to this point and the database is not a multi-region database, it means that
	// the database doesn't yet have a primary region. Since we need a primary region before
	// we can add a region, return an error here.
	if !n.desc.IsMultiRegion() {
		return errors.WithHintf(
			pgerror.Newf(pgcode.InvalidDatabaseDefinition, "cannot add region %s to database %s",
				n.n.Region.String(),
				n.n.Name.String(),
			),
			"you must add a PRIMARY REGION first using ALTER DATABASE %s SET PRIMARY REGION %s",
			n.n.Name.String(),
			n.n.Region.String(),
		)
	}

	// Add the region to the database descriptor. This function validates that the region
	// we're adding is an active member of the cluster and isn't already present in the
	// RegionConfig.
	if err := params.p.addRegionToRegionConfig(n.desc, n.n); err != nil {
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

	// Get the type descriptor for the multi-region enum.
	typeDesc, err := params.p.Descriptors().GetMutableTypeVersionByID(
		params.ctx,
		params.p.txn,
		n.desc.RegionConfig.RegionEnumID,
	)
	if err != nil {
		return err
	}

	// Find the location in the enum where we should insert the new value. We much search
	// for the location (and not append to the end), as we want to keep the values in sorted
	// order.
	loc := sort.Search(
		len(typeDesc.EnumMembers),
		func(i int) bool {
			return string(n.n.Region) < typeDesc.EnumMembers[i].LogicalRepresentation
		},
	)

	// If the above search couldn't find a value greater than the region being added, add the
	// new region at the end of the enum.
	before := true
	if loc == len(typeDesc.EnumMembers) {
		before = false
		loc = len(typeDesc.EnumMembers) - 1
	}

	// Add the new region value to the enum. This function adds the value to the enum and
	// persists the new value to the supplied type descriptor.
	jobDesc := fmt.Sprintf("Adding new region value %q to %q", tree.EnumValue(n.n.Region), tree.RegionEnum)
	if err := params.p.addEnumValue(
		params.ctx,
		typeDesc,
		&tree.AlterTypeAddValue{
			IfNotExists: false,
			NewVal:      tree.EnumValue(n.n.Region),
			Placement: &tree.AlterTypeAddValuePlacement{
				Before:      before,
				ExistingVal: tree.EnumValue(typeDesc.EnumMembers[loc].LogicalRepresentation),
			}},
		jobDesc); err != nil {
		return err
	}

	// Validate the type descriptor after the changes. We have to do this explicitly here, because
	// we're using an internal call to addEnumValue above which doesn't perform validation.
	dg := catalogkv.NewOneLevelUncachedDescGetter(params.p.txn, params.ExecCfg().Codec)
	if err := typeDesc.Validate(params.ctx, dg); err != nil {
		return err
	}

	// Update the database's zone configuration.
	if err := params.p.applyZoneConfigFromDatabaseRegionConfig(
		params.ctx,
		tree.Name(n.desc.Name),
		*n.desc.RegionConfig); err != nil {
		return err
	}

	// Log Alter Database Add Region event. This is an auditable log event and is recorded
	// in the same transaction as the database descriptor, type descriptor, and zone
	// configuration updates.
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
	return nil, unimplemented.New("alter database drop region", "implementation pending")
}

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

	_, dbDesc, err := p.Descriptors().GetMutableDatabaseByName(ctx, p.txn, n.Name.String(),
		tree.DatabaseLookupFlags{Required: true},
	)
	if err != nil {
		return nil, err
	}

	return &alterDatabasePrimaryRegionNode{n: n, desc: dbDesc}, nil
}

// switchPrimaryRegion performs the work in ALTER DATABASE ... PRIMARY REGION for the case
// where the database is already a multi-region database.
func (n *alterDatabasePrimaryRegionNode) switchPrimaryRegion(params runParams) error {
	// First check if the new primary region has been added to the database. If not, return
	// an error, as it must be added before it can be used as a primary region.
	found := false
	for _, r := range n.desc.RegionConfig.Regions {
		if r.Name == descpb.RegionName(n.n.PrimaryRegion) {
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

	// To update the primary region we need to modify the database descriptor, update the multi-region
	// enum, and write a new zone configuration.
	n.desc.RegionConfig.PrimaryRegion = descpb.RegionName(n.n.PrimaryRegion)
	if err := params.p.writeNonDropDatabaseChange(
		params.ctx,
		n.desc,
		tree.AsStringWithFQNames(n.n, params.Ann()),
	); err != nil {
		return err
	}

	// Get the type descriptor for the multi-region enum.
	typeDesc, err := params.p.Descriptors().GetMutableTypeVersionByID(
		params.ctx,
		params.p.txn,
		n.desc.RegionConfig.RegionEnumID)
	if err != nil {
		return err
	}

	// Update the primary region in the type descriptor, and write it back out.
	typeDesc.RegionConfig.PrimaryRegion = descpb.RegionName(n.n.PrimaryRegion)
	if err := params.p.writeTypeDesc(params.ctx, typeDesc); err != nil {
		return err
	}

	// Validate the type descriptor after the changes.
	dg := catalogkv.NewOneLevelUncachedDescGetter(params.p.txn, params.ExecCfg().Codec)
	if err := typeDesc.Validate(params.ctx, dg); err != nil {
		return err
	}

	// Update the database's zone configuration.
	if err := params.p.applyZoneConfigFromDatabaseRegionConfig(
		params.ctx,
		tree.Name(n.desc.Name),
		*n.desc.RegionConfig); err != nil {
		return err
	}

	return nil
}

// setInitialPrimaryRegion sets the primary region in cases where the database is already
// a multi-region database.
func (n *alterDatabasePrimaryRegionNode) setInitialPrimaryRegion(params runParams) error {
	// Create the region config structure to be added to the database descriptor.
	regionConfig, err := params.p.createRegionConfig(
		params.ctx,
		tree.SurvivalGoalDefault,
		n.n.PrimaryRegion,
		[]tree.Name{n.n.PrimaryRegion},
	)
	if err != nil {
		return err
	}

	// Write the modified database descriptor.
	n.desc.RegionConfig = regionConfig
	if err := params.p.writeNonDropDatabaseChange(
		params.ctx,
		n.desc,
		tree.AsStringWithFQNames(n.n, params.Ann()),
	); err != nil {
		return err
	}

	// Initialize that multi-region database by creating the multi-region enum
	// and the database-level zone configuration.
	return params.p.initializeMultiRegionDatabase(params.ctx, n.desc)
}

func (n *alterDatabasePrimaryRegionNode) startExec(params runParams) error {
	// To add a region, the user has to have CREATEDB privileges, or be an admin user.
	if err := params.p.CheckRoleOption(params.ctx, roleoption.CREATEDB); err != nil {
		return err
	}

	// There are two paths to consider here: either this is the first setting of the
	// primary region, OR we're updating the primary region. In the case where this
	// is the first setting of the primary region, the call will turn the database into
	// a "multi-region" database. This requires creating a RegionConfig structure in the
	// database descriptor, creating a multi-region enum, and setting up the database-level
	// zone configuration. The second case is simpler, as the multi-region infrastructure
	// is already setup. In this case we just need to update the database and type descriptor,
	// and the zone config.
	if !n.desc.IsMultiRegion() {
		err := n.setInitialPrimaryRegion(params)
		if err != nil {
			return err
		}
	} else {
		err := n.switchPrimaryRegion(params)
		if err != nil {
			return err
		}
	}

	// Log Alter Database Primary Region event. This is an auditable log event and is recorded
	// in the same transaction as the database descriptor, and zone configuration updates.
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
	_, dbDesc, err := p.Descriptors().GetMutableDatabaseByName(ctx, p.txn, n.Name.String(),
		tree.DatabaseLookupFlags{Required: true},
	)
	if err != nil {
		return nil, err
	}

	return &alterDatabaseSurvivalGoalNode{n: n, desc: dbDesc}, nil
}

func (n *alterDatabaseSurvivalGoalNode) startExec(params runParams) error {
	// To change the survival goal, the user has to have CREATEDB privileges, or be an admin user.
	if err := params.p.CheckRoleOption(params.ctx, roleoption.CREATEDB); err != nil {
		return err
	}

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

	// If we're changing to survive a region failure, validate that we have enough regions
	// in the database.
	if n.n.SurvivalGoal == tree.SurvivalGoalRegionFailure {
		regions, err := n.desc.Regions()
		if err != nil {
			return err
		}
		if len(regions) < minNumRegionsForSurviveRegionGoal {
			return errors.WithHintf(
				pgerror.Newf(pgcode.InvalidName,
					"at least %d regions are required for surviving a region failure",
					minNumRegionsForSurviveRegionGoal,
				),
				"you must add additional regions to the database using "+
					"ALTER DATABASE %s ADD REGION <region_name>",
				n.n.Name.String(),
			)
		}
	}

	// Update the survival goal in the database descriptor
	survivalGoal, err := translateSurvivalGoal(n.n.SurvivalGoal)
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

	// Update the database's zone configuration.
	if err := params.p.applyZoneConfigFromDatabaseRegionConfig(
		params.ctx,
		tree.Name(n.desc.Name),
		*n.desc.RegionConfig); err != nil {
		return err
	}

	// Update all REGIONAL BY TABLE tables' zone configurations. This is required as replica
	// placement for REGIONAL BY TABLE tables is dependant on the survival goal.
	if err := params.p.updateZoneConfigsForAllTables(params.ctx, n.desc); err != nil {
		return err
	}

	// Log Alter Database Survival Goal event. This is an auditable log event and is recorded
	// in the same transaction as the database descriptor, and zone configuration updates.
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
