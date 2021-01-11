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
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/roleoption"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
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

	// If we get to this point and the RegionConfig is nil, it means that the database doesn't
	// yet have a primary region. Since we need a primary region before we can add a region,
	// return an error here.
	if !n.desc.IsMultiRegion() {
		return pgerror.Newf(pgcode.InvalidDatabaseDefinition,
			"cannot add region %q to database %q. Please add primary region first.",
			n.n.Region.String(),
			n.n.Name.String(),
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

// AlterDatabasePrimaryRegion transforms a tree.AlterDatabasePrimaryRegion into a plan node.
func (p *planner) AlterDatabasePrimaryRegion(
	ctx context.Context, n *tree.AlterDatabasePrimaryRegion,
) (planNode, error) {
	return nil, unimplemented.New("alter database primary region", "implementation pending")
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
	return nil, unimplemented.New("alter database survive", "implementation pending")
}
