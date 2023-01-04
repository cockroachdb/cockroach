// Copyright 2016 The Cockroach Authors.
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

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/dbdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/multiregion"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemadesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/decodeusername"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgnotice"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
)

//
// This file contains routines for low-level access to stored
// descriptors.
//
// For higher levels in the SQL layer, these interface are likely not
// suitable; consider instead schema_accessors.go and resolver.go.
//

var (
	errEmptyDatabaseName = pgerror.New(pgcode.Syntax, "empty database name")
	errNoDatabase        = pgerror.New(pgcode.InvalidName, "no database specified")
	errNoSchema          = pgerror.Newf(pgcode.InvalidName, "no schema specified")
	errNoTable           = pgerror.New(pgcode.InvalidName, "no table specified")
	errNoType            = pgerror.New(pgcode.InvalidName, "no type specified")
	errNoFunction        = pgerror.New(pgcode.InvalidName, "no function specified")
	errNoMatch           = pgerror.New(pgcode.UndefinedObject, "no object matched")
)

// createDatabase takes Database descriptor and creates it if needed,
// incrementing the descriptor counter. Returns true if the descriptor
// is actually created, false if it already existed, or an error if one was
// encountered. The ifNotExists flag is used to declare if the "already existed"
// state should be an error (false) or a no-op (true).
// createDatabase implements the DatabaseDescEditor interface.
func (p *planner) createDatabase(
	ctx context.Context, database *tree.CreateDatabase, jobDesc string,
) (*dbdesc.Mutable, bool, error) {

	dbName := string(database.Name)

	if dbID, err := p.Descriptors().LookupDatabaseID(ctx, p.txn, dbName); err == nil && dbID != descpb.InvalidID {
		if database.IfNotExists {
			// Check if the database is in a dropping state
			flags := tree.DatabaseLookupFlags{
				AvoidLeased:    true,
				IncludeDropped: true,
				IncludeOffline: true,
			}
			desc, err := p.Descriptors().ByID(p.txn).WithFlags(flags).Immutable().Database(ctx, dbID)
			if err != nil {
				return nil, false, err
			}
			if desc.Dropped() {
				return nil, false, pgerror.Newf(pgcode.ObjectNotInPrerequisiteState,
					"database %q is being dropped, try again later",
					dbName)
			}
			// Noop.
			return nil, false, nil
		}
		return nil, false, sqlerrors.NewDatabaseAlreadyExistsError(dbName)
	} else if err != nil {
		return nil, false, err
	}

	id, err := p.extendedEvalCtx.DescIDGenerator.GenerateUniqueDescID(ctx)
	if err != nil {
		return nil, false, err
	}
	publicSchemaID, err := p.EvalContext().DescIDGenerator.GenerateUniqueDescID(ctx)
	if err != nil {
		return nil, false, err
	}

	if database.PrimaryRegion != tree.PrimaryRegionNotSpecifiedName {
		telemetry.Inc(sqltelemetry.CreateMultiRegionDatabaseCounter)
		telemetry.Inc(
			sqltelemetry.CreateDatabaseSurvivalGoalCounter(
				database.SurvivalGoal.TelemetryName(),
			),
		)
		if database.Placement != tree.DataPlacementUnspecified {
			telemetry.Inc(
				sqltelemetry.CreateDatabasePlacementCounter(
					database.Placement.TelemetryName(),
				),
			)
		}
	}

	regionConfig, err := p.maybeInitializeMultiRegionMetadata(
		ctx,
		database.SurvivalGoal,
		database.PrimaryRegion,
		database.Regions,
		database.Placement,
		database.SecondaryRegion,
	)
	if err != nil {
		return nil, false, err
	}

	owner := p.SessionData().User()
	if !database.Owner.Undefined() {
		owner, err = decodeusername.FromRoleSpec(
			p.SessionData(), username.PurposeValidation, database.Owner,
		)
		if err != nil {
			return nil, true, err
		}
	}

	db := dbdesc.NewInitial(
		id,
		string(database.Name),
		owner,
		dbdesc.MaybeWithDatabaseRegionConfig(regionConfig),
		dbdesc.WithPublicSchemaID(publicSchemaID),
	)
	publicSchema := schemadesc.NewBuilder(&descpb.SchemaDescriptor{
		ParentID:   id,
		Name:       tree.PublicSchema,
		ID:         publicSchemaID,
		Privileges: catpb.NewPublicSchemaPrivilegeDescriptor(),
		Version:    1,
	}).BuildCreatedMutableSchema()

	if err := p.checkCanAlterToNewOwner(ctx, db, owner); err != nil {
		return nil, true, err
	}

	if err := p.createDescriptor(ctx, db, jobDesc); err != nil {
		return nil, true, err
	}
	if err := p.createDescriptor(
		ctx,
		publicSchema,
		tree.AsStringWithFQNames(database, p.Ann()),
	); err != nil {
		return nil, true, err
	}

	// Initialize the multi-region database by creating the multi-region enum and
	// database-level zone configuration if there is a region config on the
	// descriptor.

	if err := p.maybeInitializeMultiRegionDatabase(ctx, db, regionConfig); err != nil {
		return nil, true, err
	}

	return db, true, nil
}

func (p *planner) createDescriptor(
	ctx context.Context, descriptor catalog.MutableDescriptor, jobDesc string,
) error {
	if !descriptor.IsNew() {
		return errors.AssertionFailedf(
			"expected new descriptor, not a modification of version %d",
			descriptor.OriginalVersion())
	}
	b := &kv.Batch{}
	kvTrace := p.ExtendedEvalContext().Tracing.KVTracingEnabled()
	if err := p.Descriptors().WriteDescToBatch(ctx, kvTrace, descriptor, b); err != nil {
		return err
	}
	if !descriptor.SkipNamespace() {
		if err := p.Descriptors().InsertNamespaceEntryToBatch(ctx, kvTrace, descriptor, b); err != nil {
			return err
		}
	}
	if err := p.txn.Run(ctx, b); err != nil {
		return err
	}
	if tbl, ok := descriptor.(*tabledesc.Mutable); ok && tbl.Adding() {
		// Queue a schema change job to eventually make the table public.
		if err := p.createOrUpdateSchemaChangeJob(
			ctx, tbl, jobDesc, descpb.InvalidMutationID,
		); err != nil {
			return err
		}
	}
	return nil
}

// TranslateSurvivalGoal translates a tree.SurvivalGoal into a
// descpb.SurvivalGoal.
func TranslateSurvivalGoal(g tree.SurvivalGoal) (descpb.SurvivalGoal, error) {
	switch g {
	case tree.SurvivalGoalDefault:
		return descpb.SurvivalGoal_ZONE_FAILURE, nil
	case tree.SurvivalGoalZoneFailure:
		return descpb.SurvivalGoal_ZONE_FAILURE, nil
	case tree.SurvivalGoalRegionFailure:
		return descpb.SurvivalGoal_REGION_FAILURE, nil
	default:
		return 0, errors.Newf("unknown survival goal: %d", g)
	}
}

// TranslateDataPlacement translates a tree.DataPlacement into a
// descpb.DataPlacement.
func TranslateDataPlacement(g tree.DataPlacement) (descpb.DataPlacement, error) {
	switch g {
	case tree.DataPlacementUnspecified:
		return descpb.DataPlacement_DEFAULT, nil
	case tree.DataPlacementDefault:
		return descpb.DataPlacement_DEFAULT, nil
	case tree.DataPlacementRestricted:
		return descpb.DataPlacement_RESTRICTED, nil
	default:
		return 0, errors.AssertionFailedf("unknown data placement: %d", g)
	}
}

func (p *planner) checkRegionIsCurrentlyActive(ctx context.Context, region catpb.RegionName) error {
	liveRegions, err := p.getLiveClusterRegions(ctx)
	if err != nil {
		return err
	}

	// Ensure that the region we're adding is currently active.
	return CheckClusterRegionIsLive(liveRegions, region)
}

// InitializeMultiRegionMetadataCCL is the public hook point for the
// CCL-licensed multi-region initialization code.
var InitializeMultiRegionMetadataCCL = func(
	ctx context.Context,
	descIDGenerator eval.DescIDGenerator,
	settings *cluster.Settings,
	clusterID uuid.UUID,
	liveClusterRegions LiveClusterRegions,
	survivalGoal tree.SurvivalGoal,
	primaryRegion catpb.RegionName,
	regions []tree.Name,
	dataPlacement tree.DataPlacement,
	secondaryRegion catpb.RegionName,
) (*multiregion.RegionConfig, error) {
	return nil, sqlerrors.NewCCLRequiredError(
		errors.New("creating multi-region databases requires a CCL binary"),
	)
}

// DefaultPrimaryRegionClusterSettingName is the name of the cluster setting that returns
const DefaultPrimaryRegionClusterSettingName = "sql.defaults.primary_region"

// DefaultPrimaryRegion is a cluster setting that contains the default primary region.
var DefaultPrimaryRegion = settings.RegisterStringSetting(
	settings.TenantWritable,
	DefaultPrimaryRegionClusterSettingName,
	`if not empty, all databases created without a PRIMARY REGION will `+
		`implicitly have the given PRIMARY REGION`,
	"",
).WithPublic()

// SecondaryTenantsMultiRegionAbstractionsEnabledSettingName is the name of the
// cluster setting that governs secondary tenant multi-region abstraction usage.
const SecondaryTenantsMultiRegionAbstractionsEnabledSettingName = "sql.multi_region.allow_abstractions_for_secondary_tenants.enabled"

// SecondaryTenantsMultiRegionAbstractionsEnabled controls if secondary tenants
// are allowed to use multi-region abstractions. In particular, it controls if
// secondary tenants are allowed to add a region to their database. It has no
// effect on the system tenant.
//
// This setting has no effect for existing multi-region databases that have
// already been configured. It only affects regions being added to new
// databases.
var SecondaryTenantsMultiRegionAbstractionsEnabled = settings.RegisterBoolSetting(
	settings.TenantReadOnly,
	SecondaryTenantsMultiRegionAbstractionsEnabledSettingName,
	"allow secondary tenants to use multi-region abstractions",
	false,
)

// maybeInitializeMultiRegionMetadata initializes multi-region metadata if a
// primary region is supplied and works as a pass-through otherwise. It creates
// a new region config from the given parameters and reserves an ID for the
// multi-region enum.
func (p *planner) maybeInitializeMultiRegionMetadata(
	ctx context.Context,
	survivalGoal tree.SurvivalGoal,
	primaryRegion tree.Name,
	regions []tree.Name,
	placement tree.DataPlacement,
	secondaryRegion tree.Name,
) (*multiregion.RegionConfig, error) {
	if !p.execCfg.Codec.ForSystemTenant() &&
		!SecondaryTenantsMultiRegionAbstractionsEnabled.Get(&p.execCfg.Settings.SV) {
		// There was no primary region provided, let the thing pass through.
		if primaryRegion == "" && len(regions) == 0 {
			return nil, nil
		}

		return nil, errors.WithHint(pgerror.Newf(
			pgcode.InvalidDatabaseDefinition,
			"setting %s disallows use of multi-region abstractions",
			SecondaryTenantsMultiRegionAbstractionsEnabledSettingName,
		),
			"consider omitting the primary region")
	}

	if primaryRegion == "" && len(regions) == 0 {
		defaultPrimaryRegion := DefaultPrimaryRegion.Get(&p.execCfg.Settings.SV)
		if defaultPrimaryRegion == "" {
			return nil, nil
		}
		primaryRegion = tree.Name(defaultPrimaryRegion)
		// TODO(#67156): send notice immediately, so it pops up even on error.
		p.BufferClientNotice(
			ctx,
			pgnotice.Newf("setting %s as the PRIMARY REGION as no PRIMARY REGION was specified", primaryRegion),
		)
	}

	liveRegions, err := p.getLiveClusterRegions(ctx)
	if err != nil {
		return nil, err
	}

	regionConfig, err := InitializeMultiRegionMetadataCCL(
		ctx,
		p.EvalContext().DescIDGenerator,
		p.EvalContext().Settings,
		p.ExecCfg().NodeInfo.LogicalClusterID(),
		liveRegions,
		survivalGoal,
		catpb.RegionName(primaryRegion),
		regions,
		placement,
		catpb.RegionName(secondaryRegion),
	)
	if err != nil {
		return nil, err
	}

	return regionConfig, nil
}

// GetImmutableTableInterfaceByID is part of the EvalPlanner interface.
func (p *planner) GetImmutableTableInterfaceByID(ctx context.Context, id int) (interface{}, error) {
	desc, err := p.Descriptors().ByID(p.txn).WithObjFlags(tree.ObjectLookupFlags{}).Immutable().Table(ctx, descpb.ID(id))
	if err != nil {
		return nil, err
	}
	return desc, nil
}
