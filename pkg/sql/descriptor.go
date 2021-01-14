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
	"sort"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/dbdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemadesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/cockroach/pkg/util/log"
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
	errNoMatch           = pgerror.New(pgcode.UndefinedObject, "no object matched")
)

// createdatabase takes Database descriptor and creates it if needed,
// incrementing the descriptor counter. Returns true if the descriptor
// is actually created, false if it already existed, or an error if one was
// encountered. The ifNotExists flag is used to declare if the "already existed"
// state should be an error (false) or a no-op (true).
// createDatabase implements the DatabaseDescEditor interface.
func (p *planner) createDatabase(
	ctx context.Context, database *tree.CreateDatabase, jobDesc string,
) (*dbdesc.Mutable, bool, error) {

	dbName := string(database.Name)
	shouldCreatePublicSchema := true
	dKey := catalogkv.MakeDatabaseNameKey(ctx, p.ExecCfg().Settings, dbName)
	// TODO(solon): This conditional can be removed in 20.2. Every database
	// is created with a public schema for cluster version >= 20.1, so we can remove
	// the `shouldCreatePublicSchema` logic as well.
	if !p.ExecCfg().Settings.Version.IsActive(ctx, clusterversion.NamespaceTableWithSchemas) {
		shouldCreatePublicSchema = false
	}

	if exists, _, err := catalogkv.LookupDatabaseID(ctx, p.txn, p.ExecCfg().Codec, dbName); err == nil && exists {
		if database.IfNotExists {
			// Noop.
			return nil, false, nil
		}
		return nil, false, sqlerrors.NewDatabaseAlreadyExistsError(dbName)
	} else if err != nil {
		return nil, false, err
	}

	id, err := catalogkv.GenerateUniqueDescID(ctx, p.ExecCfg().DB, p.ExecCfg().Codec)
	if err != nil {
		return nil, false, err
	}

	regionConfig, err := p.createRegionConfig(
		ctx,
		database.SurvivalGoal,
		database.PrimaryRegion,
		database.Regions,
	)
	if err != nil {
		return nil, false, err
	}

	desc := dbdesc.NewInitial(
		id,
		string(database.Name),
		p.SessionData().User(),
		dbdesc.NewInitialOptionDatabaseRegionConfig(regionConfig),
	)

	if err := p.createDescriptorWithID(ctx, dKey.Key(p.ExecCfg().Codec), id, desc, nil, jobDesc); err != nil {
		return nil, true, err
	}

	// Initialize the multi-region database by creating the multi-region enum and
	// database-level zone configuration.
	if err := p.initializeMultiRegionDatabase(ctx, desc); err != nil {
		return nil, true, err
	}

	// TODO(solon): This check should be removed and a public schema should
	// be created in every database in >= 20.2.
	if shouldCreatePublicSchema {
		// Every database must be initialized with the public schema.
		if err := p.CreateSchemaNamespaceEntry(ctx,
			catalogkeys.NewPublicSchemaKey(id).Key(p.ExecCfg().Codec), keys.PublicSchemaID); err != nil {
			return nil, true, err
		}
	}

	return desc, true, nil
}

func (p *planner) createDescriptorWithID(
	ctx context.Context,
	idKey roachpb.Key,
	id descpb.ID,
	descriptor catalog.Descriptor,
	st *cluster.Settings,
	jobDesc string,
) error {
	if descriptor.GetID() == 0 {
		// TODO(ajwerner): Return the error here rather than fatal.
		log.Fatalf(ctx, "%v", errors.AssertionFailedf("cannot create descriptor with an empty ID: %v", descriptor))
	}
	if descriptor.GetID() != id {
		log.Fatalf(ctx, "%v", errors.AssertionFailedf("cannot create descriptor with an unexpected (%v) ID: %v", id, descriptor))
	}
	// TODO(pmattis): The error currently returned below is likely going to be
	// difficult to interpret.
	//
	// TODO(pmattis): Need to handle if-not-exists here as well.
	//
	// TODO(pmattis): This is writing the namespace and descriptor table entries,
	// but not going through the normal INSERT logic and not performing a precise
	// mimicry. In particular, we're only writing a single key per table, while
	// perfect mimicry would involve writing a sentinel key for each row as well.

	b := &kv.Batch{}
	descID := descriptor.GetID()
	if p.ExtendedEvalContext().Tracing.KVTracingEnabled() {
		log.VEventf(ctx, 2, "CPut %s -> %d", idKey, descID)
	}
	b.CPut(idKey, descID, nil)
	if err := catalogkv.WriteNewDescToBatch(
		ctx,
		p.ExtendedEvalContext().Tracing.KVTracingEnabled(),
		st,
		b,
		p.ExecCfg().Codec,
		descID,
		descriptor,
	); err != nil {
		return err
	}

	mutDesc, ok := descriptor.(catalog.MutableDescriptor)
	if !ok {
		log.Fatalf(ctx, "unexpected type %T when creating descriptor", descriptor)
	}
	isTable := false
	switch desc := mutDesc.(type) {
	case *typedesc.Mutable:
		dg := catalogkv.NewOneLevelUncachedDescGetter(p.txn, p.ExecCfg().Codec)
		if err := desc.Validate(ctx, dg); err != nil {
			return err
		}
		if err := p.Descriptors().AddUncommittedDescriptor(mutDesc); err != nil {
			return err
		}
	case *tabledesc.Mutable:
		isTable = true
		if err := desc.ValidateTable(ctx); err != nil {
			return err
		}
		if err := p.Descriptors().AddUncommittedDescriptor(mutDesc); err != nil {
			return err
		}
	case *dbdesc.Mutable:
		if err := desc.Validate(); err != nil {
			return err
		}
		if err := p.Descriptors().AddUncommittedDescriptor(mutDesc); err != nil {
			return err
		}
	case *schemadesc.Mutable:
		if err := p.Descriptors().AddUncommittedDescriptor(mutDesc); err != nil {
			return err
		}
	default:
		log.Fatalf(ctx, "unexpected type %T when creating descriptor", mutDesc)
	}

	if err := p.txn.Run(ctx, b); err != nil {
		return err
	}
	if isTable && mutDesc.Adding() {
		// Queue a schema change job to eventually make the table public.
		if err := p.createOrUpdateSchemaChangeJob(
			ctx,
			mutDesc.(*tabledesc.Mutable),
			jobDesc,
			descpb.InvalidMutationID); err != nil {
			return err
		}
	}
	return nil
}

// translateSurvivalGoal translates a tree.SurvivalGoal into a
// descpb.SurvivalGoal.
func translateSurvivalGoal(g tree.SurvivalGoal) (descpb.SurvivalGoal, error) {
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

// validateDatabaseRegionConfig validates that a
// descpb.DatabaseDescriptor_RegionConfig is valid.
func validateDatabaseRegionConfig(regionConfig descpb.DatabaseDescriptor_RegionConfig) error {
	if regionConfig.RegionEnumID == descpb.InvalidID {
		return errors.AssertionFailedf("invalid multi-region enum ID")
	}
	if len(regionConfig.Regions) == 0 {
		return errors.AssertionFailedf("expected > 0 number of regions in the region config")
	}
	if regionConfig.SurvivalGoal == descpb.SurvivalGoal_REGION_FAILURE &&
		len(regionConfig.Regions) < minNumRegionsForSurviveRegionGoal {
		return pgerror.Newf(
			pgcode.InvalidParameterValue,
			"at least %d regions are required for surviving a region failure",
			minNumRegionsForSurviveRegionGoal,
		)
	}
	return nil
}

// addRegionToRegionConfig adds the supplied region to the RegionConfig in the
// supplied database descriptor.
func (p *planner) addRegionToRegionConfig(
	desc *dbdesc.Mutable, regionToAdd *tree.AlterDatabaseAddRegion,
) error {
	liveRegions, err := p.getLiveClusterRegions()
	if err != nil {
		return err
	}

	regionConfig := desc.RegionConfig

	// Ensure that the region we're adding is currently active.
	region := descpb.RegionName(regionToAdd.Region)
	if err := checkLiveClusterRegion(liveRegions, region); err != nil {
		return err
	}

	// Ensure that the region doesn't already exist in the database.
	for _, r := range regionConfig.Regions {
		if r.Name == region {
			return pgerror.Newf(
				pgcode.InvalidName,
				"region %q already added to database",
				region,
			)
		}
	}

	regionConfig.Regions = append(
		regionConfig.Regions,
		descpb.DatabaseDescriptor_RegionConfig_Region{
			Name: region,
		},
	)

	// We store the regions sorted in the regionConfig. Perform the sort now.
	sort.SliceStable(regionConfig.Regions, func(i, j int) bool {
		return regionConfig.Regions[i].Name < regionConfig.Regions[j].Name
	})

	// Validate that the region config is sane.
	if err := validateDatabaseRegionConfig(*regionConfig); err != nil {
		return err
	}

	return nil
}

// createRegionConfig creates a new region config from the given parameters.
func (p *planner) createRegionConfig(
	ctx context.Context, survivalGoal tree.SurvivalGoal, primaryRegion tree.Name, regions []tree.Name,
) (*descpb.DatabaseDescriptor_RegionConfig, error) {
	if primaryRegion == "" && len(regions) == 0 {
		return nil, nil
	}
	var regionConfig descpb.DatabaseDescriptor_RegionConfig
	var err error
	regionConfig.SurvivalGoal, err = translateSurvivalGoal(survivalGoal)
	if err != nil {
		return nil, err
	}
	liveRegions, err := p.getLiveClusterRegions()
	if err != nil {
		return nil, err
	}
	regionConfig.PrimaryRegion = descpb.RegionName(primaryRegion)
	if regionConfig.PrimaryRegion != "" {
		if err := checkLiveClusterRegion(liveRegions, regionConfig.PrimaryRegion); err != nil {
			return nil, err
		}
	}
	if len(regions) > 0 {
		if regionConfig.PrimaryRegion == "" {
			return nil, pgerror.Newf(
				pgcode.InvalidDatabaseDefinition,
				"PRIMARY REGION must be specified if REGIONS are specified",
			)
		}
		regionConfig.Regions = make([]descpb.DatabaseDescriptor_RegionConfig_Region, 0, len(regions)+1)
		seenRegions := make(map[descpb.RegionName]struct{}, len(regions)+1)
		for _, r := range regions {
			region := descpb.RegionName(r)
			if err := checkLiveClusterRegion(liveRegions, region); err != nil {
				return nil, err
			}

			if _, ok := seenRegions[region]; ok {
				return nil, pgerror.Newf(
					pgcode.InvalidName,
					"region %q defined multiple times",
					region,
				)
			}
			seenRegions[region] = struct{}{}
			regionConfig.Regions = append(
				regionConfig.Regions,
				descpb.DatabaseDescriptor_RegionConfig_Region{
					Name: region,
				},
			)
		}
		// If PRIMARY REGION is not in REGIONS, add it implicitly.
		if _, ok := seenRegions[regionConfig.PrimaryRegion]; !ok {
			regionConfig.Regions = append(
				regionConfig.Regions,
				descpb.DatabaseDescriptor_RegionConfig_Region{
					Name: regionConfig.PrimaryRegion,
				},
			)
		}
		sort.SliceStable(regionConfig.Regions, func(i, j int) bool {
			return regionConfig.Regions[i].Name < regionConfig.Regions[j].Name
		})
	} else {
		regionConfig.Regions = []descpb.DatabaseDescriptor_RegionConfig_Region{
			{Name: regionConfig.PrimaryRegion},
		}
	}

	// Generate a unique ID for the multi-region enum type descriptor here as
	// well.
	id, err := catalogkv.GenerateUniqueDescID(ctx, p.ExecCfg().DB, p.ExecCfg().Codec)
	if err != nil {
		return nil, err
	}
	regionConfig.RegionEnumID = id

	if err := validateDatabaseRegionConfig(regionConfig); err != nil {
		return nil, err
	}
	return &regionConfig, nil
}
