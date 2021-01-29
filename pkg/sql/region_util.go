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
	"sort"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/dbdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/errors"
	"github.com/gogo/protobuf/proto"
)

const minNumRegionsForSurviveRegionGoal = 3

type liveClusterRegions map[descpb.RegionName]struct{}

func (s *liveClusterRegions) isActive(region descpb.RegionName) bool {
	_, ok := (*s)[region]
	return ok
}

func (s *liveClusterRegions) toStrings() []string {
	ret := make([]string, 0, len(*s))
	for region := range *s {
		ret = append(ret, string(region))
	}
	sort.Slice(ret, func(i, j int) bool {
		return ret[i] < ret[j]
	})
	return ret
}

// getLiveClusterRegions returns a set of live region names in the cluster.
// A region name is deemed active if there is at least one alive node
// in the cluster in with locality set to a given region.
func (p *planner) getLiveClusterRegions() (liveClusterRegions, error) {
	nodes, err := getAllNodeDescriptors(p)
	if err != nil {
		return nil, err
	}
	var ret liveClusterRegions = make(map[descpb.RegionName]struct{})
	for _, node := range nodes {
		for _, tier := range node.Locality.Tiers {
			if tier.Key == "region" {
				ret[descpb.RegionName(tier.Value)] = struct{}{}
				break
			}
		}
	}
	return ret, nil
}

// checkLiveClusterRegion checks whether a region can be added to a database
// based on whether the cluster regions are alive.
func checkLiveClusterRegion(liveClusterRegions liveClusterRegions, region descpb.RegionName) error {
	if !liveClusterRegions.isActive(region) {
		return errors.WithHintf(
			pgerror.Newf(
				pgcode.InvalidName,
				"region %q does not exist",
				region,
			),
			"valid regions: %s",
			strings.Join(liveClusterRegions.toStrings(), ", "),
		)
	}
	return nil
}

func makeRequiredZoneConstraintForRegion(r descpb.RegionName) zonepb.Constraint {
	return zonepb.Constraint{
		Type:  zonepb.Constraint_REQUIRED,
		Key:   "region",
		Value: string(r),
	}
}

// zoneConfigFromRegionConfigForDatabase generates a desired ZoneConfig based
// on the region config.
// TODO(#multiregion,aayushshah15): properly configure this for region survivability and leaseholder
// preferences when new zone configuration parameters merge.
func zoneConfigFromRegionConfigForDatabase(
	regionConfig descpb.DatabaseDescriptor_RegionConfig,
) *zonepb.ZoneConfig {
	numReplicas := zoneConfigNumReplicasFromRegionConfig(regionConfig)
	conjunctions := []zonepb.ConstraintsConjunction{}
	for _, region := range regionConfig.Regions {
		conjunctions = append(
			conjunctions,
			zonepb.ConstraintsConjunction{
				NumReplicas: 1,
				Constraints: []zonepb.Constraint{makeRequiredZoneConstraintForRegion(region.Name)},
			},
		)
	}
	return &zonepb.ZoneConfig{
		NumReplicas: &numReplicas,
		LeasePreferences: []zonepb.LeasePreference{
			{Constraints: []zonepb.Constraint{makeRequiredZoneConstraintForRegion(regionConfig.PrimaryRegion)}},
		},
		Constraints: conjunctions,
	}
}

// zoneConfigFromRegionConfigForPartition generates a desired ZoneConfig based
// on the region config for the given partition.
// TODO(#multiregion,aayushshah15): properly configure this for region survivability and leaseholder
// preferences when new zone configuration parameters merge.
func zoneConfigFromRegionConfigForPartition(
	partitionRegion descpb.DatabaseDescriptor_RegionConfig_Region,
	regionConfig descpb.DatabaseDescriptor_RegionConfig,
) (zonepb.ZoneConfig, error) {
	zc := zonepb.ZoneConfig{
		NumReplicas: proto.Int32(zoneConfigNumReplicasFromRegionConfig(regionConfig)),
		LeasePreferences: []zonepb.LeasePreference{
			{Constraints: []zonepb.Constraint{makeRequiredZoneConstraintForRegion(partitionRegion.Name)}},
		},
	}
	var err error
	if zc.Constraints, err = constraintsConjunctionForRegionalLocality(
		partitionRegion.Name,
		regionConfig,
	); err != nil {
		return zc, err
	}
	return zc, err
}

// zoneConfigNumReplicasFromRegionConfig generates the number of replicas needed given a region config.
func zoneConfigNumReplicasFromRegionConfig(
	regionConfig descpb.DatabaseDescriptor_RegionConfig,
) int32 {
	// TODO(#multiregion): do we want 5 in the case of region failure?
	numReplicas := int32(len(regionConfig.Regions))
	if numReplicas < 3 {
		numReplicas = 3
	}
	return numReplicas
}

// constraintsConjunctionForRegionalLocality generates a ConstraintsConjunction clause
// to be set for a given locality.
// TODO(#multiregion,aayushshah15): properly configure constraints and replicas for
// region survivability and leaseholder preferences when new zone configuration parameters merge.
func constraintsConjunctionForRegionalLocality(
	region descpb.RegionName, regionConfig descpb.DatabaseDescriptor_RegionConfig,
) ([]zonepb.ConstraintsConjunction, error) {
	switch regionConfig.SurvivalGoal {
	case descpb.SurvivalGoal_ZONE_FAILURE:
		return []zonepb.ConstraintsConjunction{
			{
				NumReplicas: zoneConfigNumReplicasFromRegionConfig(regionConfig),
				Constraints: []zonepb.Constraint{makeRequiredZoneConstraintForRegion(region)},
			},
		}, nil
	case descpb.SurvivalGoal_REGION_FAILURE:
		return []zonepb.ConstraintsConjunction{
			{
				NumReplicas: 1,
				Constraints: []zonepb.Constraint{makeRequiredZoneConstraintForRegion(region)},
			},
		}, nil
	default:
		return nil, errors.AssertionFailedf("unknown survival goal: %v", regionConfig.SurvivalGoal)
	}
}

var multiRegionZoneConfigFields = []tree.Name{
	"constraints",
	"global_reads",
	"lease_preferences",
	"num_replicas",
	"voter_constraints",
}

// zoneConfigFromTableLocalityConfig generates a desired ZoneConfig based
// on the locality config for the database.
// Relevant multi-region configured fields swill be overwritten by the calling function
// into an existing ZoneConfig using multiRegionZoneConfigFields.
// TODO(#multiregion,aayushshah15): properly configure this for region survivability and leaseholder
// preferences when new zone configuration parameters merge.
func zoneConfigFromTableLocalityConfig(
	localityConfig descpb.TableDescriptor_LocalityConfig,
	regionConfig descpb.DatabaseDescriptor_RegionConfig,
) (zonepb.ZoneConfig, error) {
	ret := *(zonepb.NewZoneConfig())

	switch l := localityConfig.Locality.(type) {
	case *descpb.TableDescriptor_LocalityConfig_Global_:
		// Enable non-blocking transactions.
		ret.GlobalReads = proto.Bool(true)
	case *descpb.TableDescriptor_LocalityConfig_RegionalByTable_:
		// Use the same configuration as the database and return nil here.
		if l.RegionalByTable.Region == nil {
			return ret, nil
		}
		ret.NumReplicas = proto.Int32(zoneConfigNumReplicasFromRegionConfig(regionConfig))
		preferredRegion := *l.RegionalByTable.Region
		var err error
		if ret.Constraints, err = constraintsConjunctionForRegionalLocality(preferredRegion, regionConfig); err != nil {
			return ret, err
		}
		ret.LeasePreferences = []zonepb.LeasePreference{
			{Constraints: []zonepb.Constraint{makeRequiredZoneConstraintForRegion(preferredRegion)}},
		}
		ret.InheritedLeasePreferences = false
		ret.InheritedConstraints = false
	case *descpb.TableDescriptor_LocalityConfig_RegionalByRow_:
		// We purposely do not set anything here at table level - this should be done at
		// partition level instead.
		return ret, nil
	}
	return ret, nil
}

// TODO(#multiregion): everything using this should instead use getZoneConfigRaw
// and writeZoneConfig instead of calling SQL for each query.
// This removes the requirement to only call this function after writeSchemaChange
// is called on creation of tables, and potentially removes the need for ReadingOwnWrites
// for some subcommands.
// Requires some logic to "inherit" from parents.
func (p *planner) applyZoneConfigForMultiRegion(
	ctx context.Context, zs tree.ZoneSpecifier, zc *zonepb.ZoneConfig, desc string,
) error {
	// TODO(#multiregion): We should check to see if the zone configuration has been updated
	// by the user. If it has, we need to warn, and only proceed if sql_safe_updates is disabled.

	// Convert the partially filled zone config to re-run as a SQL command.
	// This avoid us having to modularize planNode logic from set_zone_config
	// and the optimizer.
	sql, err := zoneConfigToSQL(&zs, zc)
	if err != nil {
		return err
	}
	if _, err := p.ExtendedEvalContext().ExecCfg.InternalExecutor.ExecEx(
		ctx,
		desc,
		p.Txn(),
		sessiondata.InternalExecutorOverride{
			User: p.SessionData().User(),
		},
		sql,
	); err != nil {
		return err
	}
	return nil
}

// applyZoneConfigForMultiRegionTableOption is an option that can be passed into
// applyZoneConfigForMultiRegionTable.
type applyZoneConfigForMultiRegionTableOption func(
	zoneConfig zonepb.ZoneConfig,
	regionConfig descpb.DatabaseDescriptor_RegionConfig,
	table catalog.TableDescriptor,
) (hasNewSubzones bool, newZoneConfig zonepb.ZoneConfig, err error)

// applyZoneConfigForMultiRegionTableOptionNewIndex applies table zone configs
// for a newly added index which requires partitioning of individual indexes.
func applyZoneConfigForMultiRegionTableOptionNewIndex(
	indexID descpb.IndexID,
) applyZoneConfigForMultiRegionTableOption {
	return func(
		zoneConfig zonepb.ZoneConfig,
		regionConfig descpb.DatabaseDescriptor_RegionConfig,
		table catalog.TableDescriptor,
	) (hasNewSubzones bool, newZoneConfig zonepb.ZoneConfig, err error) {
		for _, region := range regionConfig.Regions {
			zc, err := zoneConfigFromRegionConfigForPartition(region, regionConfig)
			if err != nil {
				return false, zoneConfig, err
			}
			zoneConfig.SetSubzone(zonepb.Subzone{
				IndexID:       uint32(indexID),
				PartitionName: string(region.Name),
				Config:        zc,
			})
		}
		return true, zoneConfig, nil
	}
}

// applyZoneConfigForMultiRegionTableOptionTableAndIndexes applies table zone configs
// on the entire table as well as its indexes, replacing multi-region related zone
// configuration fields.
var applyZoneConfigForMultiRegionTableOptionTableAndIndexes = func(
	zc zonepb.ZoneConfig,
	regionConfig descpb.DatabaseDescriptor_RegionConfig,
	table catalog.TableDescriptor,
) (bool, zonepb.ZoneConfig, error) {
	localityConfig := *table.GetLocalityConfig()

	localityZoneConfig, err := zoneConfigFromTableLocalityConfig(
		localityConfig,
		regionConfig,
	)
	if err != nil {
		return false, zonepb.ZoneConfig{}, err
	}
	zc.CopyFromZone(localityZoneConfig, multiRegionZoneConfigFields)

	hasNewSubzones := table.IsLocalityRegionalByRow()
	if hasNewSubzones {
		// Mark the zone config as a placeholder zone config if it is currently empty and we are
		// adding subzones.
		// See zonepb.IsSubzonePlaceholder for why this is necessary.
		if zc.Equal(zonepb.NewZoneConfig()) {
			zc.NumReplicas = proto.Int32(0)
		}

		for _, region := range regionConfig.Regions {
			subzoneConfig, err := zoneConfigFromRegionConfigForPartition(region, regionConfig)
			if err != nil {
				return false, zc, err
			}
			for _, idx := range table.NonDropIndexes() {
				zc.SetSubzone(zonepb.Subzone{
					IndexID:       uint32(idx.GetID()),
					PartitionName: string(region.Name),
					Config:        subzoneConfig,
				})
			}
		}
	}
	return hasNewSubzones, zc, nil
}

// applyZoneConfigForMultiRegionTable applies zone config settings based
// on the options provided.
func applyZoneConfigForMultiRegionTable(
	ctx context.Context,
	txn *kv.Txn,
	execCfg *ExecutorConfig,
	regionConfig descpb.DatabaseDescriptor_RegionConfig,
	table catalog.TableDescriptor,
	opt applyZoneConfigForMultiRegionTableOption,
) error {
	tableID := table.GetID()
	zoneRaw, err := getZoneConfigRaw(ctx, txn, execCfg.Codec, tableID)
	if err != nil {
		return err
	}
	var zoneConfig zonepb.ZoneConfig
	if zoneRaw != nil {
		zoneConfig = *zoneRaw
	}

	var hasNewSubzones bool
	if hasNewSubzones, zoneConfig, err = opt(
		zoneConfig,
		regionConfig,
		table,
	); err != nil {
		return err
	}

	if !zoneConfig.Equal(zonepb.NewZoneConfig()) {
		if err := zoneConfig.Validate(); err != nil {
			return pgerror.Newf(
				pgcode.CheckViolation,
				"could not validate zone config: %v",
				err,
			)
		}
		if err := zoneConfig.ValidateTandemFields(); err != nil {
			return pgerror.Newf(
				pgcode.CheckViolation,
				"could not validate zone config: %v",
				err,
			)
		}

		// If we have fields that are not the default value, write in a new zone configuration
		// value.
		if _, err = writeZoneConfig(
			ctx,
			txn,
			tableID,
			table,
			&zoneConfig,
			execCfg,
			hasNewSubzones,
		); err != nil {
			return err
		}
	} else if zoneRaw != nil {
		// Delete the zone configuration if it exists but the new zone config is blank.
		if _, err = execCfg.InternalExecutor.Exec(
			ctx,
			"delete-zone",
			txn,
			"DELETE FROM system.zones WHERE id = $1",
			tableID,
		); err != nil {
			return err
		}
	}
	return nil
}

func (p *planner) applyZoneConfigFromDatabaseRegionConfig(
	ctx context.Context, dbName tree.Name, regionConfig descpb.DatabaseDescriptor_RegionConfig,
) error {
	// Convert the partially filled zone config to re-run as a SQL command.
	// This avoid us having to modularize planNode logic from set_zone_config
	// and the optimizer.
	return p.applyZoneConfigForMultiRegion(
		ctx,
		tree.ZoneSpecifier{Database: dbName},
		zoneConfigFromRegionConfigForDatabase(regionConfig),
		"database-multiregion-set-zone-config",
	)
}

// forEachTableWithLocalityConfigInDatabase loops through each schema and table
// for a table with a LocalityConfig configured.
// NOTE: this function uses cached table and schema descriptors. As a result, it may
// not be safe to run within a schema change.
func (p *planner) forEachTableWithLocalityConfigInDatabase(
	ctx context.Context,
	desc *dbdesc.Mutable,
	f func(ctx context.Context, schema string, tbName tree.TableName, tbDesc *tabledesc.Mutable) error,
) error {
	// No work to be done if the database isn't a multi-region database.
	if !desc.IsMultiRegion() {
		return nil
	}
	lookupFlags := p.CommonLookupFlags(true /*required*/)
	lookupFlags.AvoidCached = false
	schemas, err := p.Descriptors().GetSchemasForDatabase(ctx, p.txn, desc.GetID())
	if err != nil {
		return err
	}

	tblLookupFlags := p.CommonLookupFlags(true /*required*/)
	tblLookupFlags.AvoidCached = false
	tblLookupFlags.Required = false

	// Loop over all schemas, then loop over all tables.
	for _, schema := range schemas {
		tbNames, err := p.Descriptors().GetObjectNames(
			ctx,
			p.txn,
			desc,
			schema,
			tree.DatabaseListFlags{
				CommonLookupFlags: lookupFlags,
				ExplicitPrefix:    true,
			},
		)
		if err != nil {
			return err
		}
		for i := range tbNames {
			found, tbDesc, err := p.Descriptors().GetMutableTableByName(
				ctx, p.txn, &tbNames[i], tree.ObjectLookupFlags{CommonLookupFlags: tblLookupFlags},
			)
			if err != nil {
				return err
			}

			// If we couldn't find the table, or it has no LocalityConfig, there's nothing
			// to do here.
			if !found || tbDesc.LocalityConfig == nil {
				continue
			}

			if err := f(ctx, schema, tbNames[i], tbDesc); err != nil {
				return err
			}
		}
	}
	return nil
}

// updateZoneConfigsForAllTables loops through all of the tables in the
// specified database and refreshes the zone configs for all tables.
func (p *planner) updateZoneConfigsForAllTables(ctx context.Context, desc *dbdesc.Mutable) error {
	return p.forEachTableWithLocalityConfigInDatabase(
		ctx,
		desc,
		func(ctx context.Context, schema string, tbName tree.TableName, tbDesc *tabledesc.Mutable) error {
			return applyZoneConfigForMultiRegionTable(
				ctx,
				p.txn,
				p.ExecCfg(),
				*desc.RegionConfig,
				tbDesc,
				applyZoneConfigForMultiRegionTableOptionTableAndIndexes,
			)
		},
	)
}

// initializeMultiRegionDatabase initializes a multi-region database by creating
// the multi-region enum and the database-level zone configuration.
func (p *planner) initializeMultiRegionDatabase(ctx context.Context, desc *dbdesc.Mutable) error {
	// If the database is not a multi-region database, there's no work to be done.
	if !desc.IsMultiRegion() {
		return nil
	}

	// Create the multi-region enum.
	regionLabels := make(tree.EnumValueList, 0, len(desc.RegionConfig.Regions))
	for _, region := range desc.RegionConfig.Regions {
		regionLabels = append(regionLabels, tree.EnumValue(region.Name))
	}

	if err := p.createEnumWithID(
		p.RunParams(ctx),
		desc.RegionConfig.RegionEnumID,
		regionLabels,
		desc,
		tree.NewQualifiedTypeName(desc.Name, tree.PublicSchema, tree.RegionEnum),
		enumTypeMultiRegion,
	); err != nil {
		return err
	}

	// Create the database-level zone configuration.
	if err := p.applyZoneConfigFromDatabaseRegionConfig(
		ctx,
		tree.Name(desc.Name),
		*desc.RegionConfig); err != nil {
		return err
	}

	return nil
}
