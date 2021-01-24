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

// zoneConfigFromTableLocalityConfig generates a desired ZoneConfig based
// on the locality config for the database.
// This function can return a nil zonepb.ZoneConfig, meaning no update is required
// to any zone config.
// TODO(#multiregion,aayushshah15): properly configure this for region survivability and leaseholder
// preferences when new zone configuration parameters merge.
func zoneConfigFromTableLocalityConfig(
	localityConfig descpb.TableDescriptor_LocalityConfig,
	regionConfig descpb.DatabaseDescriptor_RegionConfig,
) (*zonepb.ZoneConfig, error) {
	ret := zonepb.ZoneConfig{
		NumReplicas: proto.Int32(zoneConfigNumReplicasFromRegionConfig(regionConfig)),
	}

	switch l := localityConfig.Locality.(type) {
	case *descpb.TableDescriptor_LocalityConfig_Global_:
		// Inherit everything from the database.
		ret.InheritedConstraints = true
		ret.InheritedLeasePreferences = true
	case *descpb.TableDescriptor_LocalityConfig_RegionalByTable_:
		// Use the same configuration as the database and return nil here.
		if l.RegionalByTable.Region == nil {
			return nil, nil
		}
		preferredRegion := *l.RegionalByTable.Region
		var err error
		if ret.Constraints, err = constraintsConjunctionForRegionalLocality(preferredRegion, regionConfig); err != nil {
			return nil, err
		}
		ret.LeasePreferences = []zonepb.LeasePreference{
			{Constraints: []zonepb.Constraint{makeRequiredZoneConstraintForRegion(preferredRegion)}},
		}
	case *descpb.TableDescriptor_LocalityConfig_RegionalByRow_:
		// We purposely do not set anything here at table level - this should be done at
		// partition level instead.
		return nil, nil
	}
	return &ret, nil
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

func (p *planner) applyZoneConfigFromTableLocalityConfig(
	ctx context.Context,
	tblName tree.TableName,
	desc *descpb.TableDescriptor,
	regionConfig descpb.DatabaseDescriptor_RegionConfig,
) error {
	localityConfig := *desc.LocalityConfig

	// Construct an explicit name so that CONFIGURE ZONE has the fully qualified name.
	// Without this, the table may fail to resolve.
	explicitTblName := tree.MakeTableNameWithSchema(
		tblName.CatalogName,
		tblName.SchemaName,
		tblName.ObjectName,
	)

	// Apply zone configs for each index partition.
	// TODO(otan): depending on what we decide for cascading zone configs, some of this
	// code will have to change.
	switch localityConfig.Locality.(type) {
	case *descpb.TableDescriptor_LocalityConfig_RegionalByRow_:
		for _, region := range regionConfig.Regions {
			zc, err := zoneConfigFromRegionConfigForPartition(region, regionConfig)
			if err != nil {
				return err
			}

			for _, idx := range desc.Indexes {
				if err := p.applyZoneConfigForMultiRegion(
					ctx,
					tree.ZoneSpecifier{
						TableOrIndex: tree.TableIndexName{
							Table: explicitTblName,
							Index: tree.UnrestrictedName(idx.Name),
						},
						Partition: tree.Name(region.Name),
					},
					&zc,
					"index-multiregion-set-zone-config",
				); err != nil {
					return err
				}
			}

			if err := p.applyZoneConfigForMultiRegion(
				ctx,
				tree.ZoneSpecifier{
					TableOrIndex: tree.TableIndexName{
						Table: explicitTblName,
						Index: tree.UnrestrictedName(desc.PrimaryIndex.Name),
					},
					Partition: tree.Name(region.Name),
				},
				&zc,
				"primary-index-multiregion-set-zone-config",
			); err != nil {
				return err
			}
		}
	}

	localityZoneConfig, err := zoneConfigFromTableLocalityConfig(
		localityConfig,
		regionConfig,
	)
	if err != nil {
		return err
	}
	// If we do not have to configure anything, exit early.
	if localityZoneConfig == nil {
		return nil
	}

	return p.applyZoneConfigForMultiRegion(
		ctx,
		tree.ZoneSpecifier{TableOrIndex: tree.TableIndexName{Table: explicitTblName}},
		localityZoneConfig,
		"table-multiregion-set-zone-config",
	)
}

// addNewZoneConfigSubzonesForIndex updates the ZoneConfig for the given index,
// assuming a zone config already exists for the given table and that the index
// has previously not had a subzone defined.
func (p *planner) addNewZoneConfigSubzonesForIndex(
	ctx context.Context,
	table catalog.TableDescriptor,
	indexID descpb.IndexID,
	regionConfig descpb.DatabaseDescriptor_RegionConfig,
) error {
	tableID := table.GetID()
	zone, err := getZoneConfigRaw(ctx, p.txn, p.ExecCfg().Codec, tableID)
	if err != nil {
		return err
	}
	if zone == nil {
		return errors.AssertionFailedf("expected zone config for table %d", tableID)
	}
	for _, region := range regionConfig.Regions {
		zc, err := zoneConfigFromRegionConfigForPartition(region, regionConfig)
		if err != nil {
			return err
		}
		zone.SetSubzone(zonepb.Subzone{
			IndexID:       uint32(indexID),
			PartitionName: string(region.Name),
			Config:        zc,
		})
	}

	if _, err = writeZoneConfig(
		ctx,
		p.txn,
		tableID,
		table,
		zone,
		p.ExecCfg(),
		true, /* hasNewSubzones */
	); err != nil {
		return err
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
			return p.applyZoneConfigFromTableLocalityConfig(
				ctx,
				tbName,
				tbDesc.TableDesc(),
				*desc.RegionConfig,
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
