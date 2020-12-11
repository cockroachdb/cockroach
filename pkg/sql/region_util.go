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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/errors"
	"github.com/gogo/protobuf/proto"
)

type liveClusterRegions map[descpb.Region]struct{}

func (s *liveClusterRegions) isActive(region descpb.Region) bool {
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
	var ret liveClusterRegions = make(map[descpb.Region]struct{})
	for _, node := range nodes {
		for _, tier := range node.Locality.Tiers {
			if tier.Key == "region" {
				ret[descpb.Region(tier.Value)] = struct{}{}
				break
			}
		}
	}
	return ret, nil
}

// checkLiveClusterRegion checks whether a region can be added to a database
// based on whether the cluster regions are alive.
func checkLiveClusterRegion(liveClusterRegions liveClusterRegions, region descpb.Region) error {
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

func makeRequiredZoneConstraintForRegion(r descpb.Region) zonepb.Constraint {
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
				Constraints: []zonepb.Constraint{makeRequiredZoneConstraintForRegion(region)},
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
	region descpb.Region, regionConfig descpb.DatabaseDescriptor_RegionConfig,
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
		// TODO(#multiregion): figure out correct partition/subzone fields to make this work.
		// We need to fill the table with PARTITION BY to get the ball rolling on this.
		// For now, return a dummy field for now so that other tests relying on this path of code to
		// exercise other functionality (i.e. SHOW) pass.
	}
	return &ret, nil
}

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
	localityConfig descpb.TableDescriptor_LocalityConfig,
	regionConfig descpb.DatabaseDescriptor_RegionConfig,
) error {
	localityZoneConfig, err := zoneConfigFromTableLocalityConfig(localityConfig, regionConfig)
	if err != nil {
		return err
	}
	// If we do not have to configure anything, exit early.
	if localityZoneConfig == nil {
		return nil
	}
	// Construct an explicit name so that CONFIGURE ZONE has the fully qualified name.
	// Without this, the table may fail to resolve.
	explicitTblName := tree.MakeTableNameWithSchema(
		tblName.CatalogName,
		tblName.SchemaName,
		tblName.ObjectName,
	)
	return p.applyZoneConfigForMultiRegion(
		ctx,
		tree.ZoneSpecifier{TableOrIndex: tree.TableIndexName{Table: explicitTblName}},
		localityZoneConfig,
		"table-multiregion-set-zone-config",
	)
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
