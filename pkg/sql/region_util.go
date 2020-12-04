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

// genZoneConfigFromRegionConfigForDatabase generates a desired ZoneConfig based
// on the region config.
// TODO(aayushshah15): properly configure this for region survivability and leaseholder
// preferences when new zone configuration parameters merge.
func genZoneConfigFromRegionConfigForDatabase(
	regionConfig descpb.DatabaseDescriptor_RegionConfig,
) *zonepb.ZoneConfig {
	numReplicas := int32(len(regionConfig.Regions))
	if numReplicas < 3 {
		numReplicas = 3
	}
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

func (p *planner) applyZoneConfigFromRegionConfigForDatabase(
	ctx context.Context, dbName tree.Name, regionConfig descpb.DatabaseDescriptor_RegionConfig,
) error {
	// Convert the partially filled zone config to re-run as a SQL command.
	// This avoid us having to modularize planNode logic from set_zone_config
	// and the optimizer.
	sql, err := zoneConfigToSQL(
		&tree.ZoneSpecifier{
			Database: dbName,
		},
		genZoneConfigFromRegionConfigForDatabase(regionConfig),
	)
	if err != nil {
		return err
	}
	if _, err := p.ExtendedEvalContext().ExecCfg.InternalExecutor.ExecEx(
		ctx,
		"apply-database-multiregion-set-zone-config",
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
