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
	"strings"

	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/dbdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/multiregion"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/errors"
	"github.com/gogo/protobuf/proto"
)

// LiveClusterRegions is a set representing regions that are live in
// a given cluster.
type LiveClusterRegions map[descpb.RegionName]struct{}

func (s *LiveClusterRegions) isActive(region descpb.RegionName) bool {
	_, ok := (*s)[region]
	return ok
}

func (s *LiveClusterRegions) toStrings() []string {
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
func (p *planner) getLiveClusterRegions(ctx context.Context) (LiveClusterRegions, error) {
	// Non-admin users can't access the crdb_internal.kv_node_status table, which
	// this query hits, so we must override the user here.
	override := sessiondata.InternalExecutorOverride{
		User: security.RootUserName(),
	}

	it, err := p.ExtendedEvalContext().ExecCfg.InternalExecutor.QueryIteratorEx(
		ctx,
		"get_live_cluster_regions",
		p.txn,
		override,
		"SELECT region FROM [SHOW REGIONS FROM CLUSTER]",
	)
	if err != nil {
		return nil, err
	}

	var ret LiveClusterRegions = make(map[descpb.RegionName]struct{})
	var ok bool
	for ok, err = it.Next(ctx); ok; ok, err = it.Next(ctx) {
		row := it.Cur()
		ret[descpb.RegionName(*row[0].(*tree.DString))] = struct{}{}
	}
	if err != nil {
		return nil, err
	}
	return ret, nil
}

// CheckClusterRegionIsLive checks whether a region supplied is one of the
// currently active cluster regions.
func CheckClusterRegionIsLive(
	liveClusterRegions LiveClusterRegions, region descpb.RegionName,
) error {
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

func makeRequiredConstraintForRegion(r descpb.RegionName) zonepb.Constraint {
	return zonepb.Constraint{
		Type:  zonepb.Constraint_REQUIRED,
		Key:   "region",
		Value: string(r),
	}
}

// zoneConfigForMultiRegionDatabase generates a ZoneConfig stub for a
// multi-region database such that at least one replica (voting or non-voting)
// is constrained to each region defined within the given `regionConfig` and
// some voting replicas are constrained to the primary region of the database
// depending on its prescribed survivability goal.
//
// For instance, for a database with 4 regions: A (primary), B, C, D and region
// survivability. This method will generate a ZoneConfig object representing
// the following attributes:
// num_replicas = 5
// num_voters = 5
// constraints = '{"+region=A": 1,"+region=B": 1,"+region=C": 1,"+region=D": 1}'
// voter_constraints = '{"+region=A": 2}'
// lease_preferences = [["+region=A"]]
//
// See synthesizeVoterConstraints() for explanation on why `voter_constraints`
// are set the way they are.
func zoneConfigForMultiRegionDatabase(
	regionConfig multiregion.RegionConfig,
) (zonepb.ZoneConfig, error) {
	numVoters, numReplicas := getNumVotersAndNumReplicas(regionConfig)
	constraints := make([]zonepb.ConstraintsConjunction, len(regionConfig.Regions()))
	for i, region := range regionConfig.Regions() {
		// Constrain at least 1 (voting or non-voting) replica per region.
		constraints[i] = zonepb.ConstraintsConjunction{
			NumReplicas: 1,
			Constraints: []zonepb.Constraint{makeRequiredConstraintForRegion(region)},
		}
	}

	voterConstraints, err := synthesizeVoterConstraints(regionConfig.PrimaryRegion(), regionConfig)
	if err != nil {
		return zonepb.ZoneConfig{}, err
	}

	return zonepb.ZoneConfig{
		NumReplicas: &numReplicas,
		NumVoters:   &numVoters,
		LeasePreferences: []zonepb.LeasePreference{
			{Constraints: []zonepb.Constraint{makeRequiredConstraintForRegion(regionConfig.PrimaryRegion())}},
		},
		NullVoterConstraintsIsEmpty: true,
		VoterConstraints:            voterConstraints,
		Constraints:                 constraints,
	}, nil
}

// zoneConfigForMultiRegionPartition generates a ZoneConfig stub for a partition
// that belongs to a regional by row table in a multi-region database.
//
// At the table/partition level, the only attributes that are set are
// `num_voters`, `voter_constraints`, and `lease_preferences`. We expect that
// the attributes `num_replicas` and `constraints` will be inherited from the
// database level zone config.
func zoneConfigForMultiRegionPartition(
	partitionRegion descpb.RegionName, regionConfig multiregion.RegionConfig,
) (zonepb.ZoneConfig, error) {
	numVoters, _ := getNumVotersAndNumReplicas(regionConfig)
	zc := zonepb.NewZoneConfig()
	voterConstraints, err := synthesizeVoterConstraints(partitionRegion, regionConfig)
	if err != nil {
		return zonepb.ZoneConfig{}, err
	}
	zc.NumVoters = &numVoters

	zc.NullVoterConstraintsIsEmpty = true
	zc.VoterConstraints = voterConstraints

	zc.InheritedLeasePreferences = false
	zc.LeasePreferences = []zonepb.LeasePreference{
		{Constraints: []zonepb.Constraint{makeRequiredConstraintForRegion(partitionRegion)}},
	}
	return *zc, err
}

// maxFailuresBeforeUnavailability returns the maximum number of individual
// failures that can be tolerated, among `numVoters` voting replicas, before a
// given range is unavailable.
func maxFailuresBeforeUnavailability(numVoters int32) int32 {
	return ((numVoters + 1) / 2) - 1
}

// getNumVotersAndNumReplicas computes the number of voters and the total number
// of replicas needed for a given region config.
func getNumVotersAndNumReplicas(
	regionConfig multiregion.RegionConfig,
) (numVoters, numReplicas int32) {
	const numVotersForZoneSurvival = 3
	// Under region survivability, we use 5 voting replicas to allow for a
	// theoretical (2-2-1) voting replica configuration, where the primary region
	// has 2 voting replicas and the next closest region has another 2. This
	// allows for stable read/write latencies even under single node failures.
	//
	// TODO(aayush): Until we add allocator heuristics to coalesce voting replicas
	// together based on their relative latencies to the leaseholder, we can't
	// actually ensure that the region closest to the leaseholder has 2 voting
	// replicas.
	//
	// Until the above TODO is addressed, the non-leaseholder voting replicas will
	// be allowed to "float" around among the other regions in the database. They
	// may or may not be placed geographically close to the leaseholder replica.
	const numVotersForRegionSurvival = 5

	numRegions := int32(len(regionConfig.Regions()))
	switch regionConfig.SurvivalGoal() {
	// NB: See mega-comment inside `synthesizeVoterConstraints()` for why these
	// are set the way they are.
	case descpb.SurvivalGoal_ZONE_FAILURE:
		numVoters = numVotersForZoneSurvival
		// <numVoters in the home region> + <1 replica for every other region>
		numReplicas = (numVotersForZoneSurvival) + (numRegions - 1)
	case descpb.SurvivalGoal_REGION_FAILURE:
		// <(quorum - 1) voters in the home region> + <1 replica for every other
		// region>
		numVoters = numVotersForRegionSurvival
		// We place the maximum concurrent replicas that can fail before a range
		// outage in the home region, and ensure that there's at least one replica
		// in all other regions.
		numReplicas = maxFailuresBeforeUnavailability(numVotersForRegionSurvival) + (numRegions - 1)
		if numReplicas < numVoters {
			// NumReplicas cannot be less than NumVoters. If we have <= 4 regions, all
			// replicas will be voting replicas.
			numReplicas = numVoters
		}
	}
	return numVoters, numReplicas
}

// synthesizeVoterConstraints generates a ConstraintsConjunction clause
// representing the `voter_constraints` field to be set for the primary region
// of a multi-region database or the home region of a table in such a database.
//
// Under zone survivability, we will constrain all voting replicas to be inside
// the primary/home region.
//
// Under region survivability, we will constrain exactly <quorum - 1> voting
// replicas in the primary/home region.
func synthesizeVoterConstraints(
	region descpb.RegionName, regionConfig multiregion.RegionConfig,
) ([]zonepb.ConstraintsConjunction, error) {
	numVoters, _ := getNumVotersAndNumReplicas(regionConfig)
	switch regionConfig.SurvivalGoal() {
	case descpb.SurvivalGoal_ZONE_FAILURE:
		return []zonepb.ConstraintsConjunction{
			{
				// We don't specify `NumReplicas` here to indicate that we want _all_
				// voting replicas to be constrained to this one region.
				//
				// Constraining all voting replicas to be inside the primary/home region
				// is necessary and sufficient to ensure zone survivability, even though
				// it might appear that these zone configs don't seem to spell out the
				// requirement of being resilient to zone failures. This is because, by
				// default, the allocator (see kv/kvserver/allocator.go) will maximize
				// survivability due to it's diversity heuristic (see
				// Locality.DiversityScore()) by spreading the replicas of a range
				// across nodes with the most mutual difference in their locality
				// hierarchies.
				//
				// For instance, in a 2 region deployment, each with 3 AZs, this is
				// expected to result in a configuration like the following:
				//
				// +---- Region A -----+      +---- Region B -----+
				// |                   |      |                   |
				// |   +------------+  |      |  +------------+   |
				// |   |   VOTER    |  |      |  |            |   |
				// |   |            |  |      |  |            |   |
				// |   +------------+  |      |  +------------+   |
				// |   +------------+  |      |  +------------+   |
				// |   |   VOTER    |  |      |  |            |   |
				// |   |            |  |      |  | NON-VOTER  |   |
				// |   +------------+  |      |  |            |   |
				// |   +------------+  |      |  +------------+   |
				// |   |            |  |      |  +------------+   |
				// |   |   VOTER    |  |      |  |            |   |
				// |   |            |  |      |  |            |   |
				// |   +------------+  |      |  +------------+   |
				// +-------------------+      +-------------------+
				//
				Constraints: []zonepb.Constraint{makeRequiredConstraintForRegion(region)},
			},
		}, nil
	case descpb.SurvivalGoal_REGION_FAILURE:
		return []zonepb.ConstraintsConjunction{
			{
				// We constrain <quorum - 1> voting replicas to the primary region and
				// allow the rest to "float" around. This allows the allocator inside KV
				// to make dynamic placement decisions for the voting replicas that lie
				// outside the primary/home region.
				//
				// It might appear that constraining just <quorum - 1> voting replicas
				// to the primary region leaves open the possibility of a majority
				// quorum coalescing inside of some other region. However, similar to
				// the case above, the diversity heuristic in the allocator prevents
				// this from happening as it will spread the unconstrained replicas out
				// across nodes with the most diverse locality hierarchies.
				//
				// For instance, in a 3 region deployment (minimum for a database with
				// "region" survivability), each with 3 AZs, we'd expect to see a
				// configuration like the following:
				//
				// +---- Region A ------+   +---- Region B -----+    +----- Region C -----+
				// |                    |   |                   |    |                    |
				// |   +------------+   |   |  +------------+   |    |   +------------+   |
				// |   |   VOTER    |   |   |  |   VOTER    |   |    |   |            |   |
				// |   |            |   |   |  |            |   |    |   |            |   |
				// |   +------------+   |   |  +------------+   |    |   +------------+   |
				// |   +------------+   |   |  +------------+   |    |   +------------+   |
				// |   |            |   |   |  |   VOTER    |   |    |   |   VOTER    |   |
				// |   |            |   |   |  |            |   |    |   |            |   |
				// |   +------------+   |   |  +------------+   |    |   +------------+   |
				// |   +------------+   |   |  +------------+   |    |   +------------+   |
				// |   |   VOTER    |   |   |  |            |   |    |   |            |   |
				// |   |            |   |   |  |            |   |    |   |            |   |
				// |   +------------+   |   |  +------------+   |    |   +------------+   |
				// +--------------------+   +-------------------+    +--------------------+
				//
				NumReplicas: maxFailuresBeforeUnavailability(numVoters),
				Constraints: []zonepb.Constraint{makeRequiredConstraintForRegion(region)},
			},
		}, nil
	default:
		return nil, errors.AssertionFailedf("unknown survival goal: %v", regionConfig.SurvivalGoal())
	}
}

// zoneConfigForMultiRegionTable generates a ZoneConfig stub for a
// regional-by-table or global table in a multi-region database.
//
// At the table/partition level, the only attributes that are set are
// `num_voters`, `voter_constraints`, and `lease_preferences`. We expect that
// the attributes `num_replicas` and `constraints` will be inherited from the
// database level zone config.
//
// This function can return a nil zonepb.ZoneConfig, meaning no table level zone
// configuration is required.
//
// Relevant multi-region configured fields (as defined in
// `zonepb.MultiRegionZoneConfigFields`) will be overwritten by the calling function
// into an existing ZoneConfig.
func zoneConfigForMultiRegionTable(
	localityConfig descpb.TableDescriptor_LocalityConfig, regionConfig multiregion.RegionConfig,
) (*zonepb.ZoneConfig, error) {
	// We only care about NumVoters here at the table level. NumReplicas is set at
	// the database level, not at the table/partition level.
	numVoters, _ := getNumVotersAndNumReplicas(regionConfig)
	ret := zonepb.NewZoneConfig()

	switch l := localityConfig.Locality.(type) {
	case *descpb.TableDescriptor_LocalityConfig_Global_:
		// Enable non-blocking transactions.
		ret.GlobalReads = proto.Bool(true)
		// Inherit voter_constraints and lease preferences from the database. We do
		// nothing here because `NewZoneConfig()` already marks those fields as
		// 'inherited'.
	case *descpb.TableDescriptor_LocalityConfig_RegionalByTable_:
		// Use the same configuration as the database and return nil here.
		if l.RegionalByTable.Region == nil {
			return ret, nil
		}
		preferredRegion := *l.RegionalByTable.Region
		voterConstraints, err := synthesizeVoterConstraints(preferredRegion, regionConfig)
		if err != nil {
			return nil, err
		}
		ret.NumVoters = &numVoters

		ret.NullVoterConstraintsIsEmpty = true
		ret.VoterConstraints = voterConstraints

		ret.InheritedLeasePreferences = false
		ret.LeasePreferences = []zonepb.LeasePreference{
			{Constraints: []zonepb.Constraint{makeRequiredConstraintForRegion(preferredRegion)}},
		}
	case *descpb.TableDescriptor_LocalityConfig_RegionalByRow_:
		// We purposely do not set anything here at table level - this should be done at
		// partition level instead.
		return ret, nil
	}
	return ret, nil
}

// applyZoneConfigForMultiRegionTableOption is an option that can be passed into
// applyZoneConfigForMultiRegionTable.
type applyZoneConfigForMultiRegionTableOption func(
	zoneConfig zonepb.ZoneConfig,
	regionConfig multiregion.RegionConfig,
	table catalog.TableDescriptor,
) (hasNewSubzones bool, newZoneConfig zonepb.ZoneConfig, err error)

// applyZoneConfigForMultiRegionTableOptionNewIndexes applies table zone configs
// for a newly added index which requires partitioning of individual indexes.
func applyZoneConfigForMultiRegionTableOptionNewIndexes(
	indexIDs ...descpb.IndexID,
) applyZoneConfigForMultiRegionTableOption {
	return func(
		zoneConfig zonepb.ZoneConfig,
		regionConfig multiregion.RegionConfig,
		table catalog.TableDescriptor,
	) (hasNewSubzones bool, newZoneConfig zonepb.ZoneConfig, err error) {
		for _, indexID := range indexIDs {
			for _, region := range regionConfig.Regions() {
				zc, err := zoneConfigForMultiRegionPartition(region, regionConfig)
				if err != nil {
					return false, zoneConfig, err
				}
				zoneConfig.SetSubzone(zonepb.Subzone{
					IndexID:       uint32(indexID),
					PartitionName: string(region),
					Config:        zc,
				})
			}
		}
		return true, zoneConfig, nil
	}
}

// dropZoneConfigsForMultiRegionIndexes drops the zone configs for all
// the indexes defined on a multi-region table. This function is used to clean
// up zone configs when transitioning from REGIONAL BY ROW.
func dropZoneConfigsForMultiRegionIndexes(
	indexIDs ...descpb.IndexID,
) applyZoneConfigForMultiRegionTableOption {
	return func(
		zoneConfig zonepb.ZoneConfig,
		regionConfig multiregion.RegionConfig,
		table catalog.TableDescriptor,
	) (hasNewSubzones bool, newZoneConfig zonepb.ZoneConfig, err error) {
		// Clear all multi-region fields of the subzones. If this leaves them
		// empty, they will automatically be removed.
		zoneConfig.ClearFieldsOfAllSubzones(zonepb.MultiRegionZoneConfigFields)

		// Strip placeholder status and spans if there are no more subzones.
		if len(zoneConfig.Subzones) == 0 && zoneConfig.IsSubzonePlaceholder() {
			zoneConfig.NumReplicas = nil
			zoneConfig.SubzoneSpans = nil
		}
		return false, zoneConfig, nil
	}
}

// isPlaceholderZoneConfigForMultiRegion returns whether a given zone config
// should be marked as a placeholder config for a multi-region object.
// See zonepb.IsSubzonePlaceholder for why this is necessary.
func isPlaceholderZoneConfigForMultiRegion(zc zonepb.ZoneConfig) bool {
	// Placeholders must have at least 1 subzone.
	if len(zc.Subzones) == 0 {
		return false
	}
	// Strip Subzones / SubzoneSpans, as these may contain items if migrating
	// from one REGIONAL BY ROW table to another.
	strippedZC := zc
	strippedZC.Subzones, strippedZC.SubzoneSpans = nil, nil
	return strippedZC.Equal(zonepb.NewZoneConfig())
}

// applyZoneConfigForMultiRegionTableOptionTableNewConfig applies table zone
// configs on the entire table with the given new locality config.
func applyZoneConfigForMultiRegionTableOptionTableNewConfig(
	newConfig descpb.TableDescriptor_LocalityConfig,
) applyZoneConfigForMultiRegionTableOption {
	return func(
		zc zonepb.ZoneConfig,
		regionConfig multiregion.RegionConfig,
		table catalog.TableDescriptor,
	) (bool, zonepb.ZoneConfig, error) {
		localityZoneConfig, err := zoneConfigForMultiRegionTable(
			newConfig,
			regionConfig,
		)
		if err != nil {
			return false, zonepb.ZoneConfig{}, err
		}
		zc.CopyFromZone(*localityZoneConfig, zonepb.MultiRegionZoneConfigFields)
		return false, zc, nil
	}
}

// ApplyZoneConfigForMultiRegionTableOptionTableAndIndexes applies table zone configs
// on the entire table as well as its indexes, replacing multi-region related zone
// configuration fields.
var ApplyZoneConfigForMultiRegionTableOptionTableAndIndexes = func(
	zc zonepb.ZoneConfig,
	regionConfig multiregion.RegionConfig,
	table catalog.TableDescriptor,
) (bool, zonepb.ZoneConfig, error) {
	localityConfig := *table.GetLocalityConfig()
	localityZoneConfig, err := zoneConfigForMultiRegionTable(
		localityConfig,
		regionConfig,
	)
	if err != nil {
		return false, zonepb.ZoneConfig{}, err
	}

	// Wipe out the subzone multi-region fields before we copy over the
	// multi-region fields to the zone config down below. We have to do this to
	// handle the case where users have set a zone config on an index and we're
	// ALTERing to a table locality that doesn't lay down index zone
	// configurations (e.g. GLOBAL or REGIONAL BY TABLE). Since the user will
	// have to override to perform the ALTER, we want to wipe out the index
	// zone config so that the user won't have to override again the next time
	// the want to ALTER the table locality.
	zc.ClearFieldsOfAllSubzones(zonepb.MultiRegionZoneConfigFields)

	zc.CopyFromZone(*localityZoneConfig, zonepb.MultiRegionZoneConfigFields)

	hasNewSubzones := table.IsLocalityRegionalByRow()
	if hasNewSubzones {
		for _, region := range regionConfig.Regions() {
			subzoneConfig, err := zoneConfigForMultiRegionPartition(region, regionConfig)
			if err != nil {
				return false, zc, err
			}
			for _, idx := range table.NonDropIndexes() {
				zc.SetSubzone(zonepb.Subzone{
					IndexID:       uint32(idx.GetID()),
					PartitionName: string(region),
					Config:        subzoneConfig,
				})
			}
		}
	}
	return hasNewSubzones, zc, nil
}

// applyZoneConfigForMultiRegionTableOptionRemoveGlobalZoneConfig signals
// to remove the global zone configuration for a given table.
var applyZoneConfigForMultiRegionTableOptionRemoveGlobalZoneConfig = func(
	zc zonepb.ZoneConfig,
	regionConfig multiregion.RegionConfig,
	table catalog.TableDescriptor,
) (bool, zonepb.ZoneConfig, error) {
	zc.CopyFromZone(*zonepb.NewZoneConfig(), zonepb.MultiRegionZoneConfigFields)
	return false, zc, nil
}

// ApplyZoneConfigForMultiRegionTable applies zone config settings based
// on the options provided.
func ApplyZoneConfigForMultiRegionTable(
	ctx context.Context,
	txn *kv.Txn,
	execCfg *ExecutorConfig,
	regionConfig multiregion.RegionConfig,
	table catalog.TableDescriptor,
	opts ...applyZoneConfigForMultiRegionTableOption,
) error {
	tableID := table.GetID()
	currentZoneConfig, err := getZoneConfigRaw(ctx, txn, execCfg.Codec, tableID)
	if err != nil {
		return err
	}
	newZoneConfig := *zonepb.NewZoneConfig()
	if currentZoneConfig != nil {
		newZoneConfig = *currentZoneConfig
	}

	var hasNewSubzones bool
	for _, opt := range opts {
		newHasNewSubzones, modifiedNewZoneConfig, err := opt(
			newZoneConfig,
			regionConfig,
			table,
		)
		if err != nil {
			return err
		}
		hasNewSubzones = newHasNewSubzones || hasNewSubzones
		newZoneConfig = modifiedNewZoneConfig
	}

	// Mark the NumReplicas as 0 if we have subzones but no other features
	// in the zone config. This signifies a placeholder.
	// Note we do not use hasNewSubzones here as there may be existing subzones
	// on the zone config which may still be a placeholder.
	if isPlaceholderZoneConfigForMultiRegion(newZoneConfig) {
		newZoneConfig.NumReplicas = proto.Int32(0)
	}

	// Determine if we're rewriting or deleting the zone configuration.
	newZoneConfigIsEmpty := newZoneConfig.Equal(zonepb.NewZoneConfig())
	currentZoneConfigIsEmpty := currentZoneConfig.Equal(zonepb.NewZoneConfig())
	rewriteZoneConfig := !newZoneConfigIsEmpty
	deleteZoneConfig := newZoneConfigIsEmpty && !currentZoneConfigIsEmpty

	if rewriteZoneConfig {
		if err := newZoneConfig.Validate(); err != nil {
			return pgerror.Newf(
				pgcode.CheckViolation,
				"could not validate zone config: %v",
				err,
			)
		}
		if err := newZoneConfig.ValidateTandemFields(); err != nil {
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
			&newZoneConfig,
			execCfg,
			hasNewSubzones,
		); err != nil {
			return err
		}
	} else if deleteZoneConfig {
		// Delete the zone configuration if it exists but the new zone config is
		// blank.
		if _, err = execCfg.InternalExecutor.Exec(
			ctx,
			"delete-zone-multiregion-table",
			txn,
			"DELETE FROM system.zones WHERE id = $1",
			tableID,
		); err != nil {
			return err
		}
	}
	return nil
}

// ApplyZoneConfigFromDatabaseRegionConfig applies a zone configuration to the
// database using the information in the supplied RegionConfig.
func ApplyZoneConfigFromDatabaseRegionConfig(
	ctx context.Context,
	dbID descpb.ID,
	regionConfig multiregion.RegionConfig,
	txn *kv.Txn,
	execConfig *ExecutorConfig,
) error {
	// Build a zone config based on the RegionConfig information.
	dbZoneConfig, err := zoneConfigForMultiRegionDatabase(regionConfig)
	if err != nil {
		return err
	}
	return applyZoneConfigForMultiRegionDatabase(
		ctx,
		dbID,
		dbZoneConfig,
		txn,
		execConfig,
	)
}

// discardMultiRegionFieldsForDatabaseZoneConfig resets the multi-region zone
// config fields for a multi-region database.
func discardMultiRegionFieldsForDatabaseZoneConfig(
	ctx context.Context, dbID descpb.ID, txn *kv.Txn, execConfig *ExecutorConfig,
) error {
	// Merge with an empty zone config.
	return applyZoneConfigForMultiRegionDatabase(
		ctx,
		dbID,
		*zonepb.NewZoneConfig(),
		txn,
		execConfig,
	)
}

func applyZoneConfigForMultiRegionDatabase(
	ctx context.Context,
	dbID descpb.ID,
	mergeZoneConfig zonepb.ZoneConfig,
	txn *kv.Txn,
	execConfig *ExecutorConfig,
) error {
	currentZoneConfig, err := getZoneConfigRaw(ctx, txn, execConfig.Codec, dbID)
	if err != nil {
		return err
	}
	newZoneConfig := *zonepb.NewZoneConfig()
	if currentZoneConfig != nil {
		newZoneConfig = *currentZoneConfig
	}
	newZoneConfig.CopyFromZone(
		mergeZoneConfig,
		zonepb.MultiRegionZoneConfigFields,
	)
	// If the new zone config is the same as a blank zone config, delete it.
	if newZoneConfig.Equal(zonepb.NewZoneConfig()) {
		_, err = execConfig.InternalExecutor.Exec(
			ctx,
			"delete-zone-multiregion-database",
			txn,
			"DELETE FROM system.zones WHERE id = $1",
			dbID,
		)
		return err
	}
	if _, err := writeZoneConfig(
		ctx,
		txn,
		dbID,
		nil, /* table */
		&newZoneConfig,
		execConfig,
		false, /* hasNewSubzones */
	); err != nil {
		return err
	}
	return nil
}

// updateZoneConfigsForAllTables loops through all of the tables in the
// specified database and refreshes the zone configs for all tables.
func (p *planner) updateZoneConfigsForAllTables(ctx context.Context, desc *dbdesc.Mutable) error {
	return p.forEachMutableTableInDatabase(
		ctx,
		desc,
		func(ctx context.Context, scName string, tbDesc *tabledesc.Mutable) error {
			regionConfig, err := SynthesizeRegionConfig(ctx, p.txn, desc.ID, p.Descriptors())
			if err != nil {
				return err
			}
			return ApplyZoneConfigForMultiRegionTable(
				ctx,
				p.txn,
				p.ExecCfg(),
				regionConfig,
				tbDesc,
				ApplyZoneConfigForMultiRegionTableOptionTableAndIndexes,
			)
		},
	)
}

// maybeInitializeMultiRegionDatabase initializes a multi-region database if
// there is a region config on the database descriptor and serves as a
// pass-through otherwise.
// Initializing a multi-region database involves creating the multi-region enum
// seeded with the given regionNames and applying the database-level zone
// configurations.
func (p *planner) maybeInitializeMultiRegionDatabase(
	ctx context.Context, desc *dbdesc.Mutable, regionConfig *multiregion.RegionConfig,
) error {
	// If the database is not a multi-region database, there's no work to be done.
	if !desc.IsMultiRegion() {
		return nil
	}

	// Create the multi-region enum.
	regionLabels := make(tree.EnumValueList, 0, len(regionConfig.Regions()))
	for _, regionName := range regionConfig.Regions() {
		regionLabels = append(regionLabels, tree.EnumValue(regionName))
	}

	if err := p.createEnumWithID(
		p.RunParams(ctx),
		regionConfig.RegionEnumID(),
		regionLabels,
		desc,
		tree.NewQualifiedTypeName(desc.Name, tree.PublicSchema, tree.RegionEnum),
		enumTypeMultiRegion,
	); err != nil {
		return err
	}

	// Create the database-level zone configuration.
	if err := ApplyZoneConfigFromDatabaseRegionConfig(
		ctx,
		desc.ID,
		*regionConfig,
		p.txn,
		p.execCfg); err != nil {
		return err
	}

	return nil
}

// partitionByForRegionalByRow constructs the tree.PartitionBy clause for
// REGIONAL BY ROW tables.
func partitionByForRegionalByRow(
	regionConfig multiregion.RegionConfig, col tree.Name,
) *tree.PartitionBy {
	listPartition := make([]tree.ListPartition, len(regionConfig.Regions()))
	for i, region := range regionConfig.Regions() {
		listPartition[i] = tree.ListPartition{
			Name:  tree.UnrestrictedName(region),
			Exprs: tree.Exprs{tree.NewStrVal(string(region))},
		}
	}

	return &tree.PartitionBy{
		Fields: tree.NameList{col},
		List:   listPartition,
	}
}

// ValidateAllMultiRegionZoneConfigsInCurrentDatabase is part of the tree.EvalDatabase interface.
func (p *planner) ValidateAllMultiRegionZoneConfigsInCurrentDatabase(ctx context.Context) error {
	dbDesc, err := p.Descriptors().GetImmutableDatabaseByName(
		p.EvalContext().Ctx(),
		p.txn,
		p.CurrentDatabase(),
		tree.DatabaseLookupFlags{
			Required: true,
		},
	)
	if err != nil {
		return err
	}
	if !dbDesc.IsMultiRegion() {
		return nil
	}
	regionConfig, err := SynthesizeRegionConfig(
		ctx,
		p.txn,
		dbDesc.GetID(),
		p.Descriptors(),
		SynthesizeRegionConfigOptionForValidation,
	)
	if err != nil {
		return err
	}
	return p.validateAllMultiRegionZoneConfigsInDatabase(
		ctx,
		dbDesc,
		&zoneConfigForMultiRegionValidatorValidation{
			zoneConfigForMultiRegionValidatorExistingMultiRegionObject: zoneConfigForMultiRegionValidatorExistingMultiRegionObject{
				regionConfig: regionConfig,
			},
		},
	)
}

func (p *planner) validateAllMultiRegionZoneConfigsInDatabase(
	ctx context.Context,
	dbDesc catalog.DatabaseDescriptor,
	zoneConfigForMultiRegionValidator zoneConfigForMultiRegionValidator,
) error {
	var ids []descpb.ID
	if err := p.forEachMutableTableInDatabase(
		ctx,
		dbDesc,
		func(ctx context.Context, scName string, tbDesc *tabledesc.Mutable) error {
			ids = append(ids, tbDesc.GetID())
			return nil
		},
	); err != nil {
		return err
	}
	ids = append(ids, dbDesc.GetID())

	zoneConfigs, err := getZoneConfigRawBatch(
		ctx,
		p.txn,
		p.ExecCfg().Codec,
		ids,
	)
	if err != nil {
		return err
	}

	if err := p.validateZoneConfigForMultiRegionDatabase(
		dbDesc,
		zoneConfigs[dbDesc.GetID()],
		zoneConfigForMultiRegionValidator,
	); err != nil {
		return err
	}

	return p.forEachMutableTableInDatabase(
		ctx,
		dbDesc,
		func(ctx context.Context, scName string, tbDesc *tabledesc.Mutable) error {
			return p.validateZoneConfigForMultiRegionTable(
				tbDesc,
				zoneConfigs[tbDesc.GetID()],
				zoneConfigForMultiRegionValidator,
			)
		},
	)
}

// CurrentDatabaseRegionConfig is part of the tree.EvalDatabase interface.
// CurrentDatabaseRegionConfig uses the cache to synthesize the RegionConfig
// and as such is intended for DML use. It returns nil
// if the current database is not multi-region enabled.
func (p *planner) CurrentDatabaseRegionConfig(
	ctx context.Context,
) (tree.DatabaseRegionConfig, error) {
	dbDesc, err := p.Descriptors().GetImmutableDatabaseByName(
		p.EvalContext().Ctx(),
		p.txn,
		p.CurrentDatabase(),
		tree.DatabaseLookupFlags{
			Required: true,
		},
	)
	if err != nil {
		return nil, err
	}

	if !dbDesc.IsMultiRegion() {
		return nil, nil
	}

	return SynthesizeRegionConfig(
		ctx,
		p.txn,
		dbDesc.GetID(),
		p.Descriptors(),
		SynthesizeRegionConfigOptionUseCache,
	)
}

type synthesizeRegionConfigOptions struct {
	includeOffline bool
	forValidation  bool
	useCache       bool
}

// SynthesizeRegionConfigOption is an option to pass into SynthesizeRegionConfig.
type SynthesizeRegionConfigOption func(o *synthesizeRegionConfigOptions)

// SynthesizeRegionConfigOptionIncludeOffline includes offline descriptors for use
// in RESTORE.
var SynthesizeRegionConfigOptionIncludeOffline SynthesizeRegionConfigOption = func(o *synthesizeRegionConfigOptions) {
	o.includeOffline = true
}

// SynthesizeRegionConfigOptionForValidation includes descriptors which are being dropped
// as part of the regions field, allowing validation to account for regions in the
// process of being dropped.
var SynthesizeRegionConfigOptionForValidation SynthesizeRegionConfigOption = func(o *synthesizeRegionConfigOptions) {
	o.forValidation = true
}

// SynthesizeRegionConfigOptionUseCache uses a cache for synthesizing the region
// config.
var SynthesizeRegionConfigOptionUseCache SynthesizeRegionConfigOption = func(o *synthesizeRegionConfigOptions) {
	o.useCache = true
}

// SynthesizeRegionConfig returns a RegionConfig representing the user
// configured state of a multi-region database by coalescing state from both
// the database descriptor and multi-region type descriptor. By default, it
// avoids the cache and is intended for use by DDL statements.
//
// TODO(ajwerner): Refactor this to take the database descriptor rather than
// the database ID.
func SynthesizeRegionConfig(
	ctx context.Context,
	txn *kv.Txn,
	dbID descpb.ID,
	descsCol *descs.Collection,
	opts ...SynthesizeRegionConfigOption,
) (multiregion.RegionConfig, error) {
	var o synthesizeRegionConfigOptions
	for _, opt := range opts {
		opt(&o)
	}

	regionConfig := multiregion.RegionConfig{}
	_, dbDesc, err := descsCol.GetImmutableDatabaseByID(ctx, txn, dbID, tree.DatabaseLookupFlags{
		AvoidCached:    !o.useCache,
		Required:       true,
		IncludeOffline: o.includeOffline,
	})
	if err != nil {
		return regionConfig, err
	}

	regionEnumID, err := dbDesc.MultiRegionEnumID()
	if err != nil {
		return regionConfig, err
	}

	regionEnum, err := descsCol.GetImmutableTypeByID(
		ctx,
		txn,
		regionEnumID,
		tree.ObjectLookupFlags{
			CommonLookupFlags: tree.CommonLookupFlags{
				AvoidCached:    !o.useCache,
				IncludeOffline: o.includeOffline,
			},
		},
	)
	if err != nil {
		return multiregion.RegionConfig{}, err
	}

	var regionNames descpb.RegionNames
	if o.forValidation {
		regionNames, err = regionEnum.RegionNamesForValidation()
	} else {
		regionNames, err = regionEnum.RegionNames()
	}
	if err != nil {
		return regionConfig, err
	}

	transitioningRegionNames, err := regionEnum.TransitioningRegionNames()
	if err != nil {
		return regionConfig, err
	}

	regionConfig = multiregion.MakeRegionConfig(
		regionNames,
		dbDesc.GetRegionConfig().PrimaryRegion,
		dbDesc.GetRegionConfig().SurvivalGoal,
		regionEnumID,
		multiregion.WithTransitioningRegions(transitioningRegionNames),
	)

	if err := multiregion.ValidateRegionConfig(regionConfig); err != nil {
		return multiregion.RegionConfig{}, err
	}

	return regionConfig, nil
}

// blockDiscardOfZoneConfigForMultiRegionObject determines if discarding the
// zone configuration of a multi-region table, index or partition should be
// blocked. We only block the discard if the multi-region abstractions have
// created the zone configuration. Note that this function relies on internal
// knowledge of which table locality patterns write zone configurations. We do
// things this way to avoid having to read the zone configurations directly and
// do a more explicit comparison (with a generated zone configuration). If, down
// the road, the rules around writing zone configurations change, the tests in
// multi_region_zone_configs will fail and this function will need updating.
func blockDiscardOfZoneConfigForMultiRegionObject(
	zs tree.ZoneSpecifier, tblDesc catalog.TableDescriptor,
) (bool, error) {
	isIndex := zs.TableOrIndex.Index != ""
	isPartition := zs.Partition != ""

	if isPartition {
		// Multi-region abstractions only set partition-level zone configs for
		// REGIONAL BY ROW tables.
		if tblDesc.IsLocalityRegionalByRow() {
			return true, nil
		}
	} else if isIndex {
		// Multi-region will never set a zone config on an index, so no need to
		// error if the user wants to drop the index zone config.
		return false, nil
	} else {
		// It's a table zone config that the user is trying to discard. This
		// should only be present on GLOBAL and REGIONAL BY TABLE tables in a
		// specified region.
		if tblDesc.IsLocalityGlobal() {
			return true, nil
		} else if tblDesc.IsLocalityRegionalByTable() {
			if tblDesc.GetLocalityConfig().GetRegionalByTable().Region != nil &&
				tree.Name(*tblDesc.GetLocalityConfig().GetRegionalByTable().Region) !=
					tree.PrimaryRegionNotSpecifiedName {
				return true, nil
			}
		} else if tblDesc.IsLocalityRegionalByRow() {
			// For REGIONAL BY ROW tables, no need to error if we're setting a
			// table level zone config.
			return false, nil
		} else {
			return false, errors.AssertionFailedf(
				"unknown table locality: %v",
				tblDesc.GetLocalityConfig(),
			)
		}
	}
	return false, nil
}

// CheckZoneConfigChangePermittedForMultiRegion checks if a zone config
// change is permitted for a multi-region database, table, index or partition.
// The change is permitted iff it is not modifying a protected multi-region
// field of the zone configs (as defined by zonepb.MultiRegionZoneConfigFields).
func (p *planner) CheckZoneConfigChangePermittedForMultiRegion(
	ctx context.Context, zs tree.ZoneSpecifier, options tree.KVOptions,
) error {
	// If the user has specified that they're overriding, then the world is
	// their oyster.
	if p.SessionData().OverrideMultiRegionZoneConfigEnabled {
		// Note that we increment the telemetry counter unconditionally here.
		// It's possible that this will lead to over-counting as the user may
		// have left the override on and is now updating a zone configuration
		// that is not protected by the multi-region abstractions. To get finer
		// grained counting however, would be more difficult to code, and may
		// not even prove to be that valuable, so we have decided to live with
		// the potential for over-counting.
		telemetry.Inc(sqltelemetry.OverrideMultiRegionZoneConfigurationUser)
		return nil
	}

	var err error
	var tblDesc catalog.TableDescriptor
	isDB := false
	// Check if what we're altering is a multi-region entity.
	if zs.Database != "" {
		isDB = true
		dbDesc, err := p.Descriptors().GetImmutableDatabaseByName(
			ctx,
			p.txn,
			string(zs.Database),
			tree.DatabaseLookupFlags{Required: true},
		)
		if err != nil {
			return err
		}
		if dbDesc.GetRegionConfig() == nil {
			// Not a multi-region database, we're done here.
			return nil
		}
	} else {
		// We're dealing with a table, index, or partition zone configuration
		// change.  Get the table descriptor so we can determine if this is a
		// multi-region table/index/partition.
		tblDesc, err = p.resolveTableForZone(ctx, &zs)
		if err != nil {
			return err
		}
		if tblDesc == nil || tblDesc.GetLocalityConfig() == nil {
			// Not a multi-region table, we're done here.
			return nil
		}
	}

	hint := "to override this error, SET override_multi_region_zone_config = true and reissue the command"

	// The request is to discard the zone configuration. Error in cases where
	// the zone configuration being discarded was created by the multi-region
	// abstractions.
	if options == nil {
		needToError := false
		// Determine if this zone config that we're trying to discard is
		// supposed to be there.
		if isDB {
			needToError = true
		} else {
			needToError, err = blockDiscardOfZoneConfigForMultiRegionObject(zs, tblDesc)
			if err != nil {
				return err
			}
		}

		if needToError {
			// User is trying to update a zone config value that's protected for
			// multi-region databases. Return the constructed error.
			err := errors.WithDetail(errors.Newf(
				"attempting to discard the zone configuration of a multi-region entity"),
				"discarding a multi-region zone configuration may result in sub-optimal performance or behavior",
			)
			return errors.WithHint(err, hint)
		}
	}

	// This is clearly an n^2 operation, but since there are only a single
	// digit number of zone config keys, it's likely faster to do it this way
	// than incur the memory allocation of creating a map.
	for _, opt := range options {
		for _, cfg := range zonepb.MultiRegionZoneConfigFields {
			if opt.Key == cfg {
				// User is trying to update a zone config value that's protected for
				// multi-region databases. Return the constructed error.
				err := errors.Newf("attempting to modify protected field %q of a multi-region zone configuration",
					string(opt.Key),
				)
				return errors.WithHint(err, hint)
			}
		}
	}

	return nil
}

// zoneConfigForMultiRegionValidator is an interface representing
// actions to take when validating a zone config for multi-region
// purposes.
type zoneConfigForMultiRegionValidator interface {
	getExpectedDatabaseZoneConfig() (zonepb.ZoneConfig, error)
	getExpectedTableZoneConfig(desc catalog.TableDescriptor) (zonepb.ZoneConfig, error)
	transitioningRegions() descpb.RegionNames

	newMismatchFieldError(descType string, descName string, field string) error
	newMissingSubzoneError(descType string, descName string, field string) error
	newExtraSubzoneError(descType string, descName string, field string) error
}

// zoneConfigForMultiRegionValidatorSetInitialRegion implements
// interface zoneConfigForMultiRegionValidator.
type zoneConfigForMultiRegionValidatorSetInitialRegion struct{}

var _ zoneConfigForMultiRegionValidator = (*zoneConfigForMultiRegionValidatorSetInitialRegion)(nil)

func (v *zoneConfigForMultiRegionValidatorSetInitialRegion) getExpectedDatabaseZoneConfig() (
	zonepb.ZoneConfig,
	error,
) {
	// For set initial region, we want no multi-region fields to be set.
	return *zonepb.NewZoneConfig(), nil
}

func (v *zoneConfigForMultiRegionValidatorSetInitialRegion) transitioningRegions() descpb.RegionNames {
	// There are no transitioning regions at setup time.
	return nil
}

func (v *zoneConfigForMultiRegionValidatorSetInitialRegion) getExpectedTableZoneConfig(
	desc catalog.TableDescriptor,
) (zonepb.ZoneConfig, error) {
	// For set initial region, we want no multi-region fields to be set.
	return *zonepb.NewZoneConfig(), nil
}

func (v *zoneConfigForMultiRegionValidatorSetInitialRegion) wrapErr(err error) error {
	// We currently do not allow "inherit from parent" behavior, so one must
	// discard the zone config before continuing.
	// COPY FROM PARENT copies the value but does not inherit.
	// This can be replaced with the override session variable hint when it is
	// available.
	return errors.WithHintf(
		err,
		"discard the zone config using CONFIGURE ZONE DISCARD before continuing",
	)
}

func (v *zoneConfigForMultiRegionValidatorSetInitialRegion) newMismatchFieldError(
	descType string, descName string, field string,
) error {
	return v.wrapErr(
		pgerror.Newf(
			pgcode.InvalidObjectDefinition,
			"zone configuration for %s %s has field %q set which will be overwritten when setting the the initial PRIMARY REGION",
			descType,
			descName,
			field,
		),
	)
}

func (v *zoneConfigForMultiRegionValidatorSetInitialRegion) newMissingSubzoneError(
	descType string, descName string, field string,
) error {
	// There can never be a missing subzone as we only compare against
	// blank zone configs.
	return errors.AssertionFailedf(
		"unexpected missing subzone for %s %s",
		descType,
		descName,
	)
}

func (v *zoneConfigForMultiRegionValidatorSetInitialRegion) newExtraSubzoneError(
	descType string, descName string, field string,
) error {
	return v.wrapErr(
		pgerror.Newf(
			pgcode.InvalidObjectDefinition,
			"zone configuration for %s %s has field %q set which will be overwritten when setting the initial PRIMARY REGION",
			descType,
			descName,
			field,
		),
	)
}

// zoneConfigForMultiRegionValidatorExistingMultiRegionObject partially implements
// the zoneConfigForMultiRegionValidator interface.
type zoneConfigForMultiRegionValidatorExistingMultiRegionObject struct {
	regionConfig multiregion.RegionConfig
}

func (v *zoneConfigForMultiRegionValidatorExistingMultiRegionObject) getExpectedDatabaseZoneConfig() (
	zonepb.ZoneConfig,
	error,
) {
	return zoneConfigForMultiRegionDatabase(v.regionConfig)
}

func (v *zoneConfigForMultiRegionValidatorExistingMultiRegionObject) getExpectedTableZoneConfig(
	desc catalog.TableDescriptor,
) (zonepb.ZoneConfig, error) {
	_, expectedZoneConfig, err := ApplyZoneConfigForMultiRegionTableOptionTableAndIndexes(
		*zonepb.NewZoneConfig(),
		v.regionConfig,
		desc,
	)
	if err != nil {
		return zonepb.ZoneConfig{}, err
	}
	return expectedZoneConfig, err
}

func (v *zoneConfigForMultiRegionValidatorExistingMultiRegionObject) transitioningRegions() descpb.RegionNames {
	return v.regionConfig.TransitioningRegions()
}

// zoneConfigForMultiRegionValidatorModifiedByUser implements
// interface zoneConfigForMultiRegionValidator.
type zoneConfigForMultiRegionValidatorModifiedByUser struct {
	zoneConfigForMultiRegionValidatorExistingMultiRegionObject
}

var _ zoneConfigForMultiRegionValidator = (*zoneConfigForMultiRegionValidatorModifiedByUser)(nil)

func (v *zoneConfigForMultiRegionValidatorModifiedByUser) newMismatchFieldError(
	descType string, descName string, field string,
) error {
	return v.wrapErr(
		pgerror.Newf(
			pgcode.InvalidObjectDefinition,
			"attempting to update zone configuration for %s %s which contains modified field %q",
			descType,
			descName,
			field,
		),
	)
}

func (v *zoneConfigForMultiRegionValidatorModifiedByUser) wrapErr(err error) error {
	err = errors.WithDetail(
		err,
		"the attempted operation will overwrite a user modified field",
	)
	return errors.WithHint(
		err,
		"to proceed with the overwrite, SET override_multi_region_zone_config = true, "+
			"and reissue the statement",
	)
}

func (v *zoneConfigForMultiRegionValidatorModifiedByUser) newMissingSubzoneError(
	descType string, descName string, field string,
) error {
	return v.wrapErr(
		pgerror.Newf(
			pgcode.InvalidObjectDefinition,
			"attempting to update zone config which is missing an expected zone configuration for %s %s",
			descType,
			descName,
		),
	)
}

func (v *zoneConfigForMultiRegionValidatorModifiedByUser) newExtraSubzoneError(
	descType string, descName string, field string,
) error {
	return v.wrapErr(
		pgerror.Newf(
			pgcode.InvalidObjectDefinition,
			"attempting to update zone config which contains an extra zone configuration for %s %s with field %s populated",
			descType,
			descName,
			field,
		),
	)
}

// zoneConfigForMultiRegionValidatorValidation implements
// interface zoneConfigForMultiRegionValidator.
type zoneConfigForMultiRegionValidatorValidation struct {
	zoneConfigForMultiRegionValidatorExistingMultiRegionObject
}

var _ zoneConfigForMultiRegionValidator = (*zoneConfigForMultiRegionValidatorValidation)(nil)

func (v *zoneConfigForMultiRegionValidatorValidation) newMismatchFieldError(
	descType string, descName string, field string,
) error {
	return pgerror.Newf(
		pgcode.InvalidObjectDefinition,
		"zone configuration for %s %s contains incorrectly configured field %q",
		descType,
		descName,
		field,
	)
}

func (v *zoneConfigForMultiRegionValidatorValidation) newMissingSubzoneError(
	descType string, descName string, field string,
) error {
	return pgerror.Newf(
		pgcode.InvalidObjectDefinition,
		"missing zone configuration for %s %s",
		descType,
		descName,
	)
}

func (v *zoneConfigForMultiRegionValidatorValidation) newExtraSubzoneError(
	descType string, descName string, field string,
) error {
	return pgerror.Newf(
		pgcode.InvalidObjectDefinition,
		"extraneous zone configuration for %s %s with field %s populated",
		descType,
		descName,
		field,
	)
}

// validateZoneConfigForMultiRegionDatabaseWasNotModifiedByUser validates that
// the zone configuration was not modified by the user. The function is intended
// to be called in cases where a multi-region operation will overwrite the
// database zone configuration and we wish to warn the user about that before
// it occurs (and require the FORCE option to proceed).
func (p *planner) validateZoneConfigForMultiRegionDatabaseWasNotModifiedByUser(
	ctx context.Context, dbDesc catalog.DatabaseDescriptor,
) error {
	// If the user is overriding, our work here is done.
	if p.SessionData().OverrideMultiRegionZoneConfigEnabled {
		telemetry.Inc(sqltelemetry.OverrideMultiRegionDatabaseZoneConfigurationSystem)
		return nil
	}
	currentZoneConfig, err := getZoneConfigRaw(ctx, p.txn, p.ExecCfg().Codec, dbDesc.GetID())
	if err != nil {
		return err
	}
	regionConfig, err := SynthesizeRegionConfig(
		ctx,
		p.txn,
		dbDesc.GetID(),
		p.Descriptors(),
		SynthesizeRegionConfigOptionForValidation,
	)
	if err != nil {
		return err
	}
	return p.validateZoneConfigForMultiRegionDatabase(
		dbDesc,
		currentZoneConfig,
		&zoneConfigForMultiRegionValidatorModifiedByUser{
			zoneConfigForMultiRegionValidatorExistingMultiRegionObject: zoneConfigForMultiRegionValidatorExistingMultiRegionObject{
				regionConfig: regionConfig,
			},
		},
	)
}

// validateZoneConfigForMultiRegionDatabase validates that the zone config
// for the databases matches as the multi-region database definition.
func (p *planner) validateZoneConfigForMultiRegionDatabase(
	dbDesc catalog.DatabaseDescriptor,
	currentZoneConfig *zonepb.ZoneConfig,
	zoneConfigForMultiRegionValidator zoneConfigForMultiRegionValidator,
) error {
	if currentZoneConfig == nil {
		currentZoneConfig = zonepb.NewZoneConfig()
	}
	expectedZoneConfig, err := zoneConfigForMultiRegionValidator.getExpectedDatabaseZoneConfig()
	if err != nil {
		return err
	}

	same, mismatch, err := currentZoneConfig.DiffWithZone(
		expectedZoneConfig,
		zonepb.MultiRegionZoneConfigFields,
	)
	if err != nil {
		return err
	}
	if !same {
		dbName := tree.Name(dbDesc.GetName())
		return zoneConfigForMultiRegionValidator.newMismatchFieldError(
			"database",
			dbName.String(),
			mismatch.Field,
		)
	}

	return nil
}

// validateZoneConfigForMultiRegionTableWasNotModifiedByUser validates that
// the table's zone configuration was not modified by the user. The function is
// intended to be called in cases where a multi-region operation will overwrite
// the table's (or index's/partition's) zone configuration and we wish to warn
// the user about that before it occurs (and require the
// override_multi_region_zone_config session variable to be set).
func (p *planner) validateZoneConfigForMultiRegionTableWasNotModifiedByUser(
	ctx context.Context, dbDesc catalog.DatabaseDescriptor, desc *tabledesc.Mutable,
) error {
	// If the user is overriding, or this is not a multi-region table our work here
	// is done.
	if p.SessionData().OverrideMultiRegionZoneConfigEnabled || desc.GetLocalityConfig() == nil {
		telemetry.Inc(sqltelemetry.OverrideMultiRegionTableZoneConfigurationSystem)
		return nil
	}
	currentZoneConfig, err := getZoneConfigRaw(ctx, p.txn, p.ExecCfg().Codec, desc.GetID())
	if err != nil {
		return err
	}
	regionConfig, err := SynthesizeRegionConfig(
		ctx,
		p.txn,
		dbDesc.GetID(),
		p.Descriptors(),
		SynthesizeRegionConfigOptionForValidation,
	)
	if err != nil {
		return err
	}

	return p.validateZoneConfigForMultiRegionTable(
		desc,
		currentZoneConfig,
		&zoneConfigForMultiRegionValidatorModifiedByUser{
			zoneConfigForMultiRegionValidatorExistingMultiRegionObject: zoneConfigForMultiRegionValidatorExistingMultiRegionObject{
				regionConfig: regionConfig,
			},
		},
	)
}

// validateZoneConfigForMultiRegionTableOptions validates that
// the multi-region fields of the table's zone configuration
// matches what is expected for the given table.
func (p *planner) validateZoneConfigForMultiRegionTable(
	desc catalog.TableDescriptor,
	currentZoneConfig *zonepb.ZoneConfig,
	zoneConfigForMultiRegionValidator zoneConfigForMultiRegionValidator,
) error {
	if currentZoneConfig == nil {
		currentZoneConfig = zonepb.NewZoneConfig()
	}

	tableName := tree.Name(desc.GetName())

	expectedZoneConfig, err := zoneConfigForMultiRegionValidator.getExpectedTableZoneConfig(
		desc,
	)
	if err != nil {
		return err
	}

	// When there is a transition to/from REGIONAL BY ROW, the new indexes
	// being set up will have zone configs which mismatch with the old
	// table locality config. As we validate against the old table locality
	// config (as the new indexes are not swapped in yet), exclude these
	// indexes from any zone configuration validation.
	regionalByRowNewIndexes := make(map[uint32]struct{})
	for _, mut := range desc.AllMutations() {
		if pkSwap := mut.AsPrimaryKeySwap(); pkSwap != nil {
			if pkSwap.HasLocalityConfig() {
				_ = pkSwap.ForEachNewIndexIDs(func(id descpb.IndexID) error {
					regionalByRowNewIndexes[uint32(id)] = struct{}{}
					return nil
				})
			}
			// There can only be one pkSwap at a time, so break now.
			break
		}
	}

	// Some transitioning subzones may remain on the zone configuration until it is cleaned up
	// at a later step. Remove these as well as the regional by row new indexes.
	subzoneIndexIDsToDiff := make(map[uint32]tree.Name, len(desc.NonDropIndexes()))
	for _, idx := range desc.NonDropIndexes() {
		if _, ok := regionalByRowNewIndexes[uint32(idx.GetID())]; !ok {
			subzoneIndexIDsToDiff[uint32(idx.GetID())] = tree.Name(idx.GetName())
		}
	}

	// Do not compare partitioning for these regions, as they may be in a
	// transitioning state.
	transitioningRegions := make(map[string]struct{}, len(zoneConfigForMultiRegionValidator.transitioningRegions()))
	for _, transitioningRegion := range zoneConfigForMultiRegionValidator.transitioningRegions() {
		transitioningRegions[string(transitioningRegion)] = struct{}{}
	}

	// We only want to compare against the list of subzones on active indexes
	// and partitions, so filter the subzone list based on the
	// subzoneIndexIDsToDiff computed above.
	filteredCurrentZoneConfigSubzones := currentZoneConfig.Subzones[:0]
	for _, c := range currentZoneConfig.Subzones {
		if c.PartitionName != "" {
			if _, ok := transitioningRegions[c.PartitionName]; ok {
				continue
			}
		}
		if _, ok := subzoneIndexIDsToDiff[c.IndexID]; !ok {
			continue
		}
		filteredCurrentZoneConfigSubzones = append(filteredCurrentZoneConfigSubzones, c)
	}
	currentZoneConfig.Subzones = filteredCurrentZoneConfigSubzones
	// Strip the placeholder status if there are no active subzones on the current
	// zone config.
	if len(filteredCurrentZoneConfigSubzones) == 0 && currentZoneConfig.IsSubzonePlaceholder() {
		currentZoneConfig.NumReplicas = nil
	}

	// Remove regional by row new indexes and transitioning partitions from the expected zone config.
	// These will be incorrect as ApplyZoneConfigForMultiRegionTableOptionTableAndIndexes
	// will apply the existing locality config on them instead of the
	// new locality config.
	filteredExpectedZoneConfigSubzones := expectedZoneConfig.Subzones[:0]
	for _, c := range expectedZoneConfig.Subzones {
		if c.PartitionName != "" {
			if _, ok := transitioningRegions[c.PartitionName]; ok {
				continue
			}
		}
		if _, ok := regionalByRowNewIndexes[c.IndexID]; ok {
			continue
		}
		filteredExpectedZoneConfigSubzones = append(
			filteredExpectedZoneConfigSubzones,
			c,
		)
	}
	expectedZoneConfig.Subzones = filteredExpectedZoneConfigSubzones

	// Mark the expected NumReplicas as 0 if we have a placeholder
	// and the current zone config is also a placeholder.
	// The latter check is required as in cases where non-multiregion fields
	// are set on the current zone config, the expected zone config needs
	// the placeholder marked so that DiffWithZone does not error when
	// num_replicas is expectedly different.
	// e.g. if current zone config has gc.ttlseconds set, then we
	// do not fudge num replicas to be equal to 0 -- otherwise the
	// check fails when num_replicas is different, but that is
	// expected as the current zone config is no longer a placeholder.
	if currentZoneConfig.IsSubzonePlaceholder() && isPlaceholderZoneConfigForMultiRegion(expectedZoneConfig) {
		expectedZoneConfig.NumReplicas = proto.Int32(0)
	}

	// Compare the two zone configs to see if anything is amiss.
	same, mismatch, err := currentZoneConfig.DiffWithZone(
		expectedZoneConfig,
		zonepb.MultiRegionZoneConfigFields,
	)
	if err != nil {
		return err
	}
	if !same {
		descType := "table"
		name := tableName.String()
		if mismatch.IndexID != 0 {
			indexName, ok := subzoneIndexIDsToDiff[mismatch.IndexID]
			if !ok {
				return errors.AssertionFailedf(
					"unexpected unknown index id %d on table %s (mismatch %#v)",
					mismatch.IndexID,
					tableName,
					mismatch,
				)
			}

			if mismatch.PartitionName != "" {
				descType = "partition"
				partitionName := tree.Name(mismatch.PartitionName)
				name = fmt.Sprintf(
					"%s of %s@%s",
					partitionName.String(),
					tableName.String(),
					indexName.String(),
				)
			} else {
				descType = "index"
				name = fmt.Sprintf("%s@%s", tableName.String(), indexName.String())
			}
		}

		if mismatch.IsMissingSubzone {
			return zoneConfigForMultiRegionValidator.newMissingSubzoneError(
				descType,
				name,
				mismatch.Field,
			)
		}
		if mismatch.IsExtraSubzone {
			return zoneConfigForMultiRegionValidator.newExtraSubzoneError(
				descType,
				name,
				mismatch.Field,
			)
		}
		return zoneConfigForMultiRegionValidator.newMismatchFieldError(
			descType,
			name,
			mismatch.Field,
		)
	}

	return nil
}

// checkNoRegionalByRowChangeUnderway checks that no REGIONAL BY ROW
// tables are undergoing a schema change that affect their partitions
// and no tables are transitioning to or from REGIONAL BY ROW.
func (p *planner) checkNoRegionalByRowChangeUnderway(
	ctx context.Context, dbDesc catalog.DatabaseDescriptor,
) error {
	// forEachTableDesc touches all the table keys, which prevents a race
	// with ADD/REGION committing at the same time as the user transaction.
	return p.forEachMutableTableInDatabase(
		ctx,
		dbDesc,
		func(ctx context.Context, scName string, table *tabledesc.Mutable) error {
			wrapErr := func(err error, detailSuffix string) error {
				return errors.WithHintf(
					errors.WithDetailf(
						err,
						"table %s.%s %s",
						tree.Name(scName),
						tree.Name(table.GetName()),
						detailSuffix,
					),
					"cancel the existing job or try again when the change is complete",
				)
			}
			for _, mut := range table.AllMutations() {
				// Disallow any locality related swaps.
				if pkSwap := mut.AsPrimaryKeySwap(); pkSwap != nil {
					if lcSwap := pkSwap.PrimaryKeySwapDesc().LocalityConfigSwap; lcSwap != nil {
						return wrapErr(
							pgerror.Newf(
								pgcode.ObjectNotInPrerequisiteState,
								"cannot perform database region changes while a REGIONAL BY ROW transition is underway",
							),
							"is currently transitioning to or from REGIONAL BY ROW",
						)
					}
					return wrapErr(
						pgerror.Newf(
							pgcode.ObjectNotInPrerequisiteState,
							"cannot perform database region changes while a ALTER PRIMARY KEY is underway",
						),
						"is currently undergoing an ALTER PRIMARY KEY change",
					)
				}
			}
			// Disallow index changes for REGIONAL BY ROW tables.
			// We do this on the second loop, as ALTER PRIMARY KEY may push
			// CREATE/DROP INDEX before the ALTER PRIMARY KEY mutation itself.
			// We should catch ALTER PRIMARY KEY before this ADD/DROP INDEX.
			for _, mut := range table.AllMutations() {
				if table.IsLocalityRegionalByRow() {
					if idx := mut.AsIndex(); idx != nil {
						return wrapErr(
							pgerror.Newf(
								pgcode.ObjectNotInPrerequisiteState,
								"cannot perform database region changes while an index is being created or dropped on a REGIONAL BY ROW table",
							),
							fmt.Sprintf("is currently modifying index %s", tree.Name(idx.GetName())),
						)
					}
				}
			}
			return nil
		},
	)
}

// checkNoRegionChangeUnderway checks whether the regions on the current
// database are currently being modified.
func (p *planner) checkNoRegionChangeUnderway(
	ctx context.Context, dbID descpb.ID, op string,
) error {
	// SynthesizeRegionConfig touches the type descriptor row, which
	// prevents a race with a racing conflicting schema change.
	r, err := SynthesizeRegionConfig(
		ctx,
		p.txn,
		dbID,
		p.Descriptors(),
	)
	if err != nil {
		return err
	}
	if len(r.TransitioningRegions()) > 0 {
		return errors.WithDetailf(
			errors.WithHintf(
				pgerror.Newf(
					pgcode.ObjectNotInPrerequisiteState,
					"cannot %s while a region is being added or dropped on the database",
					op,
				),
				"cancel the job which is adding or dropping the region or try again later",
			),
			"region %s is currently being added or dropped",
			r.TransitioningRegions()[0],
		)
	}
	return nil
}
