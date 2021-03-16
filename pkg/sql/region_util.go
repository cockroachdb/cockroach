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
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/keys"
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
	it, err := p.ExtendedEvalContext().ExecCfg.InternalExecutor.QueryIteratorEx(
		ctx,
		"get_live_cluster_regions",
		p.txn,
		sessiondata.InternalExecutorOverride{
			User: p.SessionData().User(),
		},
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

// CheckLiveClusterRegion checks whether a region can be added to a database
// based on whether the cluster regions are alive.
func CheckLiveClusterRegion(liveClusterRegions LiveClusterRegions, region descpb.RegionName) error {
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
	regionConfig descpb.DatabaseDescriptor_RegionConfig,
) (*zonepb.ZoneConfig, error) {
	numVoters, numReplicas := getNumVotersAndNumReplicas(regionConfig)
	constraints := make([]zonepb.ConstraintsConjunction, len(regionConfig.Regions))
	for i, region := range regionConfig.Regions {
		// Constrain at least 1 (voting or non-voting) replica per region.
		constraints[i] = zonepb.ConstraintsConjunction{
			NumReplicas: 1,
			Constraints: []zonepb.Constraint{makeRequiredConstraintForRegion(region.Name)},
		}
	}

	voterConstraints, err := synthesizeVoterConstraints(regionConfig.PrimaryRegion, regionConfig)
	if err != nil {
		return nil, err
	}

	return &zonepb.ZoneConfig{
		NumReplicas: &numReplicas,
		NumVoters:   &numVoters,
		LeasePreferences: []zonepb.LeasePreference{
			{Constraints: []zonepb.Constraint{makeRequiredConstraintForRegion(regionConfig.PrimaryRegion)}},
		},
		VoterConstraints: voterConstraints,
		Constraints:      constraints,
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
	partitionRegion descpb.DatabaseDescriptor_RegionConfig_Region,
	regionConfig descpb.DatabaseDescriptor_RegionConfig,
) (zonepb.ZoneConfig, error) {
	numVoters, _ := getNumVotersAndNumReplicas(regionConfig)
	zc := zonepb.NewZoneConfig()
	voterConstraints, err := synthesizeVoterConstraints(partitionRegion.Name, regionConfig)
	if err != nil {
		return zonepb.ZoneConfig{}, err
	}
	zc.NumVoters = &numVoters

	zc.InheritedVoterConstraints = false
	zc.VoterConstraints = voterConstraints

	zc.InheritedLeasePreferences = false
	zc.LeasePreferences = []zonepb.LeasePreference{
		{Constraints: []zonepb.Constraint{makeRequiredConstraintForRegion(partitionRegion.Name)}},
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
	regionConfig descpb.DatabaseDescriptor_RegionConfig,
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

	numRegions := int32(len(regionConfig.Regions))
	switch regionConfig.SurvivalGoal {
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
	region descpb.RegionName, regionConfig descpb.DatabaseDescriptor_RegionConfig,
) ([]zonepb.ConstraintsConjunction, error) {
	numVoters, _ := getNumVotersAndNumReplicas(regionConfig)
	switch regionConfig.SurvivalGoal {
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
		return nil, errors.AssertionFailedf("unknown survival goal: %v", regionConfig.SurvivalGoal)
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
	localityConfig descpb.TableDescriptor_LocalityConfig,
	regionConfig descpb.DatabaseDescriptor_RegionConfig,
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

		ret.InheritedVoterConstraints = false
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

// This removes the requirement to only call this function after writeSchemaChange
// is called on creation of tables, and potentially removes the need for ReadingOwnWrites
// for some subcommands.
// Requires some logic to "inherit" from parents.
func applyZoneConfigForMultiRegion(
	ctx context.Context,
	zc *zonepb.ZoneConfig,
	targetID descpb.ID,
	table catalog.TableDescriptor,
	txn *kv.Txn,
	execConfig *ExecutorConfig,
) error {
	// TODO (multiregion): Much like applyZoneConfigForMultiRegionTable we need to
	// merge the zone config that we're writing with anything previously existing
	// in there.
	if _, err := writeZoneConfig(
		ctx,
		txn,
		targetID,
		table,
		zc,
		execConfig,
		true, /* hasNewSubzones */
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

// applyZoneConfigForMultiRegionTableOptionNewIndexes applies table zone configs
// for a newly added index which requires partitioning of individual indexes.
func applyZoneConfigForMultiRegionTableOptionNewIndexes(
	indexIDs ...descpb.IndexID,
) applyZoneConfigForMultiRegionTableOption {
	return func(
		zoneConfig zonepb.ZoneConfig,
		regionConfig descpb.DatabaseDescriptor_RegionConfig,
		table catalog.TableDescriptor,
	) (hasNewSubzones bool, newZoneConfig zonepb.ZoneConfig, err error) {
		for _, indexID := range indexIDs {
			for _, region := range regionConfig.Regions {
				zc, err := zoneConfigForMultiRegionPartition(region, regionConfig)
				if err != nil {
					return false, zoneConfig, err
				}
				zoneConfig.SetSubzone(zonepb.Subzone{
					IndexID:       uint32(indexID),
					PartitionName: string(region.Name),
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
		regionConfig descpb.DatabaseDescriptor_RegionConfig,
		table catalog.TableDescriptor,
	) (hasNewSubzones bool, newZoneConfig zonepb.ZoneConfig, err error) {
		for _, indexID := range indexIDs {
			for _, region := range regionConfig.Regions {
				zoneConfig.DeleteSubzone(uint32(indexID), string(region.Name))
			}
		}
		return false, zoneConfig, nil
	}
}

// isPlaceholderZoneConfigForMultiRegion returns whether a given zone config
// should be marked as a placeholder config for a multi-region object.
// See zonepb.IsSubzonePlaceholder for why this is necessary.
func isPlaceholderZoneConfigForMultiRegion(zc zonepb.ZoneConfig) bool {
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
		regionConfig descpb.DatabaseDescriptor_RegionConfig,
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
	regionConfig descpb.DatabaseDescriptor_RegionConfig,
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
	zc.CopyFromZone(*localityZoneConfig, zonepb.MultiRegionZoneConfigFields)

	hasNewSubzones := table.IsLocalityRegionalByRow()
	if hasNewSubzones {
		for _, region := range regionConfig.Regions {
			subzoneConfig, err := zoneConfigForMultiRegionPartition(region, regionConfig)
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

// ApplyZoneConfigForMultiRegionTable applies zone config settings based
// on the options provided.
func ApplyZoneConfigForMultiRegionTable(
	ctx context.Context,
	txn *kv.Txn,
	execCfg *ExecutorConfig,
	regionConfig descpb.DatabaseDescriptor_RegionConfig,
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
	if len(newZoneConfig.Subzones) > 0 && isPlaceholderZoneConfigForMultiRegion(newZoneConfig) {
		newZoneConfig.NumReplicas = proto.Int32(0)
	}

	// Determine if we're rewriting or deleting the zone configuration.
	newZoneConfigIsEmpty := newZoneConfig.Equal(zonepb.NewZoneConfig())
	currentZoneConfigIsEmpty := currentZoneConfig.Equal(zonepb.NewZoneConfig())
	rewriteZoneConfig := !newZoneConfigIsEmpty
	deleteZoneConfig := newZoneConfigIsEmpty && !currentZoneConfigIsEmpty

	// It's possible at this point that we'll have an empty zone configuration
	// that doesn't look like an empty zone configuration (i.e. it's a placeholder
	// zone config - NumReplicas = 0 - with no subzones set). This can happen if
	// we're ALTERing from REGIONAL BY ROW and dropping all of the subzones for
	// the partitions. If we encounter this, instead of re-writing the zone
	// configuration, we want to delete it.
	numReplicasIsZero := newZoneConfig.NumReplicas != nil && *newZoneConfig.NumReplicas == 0
	if len(newZoneConfig.Subzones) == 0 && numReplicasIsZero {
		rewriteZoneConfig = false
		deleteZoneConfig = true
	}

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

// ApplyZoneConfigFromDatabaseRegionConfig applies a zone configuration to the
// database using the information in the supplied RegionConfig.
func ApplyZoneConfigFromDatabaseRegionConfig(
	ctx context.Context,
	dbID descpb.ID,
	regionConfig descpb.DatabaseDescriptor_RegionConfig,
	txn *kv.Txn,
	execConfig *ExecutorConfig,
) error {
	// Build a zone config based on the RegionConfig information.
	dbZoneConfig, err := zoneConfigForMultiRegionDatabase(regionConfig)
	if err != nil {
		return err
	}
	return applyZoneConfigForMultiRegion(
		ctx,
		dbZoneConfig,
		dbID,
		nil,
		txn,
		execConfig,
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
			return ApplyZoneConfigForMultiRegionTable(
				ctx,
				p.txn,
				p.ExecCfg(),
				*desc.RegionConfig,
				tbDesc,
				ApplyZoneConfigForMultiRegionTableOptionTableAndIndexes,
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
	if err := ApplyZoneConfigFromDatabaseRegionConfig(
		ctx,
		desc.ID,
		*desc.RegionConfig,
		p.txn,
		p.execCfg); err != nil {
		return err
	}

	return nil
}

// partitionByForRegionalByRow constructs the tree.PartitionBy clause for
// REGIONAL BY ROW tables.
func partitionByForRegionalByRow(
	regionConfig descpb.DatabaseDescriptor_RegionConfig, col tree.Name,
) *tree.PartitionBy {
	listPartition := make([]tree.ListPartition, len(regionConfig.Regions))
	for i, region := range regionConfig.Regions {
		listPartition[i] = tree.ListPartition{
			Name:  tree.UnrestrictedName(region.Name),
			Exprs: tree.Exprs{tree.NewStrVal(string(region.Name))},
		}
	}

	return &tree.PartitionBy{
		Fields: tree.NameList{col},
		List:   listPartition,
	}
}

// CurrentDatabaseRegionConfig is part of the tree.EvalDatabase interface.
func (p *planner) CurrentDatabaseRegionConfig() (tree.DatabaseRegionConfig, error) {
	_, dbDesc, err := p.Descriptors().GetImmutableDatabaseByName(
		p.EvalContext().Ctx(),
		p.txn,
		p.CurrentDatabase(),
		tree.DatabaseLookupFlags{Required: true},
	)
	if err != nil {
		return nil, err
	}
	return dbDesc.GetRegionConfig(), nil
}

// CheckZoneConfigChangePermittedForMultiRegion checks if a zone config
// change is permitted for a multi-region database, table, index or partition.
// The change is permitted iff it is not modifying a protected multi-region
// field of the zone configs (as defined by zonepb.MultiRegionZoneConfigFields).
func (p *planner) CheckZoneConfigChangePermittedForMultiRegion(
	ctx context.Context, zs tree.ZoneSpecifier, options tree.KVOptions, force bool,
) error {
	// If the user has specified the FORCE option, the world is their oyster.
	if force {
		return nil
	}

	// Check if what we're altering is a multi-region entity.
	if zs.Database != "" {
		_, dbDesc, err := p.Descriptors().GetImmutableDatabaseByName(
			ctx,
			p.txn,
			string(zs.Database),
			tree.DatabaseLookupFlags{Required: true},
		)
		if err != nil {
			return err
		}
		if dbDesc.RegionConfig == nil {
			// Not a multi-region database, we're done here.
			return nil
		}
	} else {
		// We're dealing with a table, index, or partition zone configuration
		// change.  Get the table descriptor so we can determine if this is a
		// multi-region table/index/partition.
		table, err := p.resolveTableForZone(ctx, &zs)
		if err != nil {
			return err
		}
		if table == nil || table.GetLocalityConfig() == nil {
			// Not a multi-region table, we're done here.
			return nil
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
				return errors.WithHint(err, "to override this error, "+
					"SET override_multi_region_zone_config = true and reissue the command")
			}
		}
	}

	return nil
}

// validateZoneConfigForMultiRegionDatabaseWasNotModifiedByUser validates that
// the zone configuration was not modified by the user. The function is intended
// to be called in cases where a multi-region operation will overwrite the
// database zone configuration and we wish to warn the user about that before
// it occurs (and require the FORCE option to proceed).
func validateZoneConfigForMultiRegionDatabaseWasNotModifiedByUser(
	ctx context.Context,
	dbID descpb.ID,
	dbName string,
	txn *kv.Txn,
	codec keys.SQLCodec,
	override bool,
	regionConfig descpb.DatabaseDescriptor_RegionConfig,
) error {
	// If the user is overriding, our work here is done.
	if override {
		return nil
	}

	expectedZoneConfig, err := zoneConfigForMultiRegionDatabase(regionConfig)
	if err != nil {
		return err
	}

	currentZoneConfig, err := getZoneConfigRaw(ctx, txn, codec, dbID)
	if err != nil {
		return err
	}

	same, field, err := currentZoneConfig.DiffWithZone(*expectedZoneConfig, zonepb.MultiRegionZoneConfigFields)
	if err != nil {
		return err
	}
	if !same {
		err := errors.Newf(
			"attempting to update zone configuration for database %q which contains modified field %q ",
			dbName,
			field,
		)
		err = errors.WithDetail(err, "the attempted operation will overwrite "+
			"a user modified field")
		return errors.WithHint(err, "to override this error and proceed with "+
			"the overwrite, specify \"FORCE\" at the end of the statement")
	}

	return nil
}

// validateZoneConfigForMultiRegionTableWasNotModifiedByUser validates that
// the table's zone configuration was not modified by the user. The function is
// intended to be called in cases where a multi-region operation will overwrite
// the table's (or index's/partition's) zone configuration and we wish to warn
// the user about that before it occurs (and require the
// override_multi_region_zone_config session variable to be set).
func validateZoneConfigForMultiRegionTableWasNotModifiedByUser(
	ctx context.Context,
	txn *kv.Txn,
	execCfg *ExecutorConfig,
	regionConfig descpb.DatabaseDescriptor_RegionConfig,
	desc *tabledesc.Mutable,
	toRegionalByRow bool,
	override bool,
	opts ...applyZoneConfigForMultiRegionTableOption,
) error {
	// If the user is overriding, or this is not a multi-region table our work here
	// is done.
	if override || desc.GetLocalityConfig() == nil {
		return nil
	}

	hint := "to proceed with the override, SET override_multi_region_zone_config = true, and reissue the statement"

	currentZoneConfig, err := getZoneConfigRaw(ctx, txn, execCfg.Codec, desc.GetID())
	if err != nil {
		return err
	}
	if currentZoneConfig == nil {
		currentZoneConfig = zonepb.NewZoneConfig()
	}

	// The expected zone config starts from the same base config as the current
	// zone config, so copy it over to be used down below.
	expectedZoneConfig := currentZoneConfig

	if toRegionalByRow {
		// We're going to REGIONAL BY ROW. Check to see if the to be applied zone
		// configurations will override any existing zone configurations on the
		// table's indexes. We say "override" here, and not "overwrite" because
		// REGIONAL BY ROW tables will not write zone configs at the index level,
		// but instead, at the index partition level. That being said, application
		// of a partition-level zone config will override any applied index-level
		// zone config, so it's important that we warn the user of that.
		for _, s := range currentZoneConfig.Subzones {
			if s.PartitionName == "" {
				// Found a zone config on an index. Check to see if any of its
				// multi-region fields are set.
				if isSet, str := s.Config.IsAnyMultiRegionFieldSet(); isSet {
					// Find the name of the offending index to use in the message below.
					// In the case where we can't find the name, do our best and return
					// the ID.
					indexName := fmt.Sprintf("unknown with ID = %s",
						strconv.FormatUint(uint64(s.IndexID), 10))
					for _, i := range desc.ActiveIndexes() {
						if uint32(i.GetID()) == s.IndexID {
							indexName = i.GetName()
						}
					}
					err := errors.Newf(
						"attempting to update zone configuration for table %q which "+
							"contains a zone configuration on index %q with multi-region field %q set",
						desc.GetName(),
						indexName,
						str,
					)
					err = errors.WithDetail(err, "the attempted operation will override "+
						"the index zone configuration field")
					return errors.WithHint(err, hint)
				}
			}
		}
	}

	// Fill in the expectedZoneConfig using the specified option.
	for _, opt := range opts {
		_, newZoneConfig, err := opt(
			*expectedZoneConfig,
			regionConfig,
			desc,
		)
		if err != nil {
			return err
		}
		expectedZoneConfig = &newZoneConfig
	}

	// Mark the NumReplicas as 0 if we have subzones but no other features
	// in the zone config. This signifies a placeholder.
	if len(expectedZoneConfig.Subzones) > 0 && isPlaceholderZoneConfigForMultiRegion(*expectedZoneConfig) {
		expectedZoneConfig.NumReplicas = proto.Int32(0)
	}

	// Compare the two zone configs to see if anything is amiss.
	same, field, err := currentZoneConfig.DiffWithZone(
		*expectedZoneConfig,
		zonepb.MultiRegionZoneConfigFields,
	)
	if err != nil {
		return err
	}
	if !same {
		err := errors.Newf(
			"attempting to update zone configuration for table %q which contains modified field %q ",
			desc.GetName(),
			field,
		)
		err = errors.WithDetail(err, "the attempted operation will overwrite "+
			"a user modified field")
		return errors.WithHint(err, hint)
	}

	return nil
}
