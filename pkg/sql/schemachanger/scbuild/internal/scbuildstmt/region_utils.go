// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package scbuildstmt

import (
	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/multiregion"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/regionutils"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"google.golang.org/protobuf/proto"
)

// applyZoneConfigForMultiRegionTableOption is an option that can be passed into
// applyZoneConfigForMultiRegionTable.
type applyZoneConfigForMultiRegionTableOption func(
	zoneConfig zonepb.ZoneConfig,
	regionConfig multiregion.RegionConfig,
	tableElts ElementResultSet,
) (hasNewSubzones bool, newZoneConfig zonepb.ZoneConfig)

// At the table/partition level, the only attributes that are set are
// `num_voters`, `voter_constraints`, and `lease_preferences`. We expect that
// the attributes `num_replicas` and `constraints` will be inherited from the
// database level zone config.
func zoneConfigForMultiRegionPartition(
	partitionRegion catpb.RegionName, regionConfig multiregion.RegionConfig,
) (zonepb.ZoneConfig, error) {
	zc := *zonepb.NewZoneConfig()

	numVoters, numReplicas := regionutils.GetNumVotersAndNumReplicas(regionConfig)
	zc.NumVoters = &numVoters

	if regionConfig.IsMemberOfExplicitSuperRegion(partitionRegion) {
		err := regionutils.AddConstraintsForSuperRegion(&zc, regionConfig, partitionRegion)
		if err != nil {
			return zonepb.ZoneConfig{}, err
		}
	} else if !regionConfig.RegionalInTablesInheritDatabaseConstraints(partitionRegion) {
		// If the database constraints can't be inherited to serve as the
		// constraints for this partition, define the constraints ourselves.
		zc.NumReplicas = &numReplicas

		constraints, err := regionutils.SynthesizeReplicaConstraints(regionConfig.Regions(), regionConfig.Placement())
		if err != nil {
			return zonepb.ZoneConfig{}, err
		}
		zc.Constraints = constraints
		zc.InheritedConstraints = false
	}

	voterConstraints, err := regionutils.SynthesizeVoterConstraints(partitionRegion, regionConfig)
	if err != nil {
		return zonepb.ZoneConfig{}, err
	}
	zc.VoterConstraints = voterConstraints
	zc.NullVoterConstraintsIsEmpty = true

	leasePreferences := regionutils.SynthesizeLeasePreferences(partitionRegion)
	zc.LeasePreferences = leasePreferences
	zc.InheritedLeasePreferences = false

	zc = regionConfig.ApplyZoneConfigExtensionForRegionalIn(zc, partitionRegion)
	return zc, err
}

// applyZoneConfigForMultiRegionTableOptionNewIndexes applies table zone configs
// for a newly added index which requires partitioning of individual indexes.
func applyZoneConfigForMultiRegionTableOptionNewIndexes(
	indexIDs ...descpb.IndexID,
) applyZoneConfigForMultiRegionTableOption {
	return func(
		zoneConfig zonepb.ZoneConfig,
		regionConfig multiregion.RegionConfig,
		tableElts ElementResultSet,
	) (hasNewSubzones bool, newZoneConfig zonepb.ZoneConfig) {
		for _, indexID := range indexIDs {
			for _, region := range regionConfig.Regions() {
				zc, err := zoneConfigForMultiRegionPartition(region, regionConfig)
				if err != nil {
					panic(err)
				}
				zoneConfig.SetSubzone(zonepb.Subzone{
					IndexID:       uint32(indexID),
					PartitionName: string(region),
					Config:        zc,
				})
			}
		}
		return true, zoneConfig
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

func prepareZoneConfigForMultiRegionTable(
	b BuildCtx,
	regionConfig multiregion.RegionConfig,
	tableID catid.DescID,
	opts ...applyZoneConfigForMultiRegionTableOption,
) {
	tableElts := b.QueryByID(tableID)
	_, _, currentZoneConfigElem := scpb.FindTableZoneConfig(tableElts)
	newZoneConfig := *zonepb.NewZoneConfig()
	if currentZoneConfigElem != nil && currentZoneConfigElem.ZoneConfig != nil {
		newZoneConfig = *currentZoneConfigElem.ZoneConfig
	}

	var hasNewSubzones bool
	for _, opt := range opts {
		newHasNewSubzones, modifiedNewZoneConfig := opt(
			newZoneConfig,
			regionConfig,
			tableElts,
		)
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
	currentZoneConfigIsEmpty := currentZoneConfigElem == nil || currentZoneConfigElem.ZoneConfig == nil
	rewriteZoneConfig := !newZoneConfigIsEmpty
	deleteZoneConfig := newZoneConfigIsEmpty && !currentZoneConfigIsEmpty

	if deleteZoneConfig {
		scpb.ForEachTableZoneConfig(tableElts,
			func(current scpb.Status, target scpb.TargetStatus, e *scpb.TableZoneConfig) {
				if target == scpb.ToPublic {
					b.Drop(e)
				}
			})
		return
	}
	if !rewriteZoneConfig {
		return
	}

	if err := newZoneConfig.Validate(); err != nil {
		panic(pgerror.Wrap(
			err,
			pgcode.CheckViolation,
			"could not validate zone config",
		))
	}
	if err := newZoneConfig.ValidateTandemFields(); err != nil {
		panic(pgerror.Wrap(
			err,
			pgcode.CheckViolation,
			"could not validate zone config",
		))
	}
	prepareZoneConfigWrites(
		b, tableID, tableElts, &newZoneConfig, hasNewSubzones,
	)
}

func prepareZoneConfigWrites(
	b BuildCtx,
	targetID catid.DescID,
	tbl ElementResultSet,
	zone *zonepb.ZoneConfig,
	hasNewSubzones bool,
) {
	// The spans themselves will be generated with the metadata update.
	if hasNewSubzones && len(zone.Subzones) > 0 {
		// Removing zone configs does not require a valid license.
		if err := b.CheckEnterpriseEnabled("replication zones on indexes or partitions"); err != nil {
			panic(err)
		}
	}

	if zone.IsSubzonePlaceholder() && len(zone.Subzones) == 0 {
		scpb.ForEachTableZoneConfig(tbl,
			func(current scpb.Status, target scpb.TargetStatus, e *scpb.TableZoneConfig) {
				if target == scpb.ToPublic {
					b.Drop(e)
				}
			})
		return
	}
	_, _, tblEl := scpb.FindTable(tbl)
	b.Add(&scpb.TableZoneConfig{
		ZoneConfigID: b.NextZoneConfigID(tblEl),
		TableID:      targetID,
		ZoneConfig:   zone,
	})
}
