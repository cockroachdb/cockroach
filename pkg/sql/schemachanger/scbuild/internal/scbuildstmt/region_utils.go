package scbuildstmt

import (
	"bytes"

	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/multiregion"
	"github.com/cockroachdb/cockroach/pkg/sql/covering"
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
	if len(zone.Subzones) > 0 {
		var err error
		zone.SubzoneSpans, err = GenerateSubzoneSpans(
			b, tbl, zone.Subzones, hasNewSubzones)
		if err != nil {
			panic(err)
		}
	} else {
		// To keep the Subzone and SubzoneSpan arrays consistent
		zone.SubzoneSpans = nil
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

func GenerateSubzoneSpans(
	b BuildCtx, tableElts ElementResultSet, subzones []zonepb.Subzone, hasNewSubzones bool,
) ([]zonepb.SubzoneSpan, error) {
	// Removing zone configs does not require a valid license.
	if hasNewSubzones {
		if err := b.CheckEnterpriseEnabled("replication zones on indexes or partitions"); err != nil {
			return nil, err
		}
	}

	// We already completely avoid creating subzone spans for dropped indexes.
	// Whether this was intentional is a different story, but it turns out to be
	// pretty sane. Dropped elements may refer to dropped types and we aren't
	// necessarily in a position to deal with those dropped types. Add a special
	// case to avoid generating any subzone spans in the face of being dropped.
	_, target, tbl := scpb.FindTable(tableElts)
	if target == scpb.ToAbsent {
		return nil, nil
	}

	//a := &tree.DatumAlloc{}

	subzoneIndexByIndexID := make(map[descpb.IndexID]int32)
	subzoneIndexByPartition := make(map[string]int32)
	for i, subzone := range subzones {
		if len(subzone.PartitionName) > 0 {
			subzoneIndexByPartition[subzone.PartitionName] = int32(i)
		} else {
			subzoneIndexByIndexID[descpb.IndexID(subzone.IndexID)] = int32(i)
		}
	}

	var indexCovering covering.Covering
	var partitionCoverings []covering.Covering
	forEachIndex := func(tableID catid.DescID, indexID catid.IndexID) {
		_, indexSubzoneExists := subzoneIndexByIndexID[indexID]
		if indexSubzoneExists {
			idxKey := b.IndexPrefix(tableID, indexID)
			idxSpan := roachpb.Span{
				Key:    idxKey,
				EndKey: idxKey.PrefixEnd(),
			}
			// Each index starts with a unique prefix, so (from a precedence
			// perspective) it's safe to append them all together.
			indexCovering = append(indexCovering, covering.Range{
				Start: idxSpan.Key, End: idxSpan.EndKey,
				Payload: zonepb.Subzone{IndexID: uint32(indexID)},
			})
		}

		/*	var emptyPrefix []tree.Datum
			indexPartitionCoverings, err := indexCoveringsForPartitioning(
				a, codec, tableDesc, idx, idx.GetPartitioning(), subzoneIndexByPartition, emptyPrefix)
			if err != nil {
				return err
			}
			// The returned indexPartitionCoverings are sorted with highest
			// precedence first. They all start with the index prefix, so cannot
			// overlap with the partition coverings for any other index, so (from a
			// precedence perspective) it's safe to append them all together.
			partitionCoverings = append(partitionCoverings, indexPartitionCoverings...)
		*/
	}
	scpb.ForEachSecondaryIndex(tableElts,
		func(current scpb.Status, target scpb.TargetStatus, idx *scpb.SecondaryIndex) {
			if target != scpb.ToPublic {
				return
			}
			forEachIndex(idx.TableID, idx.IndexID)
		})
	scpb.ForEachPrimaryIndex(tableElts,
		func(current scpb.Status, target scpb.TargetStatus, idx *scpb.PrimaryIndex) {
			if target != scpb.ToPublic {
				return
			}
			forEachIndex(idx.TableID, idx.IndexID)
		})

	// OverlapCoveringMerge returns the payloads for any coverings that overlap
	// in the same order they were input. So, we require that they be ordered
	// with highest precedence first, so the first payload of each range is the
	// one we need.
	ranges := covering.OverlapCoveringMerge(append(partitionCoverings, indexCovering))

	// NB: This assumes that none of the indexes are interleaved, which is
	// checked in PartitionDescriptor validation.
	sharedPrefix := b.TablePrefix(tbl.TableID)

	var subzoneSpans []zonepb.SubzoneSpan
	for _, r := range ranges {
		payloads := r.Payload.([]interface{})
		if len(payloads) == 0 {
			continue
		}
		subzoneSpan := zonepb.SubzoneSpan{
			Key:    bytes.TrimPrefix(r.Start, sharedPrefix),
			EndKey: bytes.TrimPrefix(r.End, sharedPrefix),
		}
		var ok bool
		if subzone := payloads[0].(zonepb.Subzone); len(subzone.PartitionName) > 0 {
			subzoneSpan.SubzoneIndex, ok = subzoneIndexByPartition[subzone.PartitionName]
		} else {
			subzoneSpan.SubzoneIndex, ok = subzoneIndexByIndexID[descpb.IndexID(subzone.IndexID)]
		}
		if !ok {
			continue
		}
		if bytes.Equal(subzoneSpan.Key.PrefixEnd(), subzoneSpan.EndKey) {
			subzoneSpan.EndKey = nil
		}
		subzoneSpans = append(subzoneSpans, subzoneSpan)
	}
	return subzoneSpans, nil
}
