// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package scbuildstmt

import (
	"fmt"
	"sort"

	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
)

// partitionZoneConfigObj is used to represent a partition-specific zone
// configuration object.
type partitionZoneConfigObj struct {
	indexZoneConfigObj
	partitionSubzone *zonepb.Subzone
	partitionName    string
}

var _ zoneConfigObject = &partitionZoneConfigObj{}

func (pzo *partitionZoneConfigObj) isNoOp() bool {
	return pzo.partitionSubzone == nil
}

func (pzo *partitionZoneConfigObj) getTableZoneConfig() *zonepb.ZoneConfig {
	return pzo.tableZoneConfigObj.zoneConfig
}

func (pzo *partitionZoneConfigObj) getZoneConfigElemForAdd(
	b BuildCtx,
) (scpb.Element, []scpb.Element) {
	subzones := []zonepb.Subzone{*pzo.partitionSubzone}

	// Merge the new subzones with the old subzones so that we can generate
	// accurate subzone spans.
	parentZoneConfig := pzo.getTableZoneConfig()
	if parentZoneConfig != nil {
		parentZoneConfig.SetSubzone(*pzo.partitionSubzone)
		subzones = parentZoneConfig.Subzones
	}

	ss, err := generateSubzoneSpans(b, pzo.tableID, subzones)
	if err != nil {
		panic(err)
	}

	// TODO(annie): This does a little more work than necessary -- we should be
	// able to just update the affected subzone spans. This can be done by
	// comparing the parent's subzone spans with `ss`.
	//
	// Update the partition that is represented by pzo, along with all other
	// subzones.
	idxToSpansMap := getSubzoneSpansWithIdx(len(subzones), ss)
	var szCfg scpb.Element
	var szCfgsToUpdate []scpb.Element
	for i, sub := range subzones {
		if spans, ok := idxToSpansMap[int32(i)]; ok {
			if len(sub.PartitionName) > 0 {
				if sub.PartitionName == pzo.partitionName && sub.IndexID == uint32(pzo.indexID) {
					szCfg = &scpb.PartitionZoneConfig{
						TableID:       pzo.tableID,
						IndexID:       catid.IndexID(sub.IndexID),
						PartitionName: sub.PartitionName,
						Subzone:       sub,
						SubzoneSpans:  spans,
						OldIdxRef:     -1,
						SeqNum:        pzo.seqNum + 1,
					}
				} else {
					szCfgsToUpdate = append(szCfgsToUpdate,
						constructSideEffectPartitionElem(b, pzo, -1, sub, spans))
				}
			} else {
				szCfgsToUpdate = append(szCfgsToUpdate,
					constructSideEffectIndexElem(b, pzo, -1, spans, sub))
			}
		}
	}

	return szCfg, szCfgsToUpdate
}

func (pzo *partitionZoneConfigObj) getZoneConfigElemForDrop(
	b BuildCtx,
) ([]scpb.Element, []scpb.Element) {
	var subzonesToWrite []zonepb.Subzone
	var err error

	parentZoneConfig := pzo.getTableZoneConfig()
	if parentZoneConfig != nil {
		// Get the list of subzones after we have dropped the target; this is so we
		// can correctly generate the list of subzone spans to upsert.
		modifiedZc := protoutil.Clone(parentZoneConfig).(*zonepb.ZoneConfig)
		modifiedZc.DeleteSubzone(uint32(pzo.indexID), pzo.partitionName)
		subzonesToWrite = modifiedZc.Subzones
	} else {
		// Likely in an explicit txn that has not created an overarching
		// TableZoneConfig yet.
		var toDropElems []scpb.Element
		// Ensure that we drop all elements associated with this partition. This
		// becomes more relevant in explicit txns -- where there could be multiple
		// zone config elements associated with this partition with increasing
		// seqNums.
		b.QueryByID(pzo.getTargetID()).FilterPartitionZoneConfig().
			ForEach(func(_ scpb.Status, _ scpb.TargetStatus, e *scpb.PartitionZoneConfig) {
				if e.PartitionName == pzo.partitionName && e.IndexID == pzo.indexID {
					toDropElems = append(toDropElems, e)
				}
			})

		if len(toDropElems) == 0 {
			panic(errors.AssertionFailedf(
				"attempted to drop subzone config for table [%d], partition %s of index %d that "+
					"does not exist", pzo.tableID, pzo.partitionName, pzo.indexID))
		}
		return toDropElems, nil
	}
	ss, err := generateSubzoneSpans(b, pzo.tableID, subzonesToWrite)
	if err != nil {
		panic(err)
	}

	// TODO(annie): This does a little more work than necessary -- we should be
	// able to just update the affected subzone spans. This can be done by
	// comparing the parent's subzone spans with `ss`.
	//
	// Update the partition that is represented by pzo, along with all other
	// subzones. These other subzones are "side effects" that will need their
	// subzoneSpans updated in some way.
	idxToSpansMap := getSubzoneSpansWithIdx(len(subzonesToWrite), ss)
	var szCfgsToUpdate []scpb.Element
	for i, sub := range subzonesToWrite {
		if spans, ok := idxToSpansMap[int32(i)]; ok {
			oldIdxRefToDelete := parentZoneConfig.GetSubzoneIndex(sub.IndexID, sub.PartitionName)
			if len(sub.PartitionName) > 0 {
				szCfgsToUpdate = append(szCfgsToUpdate,
					constructSideEffectPartitionElem(b, pzo, oldIdxRefToDelete, sub, spans))
			} else {
				szCfgsToUpdate = append(szCfgsToUpdate,
					constructSideEffectIndexElem(b, pzo, oldIdxRefToDelete, spans, sub))
			}
		}
	}

	var toDropElems []scpb.Element
	if pzo.seqNum > 0 {
		for i := range pzo.seqNum {
			toDropElems = append(toDropElems, &scpb.PartitionZoneConfig{
				TableID:       pzo.tableID,
				IndexID:       pzo.indexID,
				PartitionName: pzo.partitionName,
				SeqNum:        i + 1,
			})
		}
	} else {
		toDropElems = append(toDropElems, &scpb.PartitionZoneConfig{
			TableID:       pzo.tableID,
			IndexID:       pzo.indexID,
			PartitionName: pzo.partitionName,
			SeqNum:        0,
		})
	}

	return toDropElems, szCfgsToUpdate
}

func (pzo *partitionZoneConfigObj) retrievePartialZoneConfig(b BuildCtx) *zonepb.ZoneConfig {
	samePartition := func(e *scpb.PartitionZoneConfig) bool {
		return e.TableID == pzo.getTargetID() && e.IndexID == pzo.indexID &&
			e.PartitionName == pzo.partitionName
	}
	mostRecentElem := findMostRecentZoneConfig(pzo,
		func(id catid.DescID) *scpb.ElementCollection[*scpb.PartitionZoneConfig] {
			return b.QueryByID(id).FilterPartitionZoneConfig()
		}, samePartition)

	// Set the table zone config for generating proper subzone spans later on.
	_ = pzo.tableZoneConfigObj.retrievePartialZoneConfig(b)

	var partialZone *zonepb.ZoneConfig
	if mostRecentElem != nil {
		// Construct a zone config placeholder with the correct subzone. This
		// will be what we return.
		partialZone = zonepb.NewZoneConfig()
		partialZone.DeleteTableConfig()
		partialZone.Subzones = []zonepb.Subzone{mostRecentElem.Subzone}
		pzo.partitionSubzone = &mostRecentElem.Subzone
		pzo.seqNum = mostRecentElem.SeqNum
	}

	return pzo.zoneConfig
}

func (pzo *partitionZoneConfigObj) retrieveCompleteZoneConfig(
	b BuildCtx, getInheritedDefault bool,
) (*zonepb.ZoneConfig, *zonepb.Subzone, error) {
	var placeholder *zonepb.ZoneConfig
	var err error
	zc := &zonepb.ZoneConfig{}
	if getInheritedDefault {
		zc, err = pzo.getInheritedDefaultZoneConfig(b)
	} else {
		placeholder, err = pzo.getZoneConfig(b, false /* inheritDefaultRange */)
	}
	if err != nil {
		return nil, nil, err
	}

	completeZc := *zc
	if err = pzo.completeZoneConfig(b, &completeZc); err != nil {
		return nil, nil, err
	}

	var subzone *zonepb.Subzone
	indexID := pzo.indexID
	partName := pzo.partitionName
	if placeholder != nil {
		if subzone = placeholder.GetSubzone(uint32(indexID), partName); subzone != nil {
			if indexSubzone := placeholder.GetSubzone(uint32(indexID), partName); indexSubzone != nil {
				subzone.Config.InheritFromParent(&indexSubzone.Config)
			}
			subzone.Config.InheritFromParent(zc)
			return placeholder, subzone, nil
		}
	} else {
		if subzone = zc.GetSubzone(uint32(indexID), pzo.partitionName); subzone != nil {
			if indexSubzone := zc.GetSubzone(uint32(indexID), pzo.partitionName); indexSubzone != nil {
				subzone.Config.InheritFromParent(&indexSubzone.Config)
			}
			subzone.Config.InheritFromParent(zc)
		}
	}
	return zc, subzone, nil
}

func (pzo *partitionZoneConfigObj) setZoneConfigToWrite(zone *zonepb.ZoneConfig) {
	var subzoneToWrite *zonepb.Subzone
	for _, subzone := range zone.Subzones {
		if subzone.IndexID == uint32(pzo.indexID) && subzone.PartitionName == pzo.partitionName {
			subzoneToWrite = &subzone
			break
		}
	}
	pzo.partitionSubzone = subzoneToWrite
}

// getInheritedFieldsForPartialSubzone returns the set of inherited fields for
// a partial subzone based off of its parent zone.
func (pzo *partitionZoneConfigObj) getInheritedFieldsForPartialSubzone(
	b BuildCtx, partialZone *zonepb.ZoneConfig,
) (*zonepb.ZoneConfig, error) {
	// We are operating on a subZone and need to inherit all remaining
	// unset fields in its parent zone, which is partialZone.
	zoneInheritedFields := *partialZone
	if err := pzo.completeZoneConfig(b, &zoneInheritedFields); err != nil {
		return nil, err
	}
	// Since we have just a partition, we should copy from the inherited
	// zone's fields (whether that was the table or database).
	return &zoneInheritedFields, nil
}

func (pzo *partitionZoneConfigObj) getZoneConfig(
	b BuildCtx, inheritDefaultRange bool,
) (*zonepb.ZoneConfig, error) {
	_, subzones, err := lookUpSystemZonesTable(b, pzo, inheritDefaultRange, true /* isSubzoneConfig */)
	if err != nil {
		return nil, err
	}
	subzoneConfig := zonepb.NewZoneConfig()
	subzoneConfig.DeleteTableConfig()
	subzoneConfig.Subzones = subzones

	return subzoneConfig, nil
}

func (pzo *partitionZoneConfigObj) applyZoneConfig(
	b BuildCtx,
	n *tree.SetZoneConfig,
	copyFromParentList []tree.Name,
	setters []func(c *zonepb.ZoneConfig),
) (*zonepb.ZoneConfig, error) {
	pzo.fillIndexFromPartition(b, n)
	// Fill out the indexID based off of the corresponding index name; we either
	// resolved the name fully above or it had been specified by the user.
	pzo.fillIndexFromZoneSpecifier(b, n.ZoneSpecifier)
	pzo.panicIfNoPartitionExistsOnIdx(b, n)
	indexID := pzo.indexID
	tempIndexID := mustRetrieveIndexElement(b, pzo.getTargetID(), indexID).TemporaryIndexID

	// Retrieve the partial zone configuration
	partialZone := pzo.retrievePartialZoneConfig(b)

	subzonePlaceholder := false
	// No zone was found. Possibly a SubzonePlaceholder depending on the index.
	if partialZone == nil {
		// If we are trying to discard a zone config that doesn't exist in
		// system.zones, make this a no-op.
		if n.Discard {
			return nil, nil
		}
		partialZone = zonepb.NewZoneConfig()
		subzonePlaceholder = true
	}
	currentZone := protoutil.Clone(partialZone).(*zonepb.ZoneConfig)

	var partialSubzone *zonepb.Subzone
	partialSubzone = partialZone.GetSubzoneExact(uint32(indexID), pzo.partitionName)
	if partialSubzone == nil {
		partialSubzone = &zonepb.Subzone{Config: *zonepb.NewZoneConfig()}
	}

	// Retrieve the zone configuration.
	//
	// If the statement was USING DEFAULT, we want to ignore the zone
	// config that exists on targetID and instead skip to the inherited
	// default (whichever applies -- a database if targetID is a table, default
	// if targetID is a database, etc.). For this, we use the last parameter
	// getInheritedDefault to retrieveCompleteZoneConfig().
	// These zones are only used for validations. The merged zone will not
	// be written.
	completeZone, completeSubZone, err := pzo.retrieveCompleteZoneConfig(b,
		n.SetDefault /* getInheritedDefault */)
	if err != nil {
		return nil, err
	}

	// We need to inherit zone configuration information from the correct zone,
	// not completeZone.
	{
		zoneInheritedFields, err := pzo.getInheritedFieldsForPartialSubzone(b, partialZone)
		if err != nil {
			return nil, err
		}
		partialSubzone.Config.CopyFromZone(*zoneInheritedFields, copyFromParentList)
	}

	// Determine where to load the configuration.
	newZone := *completeZone
	if completeSubZone != nil {
		newZone = completeSubZone.Config
	}

	// If an existing subzone is being modified, finalZone is overridden.
	finalZone := partialSubzone.Config

	// Clone our zone config to log the old zone config as well as the new one.
	oldZone := protoutil.Clone(&newZone).(*zonepb.ZoneConfig)

	if n.SetDefault {
		finalZone = *zonepb.NewZoneConfig()
	}

	// Fill in our zone configs with var = val assignments.
	if err := loadSettingsToZoneConfigs(setters, &newZone, &finalZone); err != nil {
		return nil, err
	}

	// Validate that there are no conflicts in the zone setup.
	if err := zonepb.ValidateNoRepeatKeysInZone(&newZone); err != nil {
		return nil, err
	}

	if err := validateZoneAttrsAndLocalities(b, currentZone, &newZone); err != nil {
		return nil, err
	}

	// Fill in the final zone config with subzones.
	fillZoneConfigsForSubzones(indexID, pzo.partitionName, tempIndexID, subzonePlaceholder, completeZone,
		partialZone, newZone, finalZone)

	// Finally, revalidate everything. Validate only the completeZone config.
	if err := completeZone.Validate(); err != nil {
		return nil, pgerror.Wrap(err, pgcode.CheckViolation, "could not validate zone config")
	}

	// Finally, check for the extra protection partial zone configs would
	// require from changes made to parent zones. The extra protections are:
	//
	// RangeMinBytes and RangeMaxBytes must be set together
	// LeasePreferences cannot be set unless Constraints/VoterConstraints are
	// explicitly set
	// Per-replica constraints cannot be set unless num_replicas is explicitly
	// set
	// Per-voter constraints cannot be set unless num_voters is explicitly set
	if err := finalZone.ValidateTandemFields(); err != nil {
		err = errors.Wrap(err, "could not validate zone config")
		err = pgerror.WithCandidateCode(err, pgcode.InvalidParameterValue)
		err = errors.WithHint(err,
			"try ALTER ... CONFIGURE ZONE USING <field_name> = COPY FROM PARENT [, ...] to "+
				"populate the field")
		return nil, err
	}
	pzo.setZoneConfigToWrite(partialZone)
	return oldZone, err
}

// panicIfNoPartitionExistsOnIdx panics if the partition referenced in a
// ALTER PARTITION ... OF TABLE does not exist on the provided index.
func (pzo *partitionZoneConfigObj) panicIfNoPartitionExistsOnIdx(
	b BuildCtx, n *tree.SetZoneConfig,
) {
	zs := n.ZoneSpecifier
	if zs.TargetsPartition() && len(zs.TableOrIndex.Index) != 0 {
		partitionName := string(zs.Partition)
		var indexes []scpb.IndexPartitioning
		idxPart := b.QueryByID(pzo.tableID).FilterIndexPartitioning()
		idxPart.ForEach(func(_ scpb.Status, _ scpb.TargetStatus, e *scpb.IndexPartitioning) {
			if e.IndexID != pzo.indexID {
				return
			}
			partition := tabledesc.NewPartitioning(&e.PartitioningDescriptor)
			partition = partition.FindPartitionByName(partitionName)
			if partition != nil {
				indexes = append(indexes, *e)
			}
		})

		indexName := string(zs.TableOrIndex.Index)
		switch len(indexes) {
		case 0:
			panic(fmt.Errorf("partition %q does not exist on index %q", partitionName, indexName))
		case 1:
			return
		default:
			panic(errors.AssertionFailedf("partition %q exists multiple times on index %q",
				partitionName, indexName))
		}
	}
}

// fillIndexFromPartition finds the existing index for the given partition name
// in an `ALTER PARTITION ... OF TABLE` and saves in to our AST. In cases where
// we find the partition existing on two indexes, we check to ensure that we are
// not in a backfill case before panicking. Otherwise, we panic if the partition
// referenced does not exist or if it exists on multiple (2+) indexes.
func (pzo *partitionZoneConfigObj) fillIndexFromPartition(b BuildCtx, n *tree.SetZoneConfig) {
	zs := &n.ZoneSpecifier
	// Backward compatibility for ALTER PARTITION ... OF TABLE. Determine which
	// index has the specified partition.
	//
	// TODO(#130842): If we allow StarIndex to be set in the DSC, we will have to
	// guard against that here as well (as in the case for StarIndex, we will have
	// a TableOrIndex.Index name of len 0, but since we are referring to
	// _multiple_ indexes, we want to skip this particular check).
	if zs.TargetsPartition() && len(zs.TableOrIndex.Index) == 0 {
		partitionName := string(zs.Partition)

		var indexes []scpb.IndexPartitioning
		idxPart := b.QueryByID(pzo.tableID).FilterIndexPartitioning()
		idxPart.ForEach(func(s scpb.Status, ts scpb.TargetStatus, e *scpb.IndexPartitioning) {
			if s != scpb.Status_DROPPED && ts != scpb.ToAbsent {
				partition := tabledesc.NewPartitioning(&e.PartitioningDescriptor)
				partition = partition.FindPartitionByName(partitionName)
				if partition != nil {
					indexes = append(indexes, *e)
				}
			}
		})

		tableName := n.TableOrIndex.Table.String()
		switch len(indexes) {
		case 0:
			panic(fmt.Errorf("partition %q does not exist on table %q", partitionName, tableName))
		case 1:
			// We found the partition on a single index. Fill this index out in
			// our AST.
			idx := indexes[0]
			idxName := mustRetrieveIndexNameElem(b, pzo.tableID, idx.IndexID)
			zs.TableOrIndex.Index = tree.UnrestrictedName(idxName.Name)
		case 2:
			// Sort our indexes to guarantee proper ordering; in the case of a
			// backfill, we want to ensure that the temporary index is always
			// the second element.
			sort.Slice(indexes, func(i, j int) bool {
				return indexes[i].IndexID < indexes[j].IndexID
			})

			// Perhaps our target index is a part of a backfill. In this case,
			// temporary indexes created during backfill should always share the
			// same zone configs as the corresponding new index.
			idx := indexes[0]
			maybeTempIdx := indexes[1]
			if isCorrespondingTemporaryIndex(b, pzo.tableID, maybeTempIdx.IndexID, idx.IndexID) {
				idxName := mustRetrieveIndexNameElem(b, pzo.tableID, idx.IndexID)
				zs.TableOrIndex.Index = tree.UnrestrictedName(idxName.Name)
				break
			}
			// We are not in a backfill case -- the partition we are referencing
			// is on multiple indexes. Fallthrough.
			fallthrough
		default:
			err := fmt.Errorf(
				"partition %q exists on multiple indexes of table %q", partitionName, tableName)
			err = pgerror.WithCandidateCode(err, pgcode.InvalidParameterValue)
			err = errors.WithHint(err, "try ALTER PARTITION ... OF INDEX ...")
			panic(err)
		}
	}
}
