// Copyright 2024 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
)

// indexZoneConfigObj is used to represent a table-specific zone configuration
// object.
type indexZoneConfigObj struct {
	tableZoneConfigObj
	indexID      catid.IndexID
	indexSubzone *zonepb.Subzone
	seqNum       uint32
}

var _ zoneConfigObject = &indexZoneConfigObj{}

func (izo *indexZoneConfigObj) getTableZoneConfig() *zonepb.ZoneConfig {
	return izo.tableZoneConfigObj.zoneConfig
}

func (izo *indexZoneConfigObj) addZoneConfigToBuildCtx(b BuildCtx) scpb.Element {
	izo.seqNum += 1
	subzones := []zonepb.Subzone{*izo.indexSubzone}

	// Merge the new subzones with the old subzones so that we can generate
	// accurate subzone spans.
	parentZoneConfig := izo.getTableZoneConfig()
	if parentZoneConfig != nil {
		parentZoneConfig.SetSubzone(*izo.indexSubzone)
		subzones = parentZoneConfig.Subzones
	}

	ss, err := generateSubzoneSpans(b, izo.tableID, subzones, izo.indexID, "")
	if err != nil {
		panic(err)
	}

	elem := &scpb.IndexZoneConfig{
		TableID:      izo.tableID,
		IndexID:      izo.indexID,
		Subzone:      *izo.indexSubzone,
		SubzoneSpans: ss,
		SeqNum:       izo.seqNum,
	}
	b.Add(elem)
	return elem
}

func (izo *indexZoneConfigObj) retrievePartialZoneConfig(b BuildCtx) *zonepb.ZoneConfig {
	sameIdx := func(e *scpb.IndexZoneConfig) bool {
		return e.TableID == izo.getTargetID() && e.IndexID == izo.indexID
	}
	mostRecentElem := findMostRecentZoneConfig(izo, func(id catid.DescID) *scpb.ElementCollection[*scpb.IndexZoneConfig] {
		return b.QueryByID(id).FilterIndexZoneConfig()
	}, sameIdx)

	if mostRecentElem != nil {
		idxZc := zonepb.NewZoneConfig()
		idxZc.Subzones = []zonepb.Subzone{mostRecentElem.Subzone}
		izo.zoneConfig = idxZc
		izo.seqNum = mostRecentElem.SeqNum
	}

	return izo.zoneConfig
}

func (izo *indexZoneConfigObj) retrieveCompleteZoneConfig(
	b BuildCtx, getInheritedDefault bool,
) (*zonepb.ZoneConfig, *zonepb.Subzone, error) {
	var placeholder *zonepb.ZoneConfig
	var err error
	zc := &zonepb.ZoneConfig{}
	if getInheritedDefault {
		zc, err = izo.getInheritedDefaultZoneConfig(b)
	} else {
		zc, placeholder, err = izo.getZoneConfig(b, false /* inheritDefaultRange */)
	}
	if err != nil {
		return nil, nil, err
	}

	completeZc := *zc
	if err = izo.completeZoneConfig(b, &completeZc); err != nil {
		return nil, nil, err
	}

	var subzone *zonepb.Subzone
	indexID := izo.indexID
	if placeholder != nil {
		// TODO(annie): once we support partitions, we will need to pass in the
		// actual partition name here.
		if subzone = placeholder.GetSubzone(uint32(indexID), ""); subzone != nil {
			if indexSubzone := placeholder.GetSubzone(uint32(indexID), ""); indexSubzone != nil {
				subzone.Config.InheritFromParent(&indexSubzone.Config)
			}
			subzone.Config.InheritFromParent(zc)
			return placeholder, subzone, nil
		}
	} else {
		if subzone = zc.GetSubzone(uint32(indexID), ""); subzone != nil {
			if indexSubzone := zc.GetSubzone(uint32(indexID), ""); indexSubzone != nil {
				subzone.Config.InheritFromParent(&indexSubzone.Config)
			}
			subzone.Config.InheritFromParent(zc)
		}
	}
	return zc, subzone, nil
}

func (izo *indexZoneConfigObj) setZoneConfigToWrite(zone *zonepb.ZoneConfig) {
	var subzoneToWrite *zonepb.Subzone
	for _, subzone := range zone.Subzones {
		if subzone.IndexID == uint32(izo.indexID) && len(subzone.PartitionName) == 0 {
			subzoneToWrite = &subzone
		}
	}
	izo.indexSubzone = subzoneToWrite
}

// getInheritedFieldsForPartialSubzone returns the set of inherited fields for
// a partial subzone based off of its parent zone.
func (izo *indexZoneConfigObj) getInheritedFieldsForPartialSubzone(
	b BuildCtx, partialZone *zonepb.ZoneConfig,
) (*zonepb.ZoneConfig, error) {
	// We are operating on a subZone and need to inherit all remaining
	// unset fields in its parent zone, which is partialZone.
	zoneInheritedFields := *partialZone
	if err := izo.completeZoneConfig(b, &zoneInheritedFields); err != nil {
		return nil, err
	}
	// Since we have just an index, we should copy from the inherited
	// zone's fields (whether that was the table or database).
	return &zoneInheritedFields, nil
}

func (izo *indexZoneConfigObj) applyZoneConfig(
	b BuildCtx,
	n *tree.SetZoneConfig,
	copyFromParentList []tree.Name,
	setters []func(c *zonepb.ZoneConfig),
) error {
	// TODO(annie): once we allow configuring zones for named zones/system ranges,
	// we will need to guard against secondary tenants from configuring such
	// ranges.

	// We are configuring an index. Determine the index ID and fill this
	// information out in our zoneConfigObject.
	fillIndexAndPartitionFromZoneSpecifier(b, n.ZoneSpecifier, izo)
	indexID := izo.indexID
	tempIndexID := mustRetrieveIndexElement(b, izo.getTargetID(), indexID).TemporaryIndexID

	// Retrieve the partial zone configuration
	partialZone := izo.retrievePartialZoneConfig(b)

	subzonePlaceholder := false
	// No zone was found. Possibly a SubzonePlaceholder depending on the index.
	if partialZone == nil {
		partialZone = zonepb.NewZoneConfig()
		subzonePlaceholder = true
	}
	currentZone := protoutil.Clone(partialZone).(*zonepb.ZoneConfig)

	var partialSubzone *zonepb.Subzone
	partialSubzone = partialZone.GetSubzoneExact(uint32(indexID), "")
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
	completeZone, completeSubZone, err := izo.retrieveCompleteZoneConfig(b,
		n.SetDefault /* getInheritedDefault */)
	if err != nil {
		return err
	}

	// We need to inherit zone configuration information from the correct zone,
	// not completeZone.
	{
		zoneInheritedFields, err := izo.getInheritedFieldsForPartialSubzone(b, partialZone)
		if err != nil {
			return err
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

	if n.SetDefault {
		finalZone = *zonepb.NewZoneConfig()
	}

	// Fill in our zone configs with var = val assignments.
	if err := loadSettingsToZoneConfigs(setters, &newZone, &finalZone); err != nil {
		return err
	}

	// Validate that there are no conflicts in the zone setup.
	if err := zonepb.ValidateNoRepeatKeysInZone(&newZone); err != nil {
		return err
	}

	if err := validateZoneAttrsAndLocalities(b, currentZone, &newZone); err != nil {
		return err
	}

	// Fill in the final zone config with subzones.
	fillZoneConfigsForSubzones(indexID, "", tempIndexID, subzonePlaceholder, completeZone,
		partialZone, newZone, finalZone)

	// Finally, revalidate everything. Validate only the completeZone config.
	if err := completeZone.Validate(); err != nil {
		return pgerror.Wrap(err, pgcode.CheckViolation, "could not validate zone config")
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
		return err
	}
	izo.setZoneConfigToWrite(partialZone)
	return err
}
