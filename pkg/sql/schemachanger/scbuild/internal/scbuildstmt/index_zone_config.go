// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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

// indexZoneConfigObj is used to represent an index-specific zone configuration
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

	ss, err := generateSubzoneSpans(b, izo.tableID, subzones)
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
	mostRecentElem := findMostRecentZoneConfig(izo,
		func(id catid.DescID) *scpb.ElementCollection[*scpb.IndexZoneConfig] {
			return b.QueryByID(id).FilterIndexZoneConfig()
		}, sameIdx)

	// Since we will be performing a subzone config update, we need to retrieve
	// its parent's (table's) zone config for later use. This is because we need
	// context of any existing subzone spans to generate an accurate subzone span
	// for this subzone.
	_ = izo.tableZoneConfigObj.retrievePartialZoneConfig(b)

	var partialZone *zonepb.ZoneConfig
	if mostRecentElem != nil {
		// Construct a zone config placeholder with the correct subzone. This
		// will be what we return.
		partialZone = zonepb.NewZoneConfig()
		partialZone.DeleteTableConfig()
		partialZone.Subzones = []zonepb.Subzone{mostRecentElem.Subzone}
		izo.indexSubzone = &mostRecentElem.Subzone
		izo.seqNum = mostRecentElem.SeqNum
	}
	return partialZone
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
		zc, err = izo.tableZoneConfigObj.getZoneConfig(b, false /* inheritDefaultRange */)
		if err != nil {
			return nil, nil, err
		}
		placeholder, err = izo.getZoneConfig(b, false /* inheritDefaultRange */)
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
			break
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

func (izo *indexZoneConfigObj) getZoneConfig(
	b BuildCtx, inheritDefaultRange bool,
) (*zonepb.ZoneConfig, error) {
	_, subzones, err := lookUpSystemZonesTable(b, izo, inheritDefaultRange, true /* isSubzoneConfig */)
	if err != nil {
		return nil, err
	}
	subzoneConfig := zonepb.NewZoneConfig()
	subzoneConfig.DeleteTableConfig()
	subzoneConfig.Subzones = subzones

	return subzoneConfig, nil
}

func (izo *indexZoneConfigObj) applyZoneConfig(
	b BuildCtx,
	n *tree.SetZoneConfig,
	copyFromParentList []tree.Name,
	setters []func(c *zonepb.ZoneConfig),
) (*zonepb.ZoneConfig, error) {
	// We are configuring an index. Determine the index ID and fill this
	// information out in our zoneConfigObject.
	izo.fillIndexFromZoneSpecifier(b, n.ZoneSpecifier)
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
		return nil, err
	}

	// We need to inherit zone configuration information from the correct zone,
	// not completeZone.
	{
		zoneInheritedFields, err := izo.getInheritedFieldsForPartialSubzone(b, partialZone)
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
	fillZoneConfigsForSubzones(indexID, "", tempIndexID, subzonePlaceholder, completeZone,
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
	izo.setZoneConfigToWrite(partialZone)
	return oldZone, err
}

// fillIndexFromZoneSpecifier fills out the index id in the zone
// specifier for a indexZoneConfigObj.
func (izo *indexZoneConfigObj) fillIndexFromZoneSpecifier(b BuildCtx, zs tree.ZoneSpecifier) {
	tableID := izo.getTargetID()

	indexName := string(zs.TableOrIndex.Index)
	var indexID catid.IndexID
	if indexName == "" {
		// Use the primary index if index name is unspecified.
		primaryIndexElem := mustRetrieveCurrentPrimaryIndexElement(b, tableID)
		indexID = primaryIndexElem.IndexID
	} else {
		indexElems := b.ResolveIndex(tableID, tree.Name(indexName), ResolveParams{})
		indexID = indexElems.FilterIndexName().MustGetOneElement().IndexID
	}
	izo.indexID = indexID
}
