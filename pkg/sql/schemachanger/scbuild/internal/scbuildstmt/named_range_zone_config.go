// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package scbuildstmt

import (
	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
)

// namedRangeZoneConfigObj is used to represent a named-range-specific zone
// configuration object.
type namedRangeZoneConfigObj struct {
	rangeID    catid.DescID
	zoneConfig *zonepb.ZoneConfig
	seqNum     uint32
}

var _ zoneConfigObject = &namedRangeZoneConfigObj{}

func (rzo *namedRangeZoneConfigObj) isNoOp() bool {
	return rzo.zoneConfig == nil
}

func (rzo *namedRangeZoneConfigObj) getZoneConfigElemForAdd(
	_ BuildCtx,
) (scpb.Element, []scpb.Element) {
	elem := &scpb.NamedRangeZoneConfig{
		RangeID:    rzo.rangeID,
		ZoneConfig: rzo.zoneConfig,
		SeqNum:     rzo.seqNum + 1,
	}
	return elem, nil
}

func (rzo *namedRangeZoneConfigObj) getZoneConfigElemForDrop(
	b BuildCtx,
) ([]scpb.Element, []scpb.Element) {
	var elems []scpb.Element
	// Ensure that we drop all elements associated with this named range. This
	// becomes more relevant in explicit txns -- where there could be multiple
	// zone config elements associated with this named range with increasing
	// seqNums.
	b.QueryByID(rzo.getTargetID()).FilterNamedRangeZoneConfig().
		ForEach(func(_ scpb.Status, _ scpb.TargetStatus, e *scpb.NamedRangeZoneConfig) {
			elems = append(elems, e)
		})
	return elems, nil
}

func (rzo *namedRangeZoneConfigObj) checkPrivilegeForSetZoneConfig(
	b BuildCtx, _ tree.ZoneSpecifier,
) error {
	return b.CheckGlobalPrivilege(privilege.REPAIRCLUSTER)
}

func (rzo *namedRangeZoneConfigObj) checkZoneConfigChangePermittedForMultiRegion(
	b BuildCtx, zs tree.ZoneSpecifier, options tree.KVOptions,
) error {
	return nil
}

func (rzo *namedRangeZoneConfigObj) getTargetID() catid.DescID {
	return rzo.rangeID
}

func (rzo *namedRangeZoneConfigObj) retrievePartialZoneConfig(b BuildCtx) *zonepb.ZoneConfig {
	sameRange := func(e *scpb.NamedRangeZoneConfig) bool {
		return e.RangeID == rzo.getTargetID()
	}
	mostRecentElem := findMostRecentZoneConfig(rzo,
		func(id catid.DescID) *scpb.ElementCollection[*scpb.NamedRangeZoneConfig] {
			return b.QueryByID(id).FilterNamedRangeZoneConfig()
		}, sameRange)

	if mostRecentElem != nil {
		rzo.zoneConfig = mostRecentElem.ZoneConfig
		rzo.seqNum = mostRecentElem.SeqNum
	}

	return rzo.zoneConfig
}

func (rzo *namedRangeZoneConfigObj) retrieveCompleteZoneConfig(
	b BuildCtx, getInheritedDefault bool,
) (*zonepb.ZoneConfig, *zonepb.Subzone, error) {
	var err error
	zc := &zonepb.ZoneConfig{}
	if getInheritedDefault {
		zc, err = rzo.getInheritedDefaultZoneConfig(b)
	} else {
		zc, err = rzo.getZoneConfig(b, false /* inheritDefaultRange */)
	}
	if err != nil {
		return nil, nil, err
	}

	completeZc := *zc
	if err = rzo.completeZoneConfig(b, &completeZc); err != nil {
		return nil, nil, err
	}

	return zc, nil, nil
}

func (rzo *namedRangeZoneConfigObj) completeZoneConfig(b BuildCtx, zone *zonepb.ZoneConfig) error {
	// Check if zone is complete. If not, inherit from the default zone config
	if zone.IsComplete() {
		return nil
	}
	defaultZone, err := rzo.getZoneConfig(b, true /* inheritDefaultRange */)
	if err != nil {
		return err
	}
	zone.InheritFromParent(defaultZone)
	return nil
}

func (rzo *namedRangeZoneConfigObj) setZoneConfigToWrite(zone *zonepb.ZoneConfig) {
	rzo.zoneConfig = zone
}

func (rzo *namedRangeZoneConfigObj) getInheritedDefaultZoneConfig(
	b BuildCtx,
) (*zonepb.ZoneConfig, error) {
	// Get the zone config of the DEFAULT RANGE.
	zc, err := rzo.getZoneConfig(b, true /* inheritDefaultRange */)
	return zc, err
}

func (rzo *namedRangeZoneConfigObj) getZoneConfig(
	b BuildCtx, inheritDefaultRange bool,
) (*zonepb.ZoneConfig, error) {
	zc, _, err := lookUpSystemZonesTable(b, rzo, inheritDefaultRange, false /* isSubzoneConfig */)
	if err != nil {
		return nil, err
	}
	// If the zone config exists, return.
	if zc != nil {
		return zc, err
	}

	// Otherwise, no zone config for this ID. Retrieve the default zone config,
	// but only as long as that wasn't the ID we were trying to retrieve
	// (to avoid infinite recursion).
	if !inheritDefaultRange {
		zc, err = rzo.getZoneConfig(b, true /* inheritDefaultRange */)
		if err != nil {
			return nil, err
		}
		return zc, nil
	}

	// `targetID == keys.RootNamespaceID` but that zc config is not found
	// in `system.zones` table. Return a special, recognizable error!
	return nil, sqlerrors.ErrNoZoneConfigApplies
}

func (rzo *namedRangeZoneConfigObj) applyZoneConfig(
	b BuildCtx,
	n *tree.SetZoneConfig,
	copyFromParentList []tree.Name,
	setters []func(c *zonepb.ZoneConfig),
) (*zonepb.ZoneConfig, error) {
	// Secondary tenants are not allowed to set zone configurations on any named
	// zones other than RANGE DEFAULT.
	if !b.Codec().ForSystemTenant() {
		zoneName, found := zonepb.NamedZonesByID[uint32(rzo.rangeID)]
		if found && zoneName != zonepb.DefaultZoneName {
			return nil, pgerror.Newf(
				pgcode.CheckViolation,
				"non-system tenants cannot configure zone for %s range",
				zoneName,
			)
		}
	}
	oldZone, partialZone, err := prepareZoneConfig(b, n, copyFromParentList, setters, rzo)
	rzo.setZoneConfigToWrite(partialZone)
	return oldZone, err
}
