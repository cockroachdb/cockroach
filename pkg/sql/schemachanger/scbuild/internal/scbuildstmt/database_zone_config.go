// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package scbuildstmt

import (
	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
)

// databaseZoneConfigObj is used to represent a database-specific zone
// configuration object.
type databaseZoneConfigObj struct {
	databaseID catid.DescID
	zoneConfig *zonepb.ZoneConfig
	seqNum     uint32
}

var _ zoneConfigObject = &databaseZoneConfigObj{}

func (dzo *databaseZoneConfigObj) isNoOp() bool {
	return dzo.zoneConfig == nil
}

func (dzo *databaseZoneConfigObj) getZoneConfigElemForAdd(
	_ BuildCtx,
) (scpb.Element, []scpb.Element) {
	elem := &scpb.DatabaseZoneConfig{
		DatabaseID: dzo.databaseID,
		ZoneConfig: dzo.zoneConfig,
		SeqNum:     dzo.seqNum + 1,
	}
	return elem, nil
}

func (dzo *databaseZoneConfigObj) getZoneConfigElemForDrop(
	b BuildCtx,
) ([]scpb.Element, []scpb.Element) {
	var elems []scpb.Element
	// Ensure that we drop all elements associated with this database. This
	// becomes more relevant in explicit txns -- where there could be multiple
	// zone config elements associated with this database with increasing seqNums.
	b.QueryByID(dzo.getTargetID()).FilterDatabaseZoneConfig().
		ForEach(func(_ scpb.Status, _ scpb.TargetStatus, e *scpb.DatabaseZoneConfig) {
			elems = append(elems, e)
		})
	return elems, nil
}

func (dzo *databaseZoneConfigObj) checkPrivilegeForSetZoneConfig(
	b BuildCtx, zs tree.ZoneSpecifier,
) error {
	// For the system database, the user must be an admin. Otherwise, we
	// require CREATE or ZONECONFIG privilege on the database in question.
	if zs.Database == "system" {
		return b.CheckGlobalPrivilege(privilege.REPAIRCLUSTER)
	}

	// Can configure zone of a database if user has either CREATE or ZONECONFIG
	// privilege on the database.
	dbElem := b.ResolveDatabase(zs.Database, ResolveParams{}).FilterDatabase().MustGetOneElement()
	dbCreatePrivilegeErr := b.CheckPrivilege(dbElem, privilege.CREATE)
	dbZoneConfigPrivilegeErr := b.CheckPrivilege(dbElem, privilege.ZONECONFIG)
	if dbZoneConfigPrivilegeErr == nil || dbCreatePrivilegeErr == nil {
		return nil
	}

	reqNonAdminPrivs := []privilege.Kind{privilege.ZONECONFIG, privilege.CREATE}
	return sqlerrors.NewInsufficientPrivilegeOnDescriptorError(b.CurrentUser(),
		reqNonAdminPrivs, string(catalog.Database),
		mustRetrieveNamespaceElem(b, dbElem.DatabaseID).Name)
}

func (dzo *databaseZoneConfigObj) checkZoneConfigChangePermittedForMultiRegion(
	b BuildCtx, zs tree.ZoneSpecifier, options tree.KVOptions,
) error {
	// If the user has specified that they're overriding, then the world is
	// their oyster.
	if b.SessionData().OverrideMultiRegionZoneConfigEnabled {
		// Note that we increment the telemetry counter unconditionally here.
		// It's possible that this will lead to over-counting as the user may
		// have left the override on and is now updating a zone configuration
		// that is not protected by the multi-region abstractions. To get finer
		// grained counting, however, would be more difficult to code, and may
		// not even prove to be that valuable, so we have decided to live with
		// the potential for over-counting.
		telemetry.Inc(sqltelemetry.OverrideMultiRegionZoneConfigurationUser)
		return nil
	}

	// Check if what we're altering is a multi-region entity.
	dbRegionConfigElem := b.ResolveDatabase(zs.Database,
		ResolveParams{}).FilterDatabaseRegionConfig().MustGetZeroOrOneElement()
	if dbRegionConfigElem == nil {
		// Not a multi-region database, we're done here.
		return nil
	}

	return maybeMultiregionErrorWithHint(b, dzo, zs, options)
}

func (dzo *databaseZoneConfigObj) getTargetID() catid.DescID {
	return dzo.databaseID
}

func (dzo *databaseZoneConfigObj) retrievePartialZoneConfig(b BuildCtx) *zonepb.ZoneConfig {
	sameDB := func(e *scpb.DatabaseZoneConfig) bool {
		return e.DatabaseID == dzo.getTargetID()
	}
	mostRecentElem := findMostRecentZoneConfig(dzo,
		func(id catid.DescID) *scpb.ElementCollection[*scpb.DatabaseZoneConfig] {
			return b.QueryByID(id).FilterDatabaseZoneConfig()
		}, sameDB)

	if mostRecentElem != nil {
		dzo.zoneConfig = mostRecentElem.ZoneConfig
		dzo.seqNum = mostRecentElem.SeqNum
	}

	return dzo.zoneConfig
}

func (dzo *databaseZoneConfigObj) retrieveCompleteZoneConfig(
	b BuildCtx, getInheritedDefault bool,
) (*zonepb.ZoneConfig, *zonepb.Subzone, error) {
	var err error
	zc := &zonepb.ZoneConfig{}
	if getInheritedDefault {
		zc, err = dzo.getInheritedDefaultZoneConfig(b)
	} else {
		zc, err = dzo.getZoneConfig(b, false /* inheritDefaultRange */)
	}
	if err != nil {
		return nil, nil, err
	}

	completeZc := *zc
	if err = dzo.completeZoneConfig(b, &completeZc); err != nil {
		return nil, nil, err
	}

	return zc, nil, nil
}

func (dzo *databaseZoneConfigObj) completeZoneConfig(b BuildCtx, zone *zonepb.ZoneConfig) error {
	// Check if zone is complete. If not, inherit from the default zone config
	if zone.IsComplete() {
		return nil
	}
	defaultZone, err := dzo.getZoneConfig(b, true /* inheritDefaultRange */)
	if err != nil {
		return err
	}
	zone.InheritFromParent(defaultZone)
	return nil
}

func (dzo *databaseZoneConfigObj) setZoneConfigToWrite(zone *zonepb.ZoneConfig) {
	dzo.zoneConfig = zone
}

func (dzo *databaseZoneConfigObj) getInheritedDefaultZoneConfig(
	b BuildCtx,
) (*zonepb.ZoneConfig, error) {
	// Get the zone config of the DEFAULT RANGE.
	zc, err := dzo.getZoneConfig(b, true /* inheritDefaultRange */)
	return zc, err
}

func (dzo *databaseZoneConfigObj) getZoneConfig(
	b BuildCtx, inheritDefaultRange bool,
) (*zonepb.ZoneConfig, error) {
	zc, _, err := lookUpSystemZonesTable(b, dzo, inheritDefaultRange, false /* isSubzoneConfig */)
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
		zc, err = dzo.getZoneConfig(b, true /* inheritDefaultRange */)
		if err != nil {
			return nil, err
		}
		return zc, nil
	}

	// `targetID == keys.RootNamespaceID` but that zc config is not found
	// in `system.zones` table. Return a special, recognizable error!
	return nil, sqlerrors.ErrNoZoneConfigApplies
}

func (dzo *databaseZoneConfigObj) applyZoneConfig(
	b BuildCtx,
	n *tree.SetZoneConfig,
	copyFromParentList []tree.Name,
	setters []func(c *zonepb.ZoneConfig),
) (*zonepb.ZoneConfig, error) {
	oldZone, partialZone, err := prepareZoneConfig(b, n, copyFromParentList, setters, dzo)
	dzo.setZoneConfigToWrite(partialZone)
	return oldZone, err
}
