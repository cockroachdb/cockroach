// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package scbuildstmt

import (
	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
)

// tableZoneConfigObj is used to represent a table-specific zone configuration
// object.
type tableZoneConfigObj struct {
	databaseZoneConfigObj
	tableID    catid.DescID
	zoneConfig *zonepb.ZoneConfig
}

var _ zoneConfigObject = &tableZoneConfigObj{}

func (tzo *tableZoneConfigObj) isNoOp() bool {
	return tzo.zoneConfig == nil
}

func (tzo *tableZoneConfigObj) getZoneConfigElemForAdd(_ BuildCtx) (scpb.Element, []scpb.Element) {
	elem := &scpb.TableZoneConfig{
		TableID:    tzo.tableID,
		ZoneConfig: tzo.zoneConfig,
		SeqNum:     tzo.seqNum + 1,
	}
	return elem, nil
}

func (tzo *tableZoneConfigObj) getZoneConfigElemForDrop(
	b BuildCtx,
) ([]scpb.Element, []scpb.Element) {
	var elems []scpb.Element
	// Ensure that we drop all elements associated with this table. This becomes
	// more relevant in explicit txns -- where there could be multiple zone config
	// elements associated with this table with increasing seqNums.
	b.QueryByID(tzo.getTargetID()).FilterTableZoneConfig().
		ForEach(func(_ scpb.Status, _ scpb.TargetStatus, e *scpb.TableZoneConfig) {
			e.ZoneConfig.DeleteTableConfig()
			elems = append(elems, e)
		})
	return elems, nil
}

func (tzo *tableZoneConfigObj) checkPrivilegeForSetZoneConfig(
	b BuildCtx, zs tree.ZoneSpecifier,
) error {
	// TODO(#125882): currently, we fall back to the legacy schema changer below
	// if the zone config target is a system table. The only thing the legacy
	// schema changer will do is populate an error -- since configuring system
	// tables is not allowed. We should add this check
	// (checkIfConfigurationAllowed) back in DSC-land when our builder doesn't
	// panic on system tables.
	tblElem := mustRetrievePhysicalTableElem(b, tzo.tableID)
	tblNamespaceElem := mustRetrieveNamespaceElem(b, tzo.tableID)
	if tblNamespaceElem.DatabaseID == keys.SystemDatabaseID {
		return b.CheckGlobalPrivilege(privilege.REPAIRCLUSTER)
	}
	// Can configure zone of a table (or its index) if user has either CREATE or
	// ZONECONFIG privilege on the table.
	tableCreatePrivilegeErr := b.CheckPrivilege(tblElem, privilege.CREATE)
	tableZoneConfigPrivilegeErr := b.CheckPrivilege(tblElem, privilege.ZONECONFIG)
	if tableCreatePrivilegeErr == nil || tableZoneConfigPrivilegeErr == nil {
		return nil
	}

	reqNonAdminPrivs := []privilege.Kind{privilege.ZONECONFIG, privilege.CREATE}
	return sqlerrors.NewInsufficientPrivilegeOnDescriptorError(b.CurrentUser(),
		reqNonAdminPrivs, string(catalog.Table), tblNamespaceElem.Name)
}

func (tzo *tableZoneConfigObj) checkZoneConfigChangePermittedForMultiRegion(
	b BuildCtx, zs tree.ZoneSpecifier, options tree.KVOptions,
) error {
	// If the user has specified that they're overriding, then the world is
	// their oyster.
	if b.SessionData().OverrideMultiRegionZoneConfigEnabled {
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

	// We're dealing with a table-based zone configuration change. Determine
	// if this is a multi-region table.
	if !isMultiRegionTable(b, tzo.tableID) {
		// Not a multi-region table, we're done here.
		return nil
	}

	return maybeMultiregionErrorWithHint(b, tzo, zs, options)
}

func (tzo *tableZoneConfigObj) getTargetID() catid.DescID {
	return tzo.tableID
}

func (tzo *tableZoneConfigObj) retrievePartialZoneConfig(b BuildCtx) *zonepb.ZoneConfig {
	sameTbl := func(e *scpb.TableZoneConfig) bool {
		return e.TableID == tzo.getTargetID()
	}
	mostRecentElem := findMostRecentZoneConfig(tzo,
		func(id catid.DescID) *scpb.ElementCollection[*scpb.TableZoneConfig] {
			return b.QueryByID(id).FilterTableZoneConfig()
		}, sameTbl)

	if mostRecentElem != nil {
		tzo.zoneConfig = mostRecentElem.ZoneConfig
		tzo.seqNum = mostRecentElem.SeqNum
	}

	return tzo.zoneConfig
}

func (tzo *tableZoneConfigObj) completeZoneConfig(b BuildCtx, zone *zonepb.ZoneConfig) error {
	if zone.IsComplete() {
		return nil
	}
	// For tables, inherit from the database.
	dbID := mustRetrieveNamespaceElem(b, tzo.getTargetID()).DatabaseID
	dzo := databaseZoneConfigObj{databaseID: dbID}
	dbZone, err := dzo.getZoneConfig(b, false /* inheritDefaultRange */)
	if err != nil {
		return err
	}
	zone.InheritFromParent(dbZone)
	// Check if zone is complete. If not, inherit from the default zone config
	if zone.IsComplete() {
		return nil
	}
	defaultZone, err := tzo.getZoneConfig(b, true /* inheritDefaultRange */)
	if err != nil {
		return err
	}
	zone.InheritFromParent(defaultZone)
	return nil
}

func (tzo *tableZoneConfigObj) setZoneConfigToWrite(zone *zonepb.ZoneConfig) {
	tzo.zoneConfig = zone
}

func (tzo *tableZoneConfigObj) getInheritedDefaultZoneConfig(
	b BuildCtx,
) (zc *zonepb.ZoneConfig, err error) {
	targetID := tzo.getTargetID()
	parentDBID := mustRetrieveNamespaceElem(b, targetID).DatabaseID
	dzo := databaseZoneConfigObj{databaseID: parentDBID}
	zc, err = dzo.getZoneConfig(b, false /* inheritDefaultRange */)
	return zc, err
}

func (tzo *tableZoneConfigObj) getZoneConfig(
	b BuildCtx, inheritDefaultRange bool,
) (*zonepb.ZoneConfig, error) {
	zc, _, err := lookUpSystemZonesTable(b, tzo, inheritDefaultRange, false /* isSubzoneConfig */)
	if err != nil {
		return nil, err
	}
	// If the zone config exists and is not a subzone placeholder, return.
	if zc != nil && !zc.IsSubzonePlaceholder() {
		return zc, err
	}

	// Otherwise, since our target is a table, recursively get the zone config
	// of its parent database.
	parentDBID := mustRetrieveNamespaceElem(b, tzo.getTargetID()).DatabaseID
	dzo := databaseZoneConfigObj{databaseID: parentDBID}
	zc, err = dzo.getZoneConfig(b, inheritDefaultRange)
	if err != nil {
		return nil, err
	}
	return zc, nil
}

func (tzo *tableZoneConfigObj) applyZoneConfig(
	b BuildCtx,
	n *tree.SetZoneConfig,
	copyFromParentList []tree.Name,
	setters []func(c *zonepb.ZoneConfig),
) (*zonepb.ZoneConfig, error) {
	oldZone, partialZone, err := prepareZoneConfig(b, n, copyFromParentList, setters, tzo)
	tzo.setZoneConfigToWrite(partialZone)
	return oldZone, err
}
