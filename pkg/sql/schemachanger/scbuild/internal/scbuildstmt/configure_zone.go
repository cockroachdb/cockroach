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
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlclustersettings"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/errors"
)

// zoneConfigObjType is an enum to represent various types of "objects" that are
// supported by the CONFIGURE ZONE statement. This is used to determine the
// scpb that will be generated.
type zoneConfigObjType int

const (
	// unspecifiedObj is used when the object type is not specified.
	unspecifiedObj zoneConfigObjType = iota
	databaseObj
	tableObj
	idxObj
)

func SetZoneConfig(b BuildCtx, n *tree.SetZoneConfig) {
	// Block secondary tenants from ALTER CONFIGURE ZONE unless cluster setting is set.
	if err := sqlclustersettings.RequireSystemTenantOrClusterSetting(
		b.Codec(), b.ClusterSettings(), sqlclustersettings.SecondaryTenantZoneConfigsEnabled,
	); err != nil {
		panic(err)
	}

	// TODO(annie): implement complete support for CONFIGURE ZONE. This currently
	// Supports:
	// - Database
	// - Table
	// - Index
	// Left to support:
	// - Partition/row
	// - System Ranges
	objType, err := fallBackIfNotSupportedZoneConfig(n)
	if err != nil {
		panic(err)
	}

	// Fallback to the legacy schema changer if the table name is not referenced.
	//
	// TODO(annie): remove this when we have something equivalent to
	// expandMutableIndexName in the DSC.
	if objType == idxObj && n.TableOrIndex.Table.Table() == "" {
		panic(scerrors.NotImplementedErrorf(n, "referencing an index without a table "+
			"prefix is not supported in the DSC"))
	}

	// Fall back to the legacy schema changer if this is a YAML config (deprecated).
	// Block from using YAML config unless we are discarding a YAML config.
	if n.YAMLConfig != nil && !n.Discard {
		panic(scerrors.NotImplementedErrorf(n,
			"YAML config is deprecated and not supported in the declarative schema changer"))
	}

	if err := checkPrivilegeForSetZoneConfig(b, n, objType); err != nil {
		panic(err)
	}

	err = checkZoneConfigChangePermittedForMultiRegion(b, n.ZoneSpecifier, n.Options, objType)
	if err != nil {
		panic(err)
	}

	options, err := getUpdatedZoneConfigOptions(b, n.Options, n.ZoneSpecifier.TelemetryName())
	if err != nil {
		panic(err)
	}

	optionsStr, copyFromParentList, setters, err := evaluateZoneOptions(b, options)
	if err != nil {
		panic(err)
	}

	telemetryName := n.ZoneSpecifier.TelemetryName()
	telemetry.Inc(
		sqltelemetry.SchemaChangeAlterCounterWithExtra(telemetryName, "configure_zone"),
	)

	zc, seqNum, hasNewSubzones, indexID, err := applyZoneConfig(b, n, copyFromParentList,
		setters, objType)
	if err != nil {
		panic(err)
	}

	// For tables, we have to directly modify the AST to full resolve the table name.
	if n.TargetsTable() {
		resolvePhysicalTableName(b, n)
	}

	elem := addZoneConfigToBuildCtx(b, n, zc, seqNum, objType, indexID, hasNewSubzones)
	// Record that the change has occurred for auditing.
	eventDetails := eventpb.CommonZoneConfigDetails{
		Target:  tree.AsString(&n.ZoneSpecifier),
		Options: optionsStr,
	}
	info := &eventpb.SetZoneConfig{CommonZoneConfigDetails: eventDetails}
	b.LogEventForExistingPayload(elem, info)
}

// addZoneConfigToBuildCtx adds the zone config to the build context and returns
// the added element for logging.
func addZoneConfigToBuildCtx(
	b BuildCtx,
	n *tree.SetZoneConfig,
	zc *zonepb.ZoneConfig,
	seqNum uint32,
	objType zoneConfigObjType,
	indexID catid.IndexID,
	hasNewSubzones bool,
) scpb.Element {
	var elem scpb.Element
	// Increment the value of seqNum to ensure a new zone config is being
	// updated with a different seqNum.
	seqNum += 1
	targetID, err := getTargetIDFromZoneSpecifier(b, n.ZoneSpecifier, objType)
	if err != nil {
		panic(err)
	}
	switch objType {
	case databaseObj:
		elem = &scpb.DatabaseZoneConfig{
			DatabaseID: targetID,
			ZoneConfig: zc,
			SeqNum:     seqNum,
		}
	case tableObj:
		elem = &scpb.TableZoneConfig{
			TableID:    targetID,
			ZoneConfig: zc,
			SeqNum:     seqNum,
		}
	case idxObj:
		oldZoneConfig := b.QueryByID(targetID).FilterTableZoneConfig().MustGetZeroOrOneElement()
		subzones := zc.Subzones
		var subzoneToWrite zonepb.Subzone
		for _, subzone := range subzones {
			if subzone.IndexID == uint32(indexID) && len(subzone.PartitionName) == 0 {
				subzoneToWrite = subzone
			}
		}
		// Merge the new subzones with the old subzones so that we can generate
		// accurate subzone spans.
		if oldZoneConfig != nil {
			for _, newSubzone := range subzones {
				oldZoneConfig.ZoneConfig.SetSubzone(newSubzone)
			}
			subzones = oldZoneConfig.ZoneConfig.Subzones
		}

		ss, err := generateSubzoneSpans(b, targetID, subzones, hasNewSubzones)
		if err != nil {
			panic(err)
		}
		elem = &scpb.IndexZoneConfig{
			TableID:      targetID,
			IndexID:      indexID,
			Subzone:      subzoneToWrite,
			SubzoneSpans: ss,
			SeqNum:       seqNum,
		}
	default:
		panic(errors.AssertionFailedf("programming error: unsupported object type for " +
			"CONFIGURE ZONE"))
	}
	b.Add(elem)
	return elem
}
