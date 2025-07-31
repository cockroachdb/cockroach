// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package scbuildstmt

import (
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

func SetZoneConfig(b BuildCtx, n *tree.SetZoneConfig) {
	// Block secondary tenants from ALTER CONFIGURE ZONE unless cluster setting is set.
	if err := sqlclustersettings.RequireSystemTenantOrClusterSetting(
		b.Codec(), b.ClusterSettings(), sqlclustersettings.SecondaryTenantZoneConfigsEnabled,
	); err != nil {
		panic(err)
	}

	// Fall back to the legacy schema changer if this is a YAML config (deprecated).
	// Block from using YAML config unless we are discarding a YAML config.
	if n.YAMLConfig != nil && !n.Discard {
		panic(scerrors.NotImplementedErrorf(n,
			"YAML config is deprecated and not supported in the declarative schema changer"))
	}

	// TODO(annie): implement complete support for CONFIGURE ZONE. This currently
	// Supports:
	// - Database
	// - Table
	// - Index
	// - Partition/row
	// Left to support:
	// - System Ranges
	zco, err := astToZoneConfigObject(b, n)
	if err != nil {
		panic(err)
	}

	zs := n.ZoneSpecifier

	if err := zco.checkPrivilegeForSetZoneConfig(b, zs); err != nil {
		panic(err)
	}

	if err := zco.checkZoneConfigChangePermittedForMultiRegion(
		b, zs, n.Options,
	); err != nil {
		panic(err)
	}

	options, err := getUpdatedZoneConfigOptions(b, n.Options, zs.TelemetryName())
	if err != nil {
		panic(err)
	}

	optionsStr, copyFromParentList, setters, err := evaluateZoneOptions(b, options)
	if err != nil {
		panic(err)
	}

	telemetry.Inc(
		sqltelemetry.SchemaChangeAlterCounterWithExtra(zs.TelemetryName(), "configure_zone"),
	)

	oldZone, err := zco.applyZoneConfig(b, n, copyFromParentList, setters)
	if err != nil {
		panic(err)
	}

	// For tables, we have to directly modify the AST to full resolve the table name.
	if n.TargetsTable() {
		resolvePhysicalTableName(b, n)
	}

	elem := zco.addZoneConfigToBuildCtx(b)

	// Log event for auditing
	eventDetails := eventpb.CommonZoneConfigDetails{
		Target:  tree.AsString(&n.ZoneSpecifier),
		Options: optionsStr,
	}
	info := &eventpb.SetZoneConfig{CommonZoneConfigDetails: eventDetails,
		ResolvedOldConfig: oldZone.String()}
	b.LogEventForExistingPayload(elem, info)
}

func astToZoneConfigObject(b BuildCtx, n *tree.SetZoneConfig) (zoneConfigObject, error) {
	if n.Discard {
		return nil, scerrors.NotImplementedErrorf(n, "discarding zone configurations is not "+
			"supported in the DSC")
	}
	zs := n.ZoneSpecifier
	// We are a database object.
	if n.Database != "" {
		dbElem := b.ResolveDatabase(zs.Database, ResolveParams{}).FilterDatabase().MustGetOneElement()
		return &databaseZoneConfigObj{databaseID: dbElem.DatabaseID}, nil
	}

	// The rest of the cases are for table elements -- resolve the table ID now.
	// Fallback to the legacy schema changer if the table name is not referenced.
	//
	// TODO(annie): remove this when we have something equivalent to
	// expandMutableIndexName in the DSC.
	targetsIndex := n.TargetsIndex()
	if targetsIndex && n.TableOrIndex.Table.Table() == "" {
		return nil, scerrors.NotImplementedErrorf(n, "referencing an index without a table "+
			"prefix is not supported in the DSC")
	}

	if !n.TargetsTable() {
		return nil, scerrors.NotImplementedErrorf(n, "zone configurations on system ranges "+
			"are not supported in the DSC")
	}

	// If this is an ALTER ALL PARTITIONS statement, fallback to the legacy schema
	// changer.
	if n.TargetsPartition() && n.ZoneSpecifier.StarIndex {
		return nil, scerrors.NotImplementedErrorf(n, "zone configurations on ALL partitions "+
			"are not supported in the DSC")
	}
	tblName := zs.TableOrIndex.Table.ToUnresolvedObjectName()
	elems := b.ResolvePhysicalTable(tblName, ResolveParams{})
	panicIfSchemaChangeIsDisallowed(elems, n)
	var tableID catid.DescID
	elems.ForEach(func(_ scpb.Status, _ scpb.TargetStatus, e scpb.Element) {
		switch e := e.(type) {
		case *scpb.Table:
			tableID = e.TableID
		case *scpb.View:
			if e.IsMaterialized {
				tableID = e.ViewID
			}
		case *scpb.Sequence:
			tableID = e.SequenceID
		}
	})
	if tableID == catid.InvalidDescID {
		return nil, errors.AssertionFailedf("tableID not found for table %s", tblName)
	}
	tzo := tableZoneConfigObj{tableID: tableID}

	// We are a table object.
	if n.TargetsTable() && !n.TargetsIndex() && !n.TargetsPartition() {
		return &tzo, nil
	}

	izo := indexZoneConfigObj{tableZoneConfigObj: tzo}
	// We are an index object. Determine the index ID and fill this
	// information out in our zoneConfigObject.
	izo.fillIndexFromZoneSpecifier(b, n.ZoneSpecifier)
	if targetsIndex && !n.TargetsPartition() {
		return &izo, nil
	}

	// We are a partition object.
	if n.TargetsPartition() {
		partObj := partitionZoneConfigObj{partitionName: string(n.ZoneSpecifier.Partition),
			indexZoneConfigObj: izo}
		return &partObj, nil
	}

	return nil, errors.AssertionFailedf("unexpected zone config object")
}
