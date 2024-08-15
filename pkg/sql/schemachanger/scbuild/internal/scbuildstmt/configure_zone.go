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

	// TODO(annie): implement complete support for CONFIGURE ZONE. This currently
	// Supports:
	// - Database
	// - Table
	// - Index
	// Left to support:
	// - Partition/row
	// - System Ranges
	zoneConfigObject := astToZoneConfigObject(b, n)
	if zoneConfigObject == nil {
		panic(scerrors.NotImplementedErrorf(n, "unsupported zone config mode"))
	}

	// Fallback to the legacy schema changer if the table name is not referenced.
	//
	// TODO(annie): remove this when we have something equivalent to
	// expandMutableIndexName in the DSC.
	_, ok := zoneConfigObject.(*IndexZoneConfigObj)
	if ok && n.TableOrIndex.Table.Table() == "" {
		panic(scerrors.NotImplementedErrorf(n, "referencing an index without a table "+
			"prefix is not supported in the DSC"))
	}

	// Fall back to the legacy schema changer if this is a YAML config (deprecated).
	// Block from using YAML config unless we are discarding a YAML config.
	if n.YAMLConfig != nil && !n.Discard {
		panic(scerrors.NotImplementedErrorf(n,
			"YAML config is deprecated and not supported in the declarative schema changer"))
	}

	zs := n.ZoneSpecifier
	if err := zoneConfigObject.CheckPrivilegeForSetZoneConfig(b, zs); err != nil {
		panic(err)
	}

	if err := zoneConfigObject.CheckZoneConfigChangePermittedForMultiRegion(
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

	if err = applyZoneConfig(b, n, copyFromParentList, setters, zoneConfigObject); err != nil {
		panic(err)
	}

	// For tables, we have to directly modify the AST to full resolve the table name.
	if _, ok := zoneConfigObject.(*TableZoneConfigObj); ok {
		resolvePhysicalTableName(b, n)
	}

	elem := zoneConfigObject.AddZoneConfigToBuildCtx(b)

	// Log event for auditing
	eventDetails := eventpb.CommonZoneConfigDetails{
		Target:  tree.AsString(&n.ZoneSpecifier),
		Options: optionsStr,
	}
	info := &eventpb.SetZoneConfig{CommonZoneConfigDetails: eventDetails}
	b.LogEventForExistingPayload(elem, info)
}

func astToZoneConfigObject(b BuildCtx, n *tree.SetZoneConfig) zoneConfigObject {
	if n.Discard {
		return nil
	}
	zs := n.ZoneSpecifier
	// We are a database object.
	if n.Database != "" {
		dbElem := b.ResolveDatabase(zs.Database, ResolveParams{}).FilterDatabase().MustGetOneElement()
		return &DatabaseZoneConfigObj{databaseID: dbElem.DatabaseID}
	}

	// The rest of the cases are for table elements -- resolve the table ID now.
	tblName := zs.TableOrIndex.Table.ToUnresolvedObjectName()
	elems := b.ResolvePhysicalTable(tblName, ResolveParams{})
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
		panic(errors.AssertionFailedf("tableID not found for table %s", tblName))
	}
	tblComp := TableComponentZoneConfig{tableID: tableID}

	// We are a table object.
	if n.TargetsTable() && !n.TargetsIndex() && !n.TargetsPartition() {
		return &TableZoneConfigObj{TableComponentZoneConfig: tblComp}
	}

	// We are an index object.
	if n.TargetsIndex() && !n.TargetsPartition() {
		return &IndexZoneConfigObj{TableComponentZoneConfig: tblComp}
	}

	return nil
}
