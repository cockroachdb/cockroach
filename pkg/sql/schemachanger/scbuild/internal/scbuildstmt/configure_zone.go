// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package scbuildstmt

import (
	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
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

	// Log event for auditing
	eventDetails := eventpb.CommonZoneConfigDetails{
		Target:  tree.AsString(&n.ZoneSpecifier),
		Options: optionsStr,
	}

	// In both the cases below, we generate the element for our AST and add/drop
	// it. For index/partitions, this change includes changing the subzone's
	// corresponding subzoneSpans -- which could necessitate any existing subzone
	// to regenerate the keys for their subzoneSpans (see:
	// `alter_partition_configure_zone_subpartitions.definition` for an example).
	// Those changes are represented by affectedSubzoneConfigsToUpdate.
	if n.Discard {
		// If we are discarding the zone config and a zone config did not previously
		// exist for us to discard, then no-op.
		if zco.isNoOp() {
			return
		}

		toDropList, affectedSubzoneConfigsToUpdate := zco.getZoneConfigElemForDrop(b)
		for i, e := range toDropList {
			// Log the latest dropping element.
			dropZoneConfigElem(b, e, eventDetails, i == len(toDropList)-1 /* isLoggingNeeded */)
		}
		for _, e := range affectedSubzoneConfigsToUpdate {
			// No need to log the side effects.
			addZoneConfigElem(b, e, oldZone, eventDetails, false /* isLoggingNeeded */)
		}
	} else {
		toAdd, affectedSubzoneConfigsToUpdate := zco.getZoneConfigElemForAdd(b)
		addZoneConfigElem(b, toAdd, oldZone, eventDetails, true)
		for _, e := range affectedSubzoneConfigsToUpdate {
			// No need to log the side effects.
			addZoneConfigElem(b, e, oldZone, eventDetails, false /* isLoggingNeeded */)
		}
	}
}

func astToZoneConfigObject(b BuildCtx, n *tree.SetZoneConfig) (zoneConfigObject, error) {
	zs := n.ZoneSpecifier

	// We are named range.
	if zs.NamedZone != "" {
		namedZone := zonepb.NamedZone(zs.NamedZone)
		id, found := zonepb.NamedZones[namedZone]
		if !found {
			return nil, pgerror.Newf(pgcode.InvalidName, "%q is not a built-in zone",
				string(zs.NamedZone))
		}
		if n.Discard && id == keys.RootNamespaceID {
			return nil, pgerror.Newf(pgcode.CheckViolation, "cannot remove default zone")
		}
		return &namedRangeZoneConfigObj{rangeID: catid.DescID(id)}, nil
	}

	// We are a database object.
	if zs.Database != "" {
		dbElem := b.ResolveDatabase(zs.Database, ResolveParams{}).FilterDatabase().MustGetOneElement()
		return &databaseZoneConfigObj{databaseID: dbElem.DatabaseID}, nil
	}

	// The rest of the cases are for table elements -- resolve the table ID now.
	// Fallback to the legacy schema changer if the table name is not referenced.
	//
	// TODO(annie): remove this when we have something equivalent to
	// expandMutableIndexName in the DSC.
	targetsIndex := zs.TargetsIndex()
	if targetsIndex && zs.TableOrIndex.Table.Table() == "" {
		return nil, scerrors.NotImplementedErrorf(n, "referencing an index without a table "+
			"prefix is not supported in the DSC")
	}
	// If this is an ALTER ALL PARTITIONS statement, fallback to the legacy schema
	// changer.
	if zs.TargetsPartition() && zs.StarIndex {
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
	if zs.TargetsTable() && !zs.TargetsIndex() && !zs.TargetsPartition() {
		return &tzo, nil
	}

	izo := indexZoneConfigObj{tableZoneConfigObj: tzo}
	if targetsIndex && !zs.TargetsPartition() {
		return &izo, nil
	}

	// We are a partition object.
	if zs.TargetsPartition() {
		partObj := partitionZoneConfigObj{partitionName: string(zs.Partition),
			indexZoneConfigObj: izo}
		return &partObj, nil
	}

	return nil, errors.AssertionFailedf("unexpected zone config object")
}

func dropZoneConfigElem(
	b BuildCtx, elem scpb.Element, eventDetails eventpb.CommonZoneConfigDetails, isLoggingNeeded bool,
) {
	b.Drop(elem)
	if isLoggingNeeded {
		info := &eventpb.RemoveZoneConfig{CommonZoneConfigDetails: eventDetails}
		b.LogEventForExistingPayload(elem, info)
	}
}

func addZoneConfigElem(
	b BuildCtx,
	elem scpb.Element,
	oldZone *zonepb.ZoneConfig,
	eventDetails eventpb.CommonZoneConfigDetails,
	isLoggingNeeded bool,
) {
	b.Add(elem)
	if isLoggingNeeded {
		info := &eventpb.SetZoneConfig{CommonZoneConfigDetails: eventDetails,
			ResolvedOldConfig: oldZone.String()}
		b.LogEventForExistingPayload(elem, info)
	}
}
