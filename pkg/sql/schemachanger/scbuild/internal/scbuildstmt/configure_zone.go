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
	"bytes"
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/zone"
	"github.com/cockroachdb/cockroach/pkg/sql/covering"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc/valueside"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/transform"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlclustersettings"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
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

	zc, seqNum, err := applyZoneConfig(b, n, copyFromParentList, setters, objType)
	if err != nil {
		panic(err)
	}

	// For tables, we have to directly modify the AST to full resolve the table name.
	if n.TargetsTable() {
		resolvePhysicalTableName(b, n)
	}

	elem := addZoneConfigToBuildCtx(b, n, zc, seqNum, objType)
	// Record that the change has occurred for auditing.
	eventDetails := eventpb.CommonZoneConfigDetails{
		Target:  tree.AsString(&n.ZoneSpecifier),
		Options: optionsStr,
	}
	info := &eventpb.SetZoneConfig{CommonZoneConfigDetails: eventDetails}
	b.LogEventForExistingPayload(elem, info)
}

// resolvePhysicalTableName resolves the table name for a physical table
// in the SetZoneConfig AST by directly modifying its TableOrIndex.Table.
func resolvePhysicalTableName(b BuildCtx, n *tree.SetZoneConfig) {
	uon := n.ZoneSpecifier.TableOrIndex.Table.ToUnresolvedObjectName()
	tn := uon.ToTableName()
	elts := b.ResolvePhysicalTable(uon, ResolveParams{})
	tbl := elts.Filter(func(_ scpb.Status, _ scpb.TargetStatus, e scpb.Element) bool {
		switch e := e.(type) {
		case *scpb.Table:
			return true
		case *scpb.View:
			if e.IsMaterialized {
				return true
			}
		case *scpb.Sequence:
			return true
		}
		return false
	}).MustGetOneElement()
	tn.ObjectNamePrefix = b.NamePrefix(tbl)
	n.ZoneSpecifier.TableOrIndex.Table = tn
}

// checkPrivilegeForSetZoneConfig checks whether current user has the right
// privilege for configuring zone on the specified object(s).
func checkPrivilegeForSetZoneConfig(
	b BuildCtx, n *tree.SetZoneConfig, objType zoneConfigObjType,
) error {
	zs := n.ZoneSpecifier

	// For the system database, the user must be an admin. Otherwise, we
	// require CREATE or ZONECONFIG privilege on the database in question.
	reqNonAdminPrivs := []privilege.Kind{privilege.ZONECONFIG, privilege.CREATE}
	if objType == databaseObj {
		return checkPrivilegeForDBSetZoneConfig(b, zs, reqNonAdminPrivs)
	}

	tblID, err := getTargetIDFromZoneSpecifier(b, zs, objType)
	if err != nil {
		return err
	}
	tblElem := mustRetrievePhysicalTableElem(b, tblID)
	tblNamespaceElem := mustRetrieveNamespaceElem(b, tblID)
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

	return sqlerrors.NewInsufficientPrivilegeOnDescriptorError(b.CurrentUser(),
		reqNonAdminPrivs, string(catalog.Table), tblNamespaceElem.Name)
}

func checkPrivilegeForDBSetZoneConfig(
	b BuildCtx, zs tree.ZoneSpecifier, reqNonAdminPrivs []privilege.Kind,
) error {
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

	return sqlerrors.NewInsufficientPrivilegeOnDescriptorError(b.CurrentUser(),
		reqNonAdminPrivs, string(catalog.Database), mustRetrieveNamespaceElem(b, dbElem.DatabaseID).Name)
}

// checkZoneConfigChangePermittedForMultiRegion checks if a zone config
// change is permitted for a multi-region database or table.
// The change is permitted iff it is not modifying a protected multi-region
// field of the zone configs (as defined by zonepb.MultiRegionZoneConfigFields).
func checkZoneConfigChangePermittedForMultiRegion(
	b BuildCtx, zs tree.ZoneSpecifier, options tree.KVOptions, objType zoneConfigObjType,
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

	isDB := objType == databaseObj
	// Check if what we're altering is a multi-region entity.
	if isDB {
		dbRegionConfigElem := b.ResolveDatabase(zs.Database,
			ResolveParams{}).FilterDatabaseRegionConfig().MustGetZeroOrOneElement()
		if dbRegionConfigElem == nil {
			// Not a multi-region database, we're done here.
			return nil
		}
	} else {
		// We're dealing with a table or index zone configuration change. Get the
		// table descriptor so we can determine if this is a multi-region
		// table/index.
		tableID, err := getTargetIDFromZoneSpecifier(b, zs, objType)
		if err != nil {
			return err
		}
		if !isMultiRegionTable(b, tableID) {
			// Not a multi-region table, we're done here.
			return nil
		}
	}

	hint := "to override this error, SET override_multi_region_zone_config = true and reissue the command"

	// This is clearly an n^2 operation, but since there are only a single
	// digit number of zone config keys, it's likely faster to do it this way
	// than incur the memory allocation of creating a map.
	for _, opt := range options {
		for _, cfg := range zonepb.MultiRegionZoneConfigFields {
			if opt.Key == cfg {
				// User is trying to update a zone config value that's protected for
				// multi-region databases. Return the constructed error.
				err := errors.Newf("attempting to modify protected field %q of a multi-region zone "+
					"configuration", string(opt.Key),
				)
				return errors.WithHint(err, hint)
			}
		}
	}

	return nil
}

// isMultiRegionTable returns True if this table is a multi-region table,
// meaning it has locality GLOBAL, or REGIONAL BY TABLE, or REGIONAL BY ROW.
func isMultiRegionTable(b BuildCtx, tableID catid.DescID) bool {
	tableElems := b.QueryByID(tableID)
	globalElem := tableElems.FilterTableLocalityGlobal().MustGetZeroOrOneElement()
	primaryRegionElem := tableElems.FilterTableLocalityPrimaryRegion().MustGetZeroOrOneElement()
	secondaryRegionElem := tableElems.FilterTableLocalitySecondaryRegion().MustGetZeroOrOneElement()
	RBRElem := tableElems.FilterTableLocalityRegionalByRow().MustGetZeroOrOneElement()
	return globalElem != nil || primaryRegionElem != nil || secondaryRegionElem != nil ||
		RBRElem != nil
}

// getTargetIDFromZoneSpecifier attempts to find the ID of the target by the
// zone specifier.
// Recall that a zone specifier specifies either a database or a table. This
// function will return the ID of the database or the table.
func getTargetIDFromZoneSpecifier(
	b BuildCtx, zs tree.ZoneSpecifier, objType zoneConfigObjType,
) (catid.DescID, error) {
	if objType == databaseObj {
		dbElem := b.ResolveDatabase(zs.Database, ResolveParams{}).FilterDatabase().MustGetOneElement()
		return dbElem.DatabaseID, nil
	}
	return getTableIDFromZoneSpecifier(b, zs, objType)
}

// getTableIDFromZoneSpecifier attempts to find the table ID specified by the
// zone specifier. If the zone does not specify a table, a non-nil error is
// returned. Otherwise (for tables), the associated table ID is returned.
func getTableIDFromZoneSpecifier(
	b BuildCtx, zs tree.ZoneSpecifier, objType zoneConfigObjType,
) (catid.DescID, error) {
	if objType == databaseObj {
		return 0, errors.AssertionFailedf("programming error: zone specifier is for a " +
			"database; not a table")
	}
	if objType != tableObj && objType != idxObj {
		return 0, errors.AssertionFailedf("programming error: zs does not specify a proper " +
			"target")
	}
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
		panic("uh oh")
	}
	return tableID, nil
}

// getUpdatedZoneConfigOptions unpacks all kv options for a `CONFIGURE ZONE
// USING ...` stmt. It ensures all kv options are supported and the values are
// type-checked and normalized.
func getUpdatedZoneConfigOptions(
	b BuildCtx, n tree.KVOptions, telemetryName string,
) (map[tree.Name]zone.OptionValue, error) {

	var options map[tree.Name]zone.OptionValue
	// We have a CONFIGURE ZONE USING ... assignment.
	if n != nil {
		options = make(map[tree.Name]zone.OptionValue)
		for _, opt := range n {
			if _, alreadyExists := options[opt.Key]; alreadyExists {
				return nil, pgerror.Newf(pgcode.InvalidParameterValue,
					"duplicate zone config parameter: %q", tree.ErrString(&opt.Key))
			}
			// Here we are constrained by the supported ZoneConfig fields,
			// as described by zone.SupportedZoneConfigOptions.
			req, ok := zone.SupportedZoneConfigOptions[opt.Key]
			if !ok {
				return nil, pgerror.Newf(pgcode.InvalidParameterValue,
					"unsupported zone config parameter: %q", tree.ErrString(&opt.Key))
			}
			telemetry.Inc(
				sqltelemetry.SchemaSetZoneConfigCounter(
					telemetryName,
					string(opt.Key),
				),
			)
			if opt.Value == nil {
				options[opt.Key] = zone.OptionValue{InheritValue: true, ExplicitValue: nil}
				continue
			}

			// Type check and normalize value expr.
			typedExpr, err := tree.TypeCheckAndRequire(b, opt.Value, b.SemaCtx(), req.RequiredType,
				string(opt.Key))
			if err != nil {
				return nil, err
			}
			etctx := transform.ExprTransformContext{}
			valExpr, err := etctx.NormalizeExpr(b, b.EvalCtx(), typedExpr)
			if err != nil {
				return nil, err
			}

			options[opt.Key] = zone.OptionValue{InheritValue: false, ExplicitValue: valExpr}
		}
	}
	return options, nil
}

func evaluateZoneOptions(
	b BuildCtx, options map[tree.Name]zone.OptionValue,
) (
	optionsStr []string,
	copyFromParentList []tree.Name,
	setters []func(c *zonepb.ZoneConfig),
	err error,
) {
	if options != nil {
		// Set from var = value attributes.
		//
		// We iterate over zoneOptionKeys instead of iterating over
		// n.options directly so that the optionStr string constructed for
		// the event log remains deterministic.
		for i := range zone.ZoneOptionKeys {
			name := (*tree.Name)(&zone.ZoneOptionKeys[i])
			val, ok := options[*name]
			if !ok {
				continue
			}
			// We don't add the setters for the fields that will copy values
			// from the parents. These fields will be set by taking what
			// value would apply to the zone and setting that value explicitly.
			// Instead, we add the fields to a list that we use at a later time
			// to copy values over.
			inheritVal, expr := val.InheritValue, val.ExplicitValue
			if inheritVal {
				copyFromParentList = append(copyFromParentList, *name)
				optionsStr = append(optionsStr, fmt.Sprintf("%s = COPY FROM PARENT", name))
				continue
			}
			datum, err := eval.Expr(b, b.EvalCtx(), expr)
			if err != nil {
				return nil, nil, nil, err
			}
			if datum == tree.DNull {
				return nil, nil, nil,
					pgerror.Newf(pgcode.InvalidParameterValue, "unsupported NULL value for %q",
						tree.ErrString(name))
			}
			opt := zone.SupportedZoneConfigOptions[*name] // Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
			if opt.CheckAllowed != nil {
				if err := opt.CheckAllowed(b, b.ClusterSettings(), datum); err != nil {
					return nil, nil, nil, err
				}
			}
			setter := opt.Setter
			setters = append(setters, func(c *zonepb.ZoneConfig) { setter(c, datum) })
			optionsStr = append(optionsStr, fmt.Sprintf("%s = %s", name, datum))
		}
	}
	return optionsStr, copyFromParentList, setters, nil
}

func applyZoneConfig(
	b BuildCtx,
	n *tree.SetZoneConfig,
	copyFromParentList []tree.Name,
	setters []func(c *zonepb.ZoneConfig),
	objType zoneConfigObjType,
) (*zonepb.ZoneConfig, uint32, error) {
	// Determines the ID of the target object of the zone specifier. This is the
	// ID of either a database or a table.
	targetID, err := getTargetIDFromZoneSpecifier(b, n.ZoneSpecifier, objType)
	if err != nil {
		return nil, 0, err
	}

	// TODO(annie): once we allow configuring zones for named zones/system ranges,
	// we will need to guard against secondary tenants from configuring such
	// ranges.
	if objType == tableObj {
		// Check that we are not trying to configure a system table.
		if err = checkIfConfigurationAllowed(targetID); err != nil {
			return nil, 0, err
		}
	}

	// If we are configuring an index or a partition, then determine the index ID
	// and partition name as well.
	indexID, partition, err := getIndexAndPartitionFromZoneSpecifier(b, n.ZoneSpecifier, objType)
	if err != nil {
		return nil, 0, err
	}

	var tempIndexID catid.IndexID
	if indexID != 0 {
		tempIndexID = mustRetrieveIndexElement(b, targetID, indexID).TemporaryIndexID
	}

	// Retrieve the partial zone configuration
	partialZone, zc := retrievePartialZoneConfig(b, targetID, indexID, objType)

	subzonePlaceholder := false
	// No zone was found. Possibly a SubzonePlaceholder depending on the index.
	if partialZone == nil {
		partialZone = zonepb.NewZoneConfig()
		if indexID != 0 {
			subzonePlaceholder = true
		}
	}

	var partialSubzone *zonepb.Subzone
	if indexID != 0 {
		partialSubzone = partialZone.GetSubzoneExact(uint32(indexID), partition)
		if partialSubzone == nil {
			partialSubzone = &zonepb.Subzone{Config: *zonepb.NewZoneConfig()}
		}
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
	_, completeZone, completeSubZone, seqNum, err := retrieveCompleteZoneConfig(b, targetID, indexID,
		partition, objType, n.SetDefault /* getInheritedDefault */)
	if err != nil {
		return nil, 0, err
	}

	// We need to inherit zone configuration information from the correct zone,
	// not completeZone.
	{
		if indexID == 0 {
			// If we are operating on a zone, get all fields that the zone would
			// inherit from its parent. We do this by using an empty zoneConfig
			// and completing at the level of the current zone.
			zoneInheritedFields := zonepb.ZoneConfig{}
			if err := completeZoneConfig(b, targetID, objType, &zoneInheritedFields); err != nil {
				return nil, 0, err
			}
			partialZone.CopyFromZone(zoneInheritedFields, copyFromParentList)
		} else {
			// If we are operating on a subZone, we need to inherit all remaining
			// unset fields in its parent zone, which is partialZone.
			zoneInheritedFields := *partialZone
			if err := completeZoneConfig(b, targetID, objType, &zoneInheritedFields); err != nil {
				return nil, 0, err
			}
			// In the case we have just an index, we should copy from the inherited
			// zone's fields (whether that was the table or database).
			if partition == "" {
				partialSubzone.Config.CopyFromZone(zoneInheritedFields, copyFromParentList)
			} else {
				// In the case of updating a partition, we need to try inheriting fields
				// from the subzone's index, and inherit the remainder from the zone.
				subzoneInheritedFields := zonepb.ZoneConfig{}
				if indexSubzone := completeZone.GetSubzone(uint32(indexID), ""); indexSubzone != nil {
					subzoneInheritedFields.InheritFromParent(&indexSubzone.Config)
				}
				subzoneInheritedFields.InheritFromParent(&zoneInheritedFields)
				// After inheriting fields, copy the requested ones into the
				// partialSubzone.Config.
				partialSubzone.Config.CopyFromZone(subzoneInheritedFields, copyFromParentList)
			}
		}
	}

	// Determine where to load the configuration.
	newZone := *completeZone
	if completeSubZone != nil {
		newZone = completeSubZone.Config
	}

	// Determine where to load the partial configuration.
	// finalZone is where the new changes are unmarshalled onto.
	// It must be a fresh ZoneConfig if a new subzone if being created.
	// If an existing subzone is being modified, finalZone if overridden.
	finalZone := *partialZone
	if partialSubzone != nil {
		finalZone = partialSubzone.Config
	}

	if n.SetDefault {
		finalZone = *zonepb.NewZoneConfig()
	}

	// Load settings from var = val assignments. If there were no such
	// settings, (e.g. because the query specified CONFIGURE ZONE = or
	// USING DEFAULT), the setter slice will be empty and this will be
	// a no-op. This is innocuous.
	for _, setter := range setters {
		// A setter may fail with an error-via-panic. Catch those.
		if err := func() (err error) {
			defer func() {
				if p := recover(); p != nil {
					if errP, ok := p.(error); ok {
						// Catch and return the error.
						err = errP
					} else {
						// Nothing we know about, let it continue as a panic.
						panic(p)
					}
				}
			}()

			setter(&newZone)
			setter(&finalZone)
			return nil
		}(); err != nil {
			return nil, 0, err
		}
	}

	// Validate that there are no conflicts in the zone setup.
	if err := zonepb.ValidateNoRepeatKeysInZone(&newZone); err != nil {
		return nil, 0, err
	}

	currentZone := zonepb.NewZoneConfig()
	if zc != nil {
		currentZone = zc
	}
	if err := validateZoneAttrsAndLocalities(b, currentZone, &newZone); err != nil {
		return nil, 0, err
	}

	// Are we operating on an index?
	if indexID == 0 {
		// No: the final zone config is the one we just processed.
		completeZone = &newZone
		partialZone = &finalZone
		// Since we are writing to a zone that is not a subzone, we need to
		// make sure that the zone config is not considered a placeholder
		// anymore. If the settings applied to this zone don't touch the
		// NumReplicas field, set it to nil so that the zone isn't considered a
		// placeholder anymore.
		if partialZone.IsSubzonePlaceholder() {
			partialZone.NumReplicas = nil
		}
	} else {
		completeZone.SetSubzone(zonepb.Subzone{
			IndexID:       uint32(indexID),
			PartitionName: partition,
			Config:        newZone,
		})

		// The partial zone might just be empty. If so,
		// replace it with a SubzonePlaceholder.
		if subzonePlaceholder {
			partialZone.DeleteTableConfig()
		}

		partialZone.SetSubzone(zonepb.Subzone{
			IndexID:       uint32(indexID),
			PartitionName: partition,
			Config:        finalZone,
		})

		// Also set the same zone configs for any corresponding temporary indexes.
		if tempIndexID != 0 {
			completeZone.SetSubzone(zonepb.Subzone{
				IndexID:       uint32(tempIndexID),
				PartitionName: partition,
				Config:        newZone,
			})

			partialZone.SetSubzone(zonepb.Subzone{
				IndexID:       uint32(tempIndexID),
				PartitionName: partition,
				Config:        finalZone,
			})
		}
	}

	// Finally, revalidate everything. Validate only the completeZone config.
	if err := completeZone.Validate(); err != nil {
		return nil, 0, pgerror.Wrap(err, pgcode.CheckViolation, "could not validate zone config")
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
		return nil, 0, err
	}
	return partialZone, seqNum, nil
}

// checkIfConfigurationAllowed determines whether a zone config can be set.
// This consists of checking that we are not trying to configure a system table.
func checkIfConfigurationAllowed(targetID catid.DescID) error {
	// Zones of "system config tables" (i.e. `system.descriptor` and
	// `system.zones` table) and NamespaceTable cannot be configured.
	if descpb.IsSystemConfigID(targetID) || targetID == keys.NamespaceTableID {
		return pgerror.Newf(pgcode.InvalidParameterValue,
			`cannot set zone configs for system config tables; `+
				`try setting your config on the entire "system" database instead`)
	}
	return nil
}

// getIndexAndPartitionFromZoneSpecifier finds the index id and partition name
// in the zone specifier. If the zone specifier targets other than an index or
// partition, then the returned indexID is zero. If the zone specifier specifies
// an index, then the returned partition is empty.
func getIndexAndPartitionFromZoneSpecifier(
	b BuildCtx, zs tree.ZoneSpecifier, objType zoneConfigObjType,
) (indexID catid.IndexID, partitionName string, err error) {
	if !zs.TargetsIndex() && !zs.TargetsPartition() {
		return 0, "", nil
	}

	tableID, err := getTableIDFromZoneSpecifier(b, zs, objType)
	if err != nil {
		return 0, "", err
	}

	indexName := string(zs.TableOrIndex.Index)
	if indexName == "" {
		// Use the primary index if index name is unspecified.
		primaryIndexElem := mustRetrieveCurrentPrimaryIndexElement(b, tableID)
		indexID = primaryIndexElem.IndexID
		indexName = mustRetrieveIndexNameElem(b, tableID, indexID).Name
	} else {
		indexElems := b.ResolveIndex(tableID, tree.Name(indexName), ResolveParams{})
		indexID = indexElems.FilterIndexName().MustGetOneElement().IndexID
	}

	partitionName = string(zs.Partition)
	if partitionName != "" {
		indexPartitionElem := maybeRetrieveIndexPartitioningElem(b, tableID, indexID)
		if indexPartitionElem == nil ||
			tabledesc.NewPartitioning(&indexPartitionElem.PartitioningDescriptor).FindPartitionByName(partitionName) == nil {
			return 0, "", errors.Newf("partitionName %q does not exist on index %q", partitionName, indexName)
		}
	}
	return indexID, partitionName, nil
}

// retrievePartialZoneConfig retrieves the partial zone configuration of the
// specified targetID.
func retrievePartialZoneConfig(
	b BuildCtx, targetID catid.DescID, idxID catid.IndexID, objectType zoneConfigObjType,
) (*zonepb.ZoneConfig, *zonepb.ZoneConfig) {
	var partialZone *zonepb.ZoneConfig
	var zc *zonepb.ZoneConfig

	// Retrieve the partial zone configuration for specified objectType. Fall back
	// to the default zone configuration if the zone configuration does not exist.
	switch objectType {
	case databaseObj:
		var dbZoneConfigElem *scpb.DatabaseZoneConfig
		dbZoneElems := b.QueryByID(targetID).FilterDatabaseZoneConfig()
		dbZoneElems.ForEach(func(current scpb.Status, target scpb.TargetStatus, e *scpb.DatabaseZoneConfig) {
			// We want to get the most recent change that has not applied yet. For transactions, this will
			// be the most recent (last) zone config elem added.
			if e.DatabaseID == targetID {
				dbZoneConfigElem = e
			}
		})
		if dbZoneConfigElem != nil {
			partialZone = dbZoneConfigElem.ZoneConfig
			zc = dbZoneConfigElem.ZoneConfig
		}
	case tableObj:
		var tblZoneConfigElem *scpb.TableZoneConfig
		tblZoneElems := b.QueryByID(targetID).FilterTableZoneConfig()
		tblZoneElems.ForEach(func(current scpb.Status, target scpb.TargetStatus, e *scpb.TableZoneConfig) {
			// We want to get the most recent change that has not applied yet. For transactions, this will
			// be the most recent (last) zone config elem added.
			if e.TableID == targetID {
				tblZoneConfigElem = e
			}
		})
		if tblZoneConfigElem != nil {
			partialZone = tblZoneConfigElem.ZoneConfig
			zc = tblZoneConfigElem.ZoneConfig
		}
	case idxObj:
		if idxID == 0 {
			panic(errors.AssertionFailedf("programming error: idxID is 0 for indexObj"))
		}
		var idxZoneConfigElem *scpb.IndexZoneConfig
		idxZoneElems := b.QueryByID(targetID).FilterIndexZoneConfig()
		idxZoneElems.ForEach(func(current scpb.Status, target scpb.TargetStatus, e *scpb.IndexZoneConfig) {
			// We want to get the most recent change that has not applied yet. For transactions, this will
			// be the most recent (first) zone config elem added.
			if e.TableID == targetID && e.IndexID == idxID {
				idxZoneConfigElem = e
				// TODO(annie): we should have a FindPredicate here that does just this
				//
				// Override the index ID to an invalid ID so we don't reassign
				// idxZoneConfigElem.
				idxID = 0
			}
		})
		if idxZoneConfigElem != nil {
			partialZone = idxZoneConfigElem.ZoneConfig
			zc = idxZoneConfigElem.ZoneConfig
		}
	default:
		panic(errors.AssertionFailedf("programming error: unsupported object type for " +
			"CONFIGURE ZONE"))
	}
	return partialZone, zc
}

// retrieveCompleteZoneConfig looks up the zone and subzone for the specified
// object ID, index, and partition.
//
// If `getInheritedDefault` is true, the direct zone configuration, if it exists,
// is ignored, and the default zone config that would apply if it did not exist
// is returned instead. This is because, if the stmt is `USING DEFAULT`, we want
// to ignore the zone config that exists on targetID and instead skip to the
// inherited default.
func retrieveCompleteZoneConfig(
	b BuildCtx,
	targetID catid.DescID,
	indexID catid.IndexID,
	partition string,
	objType zoneConfigObjType,
	getInheritedDefault bool,
) (zoneID descpb.ID, zone *zonepb.ZoneConfig, subzone *zonepb.Subzone, seqNum uint32, err error) {
	var placeholderID descpb.ID
	var placeholder *zonepb.ZoneConfig
	zc := &zonepb.ZoneConfig{}
	if getInheritedDefault {
		zoneID, zc, seqNum, err = getInheritedDefaultZoneConfig(b, targetID)
	} else {
		zoneID, zc, placeholderID, placeholder, seqNum, err = getZoneConfig(b, targetID)
	}
	if err != nil {
		return 0, nil, nil, 0, err
	}

	completeZc := *zc
	if err = completeZoneConfig(b, targetID, objType, &completeZc); err != nil {
		return 0, nil, nil, 0, err
	}

	if indexID != 0 {
		if placeholder != nil {
			if subzone = placeholder.GetSubzone(uint32(indexID), partition); subzone != nil {
				if indexSubzone := placeholder.GetSubzone(uint32(indexID), ""); indexSubzone != nil {
					subzone.Config.InheritFromParent(&indexSubzone.Config)
				}
				subzone.Config.InheritFromParent(zc)
				return placeholderID, placeholder, subzone, seqNum, nil
			}
		} else {
			if subzone = zc.GetSubzone(uint32(indexID), partition); subzone != nil {
				if indexSubzone := zc.GetSubzone(uint32(indexID), ""); indexSubzone != nil {
					subzone.Config.InheritFromParent(&indexSubzone.Config)
				}
				subzone.Config.InheritFromParent(zc)
			}
		}
	}

	return zoneID, zc, subzone, seqNum, nil
}

// getInheritedDefaultZoneConfig returns the inherited default zone config of
// `targetID`. This means
//   - if `targetID` is a table ID, returns the zone config of its parent database
//     (if exists) or the DEFAULT RANGE.
//   - otherwise, returns the zone config of the DEFAULT RANGE
func getInheritedDefaultZoneConfig(
	b BuildCtx, targetID catid.DescID,
) (zoneID catid.DescID, zc *zonepb.ZoneConfig, seqNum uint32, err error) {
	// Is `targetID` a table?
	maybeTblElem := retrievePhysicalTableElem(b, targetID)
	// Yes: get the zone config of its parent database.
	if maybeTblElem != nil {
		parentDBID := mustRetrieveNamespaceElem(b, targetID).DatabaseID
		zoneID, zc, _, _, seqNum, err = getZoneConfig(b, parentDBID)
		return zoneID, zc, seqNum, err
	}
	// No: get the zone config of the DEFAULT RANGE.
	zoneID, zc, _, _, seqNum, err = getZoneConfig(b, keys.RootNamespaceID)
	return zoneID, zc, seqNum, err
}

// getZoneConfig attempts to find the zone config from `system.zones` with
// `targetID` (`targetID` is either a database ID or a table ID).
func getZoneConfig(
	b BuildCtx, targetID catid.DescID,
) (
	zoneID catid.DescID,
	zc *zonepb.ZoneConfig,
	subzoneID catid.DescID,
	subzone *zonepb.ZoneConfig,
	seqNum uint32,
	err error,
) {
	zc, seqNum, err = lookUpSystemZonesTable(b, targetID)
	if err != nil {
		return 0, nil, 0, nil, 0, err
	}

	if zc != nil {
		if !zc.IsSubzonePlaceholder() {
			return zoneID, zc, 0, nil, seqNum, nil
		}
		subzone = zc
		subzoneID = zoneID
	}

	// No zone config for this ID. If `targetID` is a table, then recursively
	// get zone config of its parent database.
	tblElem := retrievePhysicalTableElem(b, targetID)
	if tblElem != nil {
		parentDBID := mustRetrieveNamespaceElem(b, targetID).DatabaseID
		zoneID, zc, _, _, seqNum, err = getZoneConfig(b, parentDBID)
		if err != nil {
			return 0, nil, 0, nil, 0, err
		}
		return zoneID, zc, 0, nil, seqNum, nil
	}

	// Otherwise, retrieve the default zone config, but only as long as that
	// wasn't the ID we were trying to retrieve (avoid infinite recursion).
	if targetID != keys.RootNamespaceID {
		zoneID, zc, _, _, seqNum, err := getZoneConfig(b, keys.RootNamespaceID)
		if err != nil {
			return 0, nil, 0, nil, 0, err
		}
		return zoneID, zc, subzoneID, subzone, seqNum, nil
	}

	// `targetID == keys.RootNamespaceID` but that zc config is not found
	// in `system.zones` table. Return a special, recognizable error!
	return 0, nil, 0, nil, 0, sqlerrors.ErrNoZoneConfigApplies
}

// lookUpSystemZonesTable attempts to look up the zone config in `system.zones`
// table by `targetID`.
// If `targetID` is not found, a nil `zone` is returned.
func lookUpSystemZonesTable(
	b BuildCtx, targetID catid.DescID,
) (zone *zonepb.ZoneConfig, seqNum uint32, err error) {
	if keys.RootNamespaceID == uint32(targetID) {
		zc, err := b.ZoneConfigGetter().GetZoneConfig(b, targetID)
		if err != nil {
			return nil, 0, err
		}
		zone = zc.ZoneConfigProto()
	} else {
		// It's a descriptor-backed target (i.e. a database ID or a table ID)
		b.QueryByID(targetID).ForEach(func(
			_ scpb.Status, _ scpb.TargetStatus, e scpb.Element,
		) {
			switch e := e.(type) {
			case *scpb.DatabaseZoneConfig:
				if e.DatabaseID == targetID {
					zone = e.ZoneConfig
					seqNum = e.SeqNum
				}
			case *scpb.IndexZoneConfig:
				if e.TableID == targetID {
					zone = e.ZoneConfig
					seqNum = e.SeqNum
					targetID = 0
				}
			case *scpb.TableZoneConfig:
				if e.TableID == targetID {
					zone = e.ZoneConfig
					seqNum = e.SeqNum
				}
			}
		})
	}
	return zone, seqNum, nil
}

// completeZoneConfig takes a zone config pointer and fills in the
// missing fields by following the chain of inheritance.
// In the worst case, will have to inherit from the default zone config.
func completeZoneConfig(
	b BuildCtx, targetID catid.DescID, objType zoneConfigObjType, zone *zonepb.ZoneConfig,
) error {
	if zone.IsComplete() {
		return nil
	}
	// Check to see if it's a table. If so, inherit from the database.
	// For all other cases, inherit from the default.
	if objType == tableObj {
		dbID := mustRetrieveNamespaceElem(b, targetID).DatabaseID
		_, dbZone, _, _, _, err := getZoneConfig(b, dbID)
		if err != nil {
			return err
		}
		zone.InheritFromParent(dbZone)
	}
	// Check if zone is complete. If not, inherit from the default zone config
	if zone.IsComplete() {
		return nil
	}
	_, defaultZone, _, _, _, err := getZoneConfig(b, keys.RootNamespaceID)
	if err != nil {
		return err
	}
	zone.InheritFromParent(defaultZone)
	return nil
}

// validateZoneAttrsAndLocalities ensures that all constraints/lease preferences
// specified in the new zone config snippet are actually valid, meaning that
// they match at least one node. This protects against user typos causing
// zone configs that silently don't work as intended.
//
// validateZoneAttrsAndLocalities is tenant aware in its validation. Secondary
// tenants don't have access to the NodeStatusServer, and as such, aren't
// allowed to set non-locality attributes in their constraints. Furthermore,
// their access is validated using the descs.RegionProvider.
func validateZoneAttrsAndLocalities(b BuildCtx, currentZone, newZone *zonepb.ZoneConfig) error {
	// Avoid RPCs to the Node/Region server if we don't have anything to validate.
	if len(newZone.Constraints) == 0 && len(newZone.VoterConstraints) == 0 && len(newZone.LeasePreferences) == 0 {
		return nil
	}
	if b.Codec().ForSystemTenant() {
		ss, err := b.NodesStatusServer().OptionalNodesStatusServer()
		if err != nil {
			return err
		}
		return validateZoneAttrsAndLocalitiesForSystemTenant(b, ss.ListNodesInternal, currentZone, newZone)
	}
	return validateZoneLocalitiesForSecondaryTenants(
		b, b.GetRegions, currentZone, newZone, b.Codec(), b.ClusterSettings(),
	)
}

type nodeGetter func(context.Context, *serverpb.NodesRequest) (*serverpb.NodesResponse, error)
type regionsGetter func(context.Context) (*serverpb.RegionsResponse, error)

// validateZoneAttrsAndLocalitiesForSystemTenant performs constraint/ lease
// preferences validation for the system tenant. Only newly added constraints
// are validated. The system tenant is allowed to reference both locality and
// non-locality attributes as it has access to node information via the
// NodeStatusServer.
//
// For the system tenant, this only catches typos in required constraints. This
// is by design. We don't want to reject prohibited constraints whose
// attributes/localities don't match any of the current nodes because it's a
// reasonable use case to add prohibited constraints for a new set of nodes
// before adding the new nodes to the cluster. If you had to first add one of
// the nodes before creating the constraints, data could be replicated there
// that shouldn't be.
func validateZoneAttrsAndLocalitiesForSystemTenant(
	b BuildCtx, getNodes nodeGetter, currentZone, newZone *zonepb.ZoneConfig,
) error {
	nodes, err := getNodes(b, &serverpb.NodesRequest{})
	if err != nil {
		return err
	}

	toValidate := accumulateNewUniqueConstraints(currentZone, newZone)

	// Check that each constraint matches some store somewhere in the cluster.
	for _, constraint := range toValidate {
		// We skip validation for negative constraints. See the function-level comment.
		if constraint.Type == zonepb.Constraint_PROHIBITED {
			continue
		}
		var found bool
	node:
		for _, node := range nodes.Nodes {
			for _, store := range node.StoreStatuses {
				// We could alternatively use zonepb.StoreMatchesConstraint here to
				// catch typos in prohibited constraints as well, but as noted in the
				// function-level comment that could break very reasonable use cases for
				// prohibited constraints.
				if zonepb.StoreSatisfiesConstraint(store.Desc, constraint) {
					found = true
					break node
				}
			}
		}
		if !found {
			return pgerror.Newf(pgcode.CheckViolation,
				"constraint %q matches no existing nodes within the cluster - did you enter it correctly?",
				constraint)
		}
	}

	return nil
}

// validateZoneLocalitiesForSecondaryTenants performs constraint/lease
// preferences validation for secondary tenants. Only newly added constraints
// are validated. Unless SecondaryTenantsAllZoneConfigsEnabled is set to 'true',
// secondary tenants are only allowed to reference locality attributes as they
// only have access to region information via the serverpb.TenantStatusServer.
// In that case they're only allowed to reference the "region" and "zone" tiers.
//
// Unlike the system tenant, we also validate prohibited constraints. This is
// because secondary tenant must operate in the narrow view exposed via the
// serverpb.TenantStatusServer and are not allowed to configure arbitrary
// constraints (required or otherwise).
func validateZoneLocalitiesForSecondaryTenants(
	ctx context.Context,
	getRegions regionsGetter,
	currentZone, newZone *zonepb.ZoneConfig,
	codec keys.SQLCodec,
	settings *cluster.Settings,
) error {
	toValidate := accumulateNewUniqueConstraints(currentZone, newZone)

	// rs and zs will be lazily populated with regions and zones, respectively.
	// These should not be accessed directly - use getRegionsAndZones helper
	// instead.
	var rs, zs map[string]struct{}
	getRegionsAndZones := func() (regions, zones map[string]struct{}, _ error) {
		if rs != nil {
			return rs, zs, nil
		}
		resp, err := getRegions(ctx)
		if err != nil {
			return nil, nil, err
		}
		rs, zs = make(map[string]struct{}), make(map[string]struct{})
		for regionName, regionMeta := range resp.Regions {
			rs[regionName] = struct{}{}
			for _, zone := range regionMeta.Zones {
				zs[zone] = struct{}{}
			}
		}
		return rs, zs, nil
	}

	for _, constraint := range toValidate {
		switch constraint.Key {
		case "zone":
			_, zones, err := getRegionsAndZones()
			if err != nil {
				return err
			}
			_, found := zones[constraint.Value]
			if !found {
				return pgerror.Newf(
					pgcode.CheckViolation,
					"zone %q not found",
					constraint.Value,
				)
			}
		case "region":
			regions, _, err := getRegionsAndZones()
			if err != nil {
				return err
			}
			_, found := regions[constraint.Value]
			if !found {
				return pgerror.Newf(
					pgcode.CheckViolation,
					"region %q not found",
					constraint.Value,
				)
			}
		default:
			if err := sqlclustersettings.RequireSystemTenantOrClusterSetting(
				codec, settings, sqlclustersettings.SecondaryTenantsAllZoneConfigsEnabled,
			); err != nil {
				return err
			}
		}
	}
	return nil
}

// accumulateNewUniqueConstraints returns a list of unique constraints in the
// given newZone config proto that are not in the currentZone
func accumulateNewUniqueConstraints(currentZone, newZone *zonepb.ZoneConfig) []zonepb.Constraint {
	seenConstraints := make(map[zonepb.Constraint]struct{})
	retConstraints := make([]zonepb.Constraint, 0)
	addToValidate := func(c zonepb.Constraint) {
		if _, ok := seenConstraints[c]; ok {
			// Already in the list or in the current zone config, nothing to do.
			return
		}
		retConstraints = append(retConstraints, c)
		seenConstraints[c] = struct{}{}
	}
	// First scan all the current zone config constraints.
	for _, constraints := range currentZone.Constraints {
		for _, constraint := range constraints.Constraints {
			seenConstraints[constraint] = struct{}{}
		}
	}
	for _, constraints := range currentZone.VoterConstraints {
		for _, constraint := range constraints.Constraints {
			seenConstraints[constraint] = struct{}{}
		}
	}
	for _, leasePreferences := range currentZone.LeasePreferences {
		for _, constraint := range leasePreferences.Constraints {
			seenConstraints[constraint] = struct{}{}
		}
	}

	// Then scan all the new zone config constraints, adding the ones that
	// were not seen already.
	for _, constraints := range newZone.Constraints {
		for _, constraint := range constraints.Constraints {
			addToValidate(constraint)
		}
	}
	for _, constraints := range newZone.VoterConstraints {
		for _, constraint := range constraints.Constraints {
			addToValidate(constraint)
		}
	}
	for _, leasePreferences := range newZone.LeasePreferences {
		for _, constraint := range leasePreferences.Constraints {
			addToValidate(constraint)
		}
	}
	return retConstraints
}

// fallBackIfNotSupportedZoneConfig determines if the table has a database
// zone config.
func fallBackIfNotSupportedZoneConfig(n *tree.SetZoneConfig) (zoneConfigObjType, error) {
	if n.Discard {
		return unspecifiedObj,
			scerrors.NotImplementedErrorf(n, "CONFIGURE ZONE DISCARD is not supported in DSC")
	}
	if n.Database != "" {
		return databaseObj, nil
	}

	if n.TargetsTable() && !n.TargetsIndex() && !n.TargetsPartition() {
		return tableObj, nil
	}
	if n.TargetsIndex() && !n.TargetsPartition() {
		return idxObj, nil
	}
	return unspecifiedObj, scerrors.NotImplementedErrorf(n, "unsupported CONFIGURE ZONE target")
}

// addZoneConfigToBuildCtx adds the zone config to the build context and returns
// the added element for logging.
func addZoneConfigToBuildCtx(
	b BuildCtx,
	n *tree.SetZoneConfig,
	zc *zonepb.ZoneConfig,
	seqNum uint32,
	objType zoneConfigObjType,
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
		elem = &scpb.IndexZoneConfig{
			TableID:    targetID,
			ZoneConfig: zc,
			SeqNum:     seqNum,
		}
		zc.SubzoneSpans, err = generateSubzoneSpans(b, targetID, zc.Subzones, "", hasNewSubzones)
		if err != nil {
			panic(err)
		}
	default:
		panic(errors.AssertionFailedf("programming error: unsupported object type for CONFIGURE ZONE"))
	}
	b.Add(elem)
	return elem
}

func retrievePhysicalTableElem(b BuildCtx, tableID catid.DescID) scpb.Element {
	return b.QueryByID(tableID).Filter(func(
		_ scpb.Status, _ scpb.TargetStatus, e scpb.Element,
	) bool {
		switch e := e.(type) {
		case *scpb.Table:
			return e.TableID == tableID
		case *scpb.View:
			if e.IsMaterialized {
				return e.ViewID == tableID
			}
		case *scpb.Sequence:
			return e.SequenceID == tableID
		}
		return false
	}).MustGetZeroOrOneElement()
}

func mustRetrievePhysicalTableElem(b BuildCtx, tableID catid.DescID) scpb.Element {
	physicalTableElem := retrievePhysicalTableElem(b, tableID)
	if physicalTableElem == nil {
		panic(errors.AssertionFailedf("programming error: cannot find a physical table "+
			"element for table %v", tableID))
	}
	return physicalTableElem
}

// generateSubzoneSpans constructs from a TableID the entries mapping
// zone config spans to subzones for use in the SubzoneSpans field of
// zonepb.ZoneConfig. SubzoneSpans controls which splits are created, so only
// the spans corresponding to entries in subzones are returned.
//
// Zone configs target indexes and partitions via `subzones`, which are attached
// to a table-scoped row in `system.zones`. Each subzone represents one index
// (primary or secondary) or one partition (or subpartition) and contains the
// usual zone config constraints. They are saved to `system.zones` sparsely
// (only when set by a user) and are the most specific entry in the normal
// cluster-default/database/table/subzone config hierarchy.
//
// Each index and partition can be mapped to spans in the keyspace. Indexes and
// range partitions each map to one span, while each list partition maps to one
// or more spans. Each partition span is contained by some index span and each
// subpartition span is contained by one of its parent partition's spans. The
// spans for a given level of a range partitioning (corresponding to one
// `PARTITION BY` in sql or one `PartitionDescriptor`) are disjoint, but the
// spans for a given level of a list partitioning may overlap if DEFAULT is
// used. A list partitioning which includes both (1, DEFAULT) and (1, 2) will
// overlap with the latter getting precedence in the zone config hierarchy. NB:
// In a valid PartitionDescriptor, no partitions with the same number of
// DEFAULTs will overlap (this property is used by
// `indexCoveringsForPartitioning`).
//
// These subzone spans are kept denormalized to the relevant `system.zone` row
// for performance. Given a TableDescriptor, the spans for every
// index/partition/subpartition are created, filtered out if they don't have a
// config set for them, and precedence applied (via `OverlapCoveringMerge`) to
// produce a set of non-overlapping spans, which each map to a subzone. There
// may be "holes" (uncovered spans) in this set.
//
// The returned spans are returned in exactly the format required by
// `system.zones`. They must be sorted and non-overlapping. Each contains an
// IndexID, which maps to one of the input `subzones` by indexing into the
// slice. As space optimizations, all `Key`s and `EndKey`s of `SubzoneSpan` omit
// the common prefix (the encoded table ID) and if `EndKey` is equal to
// `Key.PrefixEnd()` it is omitted.
//
// This function has tests in the partitionccl package.
//
// TODO(benesch): remove the hasNewSubzones parameter when a statement to clear
// all subzones at once is introduced.
func generateSubzoneSpans(
	b BuildCtx,
	tableID catid.DescID,
	subzones []zonepb.Subzone,
	partitionName string,
	hasNewSubzones bool,
) ([]zonepb.SubzoneSpan, error) {
	// Removing zone configs does not require a valid license.
	if hasNewSubzones {
		if err := base.CheckEnterpriseEnabled(b.ClusterSettings(),
			"replication zones on indexes or partitions"); err != nil {
			return nil, err
		}
	}

	// We already completely avoid creating subzone spans for dropped indexes.
	// Whether this was intentional is a different story, but it turns out to be
	// pretty sane. Dropped elements may refer to dropped types and we aren't
	// necessarily in a position to deal with those dropped types. Add a special
	// case to avoid generating any subzone spans in the face of being dropped.
	//
	// TODO figure out if we need to think about a dropped table

	a := &tree.DatumAlloc{}

	subzoneIndexByIndexID := make(map[descpb.IndexID]int32)
	subzoneIndexByPartition := make(map[string]int32)
	for i, subzone := range subzones {
		if len(subzone.PartitionName) > 0 {
			subzoneIndexByPartition[subzone.PartitionName] = int32(i)
		} else {
			subzoneIndexByIndexID[descpb.IndexID(subzone.IndexID)] = int32(i)
		}
	}

	var indexCovering covering.Covering
	var partitionCoverings []covering.Covering

	idxColElems := b.QueryByID(tableID).FilterIndexPartitioning()
	idxColElems.ForEach(func(current scpb.Status, target scpb.TargetStatus, e *scpb.IndexPartitioning) {
		isAddMutation := current == scpb.Status_ABSENT && target == scpb.ToPublic
		publicHasNoMutation := current == scpb.Status_PUBLIC && target == scpb.InvalidTarget
		// AddMutations should be included, along with public indexes that have no
		// mutattions.
		if isAddMutation && publicHasNoMutation {
			_, indexSubzoneExists := subzoneIndexByIndexID[e.IndexID]
			if indexSubzoneExists {
				prefix := roachpb.Key(rowenc.MakeIndexKeyPrefix(b.Codec(), tableID, e.IndexID))
				idxSpan := roachpb.Span{Key: prefix, EndKey: prefix.PrefixEnd()}
				// Each index starts with a unique prefix, so (from a precedence
				// perspective) it's safe to append them all together.
				indexCovering = append(indexCovering, covering.Range{
					Start: idxSpan.Key, End: idxSpan.EndKey,
					Payload: zonepb.Subzone{IndexID: uint32(e.IndexID)},
				})
			}
		}
		var emptyPrefix []tree.Datum
		partition := tabledesc.NewPartitioning(&e.PartitioningDescriptor).
			FindPartitionByName(partitionName)
		indexPartitionCoverings, err := indexCoveringsForPartitioning(
			b, a, tableID, e, partition, subzoneIndexByPartition, emptyPrefix)
		if err != nil {
			panic(err)
		}
		// The returned indexPartitionCoverings are sorted with highest
		// precedence first. They all start with the index prefix, so cannot
		// overlap with the partition coverings for any other index, so (from a
		// precedence perspective) it's safe to append them all together.
		partitionCoverings = append(partitionCoverings, indexPartitionCoverings...)
	})

	// OverlapCoveringMerge returns the payloads for any coverings that overlap
	// in the same order they were input. So, we require that they be ordered
	// with highest precedence first, so the first payload of each range is the
	// one we need.
	ranges := covering.OverlapCoveringMerge(append(partitionCoverings, indexCovering))

	// NB: This assumes that none of the indexes are interleaved, which is
	// checked in PartitionDescriptor validation.
	sharedPrefix := b.Codec().TablePrefix(uint32(tableID))

	var subzoneSpans []zonepb.SubzoneSpan
	for _, r := range ranges {
		payloads := r.Payload.([]interface{})
		if len(payloads) == 0 {
			continue
		}
		subzoneSpan := zonepb.SubzoneSpan{
			Key:    bytes.TrimPrefix(r.Start, sharedPrefix),
			EndKey: bytes.TrimPrefix(r.End, sharedPrefix),
		}
		var ok bool
		if subzone := payloads[0].(zonepb.Subzone); len(subzone.PartitionName) > 0 {
			subzoneSpan.SubzoneIndex, ok = subzoneIndexByPartition[subzone.PartitionName]
		} else {
			subzoneSpan.SubzoneIndex, ok = subzoneIndexByIndexID[descpb.IndexID(subzone.IndexID)]
		}
		if !ok {
			continue
		}
		if bytes.Equal(subzoneSpan.Key.PrefixEnd(), subzoneSpan.EndKey) {
			subzoneSpan.EndKey = nil
		}
		subzoneSpans = append(subzoneSpans, subzoneSpan)
	}
	return subzoneSpans, nil
}

// indexCoveringsForPartitioning returns span coverings representing the
// partitions in partDesc (including subpartitions). They are sorted with
// highest precedence first and the interval.Range payloads are each a
// `zonepb.Subzone` with the PartitionName set.
func indexCoveringsForPartitioning(
	b BuildCtx,
	a *tree.DatumAlloc,
	tableID catid.DescID,
	idx *scpb.IndexPartitioning,
	part catalog.Partitioning,
	relevantPartitions map[string]int32,
	prefixDatums []tree.Datum,
) ([]covering.Covering, error) {
	if part.NumColumns() == 0 {
		return nil, nil
	}

	var coverings []covering.Covering
	var descendentCoverings []covering.Covering

	if part.NumLists() > 0 {
		// The returned spans are required to be ordered with highest precedence
		// first. The span for (1, DEFAULT) overlaps with (1, 2) and needs to be
		// returned at a lower precedence. Luckily, because of the partitioning
		// validation, we're guaranteed that all entries in a list partitioning
		// with the same number of DEFAULTs are non-overlapping. So, bucket the
		// `interval.Range`s by the number of non-DEFAULT columns and return
		// them ordered from least # of DEFAULTs to most.
		listCoverings := make([]covering.Covering, part.NumColumns()+1)
		err := part.ForEachList(func(name string, values [][]byte, subPartitioning catalog.Partitioning) error {
			for _, valueEncBuf := range values {
				t, keyPrefix, err := decodePartitionTuple(
					b, a, tableID, idx, part, valueEncBuf, prefixDatums)
				if err != nil {
					return err
				}
				if _, ok := relevantPartitions[name]; ok {
					listCoverings[len(t.Datums)] = append(listCoverings[len(t.Datums)], covering.Range{
						Start: keyPrefix, End: roachpb.Key(keyPrefix).PrefixEnd(),
						Payload: zonepb.Subzone{PartitionName: name},
					})
				}
				newPrefixDatums := append(prefixDatums, t.Datums...)
				subpartitionCoverings, err := indexCoveringsForPartitioning(
					b, a, tableID, idx, subPartitioning, relevantPartitions, newPrefixDatums)
				if err != nil {
					return err
				}
				descendentCoverings = append(descendentCoverings, subpartitionCoverings...)
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
		for i := range listCoverings {
			if covering := listCoverings[len(listCoverings)-i-1]; len(covering) > 0 {
				coverings = append(coverings, covering)
			}
		}
	}

	if part.NumRanges() > 0 {
		err := part.ForEachRange(func(name string, from, to []byte) error {
			if _, ok := relevantPartitions[name]; !ok {
				return nil
			}
			_, fromKey, err := decodePartitionTuple(
				b, a, tableID, idx, part, from, prefixDatums)
			if err != nil {
				return err
			}
			_, toKey, err := decodePartitionTuple(
				b, a, tableID, idx, part, to, prefixDatums)
			if err != nil {
				return err
			}
			if _, ok := relevantPartitions[name]; ok {
				coverings = append(coverings, covering.Covering{{
					Start: fromKey, End: toKey,
					Payload: zonepb.Subzone{PartitionName: name},
				}})
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
	}

	// descendentCoverings are from subpartitions and so get precedence; append
	// them to the front.
	return append(descendentCoverings, coverings...), nil
}

// decodePartitionTuple parses columns (which are a prefix of the columns of
// `idxDesc`) encoded with the "value" encoding and returns the parsed datums.
// It also reencodes them into a key as they would be for `idxDesc` (accounting
// for index dirs, subpartitioning, etc).
//
// For a list partitioning, this returned key can be used as a prefix scan to
// select all rows that have the given columns as a prefix (this is true even if
// the list partitioning contains DEFAULT).
//
// Examples of the key returned for a list partitioning:
//   - (1, 2) -> /table/index/1/2
//   - (1, DEFAULT) -> /table/index/1
//   - (DEFAULT, DEFAULT) -> /table/index
//
// For a range partitioning, this returned key can be used as a exclusive end
// key to select all rows strictly less than ones with the given columns as a
// prefix (this is true even if the range partitioning contains MINVALUE or
// MAXVALUE).
//
// Examples of the key returned for a range partitioning:
//   - (1, 2) -> /table/index/1/3
//   - (1, MAXVALUE) -> /table/index/2
//   - (MAXVALUE, MAXVALUE) -> (/table/index).PrefixEnd()
//
// NB: It is checked here that if an entry for a list partitioning contains
// DEFAULT, everything in that entry "after" also has to be DEFAULT. So, (1, 2,
// DEFAULT) is valid but (1, DEFAULT, 2) is not. Similarly for range
// partitioning and MINVALUE/MAXVALUE.
func decodePartitionTuple(
	b BuildCtx,
	a *tree.DatumAlloc,
	tableID catid.DescID,
	index *scpb.IndexPartitioning,
	part catalog.Partitioning,
	valueEncBuf []byte,
	prefixDatums tree.Datums,
) (*rowenc.PartitionTuple, []byte, error) {
	keyColumns := getKeyColumnIDs(b, tableID, index.IndexID)
	if len(prefixDatums)+part.NumColumns() > keyColumns.Size() {
		return nil, nil, fmt.Errorf("not enough columns in index for this partitioning")
	}

	t := &rowenc.PartitionTuple{
		Datums: make(tree.Datums, 0, part.NumColumns()),
	}

	for i := len(prefixDatums); i < keyColumns.Size() && i < len(prefixDatums)+part.NumColumns(); i++ {
		_, _, keyCol := keyColumns.Get(i)
		col := keyCol.(*scpb.IndexColumn)
		colType := b.QueryByID(tableID).FilterColumnType().Filter(
			func(_ scpb.Status, _ scpb.TargetStatus, e *scpb.ColumnType) bool {
				return e.ColumnID == col.ColumnID
			}).MustGetOneElement().Type
		// TODO might need to check that column exists on table here
		if _, dataOffset, _, typ, err := encoding.DecodeValueTag(valueEncBuf); err != nil {
			return nil, nil, errors.Wrapf(err, "decoding")
		} else if typ == encoding.NotNull {
			// NOT NULL signals that a PartitionSpecialValCode follows
			var valCode uint64
			valueEncBuf, _, valCode, err = encoding.DecodeNonsortingUvarint(valueEncBuf[dataOffset:])
			if err != nil {
				return nil, nil, err
			}
			nextSpecial := rowenc.PartitionSpecialValCode(valCode)
			if t.SpecialCount > 0 && t.Special != nextSpecial {
				return nil, nil, errors.Newf("non-%[1]s value (%[2]s) not allowed after %[1]s",
					t.Special, nextSpecial)
			}
			t.Special = nextSpecial
			t.SpecialCount++
		} else {
			var datum tree.Datum
			datum, valueEncBuf, err = valueside.Decode(a, colType, valueEncBuf)
			if err != nil {
				return nil, nil, errors.Wrapf(err, "decoding")
			}
			if t.SpecialCount > 0 {
				return nil, nil, errors.Newf("non-%[1]s value (%[2]s) not allowed after %[1]s",
					t.Special, datum)
			}
			t.Datums = append(t.Datums, datum)
		}
	}
	if len(valueEncBuf) > 0 {
		return nil, nil, errors.New("superfluous data in encoded value")
	}

	allDatums := append(prefixDatums, t.Datums...)
	var colMap catalog.TableColMap
	for i := range allDatums {
		_, _, keyCol := keyColumns.Get(i)
		col := keyCol.(*scpb.IndexColumn)
		colMap.Set(col.ColumnID, i)
	}

	indexKeyPrefix := rowenc.MakeIndexKeyPrefix(b.Codec(), tableID, index.IndexID)
	// TODO
	keyAndSuffixCols := how_do.IndexFetchSpecKeyAndSuffixColumns(index)
	if len(allDatums) > len(keyAndSuffixCols) {
		return nil, nil, errors.Errorf("encoding too many columns (%d)", len(allDatums))
	}
	key, _, err := rowenc.EncodePartialIndexKey(keyAndSuffixCols[:len(allDatums)], colMap, allDatums, indexKeyPrefix)
	if err != nil {
		return nil, nil, err
	}

	// Currently, key looks something like `/table/index/1`. Given a range
	// partitioning of (1), we're done. This can be used as the exclusive end
	// key of a scan to fetch all rows strictly less than (1).
	//
	// If `specialIdx` is not the sentinel, then we're actually in a case like
	// `(1, MAXVALUE, ..., MAXVALUE)`. Since this index could have a descending
	// nullable column, we can't rely on `/table/index/1/0xff` to be _strictly_
	// larger than everything it should match. Instead, we need `PrefixEnd()`.
	// This also intuitively makes sense; we're essentially a key that is
	// guaranteed to be less than `(2, MINVALUE, ..., MINVALUE)`.
	if t.SpecialCount > 0 && t.Special == rowenc.PartitionMaxVal {
		key = roachpb.Key(key).PrefixEnd()
	}

	return t, key, nil
}

// getKeyColumnIDs returns a list of scpb.IndexColumn[s] for the given index id.
func getKeyColumnIDs(
	b BuildCtx, tblID catid.DescID, idxID catid.IndexID,
) *scpb.ElementCollection[*scpb.IndexColumn] {
	idxCols := b.QueryByID(tblID).Filter(func(_ scpb.Status, _ scpb.TargetStatus, e scpb.Element) bool {
		idxCol, ok := e.(*scpb.IndexColumn)
		if !ok {
			return false
		}
		return idxCol.IndexID == idxID && idxCol.Kind == scpb.IndexColumn_KEY
	})
	return (*scpb.ElementCollection[*scpb.IndexColumn])(idxCols)
}
