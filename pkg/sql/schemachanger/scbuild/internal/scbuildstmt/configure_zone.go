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
	"fmt"
	"sort"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/zone"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/transform"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlclustersettings"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/cockroach/pkg/util/log/logpb"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
	"golang.org/x/exp/slices"
	"sigs.k8s.io/yaml"
)

func SetZoneConfig(b BuildCtx, n *tree.SetZoneConfig) {
	// TODO before merge (xiang): Strcuture the fallback case. For this PR, we
	// only do ALTER DATABASE ... CONFIGURE ZONE ...

	if n.Database == "" {
		panic(scerrors.NotImplementedError(n))
	}

	if n.YAMLConfig != nil && !n.Discard {
		panic(scerrors.NotImplementedErrorf(n, "YAML config is deprecated and not supported in DSC"))
	}

	if err := sqlclustersettings.RequireSystemTenantOrClusterSetting(

		b.Codec(), b.ClusterSettings(), sqlclustersettings.SecondaryTenantZoneConfigsEnabled,
	); err != nil {
		panic(err)
	}

	if err := checkPrivilegeForSetZoneConfig(b, n.ZoneSpecifier); err != nil {
		panic(err)
	}

	if err := checkZoneConfigChangePermittedForMultiRegion(b, n.ZoneSpecifier, n.Options); err != nil {
		panic(err)
	}

	// Don't support YAML
	//yamlConfig, err := p.getUpdatedZoneConfigYamlConfig(ctx, n.YAMLConfig)
	//if err != nil {
	//	return nil, err
	//}

	options, err := getUpdatedZoneConfigOptions(b, n.Options, n.ZoneSpecifier.TelemetryName())

	if err != nil {
		panic(err)
	}

	// Don't support YAML
	//yamlConfig, deleteZone, err := evaluateYAMLConfig(n.yamlConfig, params)
	//if err != nil {
	//	return err
	//}

	optionsStr, copyFromParentList, setters, err := evaluateZoneOptions(b, options)

	if err != nil {
		panic(err)
	}

	telemetry.Inc(
		sqltelemetry.SchemaChangeAlterCounterWithExtra(n.ZoneSpecifier.TelemetryName(), "configure_zone"),
	)

	// Disallow schema changes if it's a table and its schema is locked.
	{
		tableID, err := getTableIDFromZoneSpecifier(b, n.ZoneSpecifier)
		if err == nil {
			panicIfSchemaIsLocked(b.QueryByID(tableID))
		}
	}

	// Backward compatibility for `ALTER PARTITION ... OF TABLE` syntax.
	err = maybePopulateIndexInNodeForAlterPartitionOfTable(b, n)
	if err != nil {
		panic(err)
	}

	// If this is an `ALTER PARTITION partition OF INDEX table_name@*` statement,
	// we need to find all indexes with the specified partition name and apply the
	// zone configuration to all of them.
	specifiers := maybeUnpackZoneSpecifier(b, n)

	for _, zs := range specifiers {
		// Note(solon): Currently the zone configurations are applied serially for
		// each specifier. This could certainly be made more efficient. For
		// instance, we should only need to write to the system.zones table once
		// rather than once for every specifier. However, the number of specifiers
		// is expected to be low--it's bounded by the number of indexes on the
		// table--so I'm holding off on adding that complexity unless we find it's
		// necessary.
		if err := applyZoneConfig(b, zs, n); err != nil {
			panic(err)
		}
	}
}

// getIndexAndPartitionFromZoneSpecifier finds the index id and partition name
// in the zone specifier. If the zone specifier specifies a named zone, or a
// database, or a table, then the returned indexID is zero. If the zone
// specifier specifies an index, then the returned partition is empty.
func getIndexAndPartitionFromZoneSpecifier(
	b BuildCtx, zs tree.ZoneSpecifier,
) (indexID catid.IndexID, partition string, err error) {
	if !zs.TargetsIndex() && !zs.TargetsPartition() {
		return 0, "", nil
	}

	tableID, err := getTableIDFromZoneSpecifier(b, zs)
	if err != nil {
		return 0, "", err
	}

	indexName := string(zs.TableOrIndex.Index)
	if indexName == "" {
		// Use the primary index if index name is unspecified.
		primaryIndexElem := mustRetrieveCurrentPrimaryIndexElement(b, tableID)
		indexID = primaryIndexElem.IndexID
		indexName = mustRetrieveIndexNameElem(b, tableID, primaryIndexElem.IndexID).Name
	} else {
		indexID = b.ResolveIndex(tableID, tree.Name(indexName), ResolveParams{}).FilterIndexName().MustGetOneElement().IndexID
	}

	partition = string(zs.Partition)
	if partition != "" {
		indexPartitionElem := maybeRetrieveIndexPartitioningElem(b, tableID, indexID)
		if indexPartitionElem == nil ||
			tabledesc.NewPartitioning(&indexPartitionElem.PartitioningDescriptor).FindPartitionByName(partition) == nil {
			return 0, "", errors.Newf("partition %q does not exist on index %q", partition, indexName)
		}
	}

	return indexID, partition, nil
}

// checkPrivilegeForSetZoneConfig checks whether current user has the right
// privilege for configuring zone on the specified object(s).
//
// The logics are duplicated from sql.checkPrivilegeForSetZoneConfig.
func checkPrivilegeForSetZoneConfig(b BuildCtx, zs tree.ZoneSpecifier) error {
	// For system ranges, the system database, or system tables, the user must be
	// an admin. Otherwise, we require CREATE or ZONECONFIG privilege on the
	// database or table in question.
	if zs.NamedZone != "" {
		return b.CheckGlobalPrivilege(privilege.REPAIRCLUSTERMETADATA)
	}

	if zs.Database != "" {
		if zs.Database == "system" {
			return b.CheckGlobalPrivilege(privilege.REPAIRCLUSTERMETADATA)
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
			[]privilege.Kind{privilege.ZONECONFIG, privilege.CREATE}, string(catalog.Database), mustRetrieveNamespaceElem(b, dbElem.DatabaseID).Name)
	}

	tableID, err := getTableIDFromZoneSpecifier(b, zs)
	if err != nil {
		return err
	}
	tableElem := mustRetrieveTableElem(b, tableID)
	tableNamespaceElem := mustRetrieveNamespaceElem(b, tableID)
	if tableNamespaceElem.DatabaseID == keys.SystemDatabaseID {
		return b.CheckGlobalPrivilege(privilege.REPAIRCLUSTERMETADATA)
	}

	// Can configure zone of a table (or its index) if user has either CREATE or ZONECONFIG
	// privilege on the table.
	tableCreatePrivilegeErr := b.CheckPrivilege(tableElem, privilege.CREATE)
	tableZoneConfigPrivilegeErr := b.CheckPrivilege(tableElem, privilege.ZONECONFIG)
	if tableCreatePrivilegeErr == nil || tableZoneConfigPrivilegeErr == nil {
		return nil
	}
	return sqlerrors.NewInsufficientPrivilegeOnDescriptorError(b.CurrentUser(),
		[]privilege.Kind{privilege.ZONECONFIG, privilege.CREATE}, string(catalog.Table), tableNamespaceElem.Name)
}

// CheckZoneConfigChangePermittedForMultiRegion checks if a zone config
// change is permitted for a multi-region database, table, index or partition.
// The change is permitted iff it is not modifying a protected multi-region
// field of the zone configs (as defined by zonepb.MultiRegionZoneConfigFields).
//
// The logics are duplicated from sql.CheckZoneConfigChangePermittedForMultiRegion.
func checkZoneConfigChangePermittedForMultiRegion(
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

	var err error
	var tableID catid.DescID
	isDB := false
	// Check if what we're altering is a multi-region entity.
	if zs.Database != "" {
		isDB = true
		dbRegionConfigElem := b.ResolveDatabase(zs.Database, ResolveParams{}).FilterDatabaseRegionConfig().MustGetZeroOrOneElement()
		if dbRegionConfigElem == nil {
			// Not a multi-region database, we're done here.
			return nil
		}
	} else {
		// We're dealing with a table, index, or partition zone configuration
		// change. Get the table descriptor so we can determine if this is a
		// multi-region table/index/partition.
		tableID, err = getTableIDFromZoneSpecifier(b, zs)
		if err != nil {
			return err
		}
		if !isMultiRegionTable(b, tableID) {
			// Not a multi-region table, we're done here.
			return nil
		}
	}

	hint := "to override this error, SET override_multi_region_zone_config = true and reissue the command"

	// The request is to discard the zone configuration. Error in cases where
	// the zone configuration being discarded was created by the multi-region
	// abstractions.
	if options == nil {
		needToError := false
		// Determine if this zone config that we're trying to discard is
		// supposed to be there.
		if isDB {
			needToError = true
		} else {
			needToError, err = blockDiscardOfZoneConfigForMultiRegionObject(b, zs, tableID)
			if err != nil {
				return err
			}
		}

		if needToError {
			// User is trying to update a zone config value that's protected for
			// multi-region databases. Return the constructed error.
			err := errors.WithDetail(errors.Newf(
				"attempting to discard the zone configuration of a multi-region entity"),
				"discarding a multi-region zone configuration may result in sub-optimal performance or behavior",
			)
			return errors.WithHint(err, hint)
		}
	}

	// This is clearly an n^2 operation, but since there are only a single
	// digit number of zone config keys, it's likely faster to do it this way
	// than incur the memory allocation of creating a map.
	for _, opt := range options {
		for _, cfg := range zonepb.MultiRegionZoneConfigFields {
			if opt.Key == cfg {
				// User is trying to update a zone config value that's protected for
				// multi-region databases. Return the constructed error.
				err := errors.Newf("attempting to modify protected field %q of a multi-region zone configuration",
					string(opt.Key),
				)
				return errors.WithHint(err, hint)
			}
		}
	}

	return nil
}

// isMultiRegionTable return True if this table is a multi-region table, meaning
// it has locality GLOBAL, or REGIONAL BY TABLE, or REGIONAL BY ROW.
func isMultiRegionTable(b BuildCtx, tableID catid.DescID) bool {
	tableElems := b.QueryByID(tableID)
	tblLocalityGlobalElem := tableElems.FilterTableLocalityGlobal().MustGetZeroOrOneElement()
	tblLocalityPrimaryRegionElem := tableElems.FilterTableLocalityPrimaryRegion().MustGetZeroOrOneElement()
	tblLocalitySeconaryRegionElem := tableElems.FilterTableLocalitySecondaryRegion().MustGetZeroOrOneElement()
	tblLocalityRBRElem := tableElems.FilterTableLocalityRegionalByRow().MustGetZeroOrOneElement()
	if tblLocalityGlobalElem != nil || tblLocalityPrimaryRegionElem != nil ||
		tblLocalitySeconaryRegionElem != nil || tblLocalityRBRElem != nil {
		return true
	}
	return false
}

// getTableIDFromZoneSpecifier attempts to find the table ID specified by the
// zone specifier.
// If the zone specifier specifies a named range or a database, a non-nil error
// is returned. Otherwise (for table/index/partitions), the associated table ID
// is returned.
func getTableIDFromZoneSpecifier(b BuildCtx, zs tree.ZoneSpecifier) (catid.DescID, error) {
	if zs.NamedZone != "" || zs.Database != "" {
		return 0, errors.Newf("zone specifier is for a named range or database; must be on a table or index or partition")
	}

	var tableID catid.DescID
	if zs.TargetsIndex() {
		tableID = b.ResolveIndexByName(&zs.TableOrIndex, ResolveParams{}).FilterIndexName().MustGetOneElement().TableID
	} else if zs.TargetsTable() {
		tableID = b.ResolveTable(zs.TableOrIndex.Table.ToUnresolvedObjectName(), ResolveParams{}).FilterTable().MustGetOneElement().TableID
	} else {
		// shouldn't be here
		return 0, errors.AssertionFailedf("programming error: zs does not specify a named range, nor a database, nor a table, nor an index")
	}
	return tableID, nil
}

// getTargetIDFromZoneSpecifier attempts to find the ID of the target by the
// zone specifier.
// Recall that a zone specifier specifies either a named range, or a database,
// or a table/index/partition. This function will return the ID of the named
// range, or the database, or the table.
func getTargetIDFromZoneSpecifier(b BuildCtx, zs tree.ZoneSpecifier) (catid.DescID, error) {
	if zs.NamedZone != "" {
		if zonepb.NamedZone(zs.NamedZone) == zonepb.DefaultZoneName {
			return keys.RootNamespaceID, nil
		}
		if id, ok := zonepb.NamedZones[zonepb.NamedZone(zs.NamedZone)]; ok {
			return catid.DescID(id), nil
		}
		return 0, errors.Newf("%q is not a built-in zone", string(zs.NamedZone))
	}

	if zs.Database != "" {
		dbElem := b.ResolveDatabase(zs.Database, ResolveParams{}).FilterDatabase().MustGetOneElement()
		return dbElem.DatabaseID, nil
	}

	return getTableIDFromZoneSpecifier(b, zs)
}

// blockDiscardOfZoneConfigForMultiRegionObject determines if discarding the
// zone configuration of a multi-region table, index or partition should be
// blocked. We only block the discard if the multi-region abstractions have
// created the zone configuration. Note that this function relies on internal
// knowledge of which table locality patterns write zone configurations. We do
// things this way to avoid having to read the zone configurations directly and
// do a more explicit comparison (with a generated zone configuration). If, down
// the road, the rules around writing zone configurations change, the tests in
// multi_region_zone_configs will fail and this function will need updating.
func blockDiscardOfZoneConfigForMultiRegionObject(
	b BuildCtx, zs tree.ZoneSpecifier, tableID catid.DescID,
) (bool, error) {
	isIndex := zs.TableOrIndex.Index != ""
	isPartition := zs.Partition != ""

	if isPartition {
		// Multi-region abstractions only set partition-level zone configs for
		// REGIONAL BY ROW tables.
		if retrieveTableLocalityRBRElem(b, tableID) != nil {
			return true, nil
		}
	} else if isIndex {
		// Multi-region will never set a zone config on an index, so no need to
		// error if the user wants to drop the index zone config.
		return false, nil
	} else {
		// It's a table zone config that the user is trying to discard. This
		// should only be present on GLOBAL and REGIONAL BY TABLE tables in a
		// specified region.
		if retrieveTableLocalityGlobalElem(b, tableID) != nil {
			return true, nil
		} else if retrieveTableLocalityPrimaryRegionElem(b, tableID) != nil ||
			retrieveTableLocalitySecondaryRegionElem(b, tableID) != nil {
			secondaryRegionElem := retrieveTableLocalitySecondaryRegionElem(b, tableID)
			if secondaryRegionElem != nil && tree.Name(secondaryRegionElem.RegionName) != tree.PrimaryRegionNotSpecifiedName {
				return true, nil
			}
		} else if retrieveTableLocalityRBRElem(b, tableID) != nil {
			// For REGIONAL BY ROW tables, no need to error if we're setting a
			// table level zone config.
			return false, nil
		} else {
			return false, errors.AssertionFailedf("unknown table locality")
		}
	}
	return false, nil
}

// getUpdatedZoneConfigOptions unpacks all kv options for a `CONFIGURE ZONE
// USING ...` stmt. It ensures all kv options are supported and the values are
// type-checked and normalized.
func getUpdatedZoneConfigOptions(
	b BuildCtx, n tree.KVOptions, telemetryName string,
) (map[tree.Name]zone.OptionValue, error) {

	var options map[tree.Name]zone.OptionValue
	if n != nil {
		// We have a CONFIGURE ZONE USING ... assignment.
		// Here we are constrained by the supported ZoneConfig fields,
		// as described by supportedZoneConfigOptions above.

		options = make(map[tree.Name]zone.OptionValue)
		for _, opt := range n {
			if _, alreadyExists := options[opt.Key]; alreadyExists {
				return nil, pgerror.Newf(pgcode.InvalidParameterValue,
					"duplicate zone config parameter: %q", tree.ErrString(&opt.Key))
			}
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
			typedExpr, err := tree.TypeCheckAndRequire(b, opt.Value, b.SemaCtx(), req.RequiredType, string(opt.Key))
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
			// Instead we add the fields to a list that we use at a later time
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
				return nil, nil, nil, pgerror.Newf(pgcode.InvalidParameterValue,
					"unsupported NULL value for %q", tree.ErrString(name))
			}
			opt := zone.SupportedZoneConfigOptions[*name]
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

func retrieveTableLocalityGlobalElem(b BuildCtx, tableID catid.DescID) *scpb.TableLocalityGlobal {
	return b.QueryByID(tableID).FilterTableLocalityGlobal().MustGetZeroOrOneElement()
}

func retrieveTableLocalityRBRElem(
	b BuildCtx, tableID catid.DescID,
) *scpb.TableLocalityRegionalByRow {
	return b.QueryByID(tableID).FilterTableLocalityRegionalByRow().MustGetZeroOrOneElement()
}

func retrieveTableLocalityPrimaryRegionElem(
	b BuildCtx, tableID catid.DescID,
) *scpb.TableLocalityPrimaryRegion {
	return b.QueryByID(tableID).FilterTableLocalityPrimaryRegion().MustGetZeroOrOneElement()
}

func retrieveTableLocalitySecondaryRegionElem(
	b BuildCtx, tableID catid.DescID,
) *scpb.TableLocalitySecondaryRegion {
	return b.QueryByID(tableID).FilterTableLocalitySecondaryRegion().MustGetZeroOrOneElement()
}

// maybePopulateIndexInNodeForAlterPartitionOfTable maintains backward compatibility
// for ALTER PARTITION ... OF TABLE by determining which index has the specified
// partition, and that index is populated into
// `n.ZoneSpecifier.TableOrIndex.Index`.
func maybePopulateIndexInNodeForAlterPartitionOfTable(b BuildCtx, n *tree.SetZoneConfig) error {
	if n.ZoneSpecifier.TargetsPartition() && len(n.ZoneSpecifier.TableOrIndex.Index) == 0 && !n.AllIndexes {
		partitionName := string(n.ZoneSpecifier.Partition)

		tableID, err := getTableIDFromZoneSpecifier(b, n.ZoneSpecifier)
		if err != nil {
			panic(err)
		}
		indexIDs, err := idsOfPartitionedIndexWithMatchingName(b, tableID, partitionName)
		if err != nil {
			return err
		}

		ns := mustRetrieveNamespaceElem(b, tableID)
		switch len(indexIDs) {
		case 0:
			return fmt.Errorf("partition %q does not exist on table %q", partitionName, ns.Name)
		case 1:
			n.ZoneSpecifier.TableOrIndex.Index = tree.UnrestrictedName(mustRetrieveIndexNameElem(b, tableID, indexIDs[0]).Name)
		case 2:
			// Temporary indexIDs create during backfill should always share the same
			// zone configs as the corresponding new index.
			if retrieveTemporaryIndexElem(b, tableID, indexIDs[0]) == nil &&
				retrieveTemporaryIndexElem(b, tableID, indexIDs[1]) != nil &&
				indexIDs[1] == indexIDs[0]+1 {
				n.ZoneSpecifier.TableOrIndex.Index = tree.UnrestrictedName(mustRetrieveIndexNameElem(b, tableID, indexIDs[0]).Name)
				break
			}
			fallthrough
		default:
			err := fmt.Errorf(
				"partition %q exists on multiple indexIDs of table %q", partitionName, ns.Name)
			err = pgerror.WithCandidateCode(err, pgcode.InvalidParameterValue)
			err = errors.WithHint(err, "try ALTER PARTITION ... OF INDEX ...")
			return err
		}
	}
	return nil
}

// indexIDsWithMatchingPartitionName finds all non-drop, partitioned indexes with matching partitioning name.
func idsOfPartitionedIndexWithMatchingName(
	b BuildCtx, tableID catid.DescID, partitionName string,
) ([]catid.IndexID, error) {
	var indexIDs []catid.IndexID
	b.QueryByID(tableID).FilterIndexPartitioning().ForEach(func(
		current scpb.Status, target scpb.TargetStatus, e *scpb.IndexPartitioning,
	) {
		if target != scpb.ToPublic {
			return
		}
		if tabledesc.NewPartitioning(&e.PartitioningDescriptor).FindPartitionByName(partitionName) != nil {
			indexIDs = append(indexIDs, e.IndexID)
		}
	})
	sort.Slice(indexIDs, func(i, j int) bool {
		return indexIDs[i] < indexIDs[j]
	})
	return indexIDs, nil
}

// If this is an `ALTER PARTITION partition OF INDEX table_name@*` statement,
// we need to find all indexes with the specified partition name and apply the
// zone configuration to all of them.
func maybeUnpackZoneSpecifier(b BuildCtx, n *tree.SetZoneConfig) (specifiers []tree.ZoneSpecifier) {
	if !n.ZoneSpecifier.TargetsPartition() || !n.AllIndexes {
		return []tree.ZoneSpecifier{n.ZoneSpecifier}
	}

	sqltelemetry.IncrementPartitioningCounter(sqltelemetry.AlterAllPartitions)

	tableID, err := getTableIDFromZoneSpecifier(b, n.ZoneSpecifier)
	if err != nil {
		panic(err)
	}

	indexIDs, err := idsOfPartitionedIndexWithMatchingName(b, tableID, string(n.ZoneSpecifier.Partition))
	for _, indexID := range indexIDs {
		zs := n.ZoneSpecifier
		zs.TableOrIndex.Index = tree.UnrestrictedName(mustRetrieveIndexNameElem(b, tableID, indexID).Name)
	}

	return specifiers
}

func applyZoneConfig(b BuildCtx, zs tree.ZoneSpecifier, n *tree.SetZoneConfig) error {
	// Determines the ID of the target object of the zone specifier. This is the
	// ID of either a named range, or a database, or a table (for
	// table/index/partition).
	targetID, err := getTargetIDFromZoneSpecifier(b, zs)
	if err != nil {
		return err
	}

	// Zones of "system config tables" (i.e. `system.descriptor` and
	// `system.zones` table) and NamespaceTable cannot be configured.
	// Also, we cannot remove the RANGE DEFAULT zone.
	if descpb.IsSystemConfigID(targetID) || targetID == keys.NamespaceTableID {
		return pgerror.Newf(pgcode.CheckViolation,
			`cannot set zone configs for system config tables; `+
				`try setting your config on the entire "system" database instead`)
	} else if targetID == keys.RootNamespaceID && n.Discard {
		return pgerror.Newf(pgcode.CheckViolation, "cannot remove default zone")
	}

	// Secondary tenants are not allowed to set zone configurations on any named
	// zones other than the RANGE DEFAULT zone.
	if !b.Codec().ForSystemTenant() {
		zoneName, found := zonepb.NamedZonesByID[uint32(targetID)]
		if found && zoneName != zonepb.DefaultZoneName {
			return pgerror.Newf(pgcode.CheckViolation,
				"non-system tenants cannot configure zone for %s range", zoneName,
			)
		}
	}

	// If `zs` specifies an index or a partition, then determine the index ID
	// and partition name as well.
	indexID, partition, err := getIndexAndPartitionFromZoneSpecifier(b, zs)
	if err != nil {
		return err
	}
	var tempIndexID catid.IndexID
	if indexID != 0 {
		tempIndexID = mustRetrieveIndexElement(b, targetID, indexID).TemporaryIndexID
	}

	// Retrieve the partial zone configuration
	var partialZone *zonepb.ZoneConfig
	// TODO (xiang): remove this limitation so we can retrieve zone config
	// elements for non-database target. This should be easy for
	// table/index/partition bc they all have a "home descriptor" from which we
	// decompose into the corresponding `TableZoneConfig` or `IndexZoneConfig`
	// elements. For named ranges, however, we would need to add more logic to
	// resolve the range by name in which we construct and add the corresponding
	// NamedRangeZoneConfig into the builder state.
	if zs.Database == "" {
		panic(scerrors.NotImplementedErrorf(n, "only changing zone config for databases are supported now"))
	}
	databaseZoneConfigElem := b.QueryByID(targetID).FilterDatabaseZoneConfig().MustGetZeroOrOneElement()
	subzonePlaceholder := false
	if databaseZoneConfigElem != nil {
		partialZone = databaseZoneConfigElem.ZoneConfig
	} else {
		// No zone was found. Possibly a SubzonePlaceholder depending on the index.
		partialZone = zone.NewZoneConfigWithRawBytes(zonepb.NewZoneConfig(), nil).ZoneConfigProto()
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
	// default (whichever applies -- a database if targetID is a table,
	// default if targetID is a database, etc.). For this, we use the last
	// parameter getInheritedDefault to GetZoneConfigInTxn().
	// These zones are only used for validations. The merged zone will not be
	// written.
	_, completeZone, completeSubZone, err := retrieveCompleteZoneConfig(b, targetID, indexID, partition, n.SetDefault)
	if err != nil {
		panic(err)
	}

	if errors.Is(err, sqlerrors.ErrNoZoneConfigApplies) {
		// No zone config yet.
		//
		// GetZoneConfigInTxn will fail with errNoZoneConfigApplies when
		// the target ID is not a database object, i.e. one of the system
		// ranges (liveness, meta, etc.), and did not have a zone config
		// already.
		completeZone = protoutil.Clone(
			params.extendedEvalCtx.ExecCfg.DefaultZoneConfig).(*zonepb.ZoneConfig)
	} else if err != nil {
		return err
	}

	// We need to inherit zone configuration information from the correct zone,
	// not completeZone.
	{
		zcHelper := descs.AsZoneConfigHydrationHelper(params.p.Descriptors())
		if index == nil {
			// If we are operating on a zone, get all fields that the zone would
			// inherit from its parent. We do this by using an empty zoneConfig
			// and completing at the level of the current zone.
			zoneInheritedFields := zonepb.ZoneConfig{}
			if err := completeZoneConfig(
				params.ctx, &zoneInheritedFields, params.p.Txn(), zcHelper, targetID,
			); err != nil {
				return err
			}
			partialZone.CopyFromZone(zoneInheritedFields, copyFromParentList)
		} else {
			// If we are operating on a subZone, we need to inherit all remaining
			// unset fields in its parent zone, which is partialZone.
			zoneInheritedFields := *partialZone
			if err := completeZoneConfig(
				params.ctx, &zoneInheritedFields, params.p.Txn(), zcHelper, targetID,
			); err != nil {
				return err
			}
			// In the case we have just an index, we should copy from the inherited
			// zone's fields (whether that was the table or database).
			if partition == "" {
				partialSubzone.Config.CopyFromZone(zoneInheritedFields, copyFromParentList)
			} else {
				// In the case of updating a partition, we need try inheriting fields
				// from the subzone's index, and inherit the remainder from the zone.
				subzoneInheritedFields := zonepb.ZoneConfig{}
				if indexSubzone := completeZone.GetSubzone(uint32(index.GetID()), ""); indexSubzone != nil {
					subzoneInheritedFields.InheritFromParent(&indexSubzone.Config)
				}
				subzoneInheritedFields.InheritFromParent(&zoneInheritedFields)
				// After inheriting fields, copy the requested ones into the
				// partialSubzone.Config.
				partialSubzone.Config.CopyFromZone(subzoneInheritedFields, copyFromParentList)
			}
		}
	}

	if deleteZone {
		if index != nil {
			didDelete := completeZone.DeleteSubzone(uint32(index.GetID()), partition)
			_ = partialZone.DeleteSubzone(uint32(index.GetID()), partition)
			if !didDelete {
				// If we didn't do any work, return early. We'd otherwise perform an
				// update that would make it look like one row was affected.
				return nil
			}
		} else {
			completeZone.DeleteTableConfig()
			partialZone.DeleteTableConfig()
		}
	} else {
		// Validate the user input.
		if len(yamlConfig) == 0 || yamlConfig[len(yamlConfig)-1] != '\n' {
			// YAML values must always end with a newline character. If there is none,
			// for UX convenience add one.
			yamlConfig += "\n"
		}

		// Determine where to load the configuration.
		newZone := *completeZone
		if completeSubzone != nil {
			newZone = completeSubzone.Config
		}

		// Determine where to load the partial configuration.
		// finalZone is where the new changes are unmarshalled onto.
		// It must be a fresh ZoneConfig if a new subzone is being created.
		// If an existing subzone is being modified, finalZone is overridden.
		finalZone := *partialZone
		if partialSubzone != nil {
			finalZone = partialSubzone.Config
		}

		// ALTER RANGE default USING DEFAULT sets the default to the in
		// memory default value.
		if n.setDefault && keys.RootNamespaceID == uint32(targetID) {
			finalZone = *protoutil.Clone(
				params.extendedEvalCtx.ExecCfg.DefaultZoneConfig).(*zonepb.ZoneConfig)
		} else if n.setDefault {
			finalZone = *zonepb.NewZoneConfig()
		}
		// Load settings from YAML. If there was no YAML (e.g. because the
		// query specified CONFIGURE ZONE USING), the YAML string will be
		// empty, in which case the unmarshaling will be a no-op. This is
		// innocuous.
		if err := yaml.UnmarshalStrict([]byte(yamlConfig), &newZone); err != nil {
			return pgerror.Wrap(err, pgcode.CheckViolation, "could not parse zone config")
		}

		// Load settings from YAML into the partial zone as well.
		if err := yaml.UnmarshalStrict([]byte(yamlConfig), &finalZone); err != nil {
			return pgerror.Wrap(err, pgcode.CheckViolation, "could not parse zone config")
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
				return err
			}
		}

		// Validate that there are no conflicts in the zone setup.
		if err := validateNoRepeatKeysInZone(&newZone); err != nil {
			return err
		}

		currentZone := zonepb.NewZoneConfig()
		if currentZoneConfigWithRaw, err := params.p.Descriptors().GetZoneConfig(
			params.ctx, params.p.Txn(), targetID,
		); err != nil {
			return err
		} else if currentZoneConfigWithRaw != nil {
			currentZone = currentZoneConfigWithRaw.ZoneConfigProto()
		}

		if err := validateZoneAttrsAndLocalities(
			params.ctx, params.p.InternalSQLTxn().Regions(), params.p.ExecCfg(), currentZone, &newZone,
		); err != nil {
			return err
		}

		// Are we operating on an index?
		if index == nil {
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
			// If the zone config for targetID was a subzone placeholder, it'll have
			// been skipped over by GetZoneConfigInTxn. We need to load it regardless
			// to avoid blowing away other subzones.

			// TODO(ridwanmsharif): How is this supposed to change? getZoneConfigRaw
			// gives no guarantees about completeness. Some work might need to happen
			// here to complete the missing fields. The reason is because we don't know
			// here if a zone is a placeholder or not. Can we do a GetConfigInTxn here?
			// And if it is a placeholder, we use getZoneConfigRaw to create one.
			completeZoneWithRaw, err := params.p.Descriptors().GetZoneConfig(params.ctx, params.p.Txn(), targetID)
			if err != nil {
				return err
			}

			if completeZoneWithRaw == nil {
				completeZone = zonepb.NewZoneConfig()
			} else {
				completeZone = completeZoneWithRaw.ZoneConfigProto()
			}
			completeZone.SetSubzone(zonepb.Subzone{
				IndexID:       uint32(index.GetID()),
				PartitionName: partition,
				Config:        newZone,
			})

			// The partial zone might just be empty. If so,
			// replace it with a SubzonePlaceholder.
			if subzonePlaceholder {
				partialZone.DeleteTableConfig()
			}

			partialZone.SetSubzone(zonepb.Subzone{
				IndexID:       uint32(index.GetID()),
				PartitionName: partition,
				Config:        finalZone,
			})

			// Also set the same zone configs for any corresponding temporary indexes.
			if tempIndex != nil {
				completeZone.SetSubzone(zonepb.Subzone{
					IndexID:       uint32(tempIndex.GetID()),
					PartitionName: partition,
					Config:        newZone,
				})

				partialZone.SetSubzone(zonepb.Subzone{
					IndexID:       uint32(tempIndex.GetID()),
					PartitionName: partition,
					Config:        finalZone,
				})
			}
		}

		// Finally revalidate everything. Validate only the completeZone config.
		if err := completeZone.Validate(); err != nil {
			return pgerror.Wrap(err, pgcode.CheckViolation, "could not validate zone config")
		}

		// Finally check for the extra protection partial zone configs would
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
				"try ALTER ... CONFIGURE ZONE USING <field_name> = COPY FROM PARENT [, ...] to populate the field")
			return err
		}
	}

	// Write the partial zone configuration.
	hasNewSubzones := !deleteZone && index != nil
	execConfig := params.extendedEvalCtx.ExecCfg
	zoneToWrite := partialZone
	// TODO(ajwerner): This is extremely fragile because we accept a nil table
	// all the way down here.
	n.run.numAffected, err = writeZoneConfig(
		params.ctx,
		params.p.InternalSQLTxn(),
		targetID,
		table,
		zoneToWrite,
		partialZoneWithRaw.GetRawBytesInStorage(),
		execConfig,
		hasNewSubzones,
		params.extendedEvalCtx.Tracing.KVTracingEnabled(),
	)
	if err != nil {
		return err
	}

	// Record that the change has occurred for auditing.
	eventDetails := eventpb.CommonZoneConfigDetails{
		Target:  tree.AsStringWithFQNames(&zs, params.Ann()),
		Config:  strings.TrimSpace(yamlConfig),
		Options: optionsStr,
	}
	var info logpb.EventPayload
	if deleteZone {
		info = &eventpb.RemoveZoneConfig{CommonZoneConfigDetails: eventDetails}
	} else {
		info = &eventpb.SetZoneConfig{CommonZoneConfigDetails: eventDetails}
	}
	return params.p.logEvent(params.ctx, targetID, info)
}

// retrieveCompleteZoneConfig looks up the zone and subzone for the specified object ID,
// index, and partition.
//
// If `getInheritedDefault` is true, the direct zone configuration, if it exists,
// is ignored, and the default zone config that would apply if it did not exist
// is returned instead. This is because, if the stmt is `USING DEFAULT`, we want
// to ignore the zone config that exists on targetID and instead skip to the
// inherited default.
func retrieveCompleteZoneConfig(
	b BuildCtx,
	targetID catid.DescID,
	getInheritedDefault bool,
	indexID catid.IndexID,
	partition string,
) (zoneID descpb.ID, zone *zonepb.ZoneConfig, subzone *zonepb.Subzone, err error) {
	var placeholderID descpb.ID
	var placeholder *zonepb.ZoneConfig
	if getInheritedDefault {
		zoneID, zone, err = getInheritedDefaultZoneConfig(b, targetID)
	} else {
		zoneID, zone, placeholderID, placeholder, err = getZoneConfig(b, targetID)
	}
	if err != nil {
		return 0, nil, nil, err
	}

	// TODO (xiang):
	// 1. Add a commit that refactored sqlerrors.Errxxxxx
	// 2. start from here to finish `completeZoneConfig` function
	if err = completeZoneConfig(ctx, zone, txn, zcHelper, zoneID); err != nil {
		return 0, nil, nil, err
	}

	if indexID != 0 {
		if placeholder != nil {
			if subzone = placeholder.GetSubzone(uint32(indexID), partition); subzone != nil {
				if indexSubzone := placeholder.GetSubzone(uint32(indexID), ""); indexSubzone != nil {
					subzone.Config.InheritFromParent(&indexSubzone.Config)
				}
				subzone.Config.InheritFromParent(zone)
				return placeholderID, placeholder, subzone, nil
			}
		} else {
			if subzone = zone.GetSubzone(uint32(indexID), partition); subzone != nil {
				if indexSubzone := zone.GetSubzone(uint32(indexID), ""); indexSubzone != nil {
					subzone.Config.InheritFromParent(&indexSubzone.Config)
				}
				subzone.Config.InheritFromParent(zone)
			}
		}
	}
	return zoneID, zone, subzone, nil
}

// getInheritedDefaultZoneConfig returns the inherited default zone config of
// `targetID`. This means
//   - if `targetID` is a table ID, returns the zone config of its parent database (if exists)
//     or the DEFAULT RANGE.
//   - Otherwise, returns the zone config of the DEFAULT RANGE.
func getInheritedDefaultZoneConfig(
	b BuildCtx, targetID catid.DescID,
) (zoneID catid.DescID, zone *zonepb.ZoneConfig, err error) {
	// Is `targetID` a table?
	tblElem := retrieveTableElem(b, targetID)
	if tblElem != nil {
		parentDBID := mustRetrieveNamespaceElem(b, tblElem.TableID).DatabaseID
		zoneID, zone, _, _, err = getZoneConfig(b, parentDBID)
		return zoneID, zone, err
	}

	zoneID, zone, _, _, err = getZoneConfig(b, keys.RootNamespaceID)
	return zoneID, zone, err
}

// getZoneConfig attempts to find the zone config from `system.zones` with `targetID`
// (`targetID` is either a named range ID, or a database ID, or a table ID).
//
//   - If the zone config is found to be a subzone placeholder, then we further go ahead
//     to find the parent database zone config (if `targetID` is a table ID) or to find the
//     DEFAULT RANGE zone config (if `targetID` is a database ID).
//   - Otherwise, we will just return the found zone config (so `subzoneId` and `subzone` will be nil)
func getZoneConfig(
	b BuildCtx, targetID catid.DescID,
) (
	zoneID catid.DescID,
	zone *zonepb.ZoneConfig,
	subzoneID catid.DescID,
	subzone *zonepb.ZoneConfig,
	err error,
) {
	zone, err = lookUpSystemZonesTable(b, targetID)
	if err != nil {
		return 0, nil, 0, nil, err
	}
	if zone != nil {
		// Zone config with `targetID` is found! Return unless this zone config is a
		// subzone config (i.e. an index/partition config), in which case, we will
		// go head to find the "parent zone config" per function commentary.
		if !zone.IsSubzonePlaceholder() {
			return zoneID, zone, 0, nil, nil
		}
		subzoneID = targetID
		subzone = zone
	}

	// No zone config for this ID. If `targetID` is a table, then recursively
	// get zone config of its parent database.
	tblElem := retrieveTableElem(b, targetID)
	if tblElem != nil {
		parentDBID := mustRetrieveNamespaceElem(b, tblElem.TableID).DatabaseID
		zoneID, zone, _, _, err := getZoneConfig(b, parentDBID)
		if err != nil {
			return 0, nil, 0, nil, err
		}
		return zoneID, zone, subzoneID, subzone, nil
	}

	// Otherwise, retrieve the default zone config, but only as long as that wasn't the ID
	// we were trying to retrieve (avoid infinite recursion).
	if targetID != keys.RootNamespaceID {
		zoneID, zone, _, _, err := getZoneConfig(b, keys.RootNamespaceID)
		if err != nil {
			return 0, nil, 0, nil, err
		}
		return zoneID, zone, subzoneID, subzone, nil
	}

	// `targetID == keys.RootNamespaceID` but that zone config is not found
	// in `system.zones` table. Return a special, recognizable error!
	return 0, nil, 0, nil, sqlerrors.ErrNoZoneConfigApplies
}

// lookUpSystemZonesTable attempts to look up zone config in `system.zones`
// table by `targetID`.
// If `targetID` is not found, a nil `zone` is returned.
func lookUpSystemZonesTable(
	b BuildCtx, targetID catid.DescID,
) (zone *zonepb.ZoneConfig, err error) {
	if slices.Contains(keys.PseudoTableIDs, uint32(targetID)) {
		// TODO (xiang): add logic to resolve named range zone config by
		// reading from `system.zones` table.
		return nil, scerrors.NotImplementedError(nil)
	} else {
		// It's a descriptor-backed target (i.e. a database ID or a table ID)
		b.QueryByID(targetID).ForEach(func(
			current scpb.Status, target scpb.TargetStatus, e scpb.Element,
		) {
			switch e := e.(type) {
			case *scpb.DatabaseZoneConfig:
				if e.DatabaseID == targetID {
					zone = e.ZoneConfig
				}
			case *scpb.TableZoneConfig:
				if e.TableID == targetID {
					// zone = e.ZoneConfig  // Uncomment this line once TableZoneConfig has a zone config
				}
			}
		})
	}
	return zone, nil
}
