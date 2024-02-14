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
	"sigs.k8s.io/yaml"
)

func SetZoneConfig(b BuildCtx, n *tree.SetZoneConfig) {
	// TODO(before merge): Strcuture the fallback case. For this PR, we only do ALTER DATABASE ... CONFIGURE ZONE ...

	// probably need to do work to find the descID as well since this isn't always going to be a table or
	// something
	//fallBackIfSubZoneConfigExists(b, n)

	// TODO(before merge): verify this
	// Fall back to the legacy schema changer if proper cluster setting is not set (DSC doesn't support YAML config).
	if !sqlclustersettings.DeclarativeZoneConfig.Get(&b.ClusterSettings().SV) {
		panic(scerrors.NotImplementedErrorf(n,
			"sql.virtual_cluster.allow_declarative_schema_changer.enabled is not enabled"))
	}

	// Block secondary tenants from ALTER CONFIGURE ZONE unless cluster setting is set.
	if err := sqlclustersettings.RequireSystemTenantOrClusterSetting(
		b.Codec(), b.ClusterSettings(), sqlclustersettings.SecondaryTenantZoneConfigsEnabled,
	); err != nil {
		panic(err)
	}

	// Block from using YAML config unless we are discarding a YAML config.
	if n.YAMLConfig != nil && !n.Discard {
		panic(scerrors.NotImplementedErrorf(n, "YAML config is deprecated and not supported in DSC"))
	}

	// TODO(annie): implement complete support for CONFIGURE ZONE
	if n.Database == "" {
		panic(scerrors.NotImplementedErrorf(n, "ALTER DATABASE is the only CONFIGURE ZONE supported"))
	}

	if err := checkPrivilegeForSetZoneConfig(b, n); err != nil {
		panic(err)
	}

	if err := checkZoneConfigChangePermittedForMultiRegion(b, n.ZoneSpecifier, n.Options); err != nil {
		panic(err)
	}

	options, err := getUpdatedZoneConfigOptions(b, n.Options, n.ZoneSpecifier.TelemetryName())
	if err != nil {
		panic(err)
	}

	_, copyFromParentList, _, err := evaluateZoneOptions(b, options)
	if err != nil {
		panic(err)
	}

	telemetry.Inc(
		sqltelemetry.SchemaChangeAlterCounterWithExtra(n.ZoneSpecifier.TelemetryName(), "configure_zone"),
	)

	if err := applyZoneConfig(b, copyFromParentList, n.ZoneSpecifier, n); err != nil {
		panic(err)
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
// privilege for configuring zone on a database object.
func checkPrivilegeForSetZoneConfig(b BuildCtx, n *tree.SetZoneConfig) error {
	zs := n.ZoneSpecifier
	// For the system database, the user must be an admin. Otherwise, we
	// require CREATE or ZONECONFIG privilege on the database in question.
	reqNonAdminPrivs := []privilege.Kind{privilege.ZONECONFIG, privilege.CREATE}
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
		reqNonAdminPrivs, string(catalog.Database), mustRetrieveNamespaceElem(b, dbElem.DatabaseID).Name)
}

// checkZoneConfigChangePermittedForMultiRegion checks if a zone config
// change is permitted for a multi-region database.
// The change is permitted iff it is not modifying a protected multi-region
// field of the zone configs (as defined by zonepb.MultiRegionZoneConfigFields).
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

	// Check if the database we are altering is multi-region.
	dbRegionConfigElem := b.ResolveDatabase(zs.Database, ResolveParams{}).FilterDatabaseRegionConfig().MustGetZeroOrOneElement()
	if dbRegionConfigElem == nil {
		// Not a multi-region database, we're done here.
		return nil
	}

	hint := "to override this error, SET override_multi_region_zone_config = true and reissue the command"

	// The request is to discard the zone configuration. Error in cases where
	// the zone configuration being discarded was created by the multi-region
	// abstractions.
	if options == nil {
		// User is trying to update a zone config value that's protected for
		// multi-region databases. Return the constructed error.
		err := errors.WithDetail(errors.Newf(
			"attempting to discard the zone configuration of a multi-region entity"),
			"discarding a multi-region zone configuration may result in sub-optimal performance or behavior",
		)
		return errors.WithHint(err, hint)
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

// getTargetIDFromZoneSpecifier attempts to find the ID of the target by the
// zone specifier.
//
// N.B. Until we add full support of CONFIGURE ZONE in the DSC, this assumes
// we have a zone specifier that specifies a database.
func getTargetIDFromZoneSpecifier(b BuildCtx, zs tree.ZoneSpecifier) (catid.DescID, error) {
	dbElem := b.ResolveDatabase(zs.Database, ResolveParams{}).FilterDatabase().MustGetOneElement()
	return dbElem.DatabaseID, nil
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

func applyZoneConfig(
	b BuildCtx, copyFromParentList []tree.Name, zs tree.ZoneSpecifier, n *tree.SetZoneConfig,
) error {
	// Determines the ID of the target database of the zone specifier.
	targetID, err := getTargetIDFromZoneSpecifier(b, zs)
	if err != nil {
		return err
	}

	// Retrieve the partial zone configuration
	var partialZone *zonepb.ZoneConfig
	databaseZoneConfigElem := b.QueryByID(targetID).FilterDatabaseZoneConfig().MustGetZeroOrOneElement()
	partialZone = databaseZoneConfigElem.ZoneConfig

	// Retrieve the zone configuration.
	//
	// If the statement was USING DEFAULT, we want to ignore the zone
	// config that exists on targetID and instead skip to the inherited
	// default (default since the targetID is a database). For this, we
	// use the last parameter getInheritedDefault to retrieveCompleteZoneConfig().
	// These zones are only used for validations. The merged zone will not
	// be written.
	_, completeZone, err := retrieveCompleteZoneConfig(b, targetID, false /* getInheritedDefault */)
	if err != nil {
		panic(err)
	}

	// We need to inherit zone configuration information from the correct zone,
	// not completeZone.
	{
		// TODO how to get Descriptors() from planner?
		zcHelper := descs.AsZoneConfigHydrationHelper()
		// If we are operating on a zone, get all fields that the zone would
		// inherit from its parent. We do this by using an empty zoneConfig
		// and completing at the level of the current zone.
		zoneInheritedFields := zonepb.ZoneConfig{}
		if err := completeZoneConfig(b, &zoneInheritedFields); err != nil {
			return err
		}
		partialZone.CopyFromZone(zoneInheritedFields, copyFromParentList)
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

// retrieveCompleteZoneConfig looks up the zone for the specified database.
//
// If `getInheritedDefault` is true, the direct zone configuration, if it exists,
// is ignored, and the default zone config that would apply if it did not exist
// is returned instead. This is because, if the stmt is `USING DEFAULT`, we want
// to ignore the zone config that exists on targetID and instead skip to the
// inherited default.
func retrieveCompleteZoneConfig(
	b BuildCtx, targetID catid.DescID, getInheritedDefault bool,
) (zoneID descpb.ID, zone *zonepb.ZoneConfig, err error) {
	if getInheritedDefault {
		zoneID, zone, err = getInheritedDefaultZoneConfig(b, targetID)
	} else {
		zoneID, zone, err = getZoneConfig(b, targetID)
	}
	if err != nil {
		return 0, nil, err
	}

	if err = completeZoneConfig(b, zone); err != nil {
		return 0, nil, err
	}

	return zoneID, zone, nil
}

// getInheritedDefaultZoneConfig returns the inherited default zone config of
// the DEFAULT RANGE.
func getInheritedDefaultZoneConfig(
	b BuildCtx, targetID catid.DescID,
) (zoneID catid.DescID, zone *zonepb.ZoneConfig, err error) {
	zoneID, zone, err = getZoneConfig(b, keys.RootNamespaceID)
	return zoneID, zone, err
}

// getZoneConfig attempts to find the zone config from `system.zones` with `targetID`
// (`targetID` is a database ID).
func getZoneConfig(
	b BuildCtx, targetID catid.DescID,
) (zoneID catid.DescID, zone *zonepb.ZoneConfig, err error) {
	zone, err = lookUpSystemZonesTable(b, targetID)
	if err != nil {
		return 0, nil, err
	}
	if zone != nil {
		return zoneID, zone, nil
	}

	// Otherwise, retrieve the default zone config, but only as long as that wasn't the ID
	// we were trying to retrieve (avoid infinite recursion).
	if targetID != keys.RootNamespaceID {
		zoneID, zone, err := getZoneConfig(b, keys.RootNamespaceID)
		if err != nil {
			return 0, nil, err
		}
		return zoneID, zone, nil
	}

	// `targetID == keys.RootNamespaceID` but that zone config is not found
	// in `system.zones` table. Return a special, recognizable error!
	return 0, nil, sqlerrors.ErrNoZoneConfigApplies
}

// lookUpSystemZonesTable attempts to look up zone config in `system.zones`
// table by `targetID`.
// If `targetID` is not found, a nil `zone` is returned.
func lookUpSystemZonesTable(
	b BuildCtx, targetID catid.DescID,
) (zone *zonepb.ZoneConfig, err error) {
	b.QueryByID(targetID).ForEach(func(
		current scpb.Status, target scpb.TargetStatus, e scpb.Element,
	) {
		switch e := e.(type) {
		case *scpb.DatabaseZoneConfig:
			if e.DatabaseID == targetID {
				zone = e.ZoneConfig
			}
		}
	})
	return zone, nil
}

// completeZoneConfig takes a zone config pointer for a database and
// fills in missing fields by following the chain of inheritance.
// In the worst case, will have to inherit from the default zone config.
func completeZoneConfig(b BuildCtx, zone *zonepb.ZoneConfig) error {
	if zone.IsComplete() {
		return nil
	}
	// Check if zone is complete. If not, inherit from the default zone config
	if zone.IsComplete() {
		return nil
	}
	_, defaultZone, err := getZoneConfig(b, keys.RootNamespaceID)
	if err != nil {
		return err
	}
	zone.InheritFromParent(defaultZone)
	return nil
}
