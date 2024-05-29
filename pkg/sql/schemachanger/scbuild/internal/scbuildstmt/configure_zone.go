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
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
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
	// supports:
	// - Database
	// - Table
	// Left to support:
	// - Index
	// - Partition/row
	// - System Ranges
	fallBackIfNotDatabaseZoneConfig(n)

	// Fall back to the legacy schema changer if this is a YAML config (deprecated).
	// Block from using YAML config unless we are discarding a YAML config.
	if n.YAMLConfig != nil && !n.Discard {
		panic(scerrors.NotImplementedErrorf(n,
			"YAML config is deprecated and not supported in the declarative schema changer"))
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

	_, copyFromParentList, setters, err := evaluateZoneOptions(b, options)
	if err != nil {
		panic(err)
	}

	telemetry.Inc(
		sqltelemetry.SchemaChangeAlterCounterWithExtra(n.ZoneSpecifier.TelemetryName(), "configure_zone"),
	)

	elem, err := applyZoneConfig(b, copyFromParentList, n.ZoneSpecifier, n, setters)
	if err != nil {
		panic(err)
	}
	elem.SeqNum = elem.SeqNum + 1

	b.Add(elem)
}

// checkPrivilegeForSetZoneConfig checks whether current user has the right
// privilege for configuring zone on a database object.
func checkPrivilegeForSetZoneConfig(b BuildCtx, n *tree.SetZoneConfig) error {
	zs := n.ZoneSpecifier
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

	// For the system database, the user must be an admin. Otherwise, we
	// require CREATE or ZONECONFIG privilege on the database in question.
	reqNonAdminPrivs := []privilege.Kind{privilege.ZONECONFIG, privilege.CREATE}
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
	copyFromParentList []tree.Name,
	zs tree.ZoneSpecifier,
	n *tree.SetZoneConfig,
	setters []func(c *zonepb.ZoneConfig),
) (*scpb.DatabaseZoneConfig, error) {
	// Determines the ID of the target database of the zone specifier.
	targetID, err := getTargetIDFromZoneSpecifier(b, zs)
	if err != nil {
		return &scpb.DatabaseZoneConfig{}, err
	}

	// TODO(annie): once we allow configuring zones for named zones/system ranges,
	// we will need to guard against secondary tenants from configuring such
	// ranges.

	// Retrieve the partial zone configuration
	var partialZone *zonepb.ZoneConfig
	var databaseZoneConfigElem *scpb.DatabaseZoneConfig
	dbZoneElems := b.QueryByID(targetID).FilterDatabaseZoneConfig()
	dbZoneElems.ForEach(func(current scpb.Status, target scpb.TargetStatus, e *scpb.DatabaseZoneConfig) {
		// We want to get the most recent change that has not applied yet. For transactions, this will
		// be the most recent (last) zone config elem added.
		if e.DatabaseID == targetID {
			databaseZoneConfigElem = e
		}
	})

	partialZone = zonepb.NewZoneConfig()
	if databaseZoneConfigElem != nil {
		partialZone = databaseZoneConfigElem.ZoneConfig
	}

	// Retrieve the zone configuration.
	//
	// If the statement was USING DEFAULT, we want to ignore the zone
	// config that exists on targetID and instead skip to the inherited
	// default (default since the targetID is a database). For this, we
	// use the last parameter getInheritedDefault to retrieveCompleteZoneConfig().
	// These zones are only used for validations. The merged zone will not
	// be written.
	_, completeZone, seqNum, err := retrieveCompleteZoneConfig(b, targetID, false /* getInheritedDefault */)
	if err != nil {
		return &scpb.DatabaseZoneConfig{}, err
	}

	// We need to inherit zone configuration information from the correct zone,
	// not completeZone.
	{
		// If we are operating on a zone, get all fields that the zone would
		// inherit from its parent. We do this by using an empty zoneConfig
		// and completing at the level of the current zone.
		zoneInheritedFields := zonepb.ZoneConfig{}
		if err := completeZoneConfig(b, &zoneInheritedFields); err != nil {
			return &scpb.DatabaseZoneConfig{}, err
		}
		partialZone.CopyFromZone(zoneInheritedFields, copyFromParentList)
	}

	newZoneForVerification := *completeZone
	finalZone := *partialZone

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

			setter(&newZoneForVerification)
			setter(&finalZone)
			return nil
		}(); err != nil {
			return &scpb.DatabaseZoneConfig{}, err
		}
	}

	// Validate that there are no conflicts in the zone setup.
	if err := zonepb.ValidateNoRepeatKeysInZone(&newZoneForVerification); err != nil {
		return &scpb.DatabaseZoneConfig{}, err
	}

	currentZone := zonepb.NewZoneConfig()
	if databaseZoneConfigElem != nil {
		currentZone = databaseZoneConfigElem.ZoneConfig
	}
	if err := validateZoneAttrsAndLocalities(b, currentZone, &newZoneForVerification); err != nil {
		return &scpb.DatabaseZoneConfig{}, err
	}

	completeZone = &newZoneForVerification
	partialZone = &finalZone

	// Since we are writing to a zone that is not a subzone, we need to
	// make sure that the zone config is not considered a placeholder
	// anymore. If the settings applied to this zone don't touch the
	// NumReplicas field, set it to nil so that the zone isn't considered a
	// placeholder anymore.
	if databaseZoneConfigElem != nil && partialZone.IsSubzonePlaceholder() {
		partialZone.NumReplicas = nil
	}

	// Finally, revalidate everything. Validate only the completeZone config.
	if err := completeZone.Validate(); err != nil {
		return &scpb.DatabaseZoneConfig{}, pgerror.Wrap(err, pgcode.CheckViolation, "could not validate zone config")
	}

	zoneConfig := &scpb.DatabaseZoneConfig{
		DatabaseID: targetID,
		ZoneConfig: partialZone,
		SeqNum:     seqNum,
	}

	return zoneConfig, nil
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
) (zoneID descpb.ID, zone *zonepb.ZoneConfig, seqNum uint32, err error) {
	zc := &zonepb.ZoneConfig{}
	if getInheritedDefault {
		zoneID, zc, seqNum, err = getInheritedDefaultZoneConfig(b)
	} else {
		zoneID, zc, _, _, seqNum, err = getZoneConfig(b, targetID)
	}
	if err != nil {
		return 0, nil, 0, err
	}

	completeZc := *zc
	if err = completeZoneConfig(b, &completeZc); err != nil {
		return 0, nil, 0, err
	}

	zone = &completeZc
	return zoneID, zone, seqNum, nil
}

// getInheritedDefaultZoneConfig returns the inherited default zone config of
// the DEFAULT RANGE.
func getInheritedDefaultZoneConfig(
	b BuildCtx,
) (zoneID catid.DescID, zc *zonepb.ZoneConfig, seqNum uint32, err error) {
	zoneID, zc, _, _, seqNum, err = getZoneConfig(b, keys.RootNamespaceID)
	return zoneID, zc, seqNum, err
}

// getZoneConfig attempts to find the zone config from `system.zones` with `targetID`
// (`targetID` is a database ID).
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
		return zoneID, zc, 0, nil, seqNum, nil
	}

	// Otherwise, retrieve the default zc config, but only as long as that wasn't the ID
	// we were trying to retrieve (avoid infinite recursion).
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
			current scpb.Status, target scpb.TargetStatus, e scpb.Element,
		) {
			switch e := e.(type) {
			case *scpb.DatabaseZoneConfig:
				if e.DatabaseID == targetID {
					zone = e.ZoneConfig
					seqNum = e.SeqNum
				}
			}
		})
	}
	return zone, seqNum, nil
}

// completeZoneConfig takes a zone config pointer for a database and
// fills in missing fields by following the chain of inheritance.
// In the worst case, will have to inherit from the default zone config.
func completeZoneConfig(b BuildCtx, zone *zonepb.ZoneConfig) error {
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

// fallBackIfNotDatabaseZoneConfig determines if the table has a database
// zone config.
func fallBackIfNotDatabaseZoneConfig(n *tree.SetZoneConfig) {
	{
		// This might not be sufficient - let's double check after we have tests
		if n.Database == "" || n.Discard {
			panic(scerrors.NotImplementedErrorf(n, "ALTER DATABASE is the only CONFIGURE ZONE supported"))
		}
	}
}
