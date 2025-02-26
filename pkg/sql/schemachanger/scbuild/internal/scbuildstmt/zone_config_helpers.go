// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package scbuildstmt

import (
	"bytes"
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/fetchpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/multiregion"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/zone"
	"github.com/cockroachdb/cockroach/pkg/sql/covering"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/regions"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc/valueside"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/transform"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlclustersettings"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/proto"
)

type zoneConfigAuthorizer interface {
	// checkPrivilegeForSetZoneConfig checks whether the current user has the
	// right privilege for configuring zone on the specified object.
	checkPrivilegeForSetZoneConfig(b BuildCtx, zs tree.ZoneSpecifier) error

	// checkZoneConfigChangePermittedForMultiRegion checks if a zone config
	// change is permitted for a multi-region database or table.
	// The change is permitted iff it is not modifying a protected multi-region
	// field of the zone configs (as defined by
	// zonepb.MultiRegionZoneConfigFields).
	checkZoneConfigChangePermittedForMultiRegion(
		b BuildCtx, zs tree.ZoneSpecifier, options tree.KVOptions) error
}

type zoneConfigObjBuilder interface {
	// getZoneConfigElemForAdd retrieves (scpb.Element, []scpb.Element) needed for
	// adding the zone config object. The slice of multiple elements to be
	// modified becomes more relevant for subzone configs -- as configuring the
	// zone on indexes and partitions can shift the subzone spans for other
	// elements around.
	getZoneConfigElemForAdd(b BuildCtx) (scpb.Element, []scpb.Element)

	// getZoneConfigElemForDrop retrieves ([]scpb.Element, []scpb.Element) needed
	// for dropping the zone config object. The second slice of multiple elements
	// to be modified becomes more relevant for subzone configs -- as configuring
	// the zone on indexes and partitions can shift the subzone spans for other
	// elements around. The first slice is used to ensure all references with
	// varying `seqNum`s for the element are dropped (relevant only in explicit
	// transactions).
	getZoneConfigElemForDrop(b BuildCtx) ([]scpb.Element, []scpb.Element)

	// getTargetID returns the target ID of the zone config object. This is either
	// a database or a table ID.
	getTargetID() catid.DescID

	// applyZoneConfig applies the zone config to the object, returning the
	// original zone config for logging.
	applyZoneConfig(
		b BuildCtx,
		n *tree.SetZoneConfig,
		copyFromParentList []tree.Name,
		setters []func(c *zonepb.ZoneConfig),
	) (*zonepb.ZoneConfig, error)

	// setZoneConfigToWrite fills our object with the zone config/subzone config
	// we will be writing to KV.
	setZoneConfigToWrite(zone *zonepb.ZoneConfig)

	// isNoOp returns true if the zone config object is a no-op. This is defined
	// by our object having no zone config yet.
	isNoOp() bool
}

type zoneConfigRetriever interface {
	// retrievePartialZoneConfig retrieves the partial zone configuration of the
	// ID of our object. This will either be a database ID or a table ID.
	retrievePartialZoneConfig(b BuildCtx) *zonepb.ZoneConfig

	// retrieveCompleteZoneConfig looks up the zone and subzone for the specified
	// object ID, index, and partition.
	//
	// If `getInheritedDefault` is true, the direct zone configuration, if it exists,
	// is ignored, and the default zone config that would apply if it did not exist
	// is returned instead. This is because, if the stmt is `USING DEFAULT`, we want
	// to ignore the zone config that exists on targetID and instead skip to the
	// inherited default.
	retrieveCompleteZoneConfig(
		b BuildCtx, getInheritedDefault bool) (*zonepb.ZoneConfig, *zonepb.Subzone, error)

	// getInheritedDefaultZoneConfig returns the inherited default zone config of
	// the target. This means
	//   - if the target is a table, it returns the zone config of its parent
	//     database (if exists) or the DEFAULT RANGE.
	//   - otherwise, it returns the zone config of the DEFAULT RANGE
	getInheritedDefaultZoneConfig(b BuildCtx) (
		zc *zonepb.ZoneConfig, err error,
	)

	// getZoneConfig attempts to find the zone config from `system.zones` with
	// the target:
	//   - If the zone config is found to be a subzone placeholder, then we
	//     further go ahead to find the parent database zone config
	//     (if our target is a table) or to find the DEFAULT RANGE zone config
	//     (if our target is a database).
	//   - Otherwise, we will just return the found zone config
	//     (so `subzoneId` and `subzone` will be nil).
	getZoneConfig(b BuildCtx, inheritDefaultRange bool) (*zonepb.ZoneConfig, error)
}

type zoneConfigMutator interface {
	// completeZoneConfig takes a zone config pointer and fills in the
	// missing fields by following the chain of inheritance.
	// In the worst case, will have to inherit from the default zone config.
	// NOTE: This will not work for subzones. To complete subzones, find a
	// complete parent zone (index or table) and apply InheritFromParent to it.
	completeZoneConfig(b BuildCtx, zone *zonepb.ZoneConfig) error
}

// zoneConfigObject is used to represent various types of "objects" that are
// supported by the CONFIGURE ZONE statement. This is used to determine the
// scpb that will be generated.
type zoneConfigObject interface {
	zoneConfigAuthorizer
	zoneConfigObjBuilder
	zoneConfigRetriever
	zoneConfigMutator
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

// maybeMultiregionErrorWithHint returns an error if the user is trying to
// update a zone config value that's protected for multi-region databases.
func maybeMultiregionErrorWithHint(
	b BuildCtx, zco zoneConfigObject, zs tree.ZoneSpecifier, options tree.KVOptions,
) error {
	hint := "to override this error, SET override_multi_region_zone_config = true and reissue the command"
	// The request is to discard the zone configuration. Error in cases where
	// the zone configuration being discarded was created by the multi-region
	// abstractions.
	if options == nil {
		needToError := false
		// Determine if this zone config that we're trying to discard is
		// supposed to be there. zco is either a database or a table.
		_, isDB := zco.(*databaseZoneConfigObj)
		if isDB {
			needToError = true
		} else {
			var err error
			needToError, err = blockDiscardOfZoneConfigForMultiRegionObject(b, zs, zco.getTargetID())
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
				err := errors.Newf("attempting to modify protected field %q of a multi-region zone "+
					"configuration", string(opt.Key),
				)
				return errors.WithHint(err, hint)
			}
		}
	}
	return nil
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
	b BuildCtx, zs tree.ZoneSpecifier, tblID catid.DescID,
) (bool, error) {
	isIndex := zs.TableOrIndex.Index != ""
	isPartition := zs.Partition != ""
	tableElems := b.QueryByID(tblID)

	RBRElem := tableElems.FilterTableLocalityRegionalByRow().MustGetZeroOrOneElement()
	if isPartition {
		// Multi-region abstractions only set partition-level zone configs for
		// REGIONAL BY ROW tables.
		return RBRElem != nil, nil
	} else if isIndex {
		// Multi-region will never set a zone config on an index, so no need to
		// error if the user wants to drop the index zone config.
		return false, nil
	}

	// It's a table zone config that the user is trying to discard. This
	// should only be present on GLOBAL and REGIONAL BY TABLE tables in a
	// specified region.
	globalElem := tableElems.FilterTableLocalityGlobal().MustGetZeroOrOneElement()
	primaryRegionElem := tableElems.FilterTableLocalityPrimaryRegion().MustGetZeroOrOneElement()
	secondaryRegionElem := tableElems.FilterTableLocalitySecondaryRegion().MustGetZeroOrOneElement()

	if globalElem != nil {
		return true, nil
	} else if secondaryRegionElem != nil {
		return true, nil
	} else if primaryRegionElem != nil {
		// For REGIONAL BY TABLE tables, no need to error if a user-specified
		// region does not exist.
		return false, nil
	} else if RBRElem != nil {
		// For REGIONAL BY ROW tables, no need to error if we're setting a
		// table level zone config.
		return false, nil
	} else {
		return false, errors.AssertionFailedf(
			"unknown table locality %s", b.QueryByID(tblID))
	}
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

// evaluateZoneOptions gets the input options map ready for use.
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

// fillZoneConfigsForSubzones fills in the zone configs for subzones.
func fillZoneConfigsForSubzones(
	indexID catid.IndexID,
	partition string,
	tempIndexID catid.IndexID,
	subzonePlaceholder bool,
	completeZone *zonepb.ZoneConfig,
	partialZone *zonepb.ZoneConfig,
	newZone zonepb.ZoneConfig,
	finalZone zonepb.ZoneConfig,
) {
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

// loadSettingsToZoneConfigs loads settings from var = val assignments. If there
// were no such settings, (e.g. because the query specified CONFIGURE ZONE = or
// USING DEFAULT), the setter slice will be empty and this will be
// a no-op. This is innocuous.
func loadSettingsToZoneConfigs(
	setters []func(c *zonepb.ZoneConfig), newZone *zonepb.ZoneConfig, finalZone *zonepb.ZoneConfig,
) error {
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

			setter(newZone)
			setter(finalZone)
			return nil
		}(); err != nil {
			return err
		}
	}
	return nil
}

// lookUpSystemZonesTable attempts to look up the zone config in `system.zones`
// table by `targetID`.
// If `targetID` is not found, a nil `zone` is returned.
func lookUpSystemZonesTable(
	b BuildCtx, objType zoneConfigObject, inheritDefaultRange bool, isSubzoneConfig bool,
) (zone *zonepb.ZoneConfig, subzones []zonepb.Subzone, err error) {
	// Get the zone config of the DEFAULT RANGE
	if inheritDefaultRange {
		zc, err := b.ZoneConfigGetter().GetZoneConfig(b, keys.RootNamespaceID)
		if err != nil {
			return nil, nil, err
		}
		zone = zc.ZoneConfigProto()
	} else {
		// It's a descriptor-backed target (i.e. a database ID or a table ID)
		zone = objType.retrievePartialZoneConfig(b)
		// If we are dealing with index subzones, clear out the zone config and
		// just use the subzones.
		if isSubzoneConfig {
			if zone != nil && zone.Subzones != nil {
				subzones = zone.Subzones
				zone = nil
			}
		}
	}
	return zone, subzones, nil
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

// partitionKey is used to group a partition's name and its index ID for
// indexing into a map.
type partitionKey struct {
	indexID descpb.IndexID
	name    string
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
// for performance. Given a tableID, the spans for the specific
// index/partition/subpartition is created, filtered out if they don't have a
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
func generateSubzoneSpans(
	b BuildCtx, tableID catid.DescID, subzones []zonepb.Subzone,
) ([]zonepb.SubzoneSpan, error) {
	// We already completely avoid creating subzone spans for dropped indexes.
	// Whether this was intentional is a different story, but it turns out to be
	// pretty sane. Dropped elements may refer to dropped types and we aren't
	// necessarily in a position to deal with those dropped types. Add a special
	// case to avoid generating any subzone spans in the face of being dropped.
	droppedTable := b.QueryByID(tableID).FilterTable().Filter(func(current scpb.Status, target scpb.TargetStatus, e *scpb.Table) bool {
		return e.TableID == tableID && (current == scpb.Status_DROPPED || target == scpb.ToAbsent)
	})
	if droppedTable.Size() != 0 {
		return nil, nil
	}

	subzoneIndexByIndexID := make(map[descpb.IndexID]int32)
	subzoneIndexByPartition := make(map[partitionKey]int32)
	for i, subzone := range subzones {
		if len(subzone.PartitionName) > 0 {
			partKey := partitionKey{indexID: descpb.IndexID(subzone.IndexID), name: subzone.PartitionName}
			subzoneIndexByPartition[partKey] = int32(i)
		} else {
			subzoneIndexByIndexID[descpb.IndexID(subzone.IndexID)] = int32(i)
		}
	}

	a := &tree.DatumAlloc{}
	var indexCovering covering.Covering
	var partitionCoverings []covering.Covering
	var err error
	b.QueryByID(tableID).FilterIndexName().NotToAbsent().ForEach(
		func(_ scpb.Status, _ scpb.TargetStatus, e *scpb.IndexName) {
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
			var emptyPrefix []tree.Datum
			partitioning := mustRetrievePartitioningFromIndexPartitioning(b, tableID, e.IndexID)
			var indexPartitionCoverings []covering.Covering
			indexPartitionCoverings, err = indexCoveringsForPartitioning(
				b, a, tableID, e.IndexID, partitioning, subzoneIndexByPartition, emptyPrefix)
			if err != nil {
				return
			}
			partitionCoverings = append(partitionCoverings, indexPartitionCoverings...)
		})
	if err != nil {
		return nil, err
	}

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
			partKey := partitionKey{indexID: descpb.IndexID(subzone.IndexID), name: subzone.PartitionName}
			subzoneSpan.SubzoneIndex, ok = subzoneIndexByPartition[partKey]
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
	indexID catid.IndexID,
	part catalog.Partitioning,
	relevantPartitions map[partitionKey]int32,
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
				t, keyPrefix, err := decodePartitionTuple(b, a, tableID, indexID, part, valueEncBuf, prefixDatums)
				if err != nil {
					return err
				}
				partKey := partitionKey{indexID: indexID, name: name}
				if _, ok := relevantPartitions[partKey]; ok {
					listCoverings[len(t.Datums)] = append(listCoverings[len(t.Datums)], covering.Range{
						Start: keyPrefix, End: roachpb.Key(keyPrefix).PrefixEnd(),
						Payload: zonepb.Subzone{IndexID: uint32(indexID), PartitionName: name},
					})
				}
				newPrefixDatums := append(prefixDatums, t.Datums...)
				subpartitionCoverings, err := indexCoveringsForPartitioning(
					b, a, tableID, indexID, subPartitioning, relevantPartitions, newPrefixDatums)
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
			partKey := partitionKey{indexID: indexID, name: name}
			if _, ok := relevantPartitions[partKey]; !ok {
				return nil
			}
			_, fromKey, err := decodePartitionTuple(b, a, tableID, indexID, part, from, prefixDatums)
			if err != nil {
				return err
			}
			_, toKey, err := decodePartitionTuple(b, a, tableID, indexID, part, to, prefixDatums)
			if err != nil {
				return err
			}
			if _, ok := relevantPartitions[partKey]; ok {
				coverings = append(coverings, covering.Covering{{
					Start: fromKey, End: toKey,
					Payload: zonepb.Subzone{IndexID: uint32(indexID), PartitionName: name},
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
	indexID catid.IndexID,
	part catalog.Partitioning,
	valueEncBuf []byte,
	prefixDatums tree.Datums,
) (*rowenc.PartitionTuple, []byte, error) {
	keyColumns := b.QueryByID(tableID).FilterIndexColumn().
		Filter(func(_ scpb.Status, _ scpb.TargetStatus, e *scpb.IndexColumn) bool {
			return e.IndexID == indexID && e.Kind == scpb.IndexColumn_KEY
		})
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

	index := mustRetrieveIndexColumnElements(b, tableID, indexID)
	indexKeyPrefix := rowenc.MakeIndexKeyPrefix(b.Codec(), tableID, index[0].IndexID)
	var keyAndSuffixCols []fetchpb.IndexFetchSpec_KeyColumn
	for _, i := range index {
		indexCol := fetchpb.IndexFetchSpec_Column{
			ColumnID:      i.ColumnID,
			Name:          "",
			Type:          nil,
			IsNonNullable: false,
		}
		keyAndSuffixCols = append(keyAndSuffixCols, fetchpb.IndexFetchSpec_KeyColumn{
			IndexFetchSpec_Column: indexCol,
			Direction:             i.Direction,
			IsComposite:           false,
			IsInverted:            false,
		})
	}
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

// findMostRecentZoneConfig finds the most recent zone config for the targetID.
// Here, "most recent" is defined by highest seqNum.
func findMostRecentZoneConfig[T scpb.ZoneConfigElement](
	obj zoneConfigObject,
	zoneConfigElemFilter func(catid.DescID) *scpb.ElementCollection[T],
	isSpecificZoneConfigElem func(elem T) bool,
) T {
	var mostRecentElem T
	highestSeqNum := uint32(0)
	targetID := obj.getTargetID()
	elems := zoneConfigElemFilter(targetID)

	elems.ForEach(func(_ scpb.Status, _ scpb.TargetStatus, elem T) {
		if isSpecificZoneConfigElem(elem) {
			if highestSeqNum <= elem.GetSeqNum() {
				mostRecentElem = elem
				highestSeqNum = elem.GetSeqNum()
			}
		}
	})

	return mostRecentElem
}

func prepareZoneConfig(
	b BuildCtx,
	n *tree.SetZoneConfig,
	copyFromParentList []tree.Name,
	setters []func(c *zonepb.ZoneConfig),
	obj zoneConfigObject,
) (*zonepb.ZoneConfig, *zonepb.ZoneConfig, error) {
	// Retrieve the partial zone configuration
	partialZone := obj.retrievePartialZoneConfig(b)

	// No zone was found. Use an empty zone config that inherits from its parent.
	if partialZone == nil {
		// If we are trying to discard a zone config that doesn't exist in
		// system.zones, make this a no-op.
		if n.Discard {
			return nil, nil, nil
		}
		partialZone = zonepb.NewZoneConfig()
	}
	currentZone := protoutil.Clone(partialZone).(*zonepb.ZoneConfig)

	// Retrieve the zone configuration.
	//
	// If the statement was USING DEFAULT, we want to ignore the zone
	// config that exists on targetID and instead skip to the inherited
	// default (default since our target is a database). For this, we use the last
	// parameter getInheritedDefault to retrieveCompleteZoneConfig().
	// These zones are only used for validations. The merged zone will not
	// be written.
	completeZone, _, err := obj.retrieveCompleteZoneConfig(b,
		n.SetDefault /* getInheritedDefault */)
	if err != nil {
		return nil, nil, err
	}

	// We need to inherit zone configuration information from the correct zone,
	// not completeZone.
	{
		// If we are operating on a zone, get all fields that the zone would
		// inherit from its parent. We do this by using an empty zoneConfig
		// and completing at the level of the current zone.
		zoneInheritedFields := zonepb.ZoneConfig{}
		if err := obj.completeZoneConfig(b, &zoneInheritedFields); err != nil {
			return nil, nil, err
		}
		partialZone.CopyFromZone(zoneInheritedFields, copyFromParentList)
	}

	if n.Discard {
		return nil, partialZone, nil
	}

	// Determine where to load the configuration.
	newZone := *completeZone

	// Determine where to load the partial configuration.
	// finalZone is where the new changes are unmarshalled onto.
	finalZone := *partialZone

	// Clone our zone config to log the old zone config as well as the new one.
	oldZone := protoutil.Clone(completeZone).(*zonepb.ZoneConfig)

	if n.SetDefault {
		// ALTER RANGE default USING DEFAULT sets the default to the in
		// memory default value.
		if keys.RootNamespaceID == uint32(obj.getTargetID()) {
			finalZone = *protoutil.Clone(b.GetDefaultZoneConfig()).(*zonepb.ZoneConfig)
		} else {
			finalZone = *zonepb.NewZoneConfig()
		}
	}

	// Fill in our zone configs with var = val assignments.
	if err := loadSettingsToZoneConfigs(setters, &newZone, &finalZone); err != nil {
		return nil, nil, err
	}

	// Validate that there are no conflicts in the zone setup.
	if err := zonepb.ValidateNoRepeatKeysInZone(&newZone); err != nil {
		return nil, nil, err
	}

	if err := validateZoneAttrsAndLocalities(b, currentZone, &newZone); err != nil {
		return nil, nil, err
	}

	// The final zone config is the one we just processed.
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

	// Finally, revalidate everything. Validate only the completeZone config.
	if err := completeZone.Validate(); err != nil {
		return nil, nil, pgerror.Wrap(err, pgcode.CheckViolation, "could not validate zone config")
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
		return nil, nil, err
	}
	return oldZone, partialZone, nil
}

// isCorrespondingTemporaryIndex returns true iff idx is a temporary index
// created during a backfill and is the corresponding temporary index for
// otherIdx. It assumes that idx and otherIdx are both indexes from the same
// table.
func isCorrespondingTemporaryIndex(
	b BuildCtx, tableID catid.DescID, idx catid.IndexID, otherIdx catid.IndexID,
) bool {
	maybeCorresponding := b.QueryByID(tableID).FilterTemporaryIndex().
		Filter(func(_ scpb.Status, _ scpb.TargetStatus, e *scpb.TemporaryIndex) bool {
			return idx == e.TemporaryIndexID && e.TemporaryIndexID == otherIdx+1
		}).MustGetZeroOrOneElement()
	return maybeCorresponding != nil
}

// findCorrespondingTemporaryIndexByID finds the temporary index that
// corresponds to the currently mutated index identified by ID.
//
// Callers should take care that AllocateIDs() has been called before
// using this function.
func findCorrespondingTemporaryIndexByID(
	b BuildCtx, tableID catid.DescID, indexID catid.IndexID,
) *scpb.TemporaryIndex {
	return b.QueryByID(tableID).FilterTemporaryIndex().
		Filter(func(_ scpb.Status, _ scpb.TargetStatus, e *scpb.TemporaryIndex) bool {
			return e.SourceIndexID == indexID
		}).MustGetZeroOrOneElement()
}

// getSubzoneSpansWithIdx groups each subzone span by their subzoneIndexes
// for a lookup of which subzone spans a particular subzone is referred to by.
func getSubzoneSpansWithIdx(
	numSubzones int, newSubzoneSpans []zonepb.SubzoneSpan,
) map[int32][]zonepb.SubzoneSpan {
	// We initialize our map to the number of subzones to ensure it contains every
	// subzoneIndex, even if there are no associated subzoneSpans.
	idxToSpans := make(map[int32][]zonepb.SubzoneSpan, numSubzones)
	for i := range numSubzones {
		idxToSpans[int32(i)] = []zonepb.SubzoneSpan{}
	}
	for _, s := range newSubzoneSpans {
		idxToSpans[s.SubzoneIndex] = append(idxToSpans[s.SubzoneIndex], s)
	}
	return idxToSpans
}

// constructSideEffectIndexElem constructs a side effect scpb.IndexZoneConfig
// for the given subzone. A "side effect scpb.IndexZoneConfig" is only created
// for existing subzone configs -- with the intent of updating its associated
// subzone configs due to some other zone config change.
func constructSideEffectIndexElem(
	b BuildCtx,
	zco zoneConfigObject,
	oldSubzoneIdx int32,
	subzoneSpans []zonepb.SubzoneSpan,
	subzone zonepb.Subzone,
) *scpb.IndexZoneConfig {
	// Get the most recent seqNum so we can properly update the side
	// effects.
	sameIndex := func(e *scpb.IndexZoneConfig) bool {
		return uint32(e.IndexID) == subzone.IndexID
	}
	mostRecentElem := findMostRecentZoneConfig(zco,
		func(id catid.DescID) *scpb.ElementCollection[*scpb.IndexZoneConfig] {
			return b.QueryByID(id).FilterIndexZoneConfig()
		}, sameIndex)
	if mostRecentElem == nil {
		panic(errors.AssertionFailedf("subzone for index %d, table %d has no existing subzone config",
			subzone.IndexID, zco.getTargetID()))
	}
	elem := &scpb.IndexZoneConfig{
		TableID:      zco.getTargetID(),
		IndexID:      catid.IndexID(subzone.IndexID),
		Subzone:      subzone,
		SubzoneSpans: subzoneSpans,
		SeqNum:       mostRecentElem.SeqNum + 1,
		OldIdxRef:    oldSubzoneIdx,
	}
	return elem
}

// constructSideEffectPartitionElem constructs a side effect
// scpb.PartitionZoneConfig for the given subzone. A
// "side effect scpb.IndexZoneConfig" is only created for existing subzone
// configs -- with the intent of updating its associated subzone configs due to
// some other zone config change.
func constructSideEffectPartitionElem(
	b BuildCtx,
	zco zoneConfigObject,
	oldSubzoneIdx int32,
	subzone zonepb.Subzone,
	subzoneSpans []zonepb.SubzoneSpan,
) *scpb.PartitionZoneConfig {
	// Get the most recent seqNum so we can properly update the side
	// effects.
	samePartition := func(e *scpb.PartitionZoneConfig) bool {
		return e.PartitionName == subzone.PartitionName && uint32(e.IndexID) == subzone.IndexID
	}
	mostRecentElem := findMostRecentZoneConfig(zco,
		func(id catid.DescID) *scpb.ElementCollection[*scpb.PartitionZoneConfig] {
			return b.QueryByID(id).FilterPartitionZoneConfig()
		}, samePartition)
	if mostRecentElem == nil {
		panic(errors.AssertionFailedf("subzone side effect for partition %s of index "+
			"%d, table %d has no existing subzone config",
			subzone.PartitionName, subzone.IndexID, zco.getTargetID()))
	}
	elem := &scpb.PartitionZoneConfig{
		TableID:       zco.getTargetID(),
		IndexID:       catid.IndexID(subzone.IndexID),
		PartitionName: subzone.PartitionName,
		Subzone:       subzone,
		SubzoneSpans:  subzoneSpans,
		SeqNum:        mostRecentElem.SeqNum + 1,
		OldIdxRef:     oldSubzoneIdx,
	}
	return elem
}

// TODO(annie): This is unused for now.
var _ = configureZoneConfigForNewIndexPartitioning

// configureZoneConfigForNewIndexPartitioning configures the zone config for any
// new index in a REGIONAL BY ROW table.
// This *must* be done after the index ID has been allocated.
func configureZoneConfigForNewIndexPartitioning(
	b BuildCtx, tableID catid.DescID, indexDesc descpb.IndexDescriptor,
) error {
	if indexDesc.ID == 0 {
		return errors.AssertionFailedf("index %s does not have id", indexDesc.Name)
	}
	// For REGIONAL BY ROW tables, correctly configure relevant zone configurations.
	localityRBR := b.QueryByID(tableID).FilterTableLocalityRegionalByRow().MustGetZeroOrOneElement()
	if localityRBR != nil {
		dbID := b.QueryByID(tableID).FilterNamespace().MustGetOneElement().DatabaseID
		regionConfig, err := b.SynthesizeRegionConfig(b, dbID)
		if err != nil {
			return err
		}

		indexIDs := []descpb.IndexID{indexDesc.ID}
		if idx := findCorrespondingTemporaryIndexByID(b, tableID, indexDesc.ID); idx != nil {
			indexIDs = append(indexIDs, idx.IndexID)
		}

		if err := ApplyZoneConfigForMultiRegionTable(
			b,
			regionConfig,
			tableID,
			applyZoneConfigForMultiRegionTableOptionNewIndexes(indexIDs...),
		); err != nil {
			return err
		}
	}
	return nil
}

// ApplyZoneConfigForMultiRegionTable applies zone config settings based
// on the options provided and adds the scpb.TableZoneConfig to our builder.
func ApplyZoneConfigForMultiRegionTable(
	b BuildCtx,
	regionConfig multiregion.RegionConfig,
	tableID catid.DescID,
	opts ...applyZoneConfigForMultiRegionTableOption,
) error {
	currentZoneConfigWithRaw, err := b.ZoneConfigGetter().GetZoneConfig(b, tableID)
	if err != nil {
		return err
	}
	if currentZoneConfigWithRaw == nil {
		currentZoneConfigWithRaw = zone.NewZoneConfigWithRawBytes(zonepb.NewZoneConfig(), nil)
	}
	newZoneConfig := *currentZoneConfigWithRaw.ZoneConfigProto()

	for _, opt := range opts {
		modifiedNewZoneConfig, err := opt(
			newZoneConfig,
			regionConfig,
			tableID,
		)
		if err != nil {
			return err
		}
		newZoneConfig = modifiedNewZoneConfig
	}

	if regionConfig.HasSecondaryRegion() {
		var newLeasePreferences []zonepb.LeasePreference
		localityRBR := b.QueryByID(tableID).FilterTableLocalityRegionalByRow().MustGetZeroOrOneElement()
		switch {
		case localityRBR != nil:
			region := b.QueryByID(tableID).FilterTableLocalitySecondaryRegion().MustGetZeroOrOneElement()
			if region != nil {
				newLeasePreferences = regions.SynthesizeLeasePreferences(region.RegionName, regionConfig.SecondaryRegion())
			} else {
				newLeasePreferences = regions.SynthesizeLeasePreferences(regionConfig.PrimaryRegion(), regionConfig.SecondaryRegion())
			}
		default:
			newLeasePreferences = regions.SynthesizeLeasePreferences(regionConfig.PrimaryRegion(), regionConfig.SecondaryRegion())
		}
		newZoneConfig.LeasePreferences = newLeasePreferences
	}

	// Mark the NumReplicas as 0 if we have subzones but no other features
	// in the zone config. This signifies a placeholder.
	// Note we do not use hasNewSubzones here as there may be existing subzones
	// on the zone config which may still be a placeholder.
	if regions.IsPlaceholderZoneConfigForMultiRegion(newZoneConfig) {
		newZoneConfig.NumReplicas = proto.Int32(0)
	}

	// Determine if we're rewriting or deleting the zone configuration.
	newZoneConfigIsEmpty := newZoneConfig.Equal(zonepb.NewZoneConfig())
	currentZoneConfigIsEmpty := currentZoneConfigWithRaw.ZoneConfigProto().Equal(zonepb.NewZoneConfig())
	rewriteZoneConfig := !newZoneConfigIsEmpty
	deleteZoneConfig := newZoneConfigIsEmpty && !currentZoneConfigIsEmpty

	if deleteZoneConfig {
		return nil
	}
	if !rewriteZoneConfig {
		return nil
	}

	if err := newZoneConfig.Validate(); err != nil {
		return pgerror.Wrap(
			err,
			pgcode.CheckViolation,
			"could not validate zone config",
		)
	}
	if err := newZoneConfig.ValidateTandemFields(); err != nil {
		return pgerror.Wrap(
			err,
			pgcode.CheckViolation,
			"could not validate zone config",
		)
	}
	if len(newZoneConfig.Subzones) > 0 {
		newZoneConfig.SubzoneSpans, err = generateSubzoneSpans(b, tableID, newZoneConfig.Subzones)
		if err != nil {
			return err
		}
	} else {
		// To keep the Subzone and SubzoneSpan arrays consistent
		newZoneConfig.SubzoneSpans = nil
	}
	if newZoneConfig.IsSubzonePlaceholder() && len(newZoneConfig.Subzones) == 0 {
		return nil
	}
	// Adding a new zone config element entails adding a new element where the
	// seqNum is 1 greater than the most recent zone config element. This ensures
	// zone config changes are applied in the correct order during schema changes.
	maxSeq := uint32(0)
	b.QueryByID(tableID).FilterTableZoneConfig().
		ForEach(func(status scpb.Status, targetStatus scpb.TargetStatus, elem *scpb.TableZoneConfig) {
			if elem.SeqNum > maxSeq {
				maxSeq = elem.SeqNum
			}
		})
	tzc := &scpb.TableZoneConfig{
		TableID:    tableID,
		ZoneConfig: &newZoneConfig,
		SeqNum:     maxSeq + 1,
	}
	b.Add(tzc)
	return nil
}

// applyZoneConfigForMultiRegionTableOptionNewIndexes applies table zone configs
// for a newly added index which requires partitioning of individual indexes.
func applyZoneConfigForMultiRegionTableOptionNewIndexes(
	indexIDs ...descpb.IndexID,
) applyZoneConfigForMultiRegionTableOption {
	return func(
		zoneConfig zonepb.ZoneConfig,
		regionConfig multiregion.RegionConfig,
		tableID catid.DescID,
	) (newZoneConfig zonepb.ZoneConfig, err error) {
		for _, indexID := range indexIDs {
			for _, region := range regionConfig.Regions() {
				zc, err := regions.ZoneConfigForMultiRegionPartition(region, regionConfig)
				if err != nil {
					return zoneConfig, err
				}
				zoneConfig.SetSubzone(zonepb.Subzone{
					IndexID:       uint32(indexID),
					PartitionName: string(region),
					Config:        zc,
				})
			}
		}
		return zoneConfig, nil
	}
}

// applyZoneConfigForMultiRegionTableOption is an option that can be passed into
// applyZoneConfigForMultiRegionTable.
type applyZoneConfigForMultiRegionTableOption func(
	zoneConfig zonepb.ZoneConfig,
	regionConfig multiregion.RegionConfig,
	tableID catid.DescID,
) (newZoneConfig zonepb.ZoneConfig, err error)
