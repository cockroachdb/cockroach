// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package zoneconfig

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/errors"
)

func init() {
	// TODO(marc): we use a hook to avoid a dependency on the sql package. We
	// should probably move keys/protos elsewhere.
	config.ZoneConfigHook = zoneConfigHook
}

// getZoneConfig recursively looks up entries in system.zones until an
// entry that applies to the object with the specified id is
// found. Returns the ID of the matching zone, its zone config, and an
// optional placeholder ID and config if the looked-for ID was a table
// with a zone config specifying only indexes and/or partitions.
//
// This function must be kept in sync with ascendZoneSpecifier.
//
// If getInheritedDefault is true, the direct zone configuration, if it exists, is
// ignored, and the default that would apply if it did not exist is returned instead.
//
// If mayBeTable is true then we will attempt to decode the id into a table
// descriptor in order to find its parent. If false, we'll assume that this
// already is a parent and we'll not decode a descriptor.
func getZoneConfig(
	ctx context.Context,
	id descpb.ID,
	txn *kv.Txn,
	zcHelper catalog.ZoneConfigHydrationHelper,
	getInheritedDefault bool,
	mayBeTable bool,
) (descpb.ID, *zonepb.ZoneConfig, descpb.ID, *zonepb.ZoneConfig, error) {
	var placeholder *zonepb.ZoneConfig
	var placeholderID descpb.ID
	if !getInheritedDefault {
		// Look in the zones table.
		zone, err := zcHelper.MaybeGetZoneConfig(ctx, txn, id)
		if err != nil {
			return 0, nil, 0, nil, err
		}
		if zone != nil {
			if !zone.ZoneConfigProto().IsSubzonePlaceholder() {
				return id, zone.ZoneConfigProto(), 0, nil, nil
			}
			placeholder = zone.ZoneConfigProto()
			placeholderID = id
		}
	}

	// No zone config for this ID. We need to figure out if it's a table, so we
	// look up its descriptor.
	if mayBeTable {
		tbl, err := zcHelper.MaybeGetTable(ctx, txn, id)
		if err != nil {
			return 0, nil, 0, nil, err
		}
		if tbl != nil {
			dbID, zone, _, _, err := getZoneConfig(
				ctx,
				tbl.GetParentID(),
				txn,
				zcHelper,
				false, /* getInheritedDefault */
				false, /* mayBeTable */
			)
			if err != nil {
				return 0, nil, 0, nil, err
			}
			return dbID, zone, placeholderID, placeholder, nil
		}
	}

	// Retrieve the default zone config, but only as long as that wasn't the ID
	// we were trying to retrieve (avoid infinite recursion).
	if id != keys.RootNamespaceID {
		rootID, zone, _, _, err := getZoneConfig(
			ctx,
			keys.RootNamespaceID,
			txn,
			zcHelper,
			false, /* getInheritedDefault */
			false /* mayBeTable */)
		if err != nil {
			return 0, nil, 0, nil, err
		}
		return rootID, zone, placeholderID, placeholder, nil
	}

	// No descriptor or not a table.
	return 0, nil, 0, nil, sqlerrors.ErrNoZoneConfigApplies
}

// Complete takes a zone config pointer and fills in the
// missing fields by following the chain of inheritance.
// In the worst case, will have to inherit from the default zone config.
// NOTE: This will not work for subzones. To complete subzones, find a complete
// parent zone (index or table) and apply InheritFromParent to it.
func Complete(
	ctx context.Context,
	zone *zonepb.ZoneConfig,
	txn *kv.Txn,
	zcHelper catalog.ZoneConfigHydrationHelper,
	id descpb.ID,
) error {
	if zone.IsComplete() {
		return nil
	}
	// Check to see if it's a table. If so, inherit from the database.
	// For all other cases, inherit from the default.
	if tbl, err := zcHelper.MaybeGetTable(ctx, txn, id); err != nil {
		return err
	} else if tbl != nil {
		_, dbzone, _, _, err := getZoneConfig(
			ctx, tbl.GetParentID(), txn, zcHelper, false /* getInheritedDefault */, false, /* mayBeTable */
		)
		if err != nil {
			return err
		}
		zone.InheritFromParent(dbzone)
	}

	// Check if zone is complete. If not, inherit from the default zone config
	if zone.IsComplete() {
		return nil
	}
	_, defaultZone, _, _, err := getZoneConfig(ctx, keys.RootNamespaceID, txn, zcHelper, false /* getInheritedDefault */, false /* mayBeTable */)
	if err != nil {
		return err
	}
	zone.InheritFromParent(defaultZone)
	return nil
}

// zoneConfigHook returns the zone config and optional placeholder config for
// the object with id using the cached system config. The returned boolean is
// set to true when the zone config returned can be cached.
//
// zoneConfigHook is a pure function whose only inputs are a system config and
// an object ID. It does not make any external KV calls to look up additional
// state.
func zoneConfigHook(
	cfg *config.SystemConfig, codec keys.SQLCodec, id config.ObjectID,
) (*zonepb.ZoneConfig, *zonepb.ZoneConfig, bool, error) {
	helper := &systemZoneConfigHelper{cfg: cfg, codec: codec}

	const mayBeTable = true
	zoneID, zone, _, placeholder, err := getZoneConfig(
		context.TODO(), // This context won't actually be used.
		descpb.ID(id),
		nil, /* txn */
		helper,
		false, /* getInheritedDefault */
		mayBeTable,
	)
	if errors.Is(err, sqlerrors.ErrNoZoneConfigApplies) {
		return nil, nil, true, nil
	} else if err != nil {
		return nil, nil, false, err
	}
	// The context passed in won't actually be used.
	if err = Complete(context.TODO(), zone, nil /* txn */, helper, zoneID); err != nil {
		return nil, nil, false, err
	}
	return zone, placeholder, true, nil
}

// GetInTxn looks up the zone and subzone for the specified object ID,
// index, and partition. See the documentation on getZoneConfig for information
// about the getInheritedDefault parameter.
//
// Unlike ZoneConfigHook, GetInTxn does not used a cached system
// config. Instead, it uses the provided txn to make transactionally consistent
// KV lookups.
func GetInTxn(
	ctx context.Context,
	txn *kv.Txn,
	descriptors *descs.Collection,
	id descpb.ID,
	index catalog.Index,
	partition string,
	getInheritedDefault bool,
) (descpb.ID, *zonepb.ZoneConfig, *zonepb.Subzone, error) {
	zcHelper := descs.AsZoneConfigHydrationHelper(descriptors)
	zoneID, zone, placeholderID, placeholder, err := getZoneConfig(
		ctx, id, txn, zcHelper, getInheritedDefault, true, /* mayBeTable */
	)
	if err != nil {
		return 0, nil, nil, err
	}
	if err = Complete(ctx, zone, txn, zcHelper, zoneID); err != nil {
		return 0, nil, nil, err
	}
	var subzone *zonepb.Subzone
	if index != nil {
		indexID := uint32(index.GetID())
		if placeholder != nil {
			if subzone = placeholder.GetSubzone(indexID, partition); subzone != nil {
				if indexSubzone := placeholder.GetSubzone(indexID, ""); indexSubzone != nil {
					subzone.Config.InheritFromParent(&indexSubzone.Config)
				}
				subzone.Config.InheritFromParent(zone)
				return placeholderID, placeholder, subzone, nil
			}
		} else {
			if subzone = zone.GetSubzone(indexID, partition); subzone != nil {
				if indexSubzone := zone.GetSubzone(indexID, ""); indexSubzone != nil {
					subzone.Config.InheritFromParent(&indexSubzone.Config)
				}
				subzone.Config.InheritFromParent(zone)
			}
		}
	}
	return zoneID, zone, subzone, nil
}

// GetHydratedForTenantsRange returns the zone config for RANGE
// TENANTS.
func GetHydratedForTenantsRange(
	ctx context.Context, txn *kv.Txn, descriptors *descs.Collection,
) (*zonepb.ZoneConfig, error) {
	return GetHydratedForNamedZone(
		ctx,
		txn,
		descriptors,
		zonepb.TenantsZoneName,
	)
}

// GetHydratedForNamedZone returns a zone config for the given named
// zone. Any missing fields are filled through the RANGE DEFAULT zone config.
func GetHydratedForNamedZone(
	ctx context.Context, txn *kv.Txn, descriptors *descs.Collection, zoneName zonepb.NamedZone,
) (*zonepb.ZoneConfig, error) {
	id, found := zonepb.NamedZones[zoneName]
	if !found {
		return nil, errors.AssertionFailedf("id %d does not belong to a named zone", id)
	}
	zcHelper := descs.AsZoneConfigHydrationHelper(descriptors)
	zoneID, zone, _, _, err := getZoneConfig(
		ctx, descpb.ID(id), txn, zcHelper, false /* getInheritedDefault */, false, /* mayBeTable */
	)
	if err != nil {
		return nil, err
	}
	if err := Complete(ctx, zone, txn, zcHelper, zoneID); err != nil {
		return nil, err
	}
	return zone, nil
}

// GetHydratedForTable returns a fully hydrated zone config for a
// given table ID.
func GetHydratedForTable(
	ctx context.Context, txn *kv.Txn, descriptors *descs.Collection, id descpb.ID,
) (*zonepb.ZoneConfig, error) {
	zcHelper := descs.AsZoneConfigHydrationHelper(descriptors)
	zoneID, zone, _, placeholder, err := getZoneConfig(
		ctx, id, txn, zcHelper, false /* getInheritedDefault */, true, /* mayBeTable */
	)
	if err != nil {
		return nil, err
	}
	if err := Complete(ctx, zone, txn, zcHelper, zoneID); err != nil {
		return nil, err
	}

	// We've completely hydrated the zone config now. The only thing left to do
	// is to do is hydrate the subzones, if applicable.

	// A placeholder config exists only to store subzones, so we copy over that
	// information on the zone config.
	if placeholder != nil {
		// A placeholder config only exists for tables. Furthermore, if it exists,
		// then the zone config (`zone`) above must belong to an object further up
		// in the inheritance chain (such as the database or DEFAULT RANGE). As the
		// subzones field is only defined if the zone config applies to a table, it
		// follows that `zone` must not have any Subzones set on it.
		if len(zone.Subzones) != 0 {
			return nil, errors.AssertionFailedf("placeholder %v exists in conjunction with subzones on zone config %v", *zone, *placeholder)
		}
		zone.Subzones = placeholder.Subzones
		zone.SubzoneSpans = placeholder.SubzoneSpans
	}

	for i, subzone := range zone.Subzones {
		// Check if a zone configuration exists for the index this subzone applies
		// to by passing in a an empty partition below.
		indexSubzone := zone.GetSubzone(subzone.IndexID, "" /* partition  */)
		// Partitions, in terms of the inheritance hierarchy, first inherit from the
		// zone configuration fields on their parent index (if such a zone
		// configuration exists).
		// NB: If the subzone we're dealing with belongs to an index and not a
		// partition, then the call below is a no-op.
		if indexSubzone != nil {
			zone.Subzones[i].Config.InheritFromParent(&indexSubzone.Config)
		}
		// After inheriting from the index's zone configuration, any fields that are
		// left empty must be filled in from the table's zone configuration. Note
		// that the table's zone configuration was fully hydrated above and
		// inheriting from it will result in the subzone config being fully hydrated
		// as well.
		zone.Subzones[i].Config.InheritFromParent(zone)
	}

	return zone, nil
}

// GetHydratedForDatabase returns a fully hydrated zone config for a
// given database ID.
func GetHydratedForDatabase(
	ctx context.Context, txn *kv.Txn, descriptors *descs.Collection, id descpb.ID,
) (*zonepb.ZoneConfig, error) {
	zcHelper := descs.AsZoneConfigHydrationHelper(descriptors)
	zoneID, zone, _, _, err := getZoneConfig(
		ctx, id, txn, zcHelper, false /* getInheritedDefault */, false, /* mayBeTable */
	)
	if err != nil {
		return nil, err
	}
	if err := Complete(ctx, zone, txn, zcHelper, zoneID); err != nil {
		return nil, err
	}

	return zone, nil
}
