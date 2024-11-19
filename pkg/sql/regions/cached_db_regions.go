// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package regions

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/lease"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

// CachedDatabaseRegions tracks the regions that are part of
// a multi-region database.
type CachedDatabaseRegions struct {
	dbDesc           catalog.DatabaseDescriptor
	dbRegionEnumDesc catalog.TypeDescriptor
}

// NewCachedDatabaseRegionsAt creates a new region cache by leasing
// the underlying descriptors for the system database and region enum,
// with descriptors fetched at a specific timestamp.
func NewCachedDatabaseRegionsAt(
	ctx context.Context, _ *kv.DB, lm *lease.Manager, at hlc.Timestamp,
) (cdr *CachedDatabaseRegions, err error) {
	cdr = &CachedDatabaseRegions{}
	desc, err := lm.Acquire(ctx, at, keys.SystemDatabaseID)
	if err != nil {
		return nil, err
	}
	defer desc.Release(ctx)
	cdr.dbDesc = desc.Underlying().(catalog.DatabaseDescriptor)
	if !cdr.dbDesc.IsMultiRegion() {
		return cdr, nil
	}
	enumID, err := cdr.dbDesc.MultiRegionEnumID()
	if err != nil {
		return nil, err
	}

	desc, err = lm.Acquire(ctx, at, enumID)
	if err != nil {
		return nil, err
	}
	defer desc.Release(ctx)
	cdr.dbRegionEnumDesc = desc.Underlying().(catalog.EnumTypeDescriptor)
	return cdr, nil
}

// NewCachedDatabaseRegions creates a new region cache by leasing
// the underlying descriptors for the system database and region enum.
func NewCachedDatabaseRegions(
	ctx context.Context, db *kv.DB, lm *lease.Manager,
) (*CachedDatabaseRegions, error) {
	return NewCachedDatabaseRegionsAt(ctx, db, lm, db.Clock().Now())
}

// IsMultiRegion returns if the system database is multi-region.
func (c *CachedDatabaseRegions) IsMultiRegion() bool {
	return c.dbRegionEnumDesc != nil
}

// GetRegionEnumTypeDesc returns the enum type descriptor for the system
// database.
func (c *CachedDatabaseRegions) GetRegionEnumTypeDesc() catalog.RegionEnumTypeDescriptor {
	return c.dbRegionEnumDesc.AsRegionEnumTypeDescriptor()
}

// GetSystemDatabaseVersion helper to get the system database version.
func (c *CachedDatabaseRegions) GetSystemDatabaseVersion() *roachpb.Version {
	return c.dbDesc.DatabaseDesc().GetSystemDatabaseSchemaVersion()
}

func TestingModifyRegionEnum(
	c *CachedDatabaseRegions, mutateFn func(catalog.TypeDescriptor) catalog.TypeDescriptor,
) {
	c.dbRegionEnumDesc = mutateFn(c.dbRegionEnumDesc)
}
