// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package regions

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/lease"
)

// CachedDatabaseRegions tracks the regions that are part of
// a multi-region database.
type CachedDatabaseRegions struct {
	dbDesc           catalog.DatabaseDescriptor
	dbRegionEnumDesc catalog.TypeDescriptor
}

// NewCachedDatabaseRegions creates a new region cache by leasing
// the underlying descriptors for the system database and region enum.
func NewCachedDatabaseRegions(
	ctx context.Context, db *kv.DB, lm *lease.Manager,
) (cdr *CachedDatabaseRegions, err error) {
	cdr = &CachedDatabaseRegions{}
	now := db.Clock().Now()
	desc, err := lm.Acquire(ctx, now, keys.SystemDatabaseID)
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

	desc, err = lm.Acquire(ctx, now, enumID)
	if err != nil {
		return nil, err
	}
	defer desc.Release(ctx)
	cdr.dbRegionEnumDesc = desc.Underlying().(catalog.EnumTypeDescriptor)
	return cdr, nil
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
