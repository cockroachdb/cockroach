// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package regions

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/errors"
)

// RegionSet is a map where each key is a region name.
type RegionSet map[string]struct{}

// GetDatabaseRegions returns the active regions from the database's region
// enum. If the database is not multi-region, a nil RegionSet will be returned.
func GetDatabaseRegions(
	ctx context.Context, txn *kv.Txn, db catalog.DatabaseDescriptor, descs *descs.Collection,
) (RegionSet, error) {
	if !db.IsMultiRegion() {
		return nil, nil
	}
	enumID, _ := db.MultiRegionEnumID()
	typ, err := descs.ByIDWithLeased(txn).Get().Type(ctx, enumID)
	if err != nil {
		return nil, errors.NewAssertionErrorWithWrappedErrf(
			err, "failed to resolve multi-region enum for the database (%d)", enumID,
		)
	}
	t := typ.AsEnumTypeDescriptor()
	if t == nil {
		return nil, errors.WithDetailf(
			errors.AssertionFailedf(
				"multi-region type %s (%d) for the database is not an enum",
				typ.GetName(), typ.GetID(),
			), "descriptor: %v", typ)
	}
	set := make(map[string]struct{}, t.NumEnumMembers())
	for i, n := 0, t.NumEnumMembers(); i < n; i++ {
		// Skip regions which don't fully exist. This could mean that they
		// are being dropped, or they are being added.
		if !t.IsMemberReadOnly(i) {
			set[t.GetMemberLogicalRepresentation(i)] = struct{}{}
		}
	}
	return set, nil
}
