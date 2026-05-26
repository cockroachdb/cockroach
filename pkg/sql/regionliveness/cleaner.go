// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package regionliveness

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc/keyside"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/errors"
)

// GetSystemTableSpanForRegion gets the span that contains the primary index
// span related to a given region.
func GetSystemTableSpanForRegion(
	descriptor catalog.TableDescriptor, codec keys.SQLCodec, regionPhysicalRep string,
) (roachpb.Span, error) {
	if descriptor.GetParentID() != keys.SystemDatabaseID {
		return roachpb.Span{},
			errors.AssertionFailedf("clean up is only supported for system tables")
	}
	if len(descriptor.AllIndexes()) != 1 {
		return roachpb.Span{},
			errors.AssertionFailedf("clean up is only supported for system tables with a single index")
	}

	regionDatum := tree.NewDBytes(tree.DBytes(regionPhysicalRep))
	tableIndexPrefix := codec.IndexPrefix(uint32(descriptor.GetID()), uint32(descriptor.GetPrimaryIndexID()))
	tableRegionPrefixBytes, err := keyside.Encode(tableIndexPrefix, regionDatum, encoding.Ascending)
	if err != nil {
		return roachpb.Span{}, err
	}
	tableRegionPrefix := roachpb.Key(tableRegionPrefixBytes)
	return roachpb.Span{
		Key:    tableRegionPrefix,
		EndKey: tableRegionPrefix.PrefixEnd(),
	}, nil
}

func CleanupSystemTableForRegion(
	ctx context.Context, codec keys.SQLCodec, regionPhysicalRep string, txn *kv.Txn,
) error {
	// Delete all rows for the region in system.sqlliveness, system.sql_instances,
	// system.leases.
	instanceSpan, err := GetSystemTableSpanForRegion(systemschema.SQLInstancesTable(), codec, regionPhysicalRep)
	if err != nil {
		return err
	}
	leaseTableSpan, err := GetSystemTableSpanForRegion(systemschema.LeaseTable(), codec, regionPhysicalRep)
	if err != nil {
		return err
	}
	livenessTableSpan, err := GetSystemTableSpanForRegion(systemschema.SqllivenessTable(), codec, regionPhysicalRep)
	if err != nil {
		return err
	}
	// Clear all the spans from above.
	if _, err := txn.DelRange(ctx, instanceSpan.Key, instanceSpan.EndKey, false); err != nil {
		return err
	}
	if _, err := txn.DelRange(ctx, leaseTableSpan.Key, leaseTableSpan.EndKey, false); err != nil {
		return err
	}
	_, err = txn.DelRange(ctx, livenessTableSpan.Key, livenessTableSpan.EndKey, false)
	return err
}
