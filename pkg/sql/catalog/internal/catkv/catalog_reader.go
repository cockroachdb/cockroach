// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package catkv

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/nstree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// catalogReader builds catalogQuery objects and runs them, leveraging the
// SystemDatabaseCache if it is present.
type catalogReader struct {
	Codec   keys.SQLCodec
	Version clusterversion.ClusterVersion

	// systemDatabaseCache is a cache of system database catalog information.
	// Its presence is entirely optional and only serves to eliminate superfluous
	// round trips to KV.
	systemDatabaseCache *SystemDatabaseCache
}

// scanAll scans the entirety of the descriptor and namespace tables.
func (cr catalogReader) scanAll(ctx context.Context, txn *kv.Txn) (nstree.Catalog, error) {
	var mc nstree.MutableCatalog
	log.Eventf(ctx, "fetching all descriptors and namespace entries")
	cq := catalogQuery{catalogReader: cr}
	err := cq.query(ctx, txn, &mc, func(codec keys.SQLCodec, b *kv.Batch) {
		b.Header.MaxSpanRequestKeys = 0
		descsPrefix := catalogkeys.MakeAllDescsMetadataKey(codec)
		b.Scan(descsPrefix, descsPrefix.PrefixEnd())
		nsPrefix := codec.IndexPrefix(keys.NamespaceTableID, catconstants.NamespaceTablePrimaryIndexID)
		b.Scan(nsPrefix, nsPrefix.PrefixEnd())
	})
	if err != nil {
		return nstree.Catalog{}, err
	}
	return mc.Catalog, nil
}

// scanNamespaceForDatabases scans the portion of the namespace table which
// contains all database name entries.
func (cr catalogReader) scanNamespaceForDatabases(
	ctx context.Context, txn *kv.Txn,
) (nstree.Catalog, error) {
	var mc nstree.MutableCatalog
	cq := catalogQuery{catalogReader: cr}
	err := cq.query(ctx, txn, &mc, func(codec keys.SQLCodec, b *kv.Batch) {
		b.Header.MaxSpanRequestKeys = 0
		prefix := catalogkeys.MakeDatabaseNameKey(codec, "")
		b.Scan(prefix, prefix.PrefixEnd())
	})
	if err != nil {
		return nstree.Catalog{}, err
	}
	return mc.Catalog, nil
}

// getDescriptorEntries gets the descriptors for the desired IDs, but looks in
// the system database cache first.
func (cr catalogReader) getDescriptorEntries(
	ctx context.Context,
	txn *kv.Txn,
	ids []descpb.ID,
	isRequired bool,
	expectedType catalog.DescriptorType,
) (nstree.Catalog, error) {
	var mc nstree.MutableCatalog
	if len(ids) == 0 {
		return nstree.Catalog{}, nil
	}
	if log.ExpensiveLogEnabled(ctx, 2) {
		log.Infof(ctx, "looking up descriptors by id: %v", ids)
	}
	var needsQuery bool
	for _, id := range ids {
		if desc := cr.systemDatabaseCache.lookupDescriptor(cr.Version, id); desc != nil {
			mc.UpsertDescriptorEntry(desc)
		} else if id != descpb.InvalidID {
			needsQuery = true
		}
	}
	// Only run a query when absolutely necessary.
	if needsQuery {
		cq := catalogQuery{
			catalogReader: cr,
			isRequired:    isRequired,
			expectedType:  expectedType,
		}
		err := cq.query(ctx, txn, &mc, func(codec keys.SQLCodec, b *kv.Batch) {
			for _, id := range ids {
				if id != descpb.InvalidID && mc.LookupDescriptorEntry(id) == nil {
					b.Get(catalogkeys.MakeDescMetadataKey(codec, id))
				}
			}
		})
		if err != nil {
			return nstree.Catalog{}, err
		}
	}
	if isRequired {
		for _, id := range ids {
			if mc.LookupDescriptorEntry(id) == nil {
				return nstree.Catalog{}, wrapError(expectedType, id, requiredError(expectedType, id))
			}
		}
	}
	return mc.Catalog, nil
}

// getNamespaceEntries gets the descriptor IDs for the desired names, but looks
// in the system database cache first.
func (cr catalogReader) getNamespaceEntries(
	ctx context.Context, txn *kv.Txn, nameInfos []descpb.NameInfo,
) (nstree.Catalog, error) {
	if len(nameInfos) == 0 {
		return nstree.Catalog{}, nil
	}
	var mc nstree.MutableCatalog
	var needsQuery bool
	for _, nameInfo := range nameInfos {
		if id := cr.systemDatabaseCache.lookupDescriptorID(cr.Version, nameInfo); id != descpb.InvalidID {
			mc.UpsertNamespaceEntry(nameInfo, id)
		} else if nameInfo.Name != "" {
			needsQuery = true
		}
	}
	// Only run a query when absolutely necessary.
	if needsQuery {
		cq := catalogQuery{catalogReader: cr}
		err := cq.query(ctx, txn, &mc, func(codec keys.SQLCodec, b *kv.Batch) {
			for _, nameInfo := range nameInfos {
				if nameInfo.Name != "" && mc.LookupNamespaceEntry(nameInfo) == nil {
					b.Get(catalogkeys.EncodeNameKey(codec, nameInfo))
				}
			}
		})
		if err != nil {
			return nstree.Catalog{}, err
		}
	}
	return mc.Catalog, nil
}
