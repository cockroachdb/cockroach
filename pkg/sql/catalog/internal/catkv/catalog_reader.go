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

// CatalogReader queries the system.namespace and system.descriptor tables,
// leveraging the SystemDatabaseCache if it is present in the implementation.
//
// The main use case for CatalogReader should be StoredCatalog.
type CatalogReader interface {

	// Codec returns the codec used by this CatalogReader.
	Codec() keys.SQLCodec

	// ScanAll scans the entirety of the descriptor and namespace tables.
	ScanAll(ctx context.Context, txn *kv.Txn) (nstree.Catalog, error)

	// ScanNamespaceForDatabases scans the portion of the namespace table which
	// contains all database name entries.
	ScanNamespaceForDatabases(ctx context.Context, txn *kv.Txn) (nstree.Catalog, error)

	// ScanNamespaceForDatabaseSchemas scans the portion of the namespace table
	// which contains all schema name entries for a given database.
	ScanNamespaceForDatabaseSchemas(
		ctx context.Context,
		txn *kv.Txn,
		db catalog.DatabaseDescriptor,
	) (nstree.Catalog, error)

	// ScanNamespaceForSchemaObjects scans the portion of the namespace table
	// which contains all object name entries for a given schema.
	ScanNamespaceForSchemaObjects(
		ctx context.Context, txn *kv.Txn, db catalog.DatabaseDescriptor, sc catalog.SchemaDescriptor,
	) (nstree.Catalog, error)

	// GetDescriptorEntries gets the descriptors for the desired IDs, but looks in
	// the system database cache first if there is one.
	GetDescriptorEntries(
		ctx context.Context,
		txn *kv.Txn,
		ids []descpb.ID,
		isRequired bool,
		expectedType catalog.DescriptorType,
	) (nstree.Catalog, error)

	// GetNamespaceEntries gets the descriptor IDs for the desired names, but
	// looks in the system database cache first if there is one.
	GetNamespaceEntries(
		ctx context.Context, txn *kv.Txn, nameInfos []descpb.NameInfo,
	) (nstree.Catalog, error)
}

// NewCatalogReader is the constructor for the default CatalogReader
// implementation.
func NewCatalogReader(
	codec keys.SQLCodec,
	version clusterversion.ClusterVersion,
	systemDatabaseCache *SystemDatabaseCache,
) CatalogReader {
	return &catalogReader{
		codec:               codec,
		version:             version,
		systemDatabaseCache: systemDatabaseCache,
	}
}

// NewUncachedCatalogReader is the constructor for the default CatalogReader
// implementation without a SystemDatabaseCache.
func NewUncachedCatalogReader(codec keys.SQLCodec) CatalogReader {
	return &catalogReader{
		codec: codec,
	}
}

// catalogReader implements the CatalogReader interface by building catalogQuery
// objects and running them, leveraging the SystemDatabaseCache if present.
type catalogReader struct {
	codec keys.SQLCodec

	// systemDatabaseCache is a cache of system database catalog information.
	// Its presence is entirely optional and only serves to eliminate superfluous
	// round trips to KV.
	systemDatabaseCache *SystemDatabaseCache
	// version only needs to be set when systemDatabaseCache is set.
	version clusterversion.ClusterVersion
}

var _ CatalogReader = (*catalogReader)(nil)

// Codec is part of the CatalogReader interface.
func (cr catalogReader) Codec() keys.SQLCodec {
	return cr.codec
}

// ScanAll is part of the CatalogReader interface.
func (cr catalogReader) ScanAll(ctx context.Context, txn *kv.Txn) (nstree.Catalog, error) {
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

// ScanNamespaceForDatabases is part of the CatalogReader interface.
func (cr catalogReader) ScanNamespaceForDatabases(
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

// ScanNamespaceForDatabaseSchemas is part of the CatalogReader interface.
func (cr catalogReader) ScanNamespaceForDatabaseSchemas(
	ctx context.Context, txn *kv.Txn, db catalog.DatabaseDescriptor,
) (nstree.Catalog, error) {
	var mc nstree.MutableCatalog
	cq := catalogQuery{catalogReader: cr}
	err := cq.query(ctx, txn, &mc, func(codec keys.SQLCodec, b *kv.Batch) {
		b.Header.MaxSpanRequestKeys = 0
		prefix := catalogkeys.MakeSchemaNameKey(cr.codec, db.GetID(), "" /* name */)
		b.Scan(prefix, prefix.PrefixEnd())
	})
	if err != nil {
		return nstree.Catalog{}, err
	}
	return mc.Catalog, nil
}

// ScanNamespaceForSchemaObjects is part of the CatalogReader interface.
func (cr catalogReader) ScanNamespaceForSchemaObjects(
	ctx context.Context, txn *kv.Txn, db catalog.DatabaseDescriptor, sc catalog.SchemaDescriptor,
) (nstree.Catalog, error) {
	var mc nstree.MutableCatalog
	cq := catalogQuery{catalogReader: cr}
	err := cq.query(ctx, txn, &mc, func(codec keys.SQLCodec, b *kv.Batch) {
		b.Header.MaxSpanRequestKeys = 0
		prefix := catalogkeys.MakeObjectNameKey(cr.codec, db.GetID(), sc.GetID(), "" /* name */)
		b.Scan(prefix, prefix.PrefixEnd())
	})
	if err != nil {
		return nstree.Catalog{}, err
	}
	return mc.Catalog, nil
}

// GetDescriptorEntries is part of the CatalogReader interface.
func (cr catalogReader) GetDescriptorEntries(
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
		log.VEventf(ctx, 2, "looking up descriptors by id: %v", ids)
	}
	var needsQuery bool
	for _, id := range ids {
		if desc := cr.systemDatabaseCache.lookupDescriptor(cr.version, id); desc != nil {
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

// GetNamespaceEntries is part of the CatalogReader interface.
func (cr catalogReader) GetNamespaceEntries(
	ctx context.Context, txn *kv.Txn, nameInfos []descpb.NameInfo,
) (nstree.Catalog, error) {
	if len(nameInfos) == 0 {
		return nstree.Catalog{}, nil
	}
	var mc nstree.MutableCatalog
	var needsQuery bool
	for _, nameInfo := range nameInfos {
		if id, ts := cr.systemDatabaseCache.lookupDescriptorID(cr.version, nameInfo); id != descpb.InvalidID {
			mc.UpsertNamespaceEntry(nameInfo, id, ts)
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
