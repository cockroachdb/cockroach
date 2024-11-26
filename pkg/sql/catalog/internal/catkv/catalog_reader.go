// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package catkv

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/nstree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
)

// CatalogReader queries the system tables containing catalog data.
type CatalogReader interface {

	// Codec returns the codec used by this CatalogReader.
	Codec() keys.SQLCodec

	// Cache returns the whole contents of the in-memory cache in use by this
	// CatalogReader if there is one.
	Cache() nstree.Catalog

	// IsIDInCache return true when all the by-ID catalog data for this ID
	// is known to be in the cache.
	IsIDInCache(id descpb.ID) bool

	// IsNameInCache return true when all the by-name catalog data for this name
	// key is known to be in the cache.
	IsNameInCache(key descpb.NameInfo) bool

	// IsDescIDKnownToNotExist returns true when we know that there definitely
	// exists no descriptor in storage with that ID.
	IsDescIDKnownToNotExist(id, maybeParentID descpb.ID) bool

	// Reset resets any state that the CatalogReader may hold.
	Reset(ctx context.Context)

	// ScanAll scans the entirety of the descriptor and namespace tables.
	ScanAll(ctx context.Context, txn *kv.Txn) (nstree.Catalog, error)

	// ScanDescriptorsInSpans scans the descriptors specified in a given span.
	ScanDescriptorsInSpans(ctx context.Context, txn *kv.Txn, span []roachpb.Span) (nstree.Catalog, error)

	// ScanAllComments scans the entirety of the comments table as well as the namespace entries for the given database.
	// If the dbContext is nil, we scan the database-level namespace entries.
	ScanAllComments(ctx context.Context, txn *kv.Txn, db catalog.DatabaseDescriptor) (nstree.Catalog, error)

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

	// ScanNamespaceForDatabaseSchemasAndObjects scans the portion of the
	// namespace table which contains all name entries for children of a
	// given database.
	ScanNamespaceForDatabaseSchemasAndObjects(
		ctx context.Context,
		txn *kv.Txn,
		db catalog.DatabaseDescriptor,
	) (nstree.Catalog, error)

	// ScanNamespaceForSchemaObjects scans the portion of the namespace table
	// which contains all object name entries for a given schema.
	ScanNamespaceForSchemaObjects(
		ctx context.Context, txn *kv.Txn, db catalog.DatabaseDescriptor, sc catalog.SchemaDescriptor,
	) (nstree.Catalog, error)

	// GetByIDs reads the system.descriptor, system.comments and system.zone
	// entries for the desired IDs, but looks in the system database cache
	// first if there is one.
	GetByIDs(
		ctx context.Context,
		txn *kv.Txn,
		ids []descpb.ID,
		isDescriptorRequired bool,
		expectedType catalog.DescriptorType,
	) (nstree.Catalog, error)

	// GetByNames reads the system.namespace entries for the given keys, but
	// looks in the system database cache first if there is one.
	GetByNames(
		ctx context.Context, txn *kv.Txn, nameInfos []descpb.NameInfo,
	) (nstree.Catalog, error)
}

// NewUncachedCatalogReader is the constructor for the default
// CatalogReader implementation without a SystemDatabaseCache.
func NewUncachedCatalogReader(codec keys.SQLCodec) CatalogReader {
	return &catalogReader{
		codec: codec,
	}
}

// catalogReader implements the CatalogReader interface by building catalogQuery
// objects and running them, leveraging the SystemDatabaseCache if present.
type catalogReader struct {
	codec keys.SQLCodec
}

var _ CatalogReader = (*catalogReader)(nil)

// Codec is part of the CatalogReader interface.
func (cr catalogReader) Codec() keys.SQLCodec {
	return cr.codec
}

// Reset is part of the CatalogReader interface.
func (cr catalogReader) Reset(_ context.Context) {}

// Cache is part of the CatalogReader interface.
func (cr catalogReader) Cache() nstree.Catalog {
	return nstree.Catalog{}
}

// IsIDInCache is part of the CatalogReader interface.
func (cr catalogReader) IsIDInCache(_ descpb.ID) bool {
	return false
}

// IsNameInCache is part of the CatalogReader interface.
func (cr catalogReader) IsNameInCache(_ descpb.NameInfo) bool {
	return false
}

// IsDescIDKnownToNotExist is part of the CatalogReader interface.
func (cr catalogReader) IsDescIDKnownToNotExist(_, _ descpb.ID) bool {
	return false
}

// ScanAll is part of the CatalogReader interface.
func (cr catalogReader) ScanAll(ctx context.Context, txn *kv.Txn) (nstree.Catalog, error) {
	var mc nstree.MutableCatalog
	cq := catalogQuery{codec: cr.codec}
	err := cq.query(ctx, txn, &mc, func(codec keys.SQLCodec, b *kv.Batch) {
		scan(ctx, b, catalogkeys.MakeAllDescsMetadataKey(codec))
		scan(ctx, b, codec.IndexPrefix(keys.NamespaceTableID, catconstants.NamespaceTablePrimaryIndexID))
		scan(ctx, b, catalogkeys.CommentsMetadataPrefix(codec))
		scan(ctx, b, config.ZonesPrimaryIndexPrefix(codec))
	})
	if err != nil {
		return nstree.Catalog{}, err
	}
	return mc.Catalog, nil
}

// getDescriptorIDFromExclusiveKey translates an exclusive upper bound roach key
// into an upper bound descriptor ID. It does this by turning the key into a
// descriptor ID, and then moving it upwards if and only if it is not the prefix
// of the current index / table span.
func getDescriptorIDFromExclusiveKey(codec keys.SQLCodec, key roachpb.Key) (uint32, error) {
	keyWithoutTable, endID, err := codec.DecodeTablePrefix(key)
	if err != nil {
		return 0, err
	}
	if len(keyWithoutTable) == 0 {
		return endID, nil
	}

	keyWithoutIndex, _, indexId, err := codec.DecodeIndexPrefix(key)
	if err != nil {
		return 0, err
	}
	// if there's remaining bytes or the index isn't the primary, increment
	// the end so that the descriptor under the key is included.
	if len(keyWithoutIndex) != 0 || indexId > 1 {
		endID++
	}
	return endID, nil
}

// getDescriptorSpanFromSpan returns a start and end descriptor ID from a given span
func getDescriptorSpanFromSpan(codec keys.SQLCodec, span roachpb.Span) (roachpb.Span, error) {
	_, startID, err := codec.DecodeTablePrefix(span.Key)
	if err != nil {
		return roachpb.Span{}, err
	}
	endID, err := getDescriptorIDFromExclusiveKey(codec, span.EndKey)
	if err != nil {
		return roachpb.Span{}, err
	}

	return roachpb.Span{
		Key:    catalogkeys.MakeDescMetadataKey(codec, descpb.ID(startID)),
		EndKey: catalogkeys.MakeDescMetadataKey(codec, descpb.ID(endID)),
	}, nil
}

// ScanDescriptorsInSpans is part of the CatalogReader interface.
func (cr catalogReader) ScanDescriptorsInSpans(
	ctx context.Context, txn *kv.Txn, spans []roachpb.Span,
) (nstree.Catalog, error) {
	var mc nstree.MutableCatalog

	descSpans := []roachpb.Span{}
	for _, span := range spans {
		descSpan, err := getDescriptorSpanFromSpan(cr.Codec(), span)
		if err != nil {
			return mc.Catalog, err
		}
		if descSpan.ZeroLength() {
			continue
		}
		descSpans = append(descSpans, descSpan)
	}

	cq := catalogQuery{codec: cr.codec}
	err := cq.query(ctx, txn, &mc, func(codec keys.SQLCodec, b *kv.Batch) {
		for _, descSpan := range descSpans {
			scanRange(ctx, b, descSpan.Key, descSpan.EndKey)
		}
	})
	if err != nil {
		return mc.Catalog, err
	}

	return mc.Catalog, nil
}

// ScanAllComments is part of the CatalogReader interface.
func (cr catalogReader) ScanAllComments(
	ctx context.Context, txn *kv.Txn, db catalog.DatabaseDescriptor,
) (nstree.Catalog, error) {
	var mc nstree.MutableCatalog
	cq := catalogQuery{codec: cr.codec}
	err := cq.query(ctx, txn, &mc, func(codec keys.SQLCodec, b *kv.Batch) {
		// N.B. the primary key of system.namespace is ("parentID" ASC, "parentSchemaID" ASC, name ASC).
		parentID := descpb.ID(0)
		if db != nil {
			parentID = db.GetID()
		}
		scan(ctx, b, catalogkeys.MakeDatabaseChildrenNameKeyPrefix(codec, parentID))
		scan(ctx, b, catalogkeys.CommentsMetadataPrefix(codec))
	})
	if err != nil {
		return nstree.Catalog{}, err
	}
	return mc.Catalog, nil
}

func (cr catalogReader) scanNamespace(
	ctx context.Context, txn *kv.Txn, prefix roachpb.Key,
) (nstree.Catalog, error) {
	var mc nstree.MutableCatalog
	cq := catalogQuery{codec: cr.codec}
	err := cq.query(ctx, txn, &mc, func(codec keys.SQLCodec, b *kv.Batch) {
		scan(ctx, b, prefix)
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
	return cr.scanNamespace(
		ctx, txn, catalogkeys.EncodeNameKey(cr.codec, &descpb.NameInfo{}),
	)
}

// ScanNamespaceForDatabaseSchemas is part of the CatalogReader interface.
func (cr catalogReader) ScanNamespaceForDatabaseSchemas(
	ctx context.Context, txn *kv.Txn, db catalog.DatabaseDescriptor,
) (nstree.Catalog, error) {
	return cr.scanNamespace(
		ctx, txn, catalogkeys.EncodeNameKey(cr.codec, &descpb.NameInfo{ParentID: db.GetID()}),
	)
}

// ScanNamespaceForDatabaseSchemasAndObjects is part of the CatalogReader
// interface.
func (cr catalogReader) ScanNamespaceForDatabaseSchemasAndObjects(
	ctx context.Context, txn *kv.Txn, db catalog.DatabaseDescriptor,
) (nstree.Catalog, error) {
	return cr.scanNamespace(
		ctx, txn, catalogkeys.MakeDatabaseChildrenNameKeyPrefix(cr.codec, db.GetID()),
	)
}

// ScanNamespaceForSchemaObjects is part of the CatalogReader interface.
func (cr catalogReader) ScanNamespaceForSchemaObjects(
	ctx context.Context, txn *kv.Txn, db catalog.DatabaseDescriptor, sc catalog.SchemaDescriptor,
) (nstree.Catalog, error) {
	return cr.scanNamespace(ctx, txn, catalogkeys.EncodeNameKey(cr.codec, &descpb.NameInfo{
		ParentID:       db.GetID(),
		ParentSchemaID: sc.GetID(),
	}))
}

// forEachDescriptorIDSpan loops over a list of descriptor IDs and generates
// spans from them.
func forEachDescriptorIDSpan(ids []descpb.ID, spanFn func(startID descpb.ID, endID descpb.ID)) {
	// Tracks the start and end of the run of descriptor ID's.
	startIDSet := false
	runStartID := descpb.InvalidID
	runEndID := descpb.InvalidID

	for _, id := range ids {
		// Detect if we have a linear run of IDs, which case extend the batch.
		if startIDSet && id == runEndID+1 {
			runEndID = id
		} else if startIDSet {
			// The run has broken so emit whatever is left.
			spanFn(runStartID, runEndID)
			startIDSet = false
		}
		if !startIDSet {
			startIDSet = true
			runStartID = id
			runEndID = id
		}
	}
	if startIDSet {
		spanFn(runStartID, runEndID)
	}
}

// GetByIDs is part of the CatalogReader interface.
func (cr catalogReader) GetByIDs(
	ctx context.Context,
	txn *kv.Txn,
	ids []descpb.ID,
	isDescriptorRequired bool,
	expectedType catalog.DescriptorType,
) (nstree.Catalog, error) {
	var mc nstree.MutableCatalog
	if len(ids) == 0 {
		return nstree.Catalog{}, nil
	}
	cq := catalogQuery{
		codec:                cr.codec,
		isDescriptorRequired: isDescriptorRequired,
		expectedType:         expectedType,
	}

	err := cq.query(ctx, txn, &mc, func(codec keys.SQLCodec, b *kv.Batch) {
		// Attempt to generate a optimal set of requests by extracting
		// a spans of descriptors when possible.
		forEachDescriptorIDSpan(ids, func(startID descpb.ID, endID descpb.ID) {
			// Only a single descriptor run, so generate a Get request.
			if startID == endID {
				get(ctx, b, catalogkeys.MakeDescMetadataKey(codec, startID))
				for _, t := range catalogkeys.AllCommentTypes {
					scan(ctx, b, catalogkeys.MakeObjectCommentsMetadataPrefix(codec, t, startID))
				}
				get(ctx, b, config.MakeZoneKey(codec, startID))
			} else {
				// Otherwise, generate a Scan request instead. The end key is exclusive,
				// so we will need to increment the endID.
				scanRange(ctx, b, catalogkeys.MakeDescMetadataKey(codec, startID),
					catalogkeys.MakeDescMetadataKey(codec, endID+1))
				for _, t := range catalogkeys.AllCommentTypes {
					scanRange(ctx, b, catalogkeys.MakeObjectCommentsMetadataPrefix(codec, t, startID),
						catalogkeys.MakeObjectCommentsMetadataPrefix(codec, t, endID+1))
				}
				scanRange(ctx, b, config.MakeZoneKey(codec, startID),
					config.MakeZoneKey(codec, endID+1))
			}
		})
	})
	if err != nil {
		return nstree.Catalog{}, err
	}

	if isDescriptorRequired {
		for _, id := range ids {
			if mc.LookupDescriptor(id) == nil {
				return nstree.Catalog{}, wrapError(expectedType, id, requiredError(expectedType, id))
			}
		}
	}
	return mc.Catalog, nil
}

// GetByNames is part of the CatalogReader interface.
func (cr catalogReader) GetByNames(
	ctx context.Context, txn *kv.Txn, nameInfos []descpb.NameInfo,
) (nstree.Catalog, error) {
	if len(nameInfos) == 0 {
		return nstree.Catalog{}, nil
	}
	var mc nstree.MutableCatalog
	cq := catalogQuery{codec: cr.codec}
	err := cq.query(ctx, txn, &mc, func(codec keys.SQLCodec, b *kv.Batch) {
		for i := range nameInfos {
			ni := &nameInfos[i]
			if ni.Name != "" {
				get(ctx, b, catalogkeys.EncodeNameKey(codec, ni))
			}
		}
	})
	if err != nil {
		return nstree.Catalog{}, err
	}
	return mc.Catalog, nil
}

func get(ctx context.Context, b *kv.Batch, key roachpb.Key) {
	b.Get(key)
	if isEventLoggingEnabled(ctx) {
		log.VEventfDepth(ctx, 1, 2, "Get %s", key)
	}
}

func scanRange(ctx context.Context, b *kv.Batch, start roachpb.Key, end roachpb.Key) {
	b.Header.MaxSpanRequestKeys = 0
	b.Scan(start, end)
	if isEventLoggingEnabled(ctx) {
		log.VEventfDepth(ctx, 1, 2, "Scan Range %s %s", start, end)
	}
}

func scan(ctx context.Context, b *kv.Batch, prefix roachpb.Key) {
	b.Header.MaxSpanRequestKeys = 0
	b.Scan(prefix, prefix.PrefixEnd())
	if isEventLoggingEnabled(ctx) {
		log.VEventfDepth(ctx, 1, 2, "Scan %s", prefix)
	}
}

// TestingSpanOperationName is the operation name for the context
// span in place for event logging in CatalogReader implementations.
const TestingSpanOperationName = "catalog-reader-test-case"

func isEventLoggingEnabled(ctx context.Context) bool {
	// Presently, we don't want to log any events outside of tests.
	sp := tracing.SpanFromContext(ctx)
	return sp != nil && sp.IsVerbose() && sp.OperationName() == TestingSpanOperationName
}
