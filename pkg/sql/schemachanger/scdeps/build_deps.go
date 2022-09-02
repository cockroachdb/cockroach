// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package scdeps

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/resolver"
	"github.com/cockroachdb/cockroach/pkg/sql/descmetadata"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scbuild"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq/oid"
)

// NewBuilderDependencies returns an scbuild.Dependencies implementation built
// from the given arguments.
func NewBuilderDependencies(
	clusterID uuid.UUID,
	codec keys.SQLCodec,
	txn *kv.Txn,
	descsCollection *descs.Collection,
	schemaResolverFactory scbuild.SchemaResolverFactory,
	authAccessor scbuild.AuthorizationAccessor,
	astFormatter scbuild.AstFormatter,
	featureChecker scbuild.FeatureChecker,
	sessionData *sessiondata.SessionData,
	settings *cluster.Settings,
	statements []string,
	internalExecutor sqlutil.InternalExecutor,
	clientNoticeSender eval.ClientNoticeSender,
) scbuild.Dependencies {
	return &buildDeps{
		clusterID:        clusterID,
		codec:            codec,
		txn:              txn,
		descsCollection:  descsCollection,
		authAccessor:     authAccessor,
		sessionData:      sessionData,
		settings:         settings,
		statements:       statements,
		astFormatter:     astFormatter,
		featureChecker:   featureChecker,
		internalExecutor: internalExecutor,
		schemaResolver: schemaResolverFactory(
			descsCollection, sessiondata.NewStack(sessionData), txn, authAccessor,
		),
		clientNoticeSender: clientNoticeSender,
	}
}

type buildDeps struct {
	clusterID          uuid.UUID
	codec              keys.SQLCodec
	txn                *kv.Txn
	descsCollection    *descs.Collection
	schemaResolver     resolver.SchemaResolver
	authAccessor       scbuild.AuthorizationAccessor
	sessionData        *sessiondata.SessionData
	settings           *cluster.Settings
	statements         []string
	astFormatter       scbuild.AstFormatter
	featureChecker     scbuild.FeatureChecker
	internalExecutor   sqlutil.InternalExecutor
	clientNoticeSender eval.ClientNoticeSender
}

var _ scbuild.CatalogReader = (*buildDeps)(nil)

// MayResolveDatabase implements the scbuild.CatalogReader interface.
func (d *buildDeps) MayResolveDatabase(
	ctx context.Context, name tree.Name,
) catalog.DatabaseDescriptor {
	db, err := d.descsCollection.GetImmutableDatabaseByName(ctx, d.txn, string(name), tree.DatabaseLookupFlags{
		AvoidLeased: true,
	})
	if err != nil {
		panic(err)
	}
	return db
}

// MayResolveSchema implements the scbuild.CatalogReader interface.
func (d *buildDeps) MayResolveSchema(
	ctx context.Context, name tree.ObjectNamePrefix,
) (catalog.DatabaseDescriptor, catalog.SchemaDescriptor) {
	if !name.ExplicitCatalog {
		name.CatalogName = tree.Name(d.schemaResolver.CurrentDatabase())
	}
	db := d.MayResolveDatabase(ctx, name.CatalogName)
	schema, err := d.descsCollection.GetImmutableSchemaByName(ctx, d.txn, db, name.Schema(), tree.SchemaLookupFlags{
		AvoidLeased: true,
	})
	if err != nil {
		panic(err)
	}
	return db, schema
}

// MayResolveTable implements the scbuild.CatalogReader interface.
func (d *buildDeps) MayResolveTable(
	ctx context.Context, name tree.UnresolvedObjectName,
) (catalog.ResolvedObjectPrefix, catalog.TableDescriptor) {
	desc, prefix, err := resolver.ResolveExistingObject(ctx, d.schemaResolver, &name, tree.ObjectLookupFlags{
		CommonLookupFlags: tree.CommonLookupFlags{
			AvoidLeased: true,
		},
		DesiredObjectKind: tree.TableObject,
	})
	if err != nil {
		panic(err)
	}
	if desc == nil {
		return prefix, nil
	}
	return prefix, desc.(catalog.TableDescriptor)
}

// MayResolveType implements the scbuild.CatalogReader interface.
func (d *buildDeps) MayResolveType(
	ctx context.Context, name tree.UnresolvedObjectName,
) (catalog.ResolvedObjectPrefix, catalog.TypeDescriptor) {
	desc, prefix, err := resolver.ResolveExistingObject(ctx, d.schemaResolver, &name, tree.ObjectLookupFlags{
		CommonLookupFlags: tree.CommonLookupFlags{
			AvoidLeased: true,
		},
		DesiredObjectKind: tree.TypeObject,
	})
	if err != nil {
		panic(err)
	}
	if desc == nil {
		return prefix, nil
	}
	return prefix, desc.(catalog.TypeDescriptor)
}

// MayResolveIndex implements the scbuild.CatalogReader interface.
func (d *buildDeps) MayResolveIndex(
	ctx context.Context, tableIndexName tree.TableIndexName,
) (
	found bool,
	prefix catalog.ResolvedObjectPrefix,
	tbl catalog.TableDescriptor,
	idx catalog.Index,
) {
	found, prefix, tbl, idx, err := resolver.ResolveIndex(
		ctx, d.schemaResolver, &tableIndexName, d.txn, d.codec, false /* required */, false, /* requireActiveIndex */
	)
	if err != nil {
		panic(err)
	}
	return found, prefix, tbl, idx
}

// ReadObjectNamesAndIDs implements the scbuild.CatalogReader interface.
func (d *buildDeps) ReadObjectNamesAndIDs(
	ctx context.Context, db catalog.DatabaseDescriptor, schema catalog.SchemaDescriptor,
) (tree.TableNames, descpb.IDs) {
	names, ids, err := d.descsCollection.GetObjectNamesAndIDs(ctx, d.txn, db, schema.GetName(), tree.DatabaseListFlags{
		CommonLookupFlags: tree.CommonLookupFlags{
			Required:       true,
			RequireMutable: false,
			AvoidLeased:    true,
			IncludeOffline: true,
			IncludeDropped: true,
		},
		ExplicitPrefix: true,
	})
	if err != nil {
		panic(err)
	}
	return names, ids
}

// ResolveType implements the scbuild.CatalogReader interface.
func (d *buildDeps) ResolveType(
	ctx context.Context, name *tree.UnresolvedObjectName,
) (*types.T, error) {
	return d.schemaResolver.ResolveType(ctx, name)
}

// ResolveTypeByOID implements the scbuild.CatalogReader interface.
func (d *buildDeps) ResolveTypeByOID(ctx context.Context, oid oid.Oid) (*types.T, error) {
	return d.schemaResolver.ResolveTypeByOID(ctx, oid)
}

// ResolveFunction implements the scbuild.CatalogReader interface.
func (d *buildDeps) ResolveFunction(
	ctx context.Context, name *tree.UnresolvedName, path tree.SearchPath,
) (*tree.ResolvedFunctionDefinition, error) {
	return d.schemaResolver.ResolveFunction(ctx, name, path)
}

// ResolveFunctionByOID implements the scbuild.CatalogReader interface.
func (d *buildDeps) ResolveFunctionByOID(
	ctx context.Context, oid oid.Oid,
) (string, *tree.Overload, error) {
	return d.schemaResolver.ResolveFunctionByOID(ctx, oid)
}

// GetQualifiedTableNameByID implements the scbuild.CatalogReader interface.
func (d *buildDeps) GetQualifiedTableNameByID(
	ctx context.Context, id int64, requiredType tree.RequiredTableKind,
) (*tree.TableName, error) {
	return d.schemaResolver.GetQualifiedTableNameByID(ctx, id, requiredType)
}

// CurrentDatabase implements the scbuild.CatalogReader interface.
func (d *buildDeps) CurrentDatabase() string {
	return d.schemaResolver.CurrentDatabase()
}

// MustReadDescriptor implements the scbuild.CatalogReader interface.
func (d *buildDeps) MustReadDescriptor(ctx context.Context, id descpb.ID) catalog.Descriptor {
	flags := tree.CommonLookupFlags{
		Required:       true,
		RequireMutable: false,
		AvoidLeased:    true,
		IncludeOffline: true,
		IncludeDropped: true,
	}
	desc, err := d.descsCollection.GetImmutableDescriptorByID(ctx, d.txn, id, flags)
	if err != nil {
		panic(err)
	}
	return desc
}

// MustGetSchemasForDatabase  implements the scbuild.CatalogReader interface.
func (d *buildDeps) MustGetSchemasForDatabase(
	ctx context.Context, database catalog.DatabaseDescriptor,
) map[descpb.ID]string {
	schemas, err := d.descsCollection.GetSchemasForDatabase(ctx, d.txn, database)
	if err != nil {
		panic(err)
	}
	return schemas
}

// IsTableEmpty implements the scbuild.TableReader interface.
func (d *buildDeps) IsTableEmpty(
	ctx context.Context, id descpb.ID, primaryIndexID descpb.IndexID,
) bool {
	indexPrefix := rowenc.MakeIndexKeyPrefix(d.codec, id, primaryIndexID)
	span := roachpb.Key(indexPrefix)
	kvs, err := d.txn.Scan(ctx, span, span.PrefixEnd(), 1)
	if err != nil {
		panic(err)
	}
	return len(kvs) == 0
}

// CreatePartitioningCCL is the public hook point for the CCL-licensed
// partitioning creation code.
var CreatePartitioningCCL scbuild.CreatePartitioningCCLCallback

var _ scbuild.Dependencies = (*buildDeps)(nil)

// AuthorizationAccessor implements the scbuild.Dependencies interface.
func (d *buildDeps) AuthorizationAccessor() scbuild.AuthorizationAccessor {
	return d.authAccessor
}

// CatalogReader implements the scbuild.Dependencies interface.
func (d *buildDeps) CatalogReader() scbuild.CatalogReader {
	return d
}

// TableReader implements the scbuild.Dependencies interface.
func (d *buildDeps) TableReader() scbuild.TableReader {
	return d
}

// ClusterID implements the scbuild.Dependencies interface.
func (d *buildDeps) ClusterID() uuid.UUID {
	return d.clusterID
}

// Codec implements the scbuild.Dependencies interface.
func (d *buildDeps) Codec() keys.SQLCodec {
	return d.codec
}

// SessionData implements the scbuild.Dependencies interface.
func (d *buildDeps) SessionData() *sessiondata.SessionData {
	return d.sessionData
}

// ClusterSettings implements the scbuild.Dependencies interface.
func (d *buildDeps) ClusterSettings() *cluster.Settings {
	return d.settings
}

// Statements implements the scbuild.Dependencies interface.
func (d *buildDeps) Statements() []string {
	return d.statements
}

// AstFormatter implements the scbuild.Dependencies interface.
func (d *buildDeps) AstFormatter() scbuild.AstFormatter {
	return d.astFormatter
}

// FeatureChecker implements the scbuild.Dependencies interface.
func (d *buildDeps) FeatureChecker() scbuild.FeatureChecker {
	return d.featureChecker
}

// IndexPartitioningCCLCallback implements the scbuild.Dependencies interface.
func (d *buildDeps) IndexPartitioningCCLCallback() scbuild.CreatePartitioningCCLCallback {
	if CreatePartitioningCCL == nil {
		return func(
			_ context.Context,
			_ *cluster.Settings,
			_ *eval.Context,
			_ func(tree.Name) (catalog.Column, error),
			_ int,
			_ []string,
			_ *tree.PartitionBy,
			_ []tree.Name,
			_ bool,
		) ([]catalog.Column, catpb.PartitioningDescriptor, error) {
			return nil, catpb.PartitioningDescriptor{}, sqlerrors.NewCCLRequiredError(errors.New(
				"creating or manipulating partitions requires a CCL binary"))
		}
	}
	return CreatePartitioningCCL
}

// IncrementSchemaChangeAlterCounter implements the scbuild.Dependencies
// interface.
func (d *buildDeps) IncrementSchemaChangeAlterCounter(counterType string, extra ...string) {
	var maybeExtra string
	if len(extra) > 0 {
		maybeExtra = extra[0]
	}
	telemetry.Inc(sqltelemetry.SchemaChangeAlterCounterWithExtra(counterType, maybeExtra))
}

// IncrementSchemaChangeDropCounter implements the scbuild.Dependencies
// interface.
func (d *buildDeps) IncrementSchemaChangeDropCounter(counterType string) {
	telemetry.Inc(sqltelemetry.SchemaChangeDropCounter(counterType))
}

// IncrementSchemaChangeAddColumnTypeCounter implements the scbuild.Dependencies
func (d *buildDeps) IncrementSchemaChangeAddColumnTypeCounter(typeName string) {
	telemetry.Inc(sqltelemetry.SchemaNewTypeCounter(typeName))
}

// IncrementSchemaChangeAddColumnQualificationCounter implements the scbuild.Dependencies
func (d *buildDeps) IncrementSchemaChangeAddColumnQualificationCounter(qualification string) {
	telemetry.Inc(sqltelemetry.SchemaNewColumnTypeQualificationCounter(qualification))
}

// IncrementUserDefinedSchemaCounter implements the scbuild.Dependencies
// interface.
func (d *buildDeps) IncrementUserDefinedSchemaCounter(
	counterType sqltelemetry.UserDefinedSchemaTelemetryType,
) {
	sqltelemetry.IncrementUserDefinedSchemaCounter(counterType)
}

// IncrementEnumCounter implements the scbuild.Dependencies interface.
func (d *buildDeps) IncrementEnumCounter(counterType sqltelemetry.EnumTelemetryType) {
	sqltelemetry.IncrementEnumCounter(counterType)
}

// IncrementDropOwnedByCounter implements the scbuild.Dependencies interface.
func (d *buildDeps) IncrementDropOwnedByCounter() {
	telemetry.Inc(sqltelemetry.CreateDropOwnedByCounter())
}

func (d *buildDeps) DescriptorCommentCache() scbuild.CommentCache {
	return descmetadata.NewCommentCache(d.txn, d.internalExecutor)
}

func (d *buildDeps) ZoneConfigGetter() scbuild.ZoneConfigGetter {
	return descmetadata.NewZoneConfigGetter(d.txn, d.internalExecutor)
}

// ClientNoticeSender implements the scbuild.Dependencies interface.
func (d *buildDeps) ClientNoticeSender() eval.ClientNoticeSender {
	return d.clientNoticeSender
}
