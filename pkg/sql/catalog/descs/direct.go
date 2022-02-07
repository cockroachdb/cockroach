// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package descs

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/internal/catkv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/nstree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// Direct provides direct access to the underlying KV-storage.
func (tc *Collection) Direct() Direct { return &tc.direct }

// Direct provides access to the underlying key-value store directly. A key
// difference between descriptors retrieved directly vs. descriptors retrieved
// through the Collection is that the descriptors will not be hydrated.
//
// Note: If you are tempted to use this in a place which is not currently using
// it, pause, and consider the decision very carefully.
type Direct interface {
	// GetCatalogUnvalidated looks up and returns all available descriptors and
	// namespace system table entries but does not validate anything.
	// It is exported solely to be used by functions which want to perform explicit
	// validation to detect corruption.
	GetCatalogUnvalidated(
		ctx context.Context, txn *kv.Txn,
	) (nstree.Catalog, error)

	// MustGetDatabaseDescByID looks up the database descriptor given its ID,
	// returning an error if the descriptor is not found.
	MustGetDatabaseDescByID(
		ctx context.Context, txn *kv.Txn, id descpb.ID,
	) (catalog.DatabaseDescriptor, error)

	// MustGetSchemaDescByID looks up the schema descriptor given its ID,
	// returning an error if the descriptor is not found.
	MustGetSchemaDescByID(
		ctx context.Context, txn *kv.Txn, id descpb.ID,
	) (catalog.SchemaDescriptor, error)

	// MustGetTypeDescByID looks up the type descriptor given its ID,
	// returning an error if the type is not found.
	MustGetTypeDescByID(
		ctx context.Context, txn *kv.Txn, id descpb.ID,
	) (catalog.TypeDescriptor, error)

	// MustGetTableDescByID looks up the table descriptor given its ID,
	// returning an error if the table is not found.
	MustGetTableDescByID(
		ctx context.Context, txn *kv.Txn, id descpb.ID,
	) (catalog.TableDescriptor, error)

	// GetSchemaDescriptorsFromIDs returns the schema descriptors from an input
	// list of schema IDs. It will return an error if any one of the IDs is not
	// a schema.
	GetSchemaDescriptorsFromIDs(
		ctx context.Context, txn *kv.Txn, ids []descpb.ID,
	) ([]catalog.SchemaDescriptor, error)

	// ResolveSchemaID resolves a schema's ID based on db and name.
	ResolveSchemaID(
		ctx context.Context, txn *kv.Txn, dbID descpb.ID, scName string,
	) (descpb.ID, error)

	// GetDescriptorCollidingWithObject looks up the object ID and returns the
	// corresponding descriptor if it exists.
	GetDescriptorCollidingWithObject(
		ctx context.Context, txn *kv.Txn, parentID descpb.ID, parentSchemaID descpb.ID, name string,
	) (catalog.Descriptor, error)

	// CheckObjectCollision returns an error if an object already exists with the
	// same parentID, parentSchemaID and name.
	CheckObjectCollision(
		ctx context.Context,
		txn *kv.Txn,
		parentID descpb.ID,
		parentSchemaID descpb.ID,
		name tree.ObjectName,
	) error

	// LookupDatabaseID is a wrapper around LookupObjectID for databases.
	LookupDatabaseID(
		ctx context.Context, txn *kv.Txn, dbName string,
	) (descpb.ID, error)

	// LookupSchemaID is a wrapper around LookupObjectID for schemas.
	LookupSchemaID(
		ctx context.Context, txn *kv.Txn, dbID descpb.ID, schemaName string,
	) (descpb.ID, error)

	// LookupObjectID returns the table or type descriptor ID for the namespace
	// entry keyed by (parentID, parentSchemaID, name).
	// Returns descpb.InvalidID when no matching entry exists.
	LookupObjectID(
		ctx context.Context, txn *kv.Txn, dbID descpb.ID, schemaID descpb.ID, objectName string,
	) (descpb.ID, error)

	// WriteNewDescToBatch adds a CPut command writing a descriptor proto to the
	// descriptors table. It writes the descriptor desc at the id descID, asserting
	// that there was no previous descriptor at that id present already. If kvTrace
	// is enabled, it will log an event explaining the CPut that was performed.
	WriteNewDescToBatch(
		ctx context.Context, kvTrace bool, b *kv.Batch, desc catalog.Descriptor,
	) error
}

type direct struct {
	settings *cluster.Settings
	codec    keys.SQLCodec
	version  clusterversion.ClusterVersion
}

func makeDirect(ctx context.Context, codec keys.SQLCodec, s *cluster.Settings) direct {
	return direct{
		settings: s,
		codec:    codec,
		version:  s.Version.ActiveVersion(ctx),
	}
}

func (d *direct) GetCatalogUnvalidated(ctx context.Context, txn *kv.Txn) (nstree.Catalog, error) {
	return catkv.GetCatalogUnvalidated(ctx, txn, d.codec)
}
func (d *direct) MustGetDatabaseDescByID(
	ctx context.Context, txn *kv.Txn, id descpb.ID,
) (catalog.DatabaseDescriptor, error) {
	desc, err := catkv.MustGetDescriptorByID(ctx, txn, d.codec, d.version, id, catalog.Database)
	if err != nil {
		return nil, err
	}
	return desc.(catalog.DatabaseDescriptor), nil
}
func (d *direct) MustGetSchemaDescByID(
	ctx context.Context, txn *kv.Txn, id descpb.ID,
) (catalog.SchemaDescriptor, error) {
	desc, err := catkv.MustGetDescriptorByID(ctx, txn, d.codec, d.version, id, catalog.Schema)
	if err != nil {
		return nil, err
	}
	return desc.(catalog.SchemaDescriptor), nil
}
func (d *direct) MustGetTableDescByID(
	ctx context.Context, txn *kv.Txn, id descpb.ID,
) (catalog.TableDescriptor, error) {
	desc, err := catkv.MustGetDescriptorByID(ctx, txn, d.codec, d.version, id, catalog.Table)
	if err != nil {
		return nil, err
	}
	return desc.(catalog.TableDescriptor), nil
}
func (d *direct) MustGetTypeDescByID(
	ctx context.Context, txn *kv.Txn, id descpb.ID,
) (catalog.TypeDescriptor, error) {
	desc, err := catkv.MustGetDescriptorByID(ctx, txn, d.codec, d.version, id, catalog.Type)
	if err != nil {
		return nil, err
	}
	return desc.(catalog.TypeDescriptor), nil
}
func (d *direct) GetSchemaDescriptorsFromIDs(
	ctx context.Context, txn *kv.Txn, ids []descpb.ID,
) ([]catalog.SchemaDescriptor, error) {
	descs, err := catkv.MustGetDescriptorsByID(
		ctx,
		txn,
		d.codec,
		d.version,
		ids,
		catalog.Schema)
	if err != nil {
		return nil, err
	}
	ret := make([]catalog.SchemaDescriptor, len(descs))
	for i, desc := range descs {
		ret[i] = desc.(catalog.SchemaDescriptor)
	}
	return ret, nil
}
func (d *direct) ResolveSchemaID(
	ctx context.Context, txn *kv.Txn, dbID descpb.ID, scName string,
) (descpb.ID, error) {
	if !d.version.IsActive(clusterversion.PublicSchemasWithDescriptors) {
		// Try to use the system name resolution bypass. Avoids a hotspot by explicitly
		// checking for public schema.
		if scName == tree.PublicSchema {
			return keys.PublicSchemaID, nil
		}
	}
	return catkv.LookupID(ctx, txn, d.codec, dbID, keys.RootNamespaceID, scName)
}
func (d *direct) GetDescriptorCollidingWithObject(
	ctx context.Context, txn *kv.Txn, parentID descpb.ID, parentSchemaID descpb.ID, name string,
) (catalog.Descriptor, error) {
	id, err := catkv.LookupID(ctx, txn, d.codec, parentID, parentSchemaID, name)
	if err != nil || id == descpb.InvalidID {
		return nil, err
	}
	// ID is already in use by another object.
	desc, err := catkv.MaybeGetDescriptorByID(
		ctx,
		txn,
		d.codec,
		id,
		catalog.Any,
		d.version)
	if desc == nil && err == nil {
		return nil, errors.NewAssertionErrorWithWrappedErrf(
			catalog.ErrDescriptorNotFound,
			"parentID=%d parentSchemaID=%d name=%q has ID=%d",
			parentID, parentSchemaID, name, id)
	}
	if err != nil {
		return nil, sqlerrors.WrapErrorWhileConstructingObjectAlreadyExistsErr(err)
	}
	return desc, nil
}
func (d *direct) CheckObjectCollision(
	ctx context.Context,
	txn *kv.Txn,
	parentID descpb.ID,
	parentSchemaID descpb.ID,
	name tree.ObjectName,
) error {
	desc, err := d.GetDescriptorCollidingWithObject(ctx, txn, parentID, parentSchemaID, name.Object())
	if err != nil {
		return err
	}
	if desc != nil {
		maybeQualifiedName := name.Object()
		if name.Catalog() != "" && name.Schema() != "" {
			maybeQualifiedName = name.FQString()
		}
		return sqlerrors.MakeObjectAlreadyExistsError(desc.DescriptorProto(), maybeQualifiedName)
	}
	return nil
}
func (d *direct) LookupObjectID(
	ctx context.Context, txn *kv.Txn, dbID descpb.ID, schemaID descpb.ID, objectName string,
) (descpb.ID, error) {
	return catkv.LookupID(ctx, txn, d.codec, dbID, schemaID, objectName)
}
func (d *direct) LookupSchemaID(
	ctx context.Context, txn *kv.Txn, dbID descpb.ID, schemaName string,
) (descpb.ID, error) {
	return catkv.LookupID(ctx, txn, d.codec, dbID, keys.RootNamespaceID, schemaName)
}
func (d *direct) LookupDatabaseID(
	ctx context.Context, txn *kv.Txn, dbName string,
) (descpb.ID, error) {
	return catkv.LookupID(ctx, txn, d.codec, keys.RootNamespaceID, keys.RootNamespaceID, dbName)
}
func (d *direct) WriteNewDescToBatch(
	ctx context.Context, kvTrace bool, b *kv.Batch, desc catalog.Descriptor,
) error {
	descKey := catalogkeys.MakeDescMetadataKey(d.codec, desc.GetID())
	proto := desc.DescriptorProto()
	if kvTrace {
		log.VEventf(ctx, 2, "CPut %s -> %s", descKey, proto)
	}
	b.CPut(descKey, proto, nil)
	return nil
}
