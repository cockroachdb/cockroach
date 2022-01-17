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

// GetCatalogUnvalidated looks up and returns all available descriptors and
// namespace system table entries but does not validate anything.
// It is exported solely to be used by functions which want to perform explicit
// validation to detect corruption.
func (tc *Collection) GetCatalogUnvalidated(
	ctx context.Context, txn *kv.Txn,
) (nstree.Catalog, error) {
	return catkv.GetCatalogUnvalidated(ctx, txn, tc.codec())
}

// MustGetDatabaseDescByID looks up the database descriptor given its ID,
// returning an error if the descriptor is not found.
func (tc *Collection) MustGetDatabaseDescByID(
	ctx context.Context, txn *kv.Txn, id descpb.ID,
) (catalog.DatabaseDescriptor, error) {
	desc, err := catkv.MustGetDescriptorByID(ctx, txn, tc.codec(), id, catalog.Database)
	if err != nil {
		return nil, err
	}
	return desc.(catalog.DatabaseDescriptor), nil
}

// MustGetSchemaDescByID looks up the schema descriptor given its ID,
// returning an error if the descriptor is not found.
func (tc *Collection) MustGetSchemaDescByID(
	ctx context.Context, txn *kv.Txn, id descpb.ID,
) (catalog.SchemaDescriptor, error) {
	desc, err := catkv.MustGetDescriptorByID(ctx, txn, tc.codec(), id, catalog.Schema)
	if err != nil {
		return nil, err
	}
	return desc.(catalog.SchemaDescriptor), nil
}

// MustGetTableDescByID looks up the table descriptor given its ID,
// returning an error if the table is not found.
func (tc *Collection) MustGetTableDescByID(
	ctx context.Context, txn *kv.Txn, id descpb.ID,
) (catalog.TableDescriptor, error) {
	desc, err := catkv.MustGetDescriptorByID(ctx, txn, tc.codec(), id, catalog.Table)
	if err != nil {
		return nil, err
	}
	return desc.(catalog.TableDescriptor), nil
}

// MustGetTypeDescByID looks up the type descriptor given its ID,
// returning an error if the type is not found.
func (tc *Collection) MustGetTypeDescByID(
	ctx context.Context, txn *kv.Txn, id descpb.ID,
) (catalog.TypeDescriptor, error) {
	desc, err := catkv.MustGetDescriptorByID(ctx, txn, tc.codec(), id, catalog.Type)
	if err != nil {
		return nil, err
	}
	return desc.(catalog.TypeDescriptor), nil
}

// GetSchemaDescriptorsFromIDs returns the schema descriptors from an input
// list of schema IDs. It will return an error if any one of the IDs is not
// a schema.
func (tc *Collection) GetSchemaDescriptorsFromIDs(
	ctx context.Context, txn *kv.Txn, ids []descpb.ID,
) ([]catalog.SchemaDescriptor, error) {
	descs, err := catkv.MustGetDescriptorsByID(ctx, txn, tc.codec(), ids, catalog.Schema)
	if err != nil {
		return nil, err
	}
	ret := make([]catalog.SchemaDescriptor, len(descs))
	for i, desc := range descs {
		ret[i] = desc.(catalog.SchemaDescriptor)
	}
	return ret, nil
}

// GetDescriptorID looks up the ID for plainKey.
// InvalidID is returned if the name cannot be resolved.
func (tc *Collection) GetDescriptorID(
	ctx context.Context, txn *kv.Txn, plainKey catalog.NameKey,
) (descpb.ID, error) {
	_, id, err := catkv.LookupID(
		ctx,
		txn,
		tc.codec(),
		plainKey.GetParentID(),
		plainKey.GetParentSchemaID(),
		plainKey.GetName(),
	)
	return id, err
}

// ResolveSchemaID resolves a schema's ID based on db and name.
func (tc *Collection) ResolveSchemaID(
	ctx context.Context, txn *kv.Txn, dbID descpb.ID, scName string,
) (bool, descpb.ID, error) {
	if !tc.settings.Version.IsActive(ctx, clusterversion.PublicSchemasWithDescriptors) {
		// Try to use the system name resolution bypass. Avoids a hotspot by explicitly
		// checking for public schema.
		if scName == tree.PublicSchema {
			return true, keys.PublicSchemaID, nil
		}
	}
	return catkv.LookupID(ctx, txn, tc.codec(), dbID, keys.RootNamespaceID, scName)
}

// GetDescriptorCollidingWithObject looks up the object ID and returns the
// corresponding descriptor if it exists.
func (tc *Collection) GetDescriptorCollidingWithObject(
	ctx context.Context, txn *kv.Txn, parentID descpb.ID, parentSchemaID descpb.ID, name string,
) (catalog.Descriptor, error) {
	found, id, err := catkv.LookupID(ctx, txn, tc.codec(), parentID, parentSchemaID, name)
	if !found || err != nil {
		return nil, err
	}
	// ID is already in use by another object.
	desc, err := catkv.MaybeGetDescriptorByID(ctx, txn, tc.codec(), id, catalog.Any)
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

// CheckObjectCollision returns an error if an object already exists with the
// same parentID, parentSchemaID and name.
func (tc *Collection) CheckObjectCollision(
	ctx context.Context,
	txn *kv.Txn,
	parentID descpb.ID,
	parentSchemaID descpb.ID,
	name tree.ObjectName,
) error {
	desc, err := tc.GetDescriptorCollidingWithObject(ctx, txn, parentID, parentSchemaID, name.Object())
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

// LookupObjectID returns the ObjectID for the given
// (parentID, parentSchemaID, name) supplied.
func (tc *Collection) LookupObjectID(
	ctx context.Context, txn *kv.Txn, parentID descpb.ID, parentSchemaID descpb.ID, name string,
) (bool, descpb.ID, error) {
	return catkv.LookupID(ctx, txn, tc.codec(), parentID, parentSchemaID, name)
}

// LookupSchemaID is a wrapper around LookupObjectID for schemas.
func (tc *Collection) LookupSchemaID(
	ctx context.Context, txn *kv.Txn, parentID descpb.ID, name string,
) (bool, descpb.ID, error) {
	return catkv.LookupID(ctx, txn, tc.codec(), parentID, keys.RootNamespaceID, name)
}

// LookupDatabaseID is a wrapper around LookupObjectID for databases.
func (tc *Collection) LookupDatabaseID(
	ctx context.Context, txn *kv.Txn, name string,
) (bool, descpb.ID, error) {
	return catkv.LookupID(ctx, txn, tc.codec(), keys.RootNamespaceID, keys.RootNamespaceID, name)
}

// WriteNewDescToBatch adds a CPut command writing a descriptor proto to the
// descriptors table. It writes the descriptor desc at the id descID, asserting
// that there was no previous descriptor at that id present already. If kvTrace
// is enabled, it will log an event explaining the CPut that was performed.
func (tc *Collection) WriteNewDescToBatch(
	ctx context.Context, kvTrace bool, b *kv.Batch, desc catalog.Descriptor,
) error {
	descKey := catalogkeys.MakeDescMetadataKey(tc.codec(), desc.GetID())
	proto := desc.DescriptorProto()
	if kvTrace {
		log.VEventf(ctx, 2, "CPut %s -> %s", descKey, proto)
	}
	b.CPut(descKey, proto, nil)
	return nil
}
