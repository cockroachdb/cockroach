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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/internal/validate"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/nstree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

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

	// MaybeGetDescriptorByIDUnvalidated looks up the descriptor given its ID if
	// it exists. No attempt is made at validation.
	MaybeGetDescriptorByIDUnvalidated(
		ctx context.Context, txn *kv.Txn, id descpb.ID,
	) (catalog.Descriptor, error)

	// MustGetDescriptorByID looks up the descriptor given its ID and expected
	// type, returning an error if the descriptor is not found or of the wrong
	// type.
	MustGetDescriptorByID(
		ctx context.Context, txn *kv.Txn, id descpb.ID, expectedType catalog.DescriptorType,
	) (catalog.Descriptor, error)

	// LookupDescriptorID looks up the descriptor ID given its name key, returning
	// 0 if the selected entry was not found in the namespace table.
	LookupDescriptorID(
		ctx context.Context, txn *kv.Txn, parentID, parentSchemaID descpb.ID, name string,
	) (descpb.ID, error)

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

// direct wraps a StoredCatalog to implement the Direct interface.
type direct struct {
	StoredCatalog
	version clusterversion.ClusterVersion
}

var _ Direct = &direct{}

// MakeDirect returns an implementation of Direct.
func MakeDirect(codec keys.SQLCodec, version clusterversion.ClusterVersion) Direct {
	return &direct{
		StoredCatalog: StoredCatalog{
			CatalogReader: NewUncachedCatalogReader(codec),
		},
		version: version,
	}
}

// MaybeGetDescriptorByIDUnvalidated is part of the Direct interface.
func (d *direct) MaybeGetDescriptorByIDUnvalidated(
	ctx context.Context, txn *kv.Txn, id descpb.ID,
) (catalog.Descriptor, error) {
	const isNotRequired = false
	descs, err := d.readDescriptorsForDirectAccess(ctx, txn, []descpb.ID{id}, isNotRequired, catalog.Any)
	if err != nil {
		return nil, err
	}
	return descs[0], nil
}

// MustGetDescriptorsByID is part of the Direct interface.
func (d *direct) MustGetDescriptorsByID(
	ctx context.Context, txn *kv.Txn, ids []descpb.ID, expectedType catalog.DescriptorType,
) ([]catalog.Descriptor, error) {
	const isRequired = true
	descs, err := d.readDescriptorsForDirectAccess(ctx, txn, ids, isRequired, expectedType)
	if err != nil {
		return nil, err
	}
	vd := d.NewValidationDereferencer(txn)
	ve := validate.Validate(ctx, d.version, vd, catalog.ValidationReadTelemetry, validate.ImmutableRead, descs...)
	if err := ve.CombinedError(); err != nil {
		return nil, err
	}
	return descs, nil
}

// MustGetDescriptorByID is part of the Direct interface.
func (d *direct) MustGetDescriptorByID(
	ctx context.Context, txn *kv.Txn, id descpb.ID, expectedType catalog.DescriptorType,
) (catalog.Descriptor, error) {
	descs, err := d.MustGetDescriptorsByID(ctx, txn, []descpb.ID{id}, expectedType)
	if err != nil {
		return nil, err
	}
	return descs[0], err
}

func (d *direct) readDescriptorsForDirectAccess(
	ctx context.Context,
	txn *kv.Txn,
	ids []descpb.ID,
	isRequired bool,
	expectedType catalog.DescriptorType,
) ([]catalog.Descriptor, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	c, err := d.GetDescriptorEntries(ctx, txn, ids, isRequired, expectedType)
	if err != nil {
		return nil, err
	}
	descs := make([]catalog.Descriptor, len(ids))
	for i, id := range ids {
		desc := c.LookupDescriptorEntry(id)
		if desc == nil {
			continue
		}
		if err := d.ensure(ctx, desc); err != nil {
			return nil, err
		}
		descs[i] = desc
	}
	return descs, nil
}

// GetCatalogUnvalidated is part of the Direct interface.
func (d *direct) GetCatalogUnvalidated(ctx context.Context, txn *kv.Txn) (nstree.Catalog, error) {
	return d.ScanAll(ctx, txn)
}

// MustGetDatabaseDescByID is part of the Direct interface.
func (d *direct) MustGetDatabaseDescByID(
	ctx context.Context, txn *kv.Txn, id descpb.ID,
) (catalog.DatabaseDescriptor, error) {
	desc, err := d.MustGetDescriptorByID(ctx, txn, id, catalog.Database)
	if err != nil {
		return nil, err
	}
	return desc.(catalog.DatabaseDescriptor), nil
}

// MustGetSchemaDescByID is part of the Direct interface.
func (d *direct) MustGetSchemaDescByID(
	ctx context.Context, txn *kv.Txn, id descpb.ID,
) (catalog.SchemaDescriptor, error) {
	desc, err := d.MustGetDescriptorByID(ctx, txn, id, catalog.Schema)
	if err != nil {
		return nil, err
	}
	return desc.(catalog.SchemaDescriptor), nil
}

// MustGetTableDescByID is part of the Direct interface.
func (d *direct) MustGetTableDescByID(
	ctx context.Context, txn *kv.Txn, id descpb.ID,
) (catalog.TableDescriptor, error) {
	desc, err := d.MustGetDescriptorByID(ctx, txn, id, catalog.Table)
	if err != nil {
		return nil, err
	}
	return desc.(catalog.TableDescriptor), nil
}

// MustGetTypeDescByID is part of the Direct interface.
func (d *direct) MustGetTypeDescByID(
	ctx context.Context, txn *kv.Txn, id descpb.ID,
) (catalog.TypeDescriptor, error) {
	desc, err := d.MustGetDescriptorByID(ctx, txn, id, catalog.Type)
	if err != nil {
		return nil, err
	}
	return desc.(catalog.TypeDescriptor), nil
}

// GetSchemaDescriptorsFromIDs is part of the Direct interface.
func (d *direct) GetSchemaDescriptorsFromIDs(
	ctx context.Context, txn *kv.Txn, ids []descpb.ID,
) ([]catalog.SchemaDescriptor, error) {
	descs, err := d.MustGetDescriptorsByID(ctx, txn, ids, catalog.Schema)
	if err != nil {
		return nil, err
	}
	ret := make([]catalog.SchemaDescriptor, len(descs))
	for i, desc := range descs {
		ret[i] = desc.(catalog.SchemaDescriptor)
	}
	return ret, nil
}

// ResolveSchemaID is part of the Direct interface.
func (d *direct) ResolveSchemaID(
	ctx context.Context, txn *kv.Txn, dbID descpb.ID, scName string,
) (descpb.ID, error) {
	return d.LookupDescriptorID(ctx, txn, dbID, keys.RootNamespaceID, scName)
}

// GetDescriptorCollidingWithObject is part of the Direct interface.
func (d *direct) GetDescriptorCollidingWithObject(
	ctx context.Context, txn *kv.Txn, parentID descpb.ID, parentSchemaID descpb.ID, name string,
) (catalog.Descriptor, error) {
	id, err := d.LookupDescriptorID(ctx, txn, parentID, parentSchemaID, name)
	if err != nil || id == descpb.InvalidID {
		return nil, err
	}
	// ID is already in use by another object.
	// Look it up without any validation to make sure the error returned is not a
	// validation error.
	if unvalidated, err := d.MaybeGetDescriptorByIDUnvalidated(ctx, txn, id); err != nil {
		return nil, sqlerrors.WrapErrorWhileConstructingObjectAlreadyExistsErr(err)
	} else if unvalidated == nil {
		return nil, errors.NewAssertionErrorWithWrappedErrf(
			catalog.ErrDescriptorNotFound,
			"parentID=%d parentSchemaID=%d name=%q has ID=%d",
			parentID, parentSchemaID, name, id)
	}
	// Look up and return the colliding object. This should already be in the
	// cache.
	return d.MustGetDescriptorByID(ctx, txn, id, catalog.Any)
}

// CheckObjectCollision is part of the Direct interface.
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

// LookupObjectID is part of the Direct interface.
func (d *direct) LookupObjectID(
	ctx context.Context, txn *kv.Txn, dbID descpb.ID, schemaID descpb.ID, objectName string,
) (descpb.ID, error) {
	return d.LookupDescriptorID(ctx, txn, dbID, schemaID, objectName)
}

// LookupSchemaID is part of the Direct interface.
func (d *direct) LookupSchemaID(
	ctx context.Context, txn *kv.Txn, dbID descpb.ID, schemaName string,
) (descpb.ID, error) {
	return d.LookupDescriptorID(ctx, txn, dbID, keys.RootNamespaceID, schemaName)
}

// LookupDatabaseID is part of the Direct interface.
func (d *direct) LookupDatabaseID(
	ctx context.Context, txn *kv.Txn, dbName string,
) (descpb.ID, error) {
	return d.LookupDescriptorID(ctx, txn, keys.RootNamespaceID, keys.RootNamespaceID, dbName)
}

// WriteNewDescToBatch is part of the Direct interface.
func (d *direct) WriteNewDescToBatch(
	ctx context.Context, kvTrace bool, b *kv.Batch, desc catalog.Descriptor,
) error {
	descKey := catalogkeys.MakeDescMetadataKey(d.Codec(), desc.GetID())
	proto := desc.DescriptorProto()
	if kvTrace {
		log.VEventf(ctx, 2, "CPut %s -> %s", descKey, proto)
	}
	b.CPut(descKey, proto, nil)
	return nil
}
