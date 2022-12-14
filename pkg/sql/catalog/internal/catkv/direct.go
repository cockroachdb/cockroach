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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/internal/validate"
)

// Direct provides access to the underlying key-value store directly. A key
// difference between descriptors retrieved directly vs. descriptors retrieved
// through the Collection is that the descriptors will not be hydrated.
//
// Note: If you are tempted to use this in a place which is not currently using
// it, pause, and consider the decision very carefully.
type Direct interface {

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
}

// direct wraps a CatalogReader to implement the Direct interface.
type direct struct {
	cr      CatalogReader
	dvmp    DescriptorValidationModeProvider
	version clusterversion.ClusterVersion
}

var _ Direct = &direct{}

// MakeDirect returns an implementation of Direct.
func MakeDirect(
	codec keys.SQLCodec, version clusterversion.ClusterVersion, dvmp DescriptorValidationModeProvider,
) Direct {
	cr := NewCatalogReader(
		codec, version, nil /* maybeSystemDatabaseCache */, nil, /* maybeMonitor */
	)
	return &direct{cr: cr, dvmp: dvmp, version: version}
}

// DefaultDescriptorValidationModeProvider is the default implementation of
// DescriptorValidationModeProvider.
var DefaultDescriptorValidationModeProvider DescriptorValidationModeProvider = &defaultDescriptorValidationModeProvider{}

type defaultDescriptorValidationModeProvider struct{}

// ValidateDescriptorsOnRead implements DescriptorValidationModeProvider.
func (d *defaultDescriptorValidationModeProvider) ValidateDescriptorsOnRead() bool {
	return true
}

// ValidateDescriptorsOnWrite implements DescriptorValidationModeProvider.
func (d *defaultDescriptorValidationModeProvider) ValidateDescriptorsOnWrite() bool {
	return true
}

func (d *direct) mustGetDescriptorsByID(
	ctx context.Context, txn *kv.Txn, ids []descpb.ID, expectedType catalog.DescriptorType,
) ([]catalog.Descriptor, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	const isRequired = true
	c, err := d.cr.GetByIDs(ctx, txn, ids, isRequired, expectedType)
	if err != nil {
		return nil, err
	}
	descs := make([]catalog.Descriptor, len(ids))
	for i, id := range ids {
		descs[i] = c.LookupDescriptor(id)
	}
	if !d.dvmp.ValidateDescriptorsOnRead() {
		return descs, nil
	}
	vd := NewCatalogReaderBackedValidationDereferencer(d.cr, txn, d.dvmp)
	ve := validate.Validate(
		ctx, d.version, vd, catalog.ValidationReadTelemetry, validate.ImmutableRead, descs...,
	)
	if err := ve.CombinedError(); err != nil {
		return nil, err
	}
	return descs, nil
}

// MustGetDescriptorByID is part of the Direct interface.
func (d *direct) MustGetDescriptorByID(
	ctx context.Context, txn *kv.Txn, id descpb.ID, expectedType catalog.DescriptorType,
) (catalog.Descriptor, error) {
	descs, err := d.mustGetDescriptorsByID(ctx, txn, []descpb.ID{id}, expectedType)
	if err != nil {
		return nil, err
	}
	return descs[0], err
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
	descs, err := d.mustGetDescriptorsByID(ctx, txn, ids, catalog.Schema)
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

// LookupDescriptorID is part of the Direct interface.
func (d *direct) LookupDescriptorID(
	ctx context.Context, txn *kv.Txn, dbID descpb.ID, schemaID descpb.ID, objectName string,
) (descpb.ID, error) {
	key := descpb.NameInfo{ParentID: dbID, ParentSchemaID: schemaID, Name: objectName}
	c, err := d.cr.GetByNames(ctx, txn, []descpb.NameInfo{key})
	if err != nil {
		return descpb.InvalidID, err
	}
	if e := c.LookupNamespaceEntry(&key); e != nil {
		return e.GetID(), nil
	}
	return descpb.InvalidID, nil
}
