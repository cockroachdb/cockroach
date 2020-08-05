// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package catalogkv provides functions for interacting with the system catalog
// tables using the kv client.
package catalogkv

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// GenerateUniqueDescID returns the next available Descriptor ID and increments
// the counter. The incrementing is non-transactional, and the counter could be
// incremented multiple times because of retries.
func GenerateUniqueDescID(ctx context.Context, db *kv.DB, codec keys.SQLCodec) (descpb.ID, error) {
	// Increment unique descriptor counter.
	newVal, err := kv.IncrementValRetryable(ctx, db, codec.DescIDSequenceKey(), 1)
	if err != nil {
		return descpb.InvalidID, err
	}
	return descpb.ID(newVal - 1), nil
}

// GetDescriptorID looks up the ID for plainKey.
// InvalidID is returned if the name cannot be resolved.
func GetDescriptorID(
	ctx context.Context, txn *kv.Txn, codec keys.SQLCodec, plainKey sqlbase.DescriptorKey,
) (descpb.ID, error) {
	key := plainKey.Key(codec)
	log.Eventf(ctx, "looking up descriptor ID for name key %q", key)
	gr, err := txn.Get(ctx, key)
	if err != nil {
		return descpb.InvalidID, err
	}
	if !gr.Exists() {
		return descpb.InvalidID, nil
	}
	return descpb.ID(gr.ValueInt()), nil
}

// ResolveSchemaID resolves a schema's ID based on db and name.
func ResolveSchemaID(
	ctx context.Context, txn *kv.Txn, codec keys.SQLCodec, dbID descpb.ID, scName string,
) (bool, descpb.ID, error) {
	// Try to use the system name resolution bypass. Avoids a hotspot by explicitly
	// checking for public schema.
	if scName == tree.PublicSchema {
		return true, keys.PublicSchemaID, nil
	}

	sKey := sqlbase.NewSchemaKey(dbID, scName)
	schemaID, err := GetDescriptorID(ctx, txn, codec, sKey)
	if err != nil || schemaID == descpb.InvalidID {
		return false, descpb.InvalidID, err
	}

	return true, schemaID, nil
}

// TODO(ajwerner): The below flags are suspiciously similar to the flags passed
// to accessor methods. Furthermore we're pretty darn unhappy with the Accessor
// API as it provides a handle to the transaction for bad reasons.
//
// The below GetDescriptorByID function should instead get unified with the tree
// lookup flags. It then should get lifted onto an interface that becomes an
// argument into the accessor.

// Mutability indicates whether the desired descriptor is mutable. This type
// aids readability.
type Mutability bool

// Mutability values.
const (
	Immutable Mutability = false
	Mutable   Mutability = true
)

//go:generate stringer -type DescriptorKind catalogkv.go

// DescriptorKind is used to indicate the desired kind of descriptor from
// GetDescriptorByID.
type DescriptorKind int

// List of DescriptorKind values.
const (
	DatabaseDescriptorKind DescriptorKind = iota
	SchemaDescriptorKind
	TableDescriptorKind
	TypeDescriptorKind
	AnyDescriptorKind // permit any kind
)

// GetAnyDescriptorByID is a wrapper around GetDescriptorByID which permits
// missing descriptors and does not restrict the requested kind.
func GetAnyDescriptorByID(
	ctx context.Context, txn *kv.Txn, codec keys.SQLCodec, id descpb.ID, mutable Mutability,
) (catalog.Descriptor, error) {
	return GetDescriptorByID(ctx, txn, codec, id, mutable, AnyDescriptorKind, false /* required */)
}

// GetDescriptorByID looks up the descriptor for `id`. The descriptor
// will be validated if the requested descriptor is Immutable.
//
// TODO(ajwerner): Fix this odd behavior with validation which is used to hack
// around the fact that mutable descriptors are sometimes looked up while they
// are being mutated and in that period may be invalid with respect to the
// state of other descriptors in the database. Instead we ought to inject a
// higher level interface than a `txn` here for looking up other descriptors
// during validation. Ideally we'd have a handle to the transaction's
// descs.Collection and we'd maintain that when writing or retrieving
// descriptors which have been mutated we wouldn't reach back into the kv store.
func GetDescriptorByID(
	ctx context.Context,
	txn *kv.Txn,
	codec keys.SQLCodec,
	id descpb.ID,
	mutable Mutability,
	kind DescriptorKind,
	required bool,
) (catalog.Descriptor, error) {
	log.Eventf(ctx, "fetching descriptor with ID %d", id)
	descKey := sqlbase.MakeDescMetadataKey(codec, id)
	raw := &descpb.Descriptor{}
	ts, err := txn.GetProtoTs(ctx, descKey, raw)
	if err != nil {
		return nil, err
	}
	var desc catalog.Descriptor
	if mutable {
		desc, err = unwrapDescriptorMutable(ctx, txn, codec, ts, raw)
	} else {
		desc, err = unwrapDescriptor(ctx, txn, codec, ts, raw)
	}
	if err != nil {
		return nil, err
	}
	if desc == nil {
		if required {
			return nil, requiredError(kind, id)
		}
		return nil, nil
	}
	if err := desiredKindError(desc, kind, id); err != nil {
		return nil, err
	}
	return desc, nil
}

func desiredKindError(desc catalog.Descriptor, kind DescriptorKind, id descpb.ID) error {
	if kind == AnyDescriptorKind {
		return nil
	}
	var kindMismatched bool
	switch desc.(type) {
	case sqlbase.DatabaseDescriptor:
		kindMismatched = kind != DatabaseDescriptorKind
	case sqlbase.SchemaDescriptor:
		kindMismatched = kind != SchemaDescriptorKind
	case sqlbase.TableDescriptor:
		kindMismatched = kind != TableDescriptorKind
	case sqlbase.TypeDescriptor:
		kindMismatched = kind != TypeDescriptorKind
	}
	if !kindMismatched {
		return nil
	}
	return pgerror.Newf(pgcode.WrongObjectType,
		"%q with ID %d is not a %s", desc, log.Safe(id), kind.String())
}

// requiredError returns an appropriate error when a descriptor which was
// required was not found.
//
// TODO(ajwerner): This code is rather upsetting and feels like it duplicates
// some of the logic in physical_accessor.go.
func requiredError(kind DescriptorKind, id descpb.ID) error {
	var err error
	switch kind {
	case TableDescriptorKind:
		err = sqlbase.NewUndefinedRelationError(&tree.TableRef{TableID: int64(id)})
	case DatabaseDescriptorKind:
		err = sqlbase.NewUndefinedDatabaseError(fmt.Sprintf("[%d]", id))
	case SchemaDescriptorKind:
		err = sqlbase.NewUnsupportedSchemaUsageError(fmt.Sprintf("[%d]", id))
	case TypeDescriptorKind:
		err = sqlbase.NewUndefinedTypeError(tree.NewUnqualifiedTypeName(tree.Name(fmt.Sprintf("[%d]", id))))
	default:
		err = errors.Errorf("failed to find descriptor [%d]", id)
	}
	return errors.CombineErrors(sqlbase.ErrDescriptorNotFound, err)
}

// unwrapDescriptor takes a descriptor retrieved using a transaction and unwraps
// it into an immutable implementation of Descriptor. It ensures that
// the ModificationTime is set properly.
func unwrapDescriptor(
	ctx context.Context, txn *kv.Txn, codec keys.SQLCodec, ts hlc.Timestamp, desc *descpb.Descriptor,
) (catalog.Descriptor, error) {
	sqlbase.MaybeSetDescriptorModificationTimeFromMVCCTimestamp(ctx, desc, ts)
	table, database, typ, schema := sqlbase.TableFromDescriptor(desc, hlc.Timestamp{}),
		desc.GetDatabase(), desc.GetType(), desc.GetSchema()
	switch {
	case table != nil:
		immTable, err := sqlbase.NewFilledInImmutableTableDescriptor(ctx, txn, codec, table)
		if err != nil {
			return nil, err
		}
		if err := immTable.Validate(ctx, txn, codec); err != nil {
			return nil, err
		}
		return sqlbase.NewImmutableTableDescriptor(*table), nil
	case database != nil:
		dbDesc := sqlbase.NewImmutableDatabaseDescriptor(*database)
		if err := dbDesc.Validate(); err != nil {
			return nil, err
		}
		return dbDesc, nil
	case typ != nil:
		return sqlbase.NewImmutableTypeDescriptor(*typ), nil
	case schema != nil:
		return sqlbase.NewImmutableSchemaDescriptor(*schema), nil
	default:
		return nil, nil
	}
}

// unwrapDescriptorMutable takes a descriptor retrieved using a transaction and
// unwraps it into an implementation of catalog.MutableDescriptor. It ensures
// that the ModificationTime is set properly.
func unwrapDescriptorMutable(
	ctx context.Context, txn *kv.Txn, codec keys.SQLCodec, ts hlc.Timestamp, desc *descpb.Descriptor,
) (catalog.MutableDescriptor, error) {
	sqlbase.MaybeSetDescriptorModificationTimeFromMVCCTimestamp(ctx, desc, ts)
	table, database, typ, schema :=
		sqlbase.TableFromDescriptor(desc, hlc.Timestamp{}),
		desc.GetDatabase(), desc.GetType(), desc.GetSchema()
	switch {
	case table != nil:
		mutTable, err := sqlbase.NewFilledInMutableExistingTableDescriptor(ctx, txn, codec,
			false /* skipFKsWithMissingTable */, table)
		if err != nil {
			return nil, err
		}
		if err := mutTable.ValidateTable(); err != nil {
			return nil, err
		}
		return mutTable, nil
	case database != nil:
		dbDesc := sqlbase.NewMutableExistingDatabaseDescriptor(*database)
		if err := dbDesc.Validate(); err != nil {
			return nil, err
		}
		return dbDesc, nil
	case typ != nil:
		return sqlbase.NewMutableExistingTypeDescriptor(*typ), nil
	case schema != nil:
		return sqlbase.NewMutableExistingSchemaDescriptor(*schema), nil
	default:
		return nil, nil
	}
}

// CountUserDescriptors returns the number of descriptors present that were
// created by the user (i.e. not present when the cluster started).
func CountUserDescriptors(ctx context.Context, txn *kv.Txn, codec keys.SQLCodec) (int, error) {
	allDescs, err := GetAllDescriptors(ctx, txn, codec)
	if err != nil {
		return 0, err
	}

	count := 0
	for _, desc := range allDescs {
		if !sqlbase.IsDefaultCreatedDescriptor(desc.GetID()) {
			count++
		}
	}

	return count, nil
}

// GetAllDescriptors looks up and returns all available descriptors.
func GetAllDescriptors(
	ctx context.Context, txn *kv.Txn, codec keys.SQLCodec,
) ([]sqlbase.Descriptor, error) {
	log.Eventf(ctx, "fetching all descriptors")
	descsKey := sqlbase.MakeAllDescsMetadataKey(codec)
	kvs, err := txn.Scan(ctx, descsKey, descsKey.PrefixEnd(), 0)
	if err != nil {
		return nil, err
	}

	// TODO(ajwerner): Fill in ModificationTime.
	rawDescs := make([]descpb.Descriptor, len(kvs))
	descs := make([]sqlbase.Descriptor, len(kvs))
	for i, kv := range kvs {
		desc := &rawDescs[i]
		if err := kv.ValueProto(desc); err != nil {
			return nil, err
		}
		var err error
		if descs[i], err = unwrapDescriptor(ctx, txn, codec, kv.Value.Timestamp, desc); err != nil {
			return nil, err
		}
	}
	return descs, nil
}

// GetAllDatabaseDescriptorIDs looks up and returns all available database
// descriptor IDs.
func GetAllDatabaseDescriptorIDs(
	ctx context.Context, txn *kv.Txn, codec keys.SQLCodec,
) ([]descpb.ID, error) {
	log.Eventf(ctx, "fetching all database descriptor IDs")
	nameKey := sqlbase.NewDatabaseKey("" /* name */).Key(codec)
	kvs, err := txn.Scan(ctx, nameKey, nameKey.PrefixEnd(), 0 /*maxRows */)
	if err != nil {
		return nil, err
	}
	// See the comment in physical_schema_accessors.go,
	// func (a UncachedPhysicalAccessor) GetObjectNames. Same concept
	// applies here.
	// TODO(solon): This complexity can be removed in 20.2.
	nameKey = sqlbase.NewDeprecatedDatabaseKey("" /* name */).Key(codec)
	dkvs, err := txn.Scan(ctx, nameKey, nameKey.PrefixEnd(), 0 /* maxRows */)
	if err != nil {
		return nil, err
	}
	kvs = append(kvs, dkvs...)

	descIDs := make([]descpb.ID, 0, len(kvs))
	alreadySeen := make(map[descpb.ID]bool)
	for _, kv := range kvs {
		ID := descpb.ID(kv.ValueInt())
		if alreadySeen[ID] {
			continue
		}
		alreadySeen[ID] = true
		descIDs = append(descIDs, ID)
	}
	return descIDs, nil
}

// WriteDescToBatch adds a Put command writing a descriptor proto to the
// descriptors table. It writes the descriptor desc at the id descID. If kvTrace
// is enabled, it will log an event explaining the put that was performed.
func WriteDescToBatch(
	ctx context.Context,
	kvTrace bool,
	s *cluster.Settings,
	b *kv.Batch,
	codec keys.SQLCodec,
	descID descpb.ID,
	desc sqlbase.Descriptor,
) (err error) {
	descKey := sqlbase.MakeDescMetadataKey(codec, descID)
	descDesc := desc.DescriptorProto()
	if kvTrace {
		log.VEventf(ctx, 2, "Put %s -> %s", descKey, descDesc)
	}
	b.Put(descKey, descDesc)
	return nil
}

// WriteNewDescToBatch adds a CPut command writing a descriptor proto to the
// descriptors table. It writes the descriptor desc at the id descID, asserting
// that there was no previous descriptor at that id present already. If kvTrace
// is enabled, it will log an event explaining the CPut that was performed.
func WriteNewDescToBatch(
	ctx context.Context,
	kvTrace bool,
	s *cluster.Settings,
	b *kv.Batch,
	codec keys.SQLCodec,
	tableID descpb.ID,
	desc sqlbase.Descriptor,
) (err error) {
	descKey := sqlbase.MakeDescMetadataKey(codec, tableID)
	descDesc := desc.DescriptorProto()
	if kvTrace {
		log.VEventf(ctx, 2, "CPut %s -> %s", descKey, descDesc)
	}
	b.CPut(descKey, descDesc, nil)
	return nil
}

// GetDatabaseID resolves a database name into a database ID.
// Returns InvalidID on failure.
func GetDatabaseID(
	ctx context.Context, txn *kv.Txn, codec keys.SQLCodec, name string, required bool,
) (descpb.ID, error) {
	if name == sqlbase.SystemDatabaseName {
		return keys.SystemDatabaseID, nil
	}
	found, dbID, err := sqlbase.LookupDatabaseID(ctx, txn, codec, name)
	if err != nil {
		return descpb.InvalidID, err
	}
	if !found && required {
		return dbID, sqlbase.NewUndefinedDatabaseError(name)
	}
	return dbID, nil
}

// GetDatabaseDescByID looks up the database descriptor given its ID,
// returning nil if the descriptor is not found. If you want the "not
// found" condition to return an error, use mustGetDatabaseDescByID() instead.
func GetDatabaseDescByID(
	ctx context.Context, txn *kv.Txn, codec keys.SQLCodec, id descpb.ID,
) (*sqlbase.ImmutableDatabaseDescriptor, error) {
	desc, err := GetDescriptorByID(ctx, txn, codec, id, Immutable,
		DatabaseDescriptorKind, false /* required */)
	if err != nil || desc == nil {
		return nil, err
	}
	return desc.(*sqlbase.ImmutableDatabaseDescriptor), nil
}

// MustGetTableDescByID looks up the table descriptor given its ID,
// returning an error if the table is not found.
func MustGetTableDescByID(
	ctx context.Context, txn *kv.Txn, codec keys.SQLCodec, id descpb.ID,
) (*sqlbase.ImmutableTableDescriptor, error) {
	desc, err := GetDescriptorByID(ctx, txn, codec, id, Immutable,
		TableDescriptorKind, true /* required */)
	if err != nil || desc == nil {
		return nil, err
	}
	return desc.(*sqlbase.ImmutableTableDescriptor), nil
}

// MustGetDatabaseDescByID looks up the database descriptor given its ID,
// returning an error if the descriptor is not found.
func MustGetDatabaseDescByID(
	ctx context.Context, txn *kv.Txn, codec keys.SQLCodec, id descpb.ID,
) (*sqlbase.ImmutableDatabaseDescriptor, error) {
	desc, err := GetDescriptorByID(ctx, txn, codec, id, Immutable,
		DatabaseDescriptorKind, true /* required */)
	if err != nil || desc == nil {
		return nil, err
	}
	return desc.(*sqlbase.ImmutableDatabaseDescriptor), nil
}

// GetDatabaseDescriptorsFromIDs returns the database descriptors from an input
// set of database IDs. It will return an error if any one of the IDs is not a
// database. It attempts to perform this operation in a single request,
// rather than making a round trip for each ID.
func GetDatabaseDescriptorsFromIDs(
	ctx context.Context, txn *kv.Txn, codec keys.SQLCodec, ids []descpb.ID,
) ([]*sqlbase.ImmutableDatabaseDescriptor, error) {
	b := txn.NewBatch()
	for _, id := range ids {
		key := sqlbase.MakeDescMetadataKey(codec, id)
		b.Get(key)
	}
	if err := txn.Run(ctx, b); err != nil {
		return nil, err
	}
	results := make([]*sqlbase.ImmutableDatabaseDescriptor, 0, len(ids))
	for i := range b.Results {
		result := &b.Results[i]
		if result.Err != nil {
			return nil, result.Err
		}
		if len(result.Rows) != 1 {
			return nil, errors.AssertionFailedf(
				"expected one result for key %s but found %d",
				result.Keys[0],
				len(result.Rows),
			)
		}
		desc := &descpb.Descriptor{}
		if err := result.Rows[0].ValueProto(desc); err != nil {
			return nil, err
		}
		if desc.GetUnion() == nil {
			return nil, sqlbase.ErrDescriptorNotFound
		}
		db := desc.GetDatabase()
		if db == nil {
			return nil, errors.AssertionFailedf(
				"%q is not a database",
				desc.String(),
			)
		}
		sqlbase.MaybeSetDescriptorModificationTimeFromMVCCTimestamp(ctx, desc, result.Rows[0].Value.Timestamp)
		results = append(results, sqlbase.NewImmutableDatabaseDescriptor(*db))
	}
	return results, nil
}
