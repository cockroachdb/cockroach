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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/dbdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemadesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
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
	ctx context.Context, txn *kv.Txn, codec keys.SQLCodec, plainKey catalog.NameKey,
) (descpb.ID, error) {
	key := catalogkeys.EncodeNameKey(codec, plainKey)
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

	sKey := catalogkeys.NewNameKeyComponents(dbID, keys.RootNamespaceID, scName)
	schemaID, err := GetDescriptorID(ctx, txn, codec, sKey)
	if err != nil || schemaID == descpb.InvalidID {
		return false, descpb.InvalidID, err
	}

	return true, schemaID, nil
}

// NewBuilderWithMVCCTimestamp takes a descriptor as deserialized from storage,
// along with its MVCC timestamp, and returns a catalog.DescriptorBuilder object.
// Returns nil if nothing specific is found in desc.
func NewBuilderWithMVCCTimestamp(
	desc *descpb.Descriptor, mvccTimestamp hlc.Timestamp,
) catalog.DescriptorBuilder {
	table, database, typ, schema := descpb.FromDescriptorWithMVCCTimestamp(desc, mvccTimestamp)
	switch {
	case table != nil:
		return tabledesc.NewBuilder(table)
	case database != nil:
		return dbdesc.NewBuilder(database)
	case typ != nil:
		return typedesc.NewBuilder(typ)
	case schema != nil:
		return schemadesc.NewBuilder(schema)
	default:
		return nil
	}
}

// NewBuilder is a convenience function which calls NewBuilderWithMVCCTimestamp
// with an empty timestamp.
func NewBuilder(desc *descpb.Descriptor) catalog.DescriptorBuilder {
	return NewBuilderWithMVCCTimestamp(desc, hlc.Timestamp{})
}

// TODO(ajwerner): The below flags are suspiciously similar to the flags passed
// to accessor methods. Furthermore we're pretty darn unhappy with the Accessor
// API as it provides a handle to the transaction for bad reasons.
//
// The below GetDescriptorByID function should instead get unified with the tree
// lookup flags. It then should get lifted onto an interface that becomes an
// argument into the accessor.

// mutability indicates whether the desired descriptor is mutable.
// This type aids readability.
type mutability bool

// mutability values.
const (
	immutable mutability = false
	mutable   mutability = true
)

// required indicates whether the desired descriptor must be found.
// This type aids readability.
type required bool

// required values.
const (
	bestEffort required = false
	mustGet    required = true
)

// descriptorFromKeyValue unmarshals, hydrates and validates a descriptor from
// a key-value storage entry .
func descriptorFromKeyValue(
	ctx context.Context,
	codec keys.SQLCodec,
	kv kv.KeyValue,
	mutable mutability,
	expectedType catalog.DescriptorType,
	required required,
	dg catalog.DescGetter,
	validationLevel catalog.ValidationLevel,
	shouldRunPostDeserializationChanges bool,
) (catalog.Descriptor, error) {
	id, b, err := builderFromKeyValue(codec, kv, expectedType, required)
	if err != nil || b == nil {
		return nil, err
	}
	if shouldRunPostDeserializationChanges {
		err = b.RunPostDeserializationChanges(ctx, dg)
		if err != nil {
			return nil, err
		}
	}
	var desc catalog.Descriptor
	if mutable {
		desc = b.BuildExistingMutable()
	} else {
		desc = b.BuildImmutable()
	}
	if id != desc.GetID() {
		return nil, errors.AssertionFailedf("descriptor with ID %d has ID %d", id, desc.GetID())
	}
	err = catalog.Validate(ctx, dg, catalog.ValidationReadTelemetry, validationLevel, desc).CombinedError()
	if err != nil {
		return nil, err
	}
	return desc, nil
}

// builderFromKeyValue is a utility function for descriptorFromKeyValue which
// unmarshals the proto and checks that it exists and that it matches the
// expected descriptor subtype. It returns it wrapped in a DescriptorBuilder.
func builderFromKeyValue(
	codec keys.SQLCodec, kv kv.KeyValue, expectedType catalog.DescriptorType, required required,
) (descpb.ID, catalog.DescriptorBuilder, error) {
	u32ID, err := codec.DecodeDescMetadataID(kv.Key)
	if err != nil {
		return descpb.InvalidID, nil, err
	}
	id := descpb.ID(u32ID)
	var descProto descpb.Descriptor
	if err := kv.ValueProto(&descProto); err != nil {
		return id, nil, err
	}
	var ts hlc.Timestamp
	if kv.Value != nil {
		ts = kv.Value.Timestamp
	}
	b := NewBuilderWithMVCCTimestamp(&descProto, ts)
	if b == nil {
		if required {
			return id, nil, requiredError(expectedType, id)
		}
		return id, nil, nil
	}
	if expectedType != catalog.Any && b.DescriptorType() != expectedType {
		return descpb.InvalidID, nil, pgerror.Newf(pgcode.WrongObjectType,
			"descriptor with ID %d is not a %s, instead is a %s", id, expectedType, b.DescriptorType())
	}
	return id, b, nil
}

// requiredError returns an appropriate error when a descriptor which was
// required was not found.
func requiredError(expectedObjectType catalog.DescriptorType, id descpb.ID) error {
	var err error
	var wrapper func(descpb.ID, error) error
	switch expectedObjectType {
	case catalog.Table:
		err = sqlerrors.NewUndefinedRelationError(&tree.TableRef{TableID: int64(id)})
		wrapper = catalog.WrapTableDescRefErr
	case catalog.Database:
		err = sqlerrors.NewUndefinedDatabaseError(fmt.Sprintf("[%d]", id))
		wrapper = catalog.WrapDatabaseDescRefErr
	case catalog.Schema:
		err = sqlerrors.NewUndefinedSchemaError(fmt.Sprintf("[%d]", id))
		wrapper = catalog.WrapSchemaDescRefErr
	case catalog.Type:
		err = sqlerrors.NewUndefinedTypeError(tree.NewUnqualifiedTypeName(fmt.Sprintf("[%d]", id)))
		wrapper = catalog.WrapTypeDescRefErr
	default:
		err = errors.Errorf("failed to find descriptor [%d]", id)
		wrapper = func(_ descpb.ID, err error) error { return err }
	}
	return errors.CombineErrors(wrapper(id, catalog.ErrDescriptorNotFound), err)
}

// CountUserDescriptors returns the number of descriptors present that were
// created by the user (i.e. not present when the cluster started).
func CountUserDescriptors(ctx context.Context, txn *kv.Txn, codec keys.SQLCodec) (int, error) {
	allDescs, err := GetAllDescriptors(ctx, txn, codec, true /* shouldRunPostDeserializationChanges */)
	if err != nil {
		return 0, err
	}

	count := 0
	for _, desc := range allDescs {
		if !catalogkeys.IsDefaultCreatedDescriptor(desc.GetID()) {
			count++
		}
	}

	return count, nil
}

func getAllDescriptorsAndMaybeNamespaceEntriesUnvalidated(
	ctx context.Context,
	txn *kv.Txn,
	codec keys.SQLCodec,
	withNamespace bool,
	shouldRunPostDeserializationChanges bool,
) (m catalog.MapDescGetter, err error) {
	if withNamespace {
		log.Eventf(ctx, "fetching all descriptors and namespace entries")
	} else {
		log.Eventf(ctx, "fetching all descriptors")
	}
	b := txn.NewBatch()
	{ // Batch results index 0.
		descsKey := catalogkeys.MakeAllDescsMetadataKey(codec)
		b.Scan(descsKey, descsKey.PrefixEnd())
	}
	if withNamespace {
		// Batch results index 1.
		prefix := codec.IndexPrefix(
			uint32(systemschema.NamespaceTable.GetID()),
			uint32(systemschema.NamespaceTable.GetPrimaryIndexID()))
		b.Scan(prefix, prefix.PrefixEnd())
	}
	err = txn.Run(ctx, b)
	if err != nil {
		return m, err
	}
	m.Descriptors = make(map[descpb.ID]catalog.Descriptor, len(b.Results[0].Rows))
	if withNamespace {
		m.Namespace = make(map[descpb.NameInfo]descpb.ID, len(b.Results[1].Rows))
	}
	dg := NewOneLevelUncachedDescGetter(txn, codec)
	for queryIndex, results := range b.Results {
		if results.Err != nil {
			return m, results.Err
		}
		for _, row := range results.Rows {
			if !row.Exists() {
				continue
			}
			var k descpb.NameInfo
			switch queryIndex {
			case 0:
				var desc catalog.Descriptor
				desc, err = descriptorFromKeyValue(
					ctx, codec, row, immutable, catalog.Any,
					bestEffort, dg, catalog.NoValidation, shouldRunPostDeserializationChanges,
				)
				if desc != nil {
					m.Descriptors[desc.GetID()] = desc
				}
			case 1:
				k, err = catalogkeys.DecodeNameMetadataKey(codec, row.Key)
				m.Namespace[k] = descpb.ID(row.ValueInt())
			default:
				panic("missing switch case")
			}
			if err != nil {
				return m, err
			}
		}
	}
	return m, nil
}

// GetAllDescriptorsAndNamespaceEntriesUnvalidated looks up and returns all
// available descriptors and namespace entries but does not validate anything.
// It is exported solely to be used by functions which want to perform explicit
// validation to detect corruption.
func GetAllDescriptorsAndNamespaceEntriesUnvalidated(
	ctx context.Context, txn *kv.Txn, codec keys.SQLCodec,
) (m catalog.MapDescGetter, err error) {
	return getAllDescriptorsAndMaybeNamespaceEntriesUnvalidated(
		ctx,
		txn,
		codec,
		true, /* withNamespace */
		true, /* shouldRunPostDeserializationChanges */
	)
}

// GetAllDescriptors looks up and returns all available descriptors.
func GetAllDescriptors(
	ctx context.Context, txn *kv.Txn, codec keys.SQLCodec, shouldRunPostDeserializationChanges bool,
) ([]catalog.Descriptor, error) {
	m, err := getAllDescriptorsAndMaybeNamespaceEntriesUnvalidated(
		ctx,
		txn,
		codec,
		false,                               /* withNamespace */
		shouldRunPostDeserializationChanges, /* shouldRunPostDeserializationChanges */
	)
	if err != nil {
		return nil, err
	}
	descs := m.OrderedDescriptors()
	if err := catalog.ValidateSelfAndCrossReferences(ctx, m, descs...); err != nil {
		return nil, err
	}
	return descs, nil
}

// GetAllDatabaseDescriptorIDs looks up and returns all available database
// descriptor IDs.
func GetAllDatabaseDescriptorIDs(
	ctx context.Context, txn *kv.Txn, codec keys.SQLCodec,
) ([]descpb.ID, error) {
	log.Eventf(ctx, "fetching all database descriptor IDs")
	nameKey := catalogkeys.MakeDatabaseNameKey(codec, "")
	kvs, err := txn.Scan(ctx, nameKey, nameKey.PrefixEnd(), 0 /*maxRows */)
	if err != nil {
		return nil, err
	}

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
	desc catalog.Descriptor,
) (err error) {
	descKey := catalogkeys.MakeDescMetadataKey(codec, descID)
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
	desc catalog.Descriptor,
) (err error) {
	descKey := catalogkeys.MakeDescMetadataKey(codec, tableID)
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
	if name == systemschema.SystemDatabaseName {
		return keys.SystemDatabaseID, nil
	}
	found, dbID, err := LookupDatabaseID(ctx, txn, codec, name)
	if err != nil {
		return descpb.InvalidID, err
	}
	if !found && required {
		return dbID, sqlerrors.NewUndefinedDatabaseError(name)
	}
	return dbID, nil
}

// getDescriptorByID looks up the descriptor for `id` in the given `txn`.
func getDescriptorByID(
	ctx context.Context,
	txn *kv.Txn,
	codec keys.SQLCodec,
	id descpb.ID,
	mutable mutability,
	expectedType catalog.DescriptorType,
	required required,
) (desc catalog.Descriptor, err error) {
	log.Eventf(ctx, "fetching descriptor with ID %d", id)
	descKey := catalogkeys.MakeDescMetadataKey(codec, id)
	r, err := txn.Get(ctx, descKey)
	if err != nil {
		return nil, err
	}
	dg := NewOneLevelUncachedDescGetter(txn, codec)
	const level = catalog.ValidationLevelCrossReferences
	return descriptorFromKeyValue(
		ctx, codec, r, mutable, expectedType, required, dg, level, true, /* shouldRunPostDeserializationChanges */
	)
}

// GetDatabaseDescByID looks up the database descriptor given its ID,
// returning nil if the descriptor is not found. If you want the "not
// found" condition to return an error, use MustGetDatabaseDescByID instead.
func GetDatabaseDescByID(
	ctx context.Context, txn *kv.Txn, codec keys.SQLCodec, id descpb.ID,
) (catalog.DatabaseDescriptor, error) {
	desc, err := getDescriptorByID(ctx, txn, codec, id, immutable, catalog.Database, bestEffort)
	if err != nil || desc == nil {
		return nil, err
	}
	return desc.(catalog.DatabaseDescriptor), nil
}

// MustGetTableDescByID looks up the table descriptor given its ID,
// returning an error if the table is not found.
func MustGetTableDescByID(
	ctx context.Context, txn *kv.Txn, codec keys.SQLCodec, id descpb.ID,
) (catalog.TableDescriptor, error) {
	desc, err := getDescriptorByID(ctx, txn, codec, id, immutable, catalog.Table, mustGet)
	if err != nil {
		return nil, err
	}
	return desc.(catalog.TableDescriptor), nil
}

// MustGetMutableTableDescByID looks up the mutable table descriptor given its ID,
// returning an error if the table is not found.
func MustGetMutableTableDescByID(
	ctx context.Context, txn *kv.Txn, codec keys.SQLCodec, id descpb.ID,
) (*tabledesc.Mutable, error) {
	desc, err := getDescriptorByID(ctx, txn, codec, id, mutable, catalog.Table, mustGet)
	if err != nil {
		return nil, err
	}
	return desc.(*tabledesc.Mutable), nil
}

// MustGetTypeDescByID looks up the type descriptor given its ID,
// returning an error if the type is not found.
func MustGetTypeDescByID(
	ctx context.Context, txn *kv.Txn, codec keys.SQLCodec, id descpb.ID,
) (catalog.TypeDescriptor, error) {
	desc, err := getDescriptorByID(ctx, txn, codec, id, immutable, catalog.Type, mustGet)
	if err != nil {
		return nil, err
	}
	return desc.(catalog.TypeDescriptor), nil
}

// MustGetDatabaseDescByID looks up the database descriptor given its ID,
// returning an error if the descriptor is not found.
func MustGetDatabaseDescByID(
	ctx context.Context, txn *kv.Txn, codec keys.SQLCodec, id descpb.ID,
) (catalog.DatabaseDescriptor, error) {
	desc, err := getDescriptorByID(ctx, txn, codec, id, immutable, catalog.Database, mustGet)
	if err != nil {
		return nil, err
	}
	return desc.(catalog.DatabaseDescriptor), nil
}

// MustGetSchemaDescByID looks up the schema descriptor given its ID,
// returning an error if the descriptor is not found.
func MustGetSchemaDescByID(
	ctx context.Context, txn *kv.Txn, codec keys.SQLCodec, id descpb.ID,
) (catalog.SchemaDescriptor, error) {
	desc, err := getDescriptorByID(ctx, txn, codec, id, immutable, catalog.Schema, mustGet)
	if err != nil {
		return nil, err
	}
	return desc.(catalog.SchemaDescriptor), nil
}

// GetDescriptorByID looks up the descriptor given its ID,
// returning nil if the descriptor is not found. If you want the "not
// found" condition to return an error, use MustGetDescriptorByID instead.
func GetDescriptorByID(
	ctx context.Context, txn *kv.Txn, codec keys.SQLCodec, id descpb.ID,
) (catalog.Descriptor, error) {
	return getDescriptorByID(ctx, txn, codec, id, immutable, catalog.Any, bestEffort)
}

// MustGetDescriptorByID looks up the descriptor given its ID,
// returning an error if the descriptor is not found.
func MustGetDescriptorByID(
	ctx context.Context, txn *kv.Txn, codec keys.SQLCodec, id descpb.ID,
) (catalog.Descriptor, error) {
	return getDescriptorByID(ctx, txn, codec, id, immutable, catalog.Any, mustGet)
}

// GetMutableDescriptorByID looks up the mutable descriptor given its ID,
// returning nil if the descriptor is not found. If you want the "not found"
// condition to return an error, use MustGetMutableDescriptorByID instead.
func GetMutableDescriptorByID(
	ctx context.Context, txn *kv.Txn, codec keys.SQLCodec, id descpb.ID,
) (catalog.MutableDescriptor, error) {
	desc, err := getDescriptorByID(ctx, txn, codec, id, mutable, catalog.Any, bestEffort)
	if err != nil || desc == nil {
		return nil, err
	}
	return desc.(catalog.MutableDescriptor), err
}

// MustGetMutableDescriptorByID looks up the mutable descriptor given its ID,
// returning an error if the descriptor is not found.
func MustGetMutableDescriptorByID(
	ctx context.Context, txn *kv.Txn, codec keys.SQLCodec, id descpb.ID,
) (catalog.MutableDescriptor, error) {
	desc, err := getDescriptorByID(ctx, txn, codec, id, mutable, catalog.Any, mustGet)
	if err != nil {
		return nil, err
	}
	return desc.(catalog.MutableDescriptor), err
}

func getDescriptorsFromIDs(
	ctx context.Context,
	txn *kv.Txn,
	codec keys.SQLCodec,
	ids []descpb.ID,
	wrapFn func(id descpb.ID, err error) error,
) ([]catalog.Descriptor, error) {
	b := txn.NewBatch()
	for _, id := range ids {
		key := catalogkeys.MakeDescMetadataKey(codec, id)
		b.Get(key)
	}
	if err := txn.Run(ctx, b); err != nil {
		return nil, err
	}
	dg := NewOneLevelUncachedDescGetter(txn, codec)
	results := make([]catalog.Descriptor, 0, len(ids))
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
		desc, err := descriptorFromKeyValue(
			ctx,
			codec,
			result.Rows[0],
			immutable,
			catalog.Any,
			bestEffort,
			dg,
			catalog.ValidationLevelCrossReferences,
			true, /* shouldRunPostDeserializationChanges */
		)
		if err != nil {
			return nil, err
		}
		if desc == nil {
			return nil, wrapFn(ids[i], catalog.ErrDescriptorNotFound)
		}
		results = append(results, desc)
	}
	return results, nil
}

// GetDatabaseDescriptorsFromIDs returns the database descriptors from an input
// set of database IDs. It will return an error if any one of the IDs is not a
// database. It attempts to perform this operation in a single request,
// rather than making a round trip for each ID.
// If the argument allowMissingDesc is true the function will tolerate nil
// descriptors otherwise it will throw an error.
func GetDatabaseDescriptorsFromIDs(
	ctx context.Context, txn *kv.Txn, codec keys.SQLCodec, ids []descpb.ID,
) ([]catalog.DatabaseDescriptor, error) {
	descs, err := getDescriptorsFromIDs(ctx, txn, codec, ids, catalog.WrapDatabaseDescRefErr)
	if err != nil {
		return nil, err
	}
	res := make([]catalog.DatabaseDescriptor, len(descs))
	for i, id := range ids {
		desc := descs[i]
		if desc == nil {
			return nil, catalog.WrapDatabaseDescRefErr(id, catalog.ErrDescriptorNotFound)
		}
		db, ok := desc.(catalog.DatabaseDescriptor)
		if !ok {
			return nil, catalog.WrapDatabaseDescRefErr(id, catalog.NewDescriptorTypeError(desc))
		}
		res[i] = db
	}
	return res, nil
}

// GetSchemaDescriptorsFromIDs returns the schema descriptors from an input
// list of schema IDs. It will return an error if any one of the IDs is not
// a schema.
func GetSchemaDescriptorsFromIDs(
	ctx context.Context, txn *kv.Txn, codec keys.SQLCodec, ids []descpb.ID,
) ([]catalog.SchemaDescriptor, error) {
	descs, err := getDescriptorsFromIDs(ctx, txn, codec, ids, catalog.WrapSchemaDescRefErr)
	if err != nil {
		return nil, err
	}
	res := make([]catalog.SchemaDescriptor, len(descs))
	for i, id := range ids {
		desc := descs[i]
		if desc == nil {
			return nil, catalog.WrapSchemaDescRefErr(id, catalog.ErrDescriptorNotFound)
		}
		schema, ok := desc.(catalog.SchemaDescriptor)
		if !ok {
			return nil, catalog.WrapSchemaDescRefErr(id, catalog.NewDescriptorTypeError(desc))
		}
		res[i] = schema
	}
	return res, nil
}

// GetDescriptorCollidingWithObject looks up the object ID and returns the
// corresponding descriptor if it exists.
func GetDescriptorCollidingWithObject(
	ctx context.Context,
	txn *kv.Txn,
	codec keys.SQLCodec,
	parentID descpb.ID,
	parentSchemaID descpb.ID,
	name string,
) (catalog.Descriptor, error) {
	found, id, err := LookupObjectID(ctx, txn, codec, parentID, parentSchemaID, name)
	if !found || err != nil {
		return nil, err
	}
	// ID is already in use by another object.
	desc, err := GetDescriptorByID(ctx, txn, codec, id)
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
func CheckObjectCollision(
	ctx context.Context,
	txn *kv.Txn,
	codec keys.SQLCodec,
	parentID descpb.ID,
	parentSchemaID descpb.ID,
	name tree.ObjectName,
) error {
	desc, err := GetDescriptorCollidingWithObject(ctx, txn, codec, parentID, parentSchemaID, name.Object())
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
