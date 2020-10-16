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
	ctx context.Context, txn *kv.Txn, codec keys.SQLCodec, plainKey catalogkeys.DescriptorKey,
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

	sKey := catalogkeys.NewSchemaKey(dbID, scName)
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
	descKey := catalogkeys.MakeDescMetadataKey(codec, id)
	raw := &descpb.Descriptor{}
	ts, err := txn.GetProtoTs(ctx, descKey, raw)
	if err != nil {
		return nil, err
	}
	var desc catalog.Descriptor
	dg := NewOneLevelUncachedDescGetter(txn, codec)
	if mutable {
		desc, err = unwrapDescriptorMutable(ctx, dg, ts, raw)
	} else {
		desc, err = unwrapDescriptor(ctx, dg, ts, raw, true /* validate */)
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
	case catalog.DatabaseDescriptor:
		kindMismatched = kind != DatabaseDescriptorKind
	case catalog.SchemaDescriptor:
		kindMismatched = kind != SchemaDescriptorKind
	case catalog.TableDescriptor:
		kindMismatched = kind != TableDescriptorKind
	case catalog.TypeDescriptor:
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
		err = sqlerrors.NewUndefinedRelationError(&tree.TableRef{TableID: int64(id)})
	case DatabaseDescriptorKind:
		err = sqlerrors.NewUndefinedDatabaseError(fmt.Sprintf("[%d]", id))
	case SchemaDescriptorKind:
		err = sqlerrors.NewUnsupportedSchemaUsageError(fmt.Sprintf("[%d]", id))
	case TypeDescriptorKind:
		err = sqlerrors.NewUndefinedTypeError(tree.NewUnqualifiedTypeName(tree.Name(fmt.Sprintf("[%d]", id))))
	default:
		err = errors.Errorf("failed to find descriptor [%d]", id)
	}
	return errors.CombineErrors(catalog.ErrDescriptorNotFound, err)
}

// NewOneLevelUncachedDescGetter returns a new DescGetter backed by the passed
// Txn. It will use the transaction to resolve mutable descriptors using
// GetDescriptorByID but will pass a nil DescGetter into those lookup calls to
// ensure that the entire graph of dependencies is not traversed.
func NewOneLevelUncachedDescGetter(txn *kv.Txn, codec keys.SQLCodec) catalog.DescGetter {
	return &oneLevelUncachedDescGetter{
		txn:   txn,
		codec: codec,
	}
}

type oneLevelUncachedDescGetter struct {
	codec keys.SQLCodec
	txn   *kv.Txn
}

func (t *oneLevelUncachedDescGetter) GetDesc(
	ctx context.Context, id descpb.ID,
) (catalog.Descriptor, error) {
	descKey := catalogkeys.MakeDescMetadataKey(t.codec, id)
	raw := &descpb.Descriptor{}
	ts, err := t.txn.GetProtoTs(ctx, descKey, raw)
	if err != nil {
		return nil, err
	}
	// This mutable unwrapping with a nil desc-getter will avoid doing anything
	// crazy.
	return unwrapDescriptorMutable(ctx, nil, ts, raw)
}

func (t *oneLevelUncachedDescGetter) GetDescs(
	ctx context.Context, reqs []descpb.ID,
) ([]catalog.Descriptor, error) {
	ba := t.txn.NewBatch()
	for _, id := range reqs {
		descKey := catalogkeys.MakeDescMetadataKey(t.codec, id)
		ba.Get(descKey)
	}
	if err := t.txn.Run(ctx, ba); err != nil {
		return nil, err
	}
	ret := make([]catalog.Descriptor, len(reqs))
	for i, res := range ba.Results {
		var desc descpb.Descriptor
		if err := res.Rows[0].ValueProto(&desc); err != nil {
			return nil, err
		}
		if desc != (descpb.Descriptor{}) {
			unwrapped, err := unwrapDescriptorMutable(ctx, nil, res.Rows[0].Value.Timestamp, &desc)
			if err != nil {
				return nil, err
			}
			ret[i] = unwrapped
		}

	}
	return ret, nil

}

var _ catalog.DescGetter = (*oneLevelUncachedDescGetter)(nil)

// unwrapDescriptor takes a descriptor retrieved using a transaction and unwraps
// it into an immutable implementation of Descriptor. It ensures that
// the ModificationTime is set properly and will validate the descriptor if
// validate is true.
func unwrapDescriptor(
	ctx context.Context,
	dg catalog.DescGetter,
	ts hlc.Timestamp,
	desc *descpb.Descriptor,
	validate bool,
) (catalog.Descriptor, error) {
	descpb.MaybeSetDescriptorModificationTimeFromMVCCTimestamp(ctx, desc, ts)
	table, database, typ, schema := descpb.TableFromDescriptor(desc, hlc.Timestamp{}),
		desc.GetDatabase(), desc.GetType(), desc.GetSchema()
	switch {
	case table != nil:
		immTable, err := tabledesc.NewFilledInImmutable(ctx, dg, table)
		if err != nil {
			return nil, err
		}
		if validate {
			if err := immTable.Validate(ctx, dg); err != nil {
				return nil, err
			}
		}
		return tabledesc.NewImmutable(*table), nil
	case database != nil:
		dbDesc := dbdesc.NewImmutable(*database)
		if validate {
			if err := dbDesc.Validate(); err != nil {
				return nil, err
			}
		}
		return dbDesc, nil
	case typ != nil:
		return typedesc.NewImmutable(*typ), nil
	case schema != nil:
		return schemadesc.NewImmutable(*schema), nil
	default:
		return nil, nil
	}
}

// unwrapDescriptorMutable takes a descriptor retrieved using a transaction and
// unwraps it into an implementation of catalog.MutableDescriptor. It ensures
// that the ModificationTime is set properly.
func unwrapDescriptorMutable(
	ctx context.Context, dg catalog.DescGetter, ts hlc.Timestamp, desc *descpb.Descriptor,
) (catalog.MutableDescriptor, error) {
	descpb.MaybeSetDescriptorModificationTimeFromMVCCTimestamp(ctx, desc, ts)
	table, database, typ, schema :=
		descpb.TableFromDescriptor(desc, hlc.Timestamp{}),
		desc.GetDatabase(), desc.GetType(), desc.GetSchema()
	switch {
	case table != nil:
		mutTable, err := tabledesc.NewFilledInExistingMutable(ctx, dg, false /* skipFKsWithMissingTable */, table)
		if err != nil {
			return nil, err
		}
		if err := mutTable.ValidateTable(ctx); err != nil {
			return nil, err
		}
		return mutTable, nil
	case database != nil:
		dbDesc := dbdesc.NewExistingMutable(*database)
		if err := dbDesc.Validate(); err != nil {
			return nil, err
		}
		return dbDesc, nil
	case typ != nil:
		return typedesc.NewExistingMutable(*typ), nil
	case schema != nil:
		return schemadesc.NewMutableExisting(*schema), nil
	default:
		return nil, nil
	}
}

// CountUserDescriptors returns the number of descriptors present that were
// created by the user (i.e. not present when the cluster started).
func CountUserDescriptors(ctx context.Context, txn *kv.Txn, codec keys.SQLCodec) (int, error) {
	allDescs, err := GetAllDescriptors(ctx, txn, codec, true /* validate */)
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

// GetAllDescriptors looks up and returns all available descriptors. If validate
// is set to true, it will also validate them.
func GetAllDescriptors(
	ctx context.Context, txn *kv.Txn, codec keys.SQLCodec, validate bool,
) ([]catalog.Descriptor, error) {
	log.Eventf(ctx, "fetching all descriptors")
	descsKey := catalogkeys.MakeAllDescsMetadataKey(codec)
	kvs, err := txn.Scan(ctx, descsKey, descsKey.PrefixEnd(), 0)
	if err != nil {
		return nil, err
	}

	rawDescs := make([]descpb.Descriptor, len(kvs))
	descs := make([]catalog.Descriptor, len(kvs))
	dg := NewOneLevelUncachedDescGetter(txn, codec)
	for i, kv := range kvs {
		desc := &rawDescs[i]
		if err := kv.ValueProto(desc); err != nil {
			return nil, err
		}
		var err error
		if descs[i], err = unwrapDescriptor(ctx, dg, kv.Value.Timestamp, desc, validate); err != nil {
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
	nameKey := catalogkeys.NewDatabaseKey("" /* name */).Key(codec)
	kvs, err := txn.Scan(ctx, nameKey, nameKey.PrefixEnd(), 0 /*maxRows */)
	if err != nil {
		return nil, err
	}
	// See the comment in physical_schema_accessors.go,
	// func (a UncachedPhysicalAccessor) GetObjectNames. Same concept
	// applies here.
	// TODO(solon): This complexity can be removed in 20.2.
	nameKey = catalogkeys.NewDeprecatedDatabaseKey("" /* name */).Key(codec)
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

// GetDatabaseDescByID looks up the database descriptor given its ID,
// returning nil if the descriptor is not found. If you want the "not
// found" condition to return an error, use mustGetDatabaseDescByID() instead.
func GetDatabaseDescByID(
	ctx context.Context, txn *kv.Txn, codec keys.SQLCodec, id descpb.ID,
) (*dbdesc.Immutable, error) {
	desc, err := GetDescriptorByID(ctx, txn, codec, id, Immutable,
		DatabaseDescriptorKind, false /* required */)
	if err != nil || desc == nil {
		return nil, err
	}
	return desc.(*dbdesc.Immutable), nil
}

// MustGetTableDescByID looks up the table descriptor given its ID,
// returning an error if the table is not found.
func MustGetTableDescByID(
	ctx context.Context, txn *kv.Txn, codec keys.SQLCodec, id descpb.ID,
) (*tabledesc.Immutable, error) {
	desc, err := GetDescriptorByID(ctx, txn, codec, id, Immutable,
		TableDescriptorKind, true /* required */)
	if err != nil || desc == nil {
		return nil, err
	}
	return desc.(*tabledesc.Immutable), nil
}

// MustGetDatabaseDescByID looks up the database descriptor given its ID,
// returning an error if the descriptor is not found.
func MustGetDatabaseDescByID(
	ctx context.Context, txn *kv.Txn, codec keys.SQLCodec, id descpb.ID,
) (*dbdesc.Immutable, error) {
	desc, err := GetDescriptorByID(ctx, txn, codec, id, Immutable,
		DatabaseDescriptorKind, true /* required */)
	if err != nil {
		return nil, err
	}
	return desc.(*dbdesc.Immutable), nil
}

// MustGetSchemaDescByID looks up the schema descriptor given its ID,
// returning an error if the descriptor is not found.
func MustGetSchemaDescByID(
	ctx context.Context, txn *kv.Txn, codec keys.SQLCodec, id descpb.ID,
) (*schemadesc.Immutable, error) {
	desc, err := GetDescriptorByID(ctx, txn, codec, id, Immutable,
		SchemaDescriptorKind, true /* required */)
	if err != nil {
		return nil, err
	}
	sc, ok := desc.(*schemadesc.Immutable)
	if !ok {
		return nil, errors.Newf("descriptor with id %d was not a schema", id)
	}
	return sc, nil
}

func getDescriptorsFromIDs(
	ctx context.Context, txn *kv.Txn, codec keys.SQLCodec, ids []descpb.ID, allowMissingDesc bool,
) ([]catalog.Descriptor, error) {
	b := txn.NewBatch()
	for _, id := range ids {
		key := catalogkeys.MakeDescMetadataKey(codec, id)
		b.Get(key)
	}
	if err := txn.Run(ctx, b); err != nil {
		return nil, err
	}
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
		desc := &descpb.Descriptor{}
		if err := result.Rows[0].ValueProto(desc); err != nil {
			return nil, err
		}

		var catalogDesc catalog.Descriptor
		if desc.Union != nil {
			var err error
			catalogDesc, err = unwrapDescriptor(ctx, nil /* descGetter */, result.Rows[0].Value.Timestamp, desc, true)
			if err != nil {
				return nil, err
			}
		}

		if catalogDesc == nil && !allowMissingDesc {
			return nil, catalog.ErrDescriptorNotFound
		}
		results = append(results, catalogDesc)
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
	ctx context.Context, txn *kv.Txn, codec keys.SQLCodec, ids []descpb.ID, allowMissingDesc bool,
) ([]*dbdesc.Immutable, error) {
	descs, err := getDescriptorsFromIDs(ctx, txn, codec, ids, allowMissingDesc)
	if err != nil {
		return nil, err
	}
	res := make([]*dbdesc.Immutable, len(descs))
	for i := range descs {
		desc := descs[i]
		if desc == nil {
			if allowMissingDesc {
				continue
			}
			return nil, catalog.ErrDescriptorNotFound
		}
		db, ok := desc.(*dbdesc.Immutable)
		if !ok {
			return nil, errors.AssertionFailedf("%q is not a database", desc.GetName())
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
) ([]*schemadesc.Immutable, error) {
	descs, err := getDescriptorsFromIDs(ctx, txn, codec, ids, false /* allowMissingDesc */)
	if err != nil {
		return nil, err
	}
	res := make([]*schemadesc.Immutable, len(descs))
	for i := range descs {
		desc := descs[i]
		schema, ok := desc.(*schemadesc.Immutable)
		if !ok {
			return nil, errors.AssertionFailedf("%q is not a schema", desc.GetName())
		}
		res[i] = schema
	}
	return res, nil
}

// UnwrapDescriptorRaw takes a descriptor retrieved from a backup manifest or
// as input to the sql doctor and constructs the appropriate MutableDescriptor
// object implied by that object. It assumes and will panic if the
// ModificationTime for the descriptors are already set.
//
// TODO(ajwerner): This may prove problematic for backups of database
// descriptors without modification time.
//
// TODO(ajwerner): unify this with the other unwrapping logic.
func UnwrapDescriptorRaw(ctx context.Context, desc *descpb.Descriptor) catalog.MutableDescriptor {
	descpb.MaybeSetDescriptorModificationTimeFromMVCCTimestamp(ctx, desc, hlc.Timestamp{})
	table, database, typ, schema := descpb.TableFromDescriptor(desc, hlc.Timestamp{}),
		desc.GetDatabase(), desc.GetType(), desc.GetSchema()
	switch {
	case table != nil:
		return tabledesc.NewExistingMutable(*table)
	case database != nil:
		return dbdesc.NewExistingMutable(*database)
	case typ != nil:
		return typedesc.NewExistingMutable(*typ)
	case schema != nil:
		return schemadesc.NewMutableExisting(*schema)
	default:
		log.Fatalf(ctx, "failed to unwrap descriptor of type %T", desc.Union)
		return nil // unreachable
	}
}
