// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package descs provides abstractions for dealing with sets of descriptors.
// It is utilized during schema changes and by catalog.Accessor implementations.
package descs

import (
	"context"
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/bootstrap"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/hydrateddesccache"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/internal/catkv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/internal/validate"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/lease"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/nstree"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemadesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/iterutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// Collection is a collection of descriptors held by a single session that
// serves SQL requests, or a background job using descriptors. The
// collection is cleared using ReleaseAll() which is called at the
// end of each transaction on the session, or on hitting conditions such
// as errors, or retries that result in transaction timestamp changes.
//
// TODO(ajwerner): Remove the txn argument from the Collection by more tightly
// binding a collection to a *kv.Txn.
type Collection struct {

	// settings dictate whether we validate descriptors on write.
	settings *cluster.Settings

	// version used for validation
	version clusterversion.ClusterVersion

	// virtualSchemas optionally holds the virtual schemas.
	virtual virtualDescriptors

	// A collection of descriptors valid for the timestamp. They are released once
	// the transaction using them is complete.
	leased leasedDescriptors

	// A collection of descriptors with changes not yet committed.
	// These descriptors have been modified by the uncommitted transaction
	// affiliated with this Collection and should be written to storage
	// upon commit.
	//
	// This layer allows a transaction to see its own modifications while
	// bypassing the descriptor lease mechanism. The lease mechanism will have its
	// own transaction to read the descriptor and will hang waiting for the
	// uncommitted changes to the descriptor if this transaction is PRIORITY HIGH.
	//
	// These descriptors are local to this Collection and their state is thus
	// not visible to other transactions.
	uncommitted uncommittedDescriptors

	uncommittedComments uncommittedComments

	uncommittedZoneConfigs uncommittedZoneConfigs

	// A cached implementation of catkv.CatalogReader used for accessing stored
	// descriptors, namespace entries, comments and zone configs. The cache
	// accumulates every descriptor and other catalog data ever read from KV
	// in the transaction affiliated with this Collection.
	cr catkv.CatalogReader

	// shadowedNames is the set of name keys which should not be looked up in
	// storage. Maintaining this set is necessary to properly handle the renaming
	// of descriptors, especially when attempting to reuse an old name: without
	// this set the check that verifies that the target name is not already in
	// use would systematically fail.
	shadowedNames map[descpb.NameInfo]struct{}

	// validationLevels is the highest-known level of validation that the
	// descriptor with the corresponding ID has reached. This is used to avoid
	// redundant validation checks which can be quite expensive.
	validationLevels map[descpb.ID]catalog.ValidationLevel

	// syntheticDescriptors contains in-memory descriptors which override all
	// other matching descriptors during immutable descriptor resolution (by name
	// or by ID), but should not be written to disk. These support internal
	// queries which need to use a special modified descriptor (e.g. validating
	// non-public schema elements during a schema change).
	synthetic syntheticDescriptors

	// temporarySchemaProvider is used to access temporary schema descriptors.
	temporarySchemaProvider TemporarySchemaProvider

	// validationModeProvider is used to access the session var which determines
	// the descriptor validation mode: 'on', 'off' or 'read_only'.
	validationModeProvider DescriptorValidationModeProvider

	// hydrated is node-level cache of table descriptors which utilize
	// user-defined types.
	hydrated *hydrateddesccache.Cache

	// skipValidationOnWrite should only be set to true during forced descriptor
	// repairs.
	skipValidationOnWrite bool

	// readerCatalogSetup indicates that replicated descriptors can be modified
	// by this collection.
	readerCatalogSetup bool

	// deletedDescs that will not need to wait for new lease versions.
	deletedDescs catalog.DescriptorIDSet

	// maxTimestampBoundDeadlineHolder contains the maximum timestamp to read
	// schemas at. This is only set during the retries of bounded_staleness when
	// nearest_only=True, in which we want a schema read that should be no older
	// than MaxTimestampBound.
	maxTimestampBoundDeadlineHolder maxTimestampBoundDeadlineHolder

	// Session is a sqlliveness.Session which may be optionally set.
	// It must be set in the multi-tenant environment for ephemeral
	// SQL pods. It should not be set otherwise.
	sqlLivenessSession sqlliveness.Session

	// LeaseGeneration is the first generation value observed by this
	// txn. This guarantees the generation for long-running transactions
	// this value stays the same for the life of the transaction.
	leaseGeneration int64
}

// FromTxn is a convenience function to extract a descs.Collection which is
// being interface-smuggled through an isql.Txn. It may return nil.
func FromTxn(txn isql.Txn) *Collection {
	if g, ok := txn.(Txn); ok {
		return g.Descriptors()
	}
	return nil
}

// GetDeletedDescs returns the deleted descriptors of the collection.
func (tc *Collection) GetDeletedDescs() catalog.DescriptorIDSet {
	return tc.deletedDescs
}

// MaybeUpdateDeadline updates the deadline in a given transaction
// based on the leased descriptors in this collection. This update is
// only done when a deadline exists.
func (tc *Collection) MaybeUpdateDeadline(ctx context.Context, txn *kv.Txn) (err error) {
	return tc.leased.maybeUpdateDeadline(ctx, txn, tc.sqlLivenessSession)
}

// SetMaxTimestampBound sets the maximum timestamp to read schemas at.
func (tc *Collection) SetMaxTimestampBound(maxTimestampBound hlc.Timestamp) {
	tc.maxTimestampBoundDeadlineHolder.maxTimestampBound = maxTimestampBound
}

// ResetMaxTimestampBound resets the maximum timestamp to read schemas at.
func (tc *Collection) ResetMaxTimestampBound() {
	tc.maxTimestampBoundDeadlineHolder.maxTimestampBound = hlc.Timestamp{}
}

// GetMaxTimestampBound returns the maximum timestamp to read schemas at.
func (tc *Collection) GetMaxTimestampBound() hlc.Timestamp {
	return tc.maxTimestampBoundDeadlineHolder.maxTimestampBound
}

// SkipValidationOnWrite avoids validating stored descriptors prior to
// a transaction commit.
func (tc *Collection) SkipValidationOnWrite() {
	tc.skipValidationOnWrite = true
}

// SetReaderCatalogSetup indicates this collection is being used to
// modify reader catalogs.
func (tc *Collection) SetReaderCatalogSetup() {
	tc.readerCatalogSetup = true
}

// ReleaseSpecifiedLeases releases the leases for the descriptors with ids in
// the passed slice. Errors are logged but ignored.
func (tc *Collection) ReleaseSpecifiedLeases(ctx context.Context, descs []lease.IDVersion) {
	tc.leased.release(ctx, descs)
}

// ReleaseLeases releases all leases. Errors are logged but ignored.
func (tc *Collection) ReleaseLeases(ctx context.Context) {
	tc.leased.releaseAll(ctx)
	// Clear the associated sqlliveness.session
	tc.sqlLivenessSession = nil
	tc.leaseGeneration = 0
}

// ReleaseAll releases all state currently held by the Collection.
// ReleaseAll calls ReleaseLeases.
func (tc *Collection) ReleaseAll(ctx context.Context) {
	tc.ReleaseLeases(ctx)
	tc.ResetUncommitted(ctx)
	tc.cr.Reset(ctx)
	tc.skipValidationOnWrite = false
}

// ResetLeaseGeneration selects an initial value at the beginning of a txn
// for lease generation.
func (tc *Collection) ResetLeaseGeneration() {
	// Note: If a collection doesn't have a lease manager assigned, then
	// no generation will be selected. This can only happen with either
	// bare-bones collections or test cases.
	if tc.leased.lm != nil {
		tc.leaseGeneration = tc.leased.lm.GetLeaseGeneration()
	}
}

// GetLeaseGeneration provides an integer which will change whenever new
// descriptor versions are available. This can be used for fast comparisons
// to make sure previously looked up information is still valid.
func (tc *Collection) GetLeaseGeneration() int64 {
	// Sanity: Pick a lease generation if one hasn't been set.
	if tc.leaseGeneration == 0 {
		tc.ResetLeaseGeneration()
	}
	// Return the cached lease generation, one should have been set earlier.
	return tc.leaseGeneration
}

// HasUncommittedTables returns true if the Collection contains uncommitted
// tables.
func (tc *Collection) HasUncommittedTables() (has bool) {
	_ = tc.uncommitted.iterateUncommittedByID(func(desc catalog.Descriptor) error {
		if _, has = desc.(catalog.TableDescriptor); has {
			return iterutil.StopIteration()
		}
		return nil
	})
	return has
}

// HasUncommittedDescriptors returns true if the collection contains any
// uncommitted descriptors.
func (tc *Collection) HasUncommittedDescriptors() bool {
	return tc.uncommitted.uncommitted.Len() > 0
}

// HasUncommittedNewOrDroppedDescriptors returns true if the collection contains
// any uncommitted descriptors that are newly created or dropped.
func (tc *Collection) HasUncommittedNewOrDroppedDescriptors() bool {
	isNewDescriptor := false
	err := tc.uncommitted.iterateUncommittedByID(func(desc catalog.Descriptor) error {
		if desc.GetVersion() == 1 || desc.Dropped() {
			isNewDescriptor = true
			return iterutil.StopIteration()
		}
		return nil
	})
	if err != nil {
		return false
	}
	return isNewDescriptor
}

// HasUncommittedTypes returns true if the Collection contains uncommitted
// types.
func (tc *Collection) HasUncommittedTypes() (has bool) {
	_ = tc.uncommitted.iterateUncommittedByID(func(desc catalog.Descriptor) error {
		if _, has = desc.(catalog.TypeDescriptor); has {
			return iterutil.StopIteration()
		}
		return nil
	})
	return has
}

// AddUncommittedDescriptor adds an uncommitted descriptor modified in the
// transaction to the Collection. The descriptor must either be a new descriptor
// or carry the original version or carry the subsequent version to the original
// version.
//
// Subsequent attempts to resolve this descriptor mutably, either by name or ID
// will return this exact object. Subsequent attempts to resolve this descriptor
// immutably will return a copy of the descriptor in the current state. A deep
// copy is performed in this call. This descriptor is evicted from the name
// index of the stored descriptors layer in order for this layer to properly
// shadow it.
//
// An uncommitted descriptor cannot coexist with a synthetic descriptor with the
// same ID or the same name.
func (tc *Collection) AddUncommittedDescriptor(
	ctx context.Context, desc catalog.MutableDescriptor,
) (err error) {
	if !desc.IsUncommittedVersion() {
		return nil
	}
	defer func() {
		err = DecorateDescriptorError(desc, err)
	}()
	if tc.synthetic.getSyntheticByID(desc.GetID()) != nil {
		return errors.AssertionFailedf(
			"cannot add uncommitted %s %q (%d) when a synthetic descriptor with the same ID exists",
			desc.DescriptorType(), desc.GetName(), desc.GetID())
	}
	if tc.synthetic.getSyntheticByName(desc.GetParentID(), desc.GetParentSchemaID(), desc.GetName()) != nil {
		return errors.AssertionFailedf(
			"cannot add uncommitted %s %q (%d) when a synthetic descriptor with the same name exists",
			desc.DescriptorType(), desc.GetName(), desc.GetID())
	}
	tc.markAsShadowedName(desc.GetID())
	return tc.uncommitted.upsert(ctx, desc)
}

// WriteDescToBatch calls MaybeIncrementVersion, adds the descriptor to the
// collection as an uncommitted descriptor, and writes it into b.
func (tc *Collection) WriteDescToBatch(
	ctx context.Context, kvTrace bool, desc catalog.MutableDescriptor, b *kv.Batch,
) error {
	if desc.GetID() == descpb.InvalidID {
		return errors.AssertionFailedf("cannot write descriptor with an empty ID: %v", desc)
	}
	desc.MaybeIncrementVersion()
	// Replicated PCR descriptors cannot be modified unless the collection
	// is setup for updating them.
	if !tc.readerCatalogSetup && desc.GetReplicatedPCRVersion() != 0 {
		return pgerror.Newf(pgcode.ReadOnlySQLTransaction,
			"replicated %s %s (%d) cannot be mutated",
			desc.GetObjectTypeString(),
			desc.GetName(),
			desc.GetID())
	}
	if !tc.skipValidationOnWrite && tc.validationModeProvider.ValidateDescriptorsOnWrite() {
		if err := validate.Self(tc.version, desc); err != nil {
			return err
		}
	}
	// Retrieve the expected bytes of `desc` in storage.
	// If this is the first time we write to `desc` in the transaction, its
	// expected bytes will be retrieved when we read it into this desc.Collection,
	// and carried over in it.
	// If, however, this is not the first time we write to `desc` in the transaction,
	// which means it has existed in `tc.uncommitted`, we will retrieve the expected
	// bytes from there.
	var expected []byte
	if exist := tc.uncommitted.getUncommittedByID(desc.GetID()); exist != nil {
		expected = exist.GetRawBytesInStorage()
	} else {
		expected = desc.GetRawBytesInStorage()
	}

	if err := tc.AddUncommittedDescriptor(ctx, desc); err != nil {
		return err
	}
	descKey := catalogkeys.MakeDescMetadataKey(tc.codec(), desc.GetID())
	proto := desc.DescriptorProto()
	if kvTrace {
		log.VEventf(ctx, 2, "CPut %s -> %s", descKey, proto)
	}
	b.CPut(descKey, proto, expected)
	return nil
}

// DeleteDescToBatch adds a delete from system.descriptor to the batch.
func (tc *Collection) DeleteDescToBatch(
	ctx context.Context, kvTrace bool, id descpb.ID, b *kv.Batch,
) error {
	if id == descpb.InvalidID {
		return errors.AssertionFailedf("cannot delete descriptor with an empty ID: %v", id)
	}
	descKey := catalogkeys.MakeDescMetadataKey(tc.codec(), id)
	if kvTrace {
		log.VEventf(ctx, 2, "Del %s", descKey)
	}
	b.Del(descKey)
	tc.NotifyOfDeletedDescriptor(id)
	return nil
}

// InsertNamespaceEntryToBatch adds an insertion into system.namespace to the
// batch.
func (tc *Collection) InsertNamespaceEntryToBatch(
	ctx context.Context, kvTrace bool, e catalog.NameEntry, b *kv.Batch,
) error {
	if ns := tc.cr.Cache().LookupNamespaceEntry(catalog.MakeNameInfo(e)); ns != nil {
		tc.markAsShadowedName(ns.GetID())
	}
	tc.markAsShadowedName(e.GetID())
	if e.GetName() == "" || e.GetID() == descpb.InvalidID {
		return errors.AssertionFailedf(
			"cannot insert namespace entry (%d, %d, %q) -> %d with an empty name or ID",
			e.GetParentID(), e.GetParentSchemaID(), e.GetName(), e.GetID(),
		)
	}
	nameKey := catalogkeys.EncodeNameKey(tc.codec(), e)
	if kvTrace {
		log.VEventf(ctx, 2, "CPut %s -> %d", nameKey, e.GetID())
	}
	b.CPut(nameKey, e.GetID(), nil /* expValue */)
	return nil
}

// UpsertNamespaceEntryToBatch adds an upsert into system.namespace to the
// batch.
func (tc *Collection) UpsertNamespaceEntryToBatch(
	ctx context.Context, kvTrace bool, e catalog.NameEntry, b *kv.Batch,
) error {
	if ns := tc.cr.Cache().LookupNamespaceEntry(catalog.MakeNameInfo(e)); ns != nil {
		tc.markAsShadowedName(ns.GetID())
	}
	tc.markAsShadowedName(e.GetID())
	if e.GetName() == "" || e.GetID() == descpb.InvalidID {
		return errors.AssertionFailedf(
			"cannot upsert namespace entry (%d, %d, %q) -> %d with an empty name or ID",
			e.GetParentID(), e.GetParentSchemaID(), e.GetName(), e.GetID(),
		)
	}
	nameKey := catalogkeys.EncodeNameKey(tc.codec(), e)
	if kvTrace {
		log.VEventf(ctx, 2, "Put %s -> %d", nameKey, e.GetID())
	}
	b.Put(nameKey, e.GetID())
	return nil
}

// DeleteNamespaceEntryToBatch adds a deletion from system.namespace to the
// batch.
func (tc *Collection) DeleteNamespaceEntryToBatch(
	ctx context.Context, kvTrace bool, k catalog.NameKey, b *kv.Batch,
) error {
	if ns := tc.cr.Cache().LookupNamespaceEntry(catalog.MakeNameInfo(k)); ns != nil {
		tc.markAsShadowedName(ns.GetID())
	}
	nameKey := catalogkeys.EncodeNameKey(tc.codec(), k)
	if kvTrace {
		log.VEventf(ctx, 2, "Del %s", nameKey)
	}
	b.Del(nameKey)
	return nil
}

func (tc *Collection) markAsShadowedName(id descpb.ID) {
	desc := tc.cr.Cache().LookupDescriptor(id)
	if desc == nil {
		return
	}
	if tc.shadowedNames == nil {
		tc.shadowedNames = make(map[descpb.NameInfo]struct{})
	}
	tc.shadowedNames[descpb.NameInfo{
		ParentID:       desc.GetParentID(),
		ParentSchemaID: desc.GetParentSchemaID(),
		Name:           desc.GetName(),
	}] = struct{}{}
}

func (tc *Collection) isShadowedName(nameKey descpb.NameInfo) bool {
	_, ok := tc.shadowedNames[nameKey]
	return ok
}

// WriteCommentToBatch adds the comment changes to uncommitted layer and writes
// to the kv batch.
func (tc *Collection) WriteCommentToBatch(
	ctx context.Context, kvTrace bool, b *kv.Batch, key catalogkeys.CommentKey, cmt string,
) error {
	cmtWriter := bootstrap.MakeKVWriter(tc.codec(), systemschema.CommentsTable)
	values := []tree.Datum{
		tree.NewDInt(tree.DInt(key.CommentType)),
		tree.NewDInt(tree.DInt(key.ObjectID)),
		tree.NewDInt(tree.DInt(key.SubID)),
		tree.NewDString(cmt),
	}

	var expValues []tree.Datum
	if oldCmt, found := tc.GetComment(key); found {
		expValues = []tree.Datum{
			tree.NewDInt(tree.DInt(key.CommentType)),
			tree.NewDInt(tree.DInt(key.ObjectID)),
			tree.NewDInt(tree.DInt(key.SubID)),
			tree.NewDString(oldCmt),
		}
	}

	var err error
	if expValues == nil {
		err = cmtWriter.Insert(ctx, b, kvTrace, values...)
	} else {
		err = cmtWriter.Update(ctx, b, kvTrace, values, expValues)
	}
	if err != nil {
		return err
	}

	tc.AddUncommittedComment(key, cmt)
	return nil
}

// DeleteCommentInBatch deletes a comment with the given (objID, subID, cmtType) key in
// the same batch and marks it as deleted
func (tc *Collection) DeleteCommentInBatch(
	ctx context.Context, kvTrace bool, b *kv.Batch, key catalogkeys.CommentKey,
) error {
	cmtWriter := bootstrap.MakeKVWriter(tc.codec(), systemschema.CommentsTable)
	values := []tree.Datum{
		tree.NewDInt(tree.DInt(key.CommentType)),
		tree.NewDInt(tree.DInt(key.ObjectID)),
		tree.NewDInt(tree.DInt(key.SubID)),
		// kv delete only care about keys, so it's fine to just use an empty string
		// for comment here since it's not part of any index.
		tree.NewDString(""),
	}

	if err := cmtWriter.Delete(ctx, b, kvTrace, values...); err != nil {
		return err
	}

	tc.MarkUncommittedCommentDeleted(key)
	return nil
}

// DeleteTableComments deletes all comment on a table.
func (tc *Collection) DeleteTableComments(
	ctx context.Context, kvTrace bool, b *kv.Batch, tblID descpb.ID,
) error {
	for _, t := range catalogkeys.AllTableCommentTypes {
		cmtKeyPrefix := catalogkeys.MakeObjectCommentsMetadataPrefix(tc.codec(), t, tblID)
		b.DelRange(cmtKeyPrefix, cmtKeyPrefix.PrefixEnd(), false /* returnKeys */)
		if kvTrace {
			log.VEventf(ctx, 2, "DelRange %s", cmtKeyPrefix)
		}
	}
	tc.MarkUncommittedCommentDeletedForTable(tblID)
	return nil
}

// WriteZoneConfigToBatch adds the new zoneconfig to uncommitted layer and
// writes to the kv batch.
func (tc *Collection) WriteZoneConfigToBatch(
	ctx context.Context, kvTrace bool, b *kv.Batch, descID descpb.ID, zc catalog.ZoneConfig,
) error {
	zcWriter := bootstrap.MakeKVWriter(tc.codec(), systemschema.ZonesTable, 0 /* SkippedColumnFamilyIDs*/)
	var val roachpb.Value
	if err := val.SetProto(zc.ZoneConfigProto()); err != nil {
		return err
	}
	valBytes, err := val.GetBytes()
	if err != nil {
		return err
	}
	values := []tree.Datum{
		tree.NewDInt(tree.DInt(descID)),
		tree.NewDBytes(tree.DBytes(valBytes)),
	}

	var expValues []tree.Datum
	if zc.GetRawBytesInStorage() != nil {
		expValues = []tree.Datum{
			tree.NewDInt(tree.DInt(descID)),
			tree.NewDBytes(tree.DBytes(zc.GetRawBytesInStorage())),
		}
	}

	if expValues == nil {
		err = zcWriter.Insert(ctx, b, kvTrace, values...)
	} else {
		err = zcWriter.Update(ctx, b, kvTrace, values, expValues)
	}
	if err != nil {
		return err
	}

	return tc.AddUncommittedZoneConfig(descID, zc.ZoneConfigProto())
}

// DeleteZoneConfigInBatch deletes zone config of the table.
func (tc *Collection) DeleteZoneConfigInBatch(
	ctx context.Context, kvTrace bool, b *kv.Batch, descID descpb.ID,
) error {
	// Check if it's actually deleting something.
	zcWriter := bootstrap.MakeKVWriter(tc.codec(), systemschema.ZonesTable)
	values := []tree.Datum{
		tree.NewDInt(tree.DInt(descID)),
		tree.NewDBytes(""),
	}
	if err := zcWriter.Delete(ctx, b, kvTrace, values...); err != nil {
		return err
	}

	tc.MarkUncommittedZoneConfigDeleted(descID)
	return nil
}

// WriteDesc constructs a new Batch, calls WriteDescToBatch and runs it.
func (tc *Collection) WriteDesc(
	ctx context.Context, kvTrace bool, desc catalog.MutableDescriptor, txn *kv.Txn,
) error {
	b := txn.NewBatch()
	if err := tc.WriteDescToBatch(ctx, kvTrace, desc, b); err != nil {
		return err
	}
	return txn.Run(ctx, b)
}

// LookupDatabaseID returns the descriptor ID assigned to a database name.
func (tc *Collection) LookupDatabaseID(
	ctx context.Context, txn *kv.Txn, dbName string,
) (descpb.ID, error) {
	return tc.lookupDescriptorID(ctx, txn, descpb.NameInfo{Name: dbName})
}

// LookupSchemaID returns the descriptor ID assigned to a schema name.
func (tc *Collection) LookupSchemaID(
	ctx context.Context, txn *kv.Txn, dbID descpb.ID, schemaName string,
) (descpb.ID, error) {
	key := descpb.NameInfo{ParentID: dbID, Name: schemaName}
	return tc.lookupDescriptorID(ctx, txn, key)
}

// LookupObjectID returns the descriptor ID assigned to an object name.
func (tc *Collection) LookupObjectID(
	ctx context.Context, txn *kv.Txn, dbID descpb.ID, schemaID descpb.ID, objectName string,
) (descpb.ID, error) {
	key := descpb.NameInfo{ParentID: dbID, ParentSchemaID: schemaID, Name: objectName}
	return tc.lookupDescriptorID(ctx, txn, key)
}

// lookupDescriptorID returns the descriptor ID assigned to an object name.
func (tc *Collection) lookupDescriptorID(
	ctx context.Context, txn *kv.Txn, key descpb.NameInfo,
) (descpb.ID, error) {
	// First look up in-memory descriptors in collection,
	// except for leased descriptors.
	objInMemory, err := func() (catalog.Descriptor, error) {
		flags := defaultUnleasedFlags()
		flags.layerFilters.withoutStorage = true
		flags.descFilters.withoutDropped = true
		var db catalog.DatabaseDescriptor
		var sc catalog.SchemaDescriptor
		expectedType := catalog.Database
		if key.ParentID != descpb.InvalidID {
			var parentDescs [2]catalog.Descriptor
			var err error
			if key.ParentSchemaID != descpb.InvalidID {
				err = getDescriptorsByID(ctx, tc, txn, flags, parentDescs[:], key.ParentID, key.ParentSchemaID)
			} else {
				err = getDescriptorsByID(ctx, tc, txn, flags, parentDescs[:1], key.ParentID)
			}
			if err != nil {
				return nil, err
			}
			db, err = catalog.AsDatabaseDescriptor(parentDescs[0])
			if err != nil {
				return nil, err
			}
			expectedType = catalog.Schema
			if key.ParentSchemaID != descpb.InvalidID {
				expectedType = catalog.Any
				sc, err = catalog.AsSchemaDescriptor(parentDescs[1])
				if err != nil {
					return nil, err
				}
			}
		}
		return getDescriptorByName(ctx, txn, tc, db, sc, key.Name, flags, expectedType)
	}()
	if err != nil && !errors.Is(err, catalog.ErrDescriptorNotFound) {
		return descpb.InvalidID, err
	}
	if objInMemory != nil {
		return objInMemory.GetID(), nil
	}
	// Look up ID in storage if nothing was found in memory.
	if tc.isShadowedName(key) {
		return descpb.InvalidID, nil
	}
	read, err := tc.cr.GetByNames(ctx, txn, []descpb.NameInfo{key})
	if err != nil {
		return descpb.InvalidID, err
	}
	if e := read.LookupNamespaceEntry(key); e != nil {
		return e.GetID(), nil
	}
	return descpb.InvalidID, nil
}

// GetOriginalPreviousIDVersionsForUncommitted returns all the IDVersion
// pairs for descriptors that have undergone a schema change.
// Returns an empty slice for no schema changes.
//
// The version returned for each schema change is clusterVersion - 1, because
// that's the one that will be used when checking for table descriptor
// two-version invariance.
func (tc *Collection) GetOriginalPreviousIDVersionsForUncommitted() (
	withNewVersions []lease.IDVersion,
	err error,
) {
	err = tc.uncommitted.iterateUncommittedByID(func(uncommitted catalog.Descriptor) error {
		original := tc.uncommitted.getOriginalByID(uncommitted.GetID())
		// Ignore new descriptors.
		if original == nil {
			return nil
		}
		// Sanity checks. If AddUncommittedDescriptor is implemented and used
		// correctly then these should never fail.
		if original.GetVersion() == 0 {
			return errors.AssertionFailedf(
				"expected original version of uncommitted %s %q (%d) to be non-zero",
				uncommitted.DescriptorType(), uncommitted.GetName(), uncommitted.GetID())
		}
		if expected, actual := uncommitted.GetVersion()-1, original.GetVersion(); expected != actual {
			return errors.AssertionFailedf(
				"expected original version of uncommitted %s %q (%d) to be %d, instead is %d",
				uncommitted.DescriptorType(), uncommitted.GetName(), uncommitted.GetID(), expected, actual)
		}
		prev := lease.NewIDVersionPrev(original.GetName(), original.GetID(), original.GetVersion())
		withNewVersions = append(withNewVersions, prev)
		return nil
	})
	return withNewVersions, err
}

// GetUncommittedTables returns all the tables updated or created in the
// transaction.
func (tc *Collection) GetUncommittedTables() (tables []catalog.TableDescriptor) {
	_ = tc.uncommitted.iterateUncommittedByID(func(desc catalog.Descriptor) error {
		if table, ok := desc.(catalog.TableDescriptor); ok {
			tables = append(tables, table)
		}
		return nil
	})
	return tables
}

// GetUncommittedDatabases returns all the databases updated or created in the
// transaction.
func (tc *Collection) GetUncommittedDatabases() (databases []catalog.DatabaseDescriptor) {
	_ = tc.uncommitted.iterateUncommittedByID(func(desc catalog.Descriptor) error {
		if database, ok := desc.(catalog.DatabaseDescriptor); ok {
			databases = append(databases, database)
		}
		return nil
	})
	return databases
}

func newMutableSyntheticDescriptorAssertionError(id descpb.ID) error {
	return errors.AssertionFailedf("attempted mutable access of synthetic descriptor %d", id)
}

// GetAll returns all descriptors, namespace entries, comments and
// zone configs visible by the transaction.
func (tc *Collection) GetAll(ctx context.Context, txn *kv.Txn) (nstree.Catalog, error) {
	stored, err := tc.cr.ScanAll(ctx, txn)
	if err != nil {
		return nstree.Catalog{}, err
	}
	ret, err := tc.aggregateAllLayers(ctx, txn, stored)
	if err != nil {
		return nstree.Catalog{}, err
	}
	return ret.Catalog, nil
}

// GetDescriptorsInSpans returns all descriptors within a given span.
func (tc *Collection) GetDescriptorsInSpans(
	ctx context.Context, txn *kv.Txn, spans []roachpb.Span,
) (nstree.Catalog, error) {
	return tc.cr.ScanDescriptorsInSpans(ctx, txn, spans)
}

// GetAllComments gets all comments for all descriptors in the given database.
// This method never returns the underlying catalog, since it will be incomplete and only
// contain comments.
// If the dbContext is nil, we return the database-level comments.
func (tc *Collection) GetAllComments(
	ctx context.Context, txn *kv.Txn, db catalog.DatabaseDescriptor,
) (nstree.CommentCatalog, error) {
	kvComments, err := tc.cr.ScanAllComments(ctx, txn, db)
	if err != nil {
		return nil, err
	}
	comments, err := tc.aggregateAllLayers(ctx, txn, kvComments)
	if err != nil {
		return nil, err
	}
	return comments, nil
}

// GetAllFromStorageUnvalidated delegates to an uncached catkv.CatalogReader's
// ScanAll method. Nothing is cached, validated or hydrated. This is to be used
// sparingly and only in situations which warrant it, where an unmediated view
// of the stored catalog is explicitly desired for observability.
func (tc *Collection) GetAllFromStorageUnvalidated(
	ctx context.Context, txn *kv.Txn,
) (nstree.Catalog, error) {
	return catkv.NewUncachedCatalogReader(tc.codec()).ScanAll(ctx, txn)
}

// GetAllDatabases is like GetAll but filtered to non-dropped databases.
func (tc *Collection) GetAllDatabases(ctx context.Context, txn *kv.Txn) (nstree.Catalog, error) {
	stored, err := tc.cr.ScanNamespaceForDatabases(ctx, txn)
	if err != nil {
		return nstree.Catalog{}, err
	}
	ret, err := tc.aggregateAllLayers(ctx, txn, stored)
	if err != nil {
		return nstree.Catalog{}, err
	}
	var dbIDs catalog.DescriptorIDSet
	_ = ret.ForEachDescriptor(func(desc catalog.Descriptor) error {
		if desc.DescriptorType() != catalog.Database {
			return nil
		}
		dbIDs.Add(desc.GetID())
		return nil
	})
	return ret.FilterByIDs(dbIDs.Ordered()), nil
}

// GetAllSchemasInDatabase is like GetAll but filtered to the schemas with
// the specified parent database. Includes virtual schemas.
func (tc *Collection) GetAllSchemasInDatabase(
	ctx context.Context, txn *kv.Txn, db catalog.DatabaseDescriptor,
) (nstree.Catalog, error) {
	stored, err := tc.cr.ScanNamespaceForDatabaseSchemas(ctx, txn, db)
	if err != nil {
		return nstree.Catalog{}, err
	}
	var ret nstree.MutableCatalog
	if db.HasPublicSchemaWithDescriptor() {
		ret, err = tc.aggregateAllLayers(ctx, txn, stored)
	} else {
		ret, err = tc.aggregateAllLayers(ctx, txn, stored, schemadesc.GetPublicSchema())
	}
	if err != nil {
		return nstree.Catalog{}, err
	}
	var schemaIDs catalog.DescriptorIDSet
	_ = ret.ForEachDescriptor(func(desc catalog.Descriptor) error {
		sc, ok := desc.(catalog.SchemaDescriptor)
		if !ok {
			return nil
		}
		switch sc.SchemaKind() {
		case catalog.SchemaTemporary, catalog.SchemaUserDefined:
			if sc.GetParentID() != db.GetID() {
				return nil
			}
		}
		schemaIDs.Add(desc.GetID())
		return nil
	})
	return ret.FilterByIDs(schemaIDs.Ordered()), nil
}

// GetAllObjectsInSchema is like GetAll but filtered to the objects with
// the specified parent schema. Includes virtual objects. Does not include
// dropped objects.
func (tc *Collection) GetAllObjectsInSchema(
	ctx context.Context, txn *kv.Txn, db catalog.DatabaseDescriptor, sc catalog.SchemaDescriptor,
) (nstree.Catalog, error) {
	var ret nstree.MutableCatalog
	if sc.SchemaKind() == catalog.SchemaVirtual {
		tc.virtual.addAllToCatalog(ret)
	} else {
		stored, err := tc.cr.ScanNamespaceForSchemaObjects(ctx, txn, db, sc)
		if err != nil {
			return nstree.Catalog{}, err
		}
		ret, err = tc.aggregateAllLayers(ctx, txn, stored, sc)
		if err != nil {
			return nstree.Catalog{}, err
		}
	}
	var objectIDs catalog.DescriptorIDSet
	_ = ret.ForEachDescriptor(func(desc catalog.Descriptor) error {
		if desc.GetParentSchemaID() == sc.GetID() {
			objectIDs.Add(desc.GetID())
		}
		return nil
	})
	return ret.FilterByIDs(objectIDs.Ordered()), nil
}

// GetAllInDatabase is like the union of GetAllSchemasInDatabase and
// GetAllObjectsInSchema applied to each of those schemas.
// Includes virtual objects. Does not include dropped objects.
func (tc *Collection) GetAllInDatabase(
	ctx context.Context, txn *kv.Txn, db catalog.DatabaseDescriptor,
) (nstree.Catalog, error) {
	stored, err := tc.cr.ScanNamespaceForDatabaseSchemasAndObjects(ctx, txn, db)
	if err != nil {
		return nstree.Catalog{}, err
	}
	schemas, err := tc.GetAllSchemasInDatabase(ctx, txn, db)
	if err != nil {
		return nstree.Catalog{}, err
	}
	var schemasSlice []catalog.SchemaDescriptor
	if err := schemas.ForEachDescriptor(func(desc catalog.Descriptor) error {
		sc, err := catalog.AsSchemaDescriptor(desc)
		schemasSlice = append(schemasSlice, sc)
		return err
	}); err != nil {
		return nstree.Catalog{}, err
	}
	ret, err := tc.aggregateAllLayers(ctx, txn, stored, schemasSlice...)
	if err != nil {
		return nstree.Catalog{}, err
	}

	var inDatabaseIDs catalog.DescriptorIDSet
	if err := ret.ForEachDescriptor(func(desc catalog.Descriptor) error {
		if desc.DescriptorType() == catalog.Schema {
			if dbID := desc.GetParentID(); dbID != descpb.InvalidID && dbID != db.GetID() {
				return nil
			}
		} else {
			if schemas.LookupDescriptor(desc.GetParentSchemaID()) == nil {
				return nil
			}
		}
		inDatabaseIDs.Add(desc.GetID())
		return nil
	}); err != nil {
		return nstree.Catalog{}, err
	}

	return ret.FilterByIDs(inDatabaseIDs.Ordered()), nil
}

// GetAllTablesInDatabase is like GetAllInDatabase but filtered to tables.
// Includes virtual objects. Does not include dropped objects.
func (tc *Collection) GetAllTablesInDatabase(
	ctx context.Context, txn *kv.Txn, db catalog.DatabaseDescriptor,
) (nstree.Catalog, error) {
	stored, err := tc.cr.ScanNamespaceForDatabaseSchemasAndObjects(ctx, txn, db)
	if err != nil {
		return nstree.Catalog{}, err
	}
	var ret nstree.MutableCatalog
	if db.HasPublicSchemaWithDescriptor() {
		ret, err = tc.aggregateAllLayers(ctx, txn, stored)
	} else {
		ret, err = tc.aggregateAllLayers(ctx, txn, stored, schemadesc.GetPublicSchema())
	}
	if err != nil {
		return nstree.Catalog{}, err
	}
	var inDatabaseIDs catalog.DescriptorIDSet
	_ = ret.ForEachDescriptor(func(desc catalog.Descriptor) error {
		if desc.DescriptorType() != catalog.Table {
			return nil
		}
		if dbID := desc.GetParentID(); dbID != descpb.InvalidID && dbID != db.GetID() {
			return nil
		}
		inDatabaseIDs.Add(desc.GetID())
		return nil
	})
	return ret.FilterByIDs(inDatabaseIDs.Ordered()), nil
}

// aggregateAllLayers is the helper function used by GetAll* methods which
// takes care to stack all of the Collection's layer appropriately and ensures
// that the returned descriptors are properly hydrated and validated.
func (tc *Collection) aggregateAllLayers(
	ctx context.Context, txn *kv.Txn, stored nstree.Catalog, schemas ...catalog.SchemaDescriptor,
) (ret nstree.MutableCatalog, _ error) {
	// Descriptors need to be re-read to ensure proper validation hydration etc.
	// We collect their IDs for this purpose and we'll re-add them later.
	var descIDs catalog.DescriptorIDSet
	// Start with the known function descriptor IDs.
	for _, sc := range schemas {
		if sc.SchemaKind() == catalog.SchemaPublic {
			// This is needed at least for the temp system db during restores.
			descIDs.Add(sc.GetID())
		}
		_ = sc.ForEachFunctionSignature(func(sig descpb.SchemaDescriptor_FunctionSignature) error {
			descIDs.Add(sig.ID)
			return nil
		})
	}
	// Add IDs from descriptors retrieved from the storage layer.
	_ = stored.ForEachDescriptor(func(desc catalog.Descriptor) error {
		descIDs.Add(desc.GetID())
		if sc, ok := desc.(catalog.SchemaDescriptor); ok {
			_ = sc.ForEachFunctionSignature(func(sig descpb.SchemaDescriptor_FunctionSignature) error {
				descIDs.Add(sig.ID)
				return nil
			})
		}
		return nil
	})
	// Add stored namespace entries which are not shadowed.
	_ = stored.ForEachNamespaceEntry(func(e nstree.NamespaceEntry) error {
		if tc.isShadowedName(catalog.MakeNameInfo(e)) {
			return nil
		}
		// Temporary schemas don't have descriptors and are persisted only
		// as namespace table entries.
		if e.GetParentID() != descpb.InvalidID && e.GetParentSchemaID() == descpb.InvalidID &&
			strings.HasPrefix(e.GetName(), catconstants.PgTempSchemaName) {
			ret.UpsertDescriptor(schemadesc.NewTemporarySchema(e.GetName(), e.GetID(), e.GetParentID()))
		} else {
			descIDs.Add(e.GetID())
		}
		ret.UpsertNamespaceEntry(e, e.GetID(), e.GetMVCCTimestamp())
		return nil
	})
	// Add stored comments which are not shadowed.
	_ = stored.ForEachComment(func(key catalogkeys.CommentKey, cmt string) error {
		if _, _, isShadowed := tc.uncommittedComments.getUncommitted(key); !isShadowed {
			return ret.UpsertComment(key, cmt)
		}
		return nil
	})
	// Add stored zone configs which are not shadowed.
	_ = stored.ForEachZoneConfig(func(id descpb.ID, zc catalog.ZoneConfig) error {
		if _, isShadowed := tc.uncommittedZoneConfigs.getUncommitted(id); !isShadowed {
			ret.UpsertZoneConfig(id, zc.ZoneConfigProto(), zc.GetRawBytesInStorage())
		}
		return nil
	})
	// Add uncommitted and synthetic namespace entries from descriptors,
	// collect descriptor IDs to re-read.
	for _, iterator := range []func(func(desc catalog.Descriptor) error) error{
		tc.uncommitted.iterateUncommittedByID,
		tc.synthetic.iterateSyntheticByID,
	} {
		_ = iterator(func(desc catalog.Descriptor) error {
			descIDs.Add(desc.GetID())
			if sc, ok := desc.(catalog.SchemaDescriptor); ok {
				_ = sc.ForEachFunctionSignature(func(sig descpb.SchemaDescriptor_FunctionSignature) error {
					descIDs.Add(sig.ID)
					return nil
				})
			}
			if !desc.Dropped() && !desc.SkipNamespace() {
				ret.UpsertNamespaceEntry(desc, desc.GetID(), desc.GetModificationTime())
			}
			return nil
		})
	}
	// Add in-memory temporary schema IDs.
	if tc.temporarySchemaProvider.HasTemporarySchema() {
		tempSchemaName := tc.temporarySchemaProvider.GetTemporarySchemaName()
		descIDs.ForEach(func(maybeDatabaseID descpb.ID) {
			schemaID := tc.temporarySchemaProvider.GetTemporarySchemaIDForDB(maybeDatabaseID)
			if schemaID == descpb.InvalidID {
				return
			}
			ret.UpsertDescriptor(schemadesc.NewTemporarySchema(tempSchemaName, schemaID, maybeDatabaseID))
		})
	}
	// Add uncommitted comments and zone configs.
	if err := tc.uncommittedComments.addAllToCatalog(ret); err != nil {
		return nstree.MutableCatalog{}, err
	}
	tc.uncommittedZoneConfigs.addAllToCatalog(ret)
	// Remove deleted descriptors from consideration, re-read and add the rest.
	tc.deletedDescs.ForEach(descIDs.Remove)
	allDescs := make([]catalog.Descriptor, descIDs.Len())
	if err := getDescriptorsByID(
		ctx, tc, txn, defaultUnleasedFlags(), allDescs, descIDs.Ordered()...,
	); err != nil {
		return nstree.MutableCatalog{}, err
	}
	for _, desc := range allDescs {
		ret.UpsertDescriptor(desc)
	}
	// Add the virtual catalog.
	tc.virtual.addAllToCatalog(ret)
	return ret, nil
}

// GetAllDescriptorsForDatabase retrieves the complete set of descriptors
// in the requested database.
// Deprecated: prefer GetAllInDatabase.
func (tc *Collection) GetAllDescriptorsForDatabase(
	ctx context.Context, txn *kv.Txn, db catalog.DatabaseDescriptor,
) (nstree.Catalog, error) {
	// Re-read database descriptor to have the freshest version.
	{
		var err error
		db, err = ByIDGetter(makeGetterBase(txn, tc, defaultUnleasedFlags())).Database(ctx, db.GetID())
		if err != nil {
			return nstree.Catalog{}, err
		}
	}
	c, err := tc.GetAllInDatabase(ctx, txn, db)
	if err != nil {
		return nstree.Catalog{}, err
	}
	var ret nstree.MutableCatalog
	ret.UpsertDescriptor(db)
	_ = c.ForEachDescriptor(func(desc catalog.Descriptor) error {
		ret.UpsertDescriptor(desc)
		return nil
	})
	return ret.Catalog, nil
}

// GetAllDescriptors returns all physical descriptors visible by the
// transaction.
// Deprecated: prefer GetAll.
func (tc *Collection) GetAllDescriptors(ctx context.Context, txn *kv.Txn) (nstree.Catalog, error) {
	all, err := tc.GetAll(ctx, txn)
	if err != nil {
		return nstree.Catalog{}, err
	}
	var ret nstree.MutableCatalog
	_ = all.ForEachDescriptor(func(desc catalog.Descriptor) error {
		switch d := desc.(type) {
		case catalog.SchemaDescriptor:
			switch d.SchemaKind() {
			case catalog.SchemaPublic, catalog.SchemaVirtual, catalog.SchemaTemporary:
				return nil
			}
		case catalog.TableDescriptor:
			if d.IsVirtualTable() {
				return nil
			}
		}
		ret.UpsertDescriptor(desc)
		return nil
	})
	return ret.Catalog, nil
}

// GetAllDatabaseDescriptors returns all database descriptors visible by the
// transaction, ordered by name.
// Deprecated: prefer GetAllDatabases.
func (tc *Collection) GetAllDatabaseDescriptors(
	ctx context.Context, txn *kv.Txn,
) (ret []catalog.DatabaseDescriptor, _ error) {
	c, err := tc.GetAllDatabases(ctx, txn)
	if err != nil {
		return nil, err
	}
	// Returned slice must be ordered by name.
	if err := c.ForEachDatabaseNamespaceEntry(func(e nstree.NamespaceEntry) error {
		desc := c.LookupDescriptor(e.GetID())
		db, err := catalog.AsDatabaseDescriptor(desc)
		ret = append(ret, db)
		return err
	}); err != nil {
		return nil, err
	}
	return ret, nil
}

// GetAllDatabaseDescriptorsMap returns the results of
// GetAllDatabaseDescriptors but as a map with the database ID as the
// key.
func (tc *Collection) GetAllDatabaseDescriptorsMap(
	ctx context.Context, txn *kv.Txn,
) (map[descpb.ID]catalog.DatabaseDescriptor, error) {
	descriptors, err := tc.GetAllDatabaseDescriptors(ctx, txn)
	result := map[descpb.ID]catalog.DatabaseDescriptor{}
	if err != nil {
		return nil, err
	}

	for _, descriptor := range descriptors {
		result[descriptor.GetID()] = descriptor
	}

	return result, nil
}

// GetSchemasForDatabase returns the schemas for a given database
// visible by the transaction.
// Deprecated: prefer GetAllSchemasInDatabase.
func (tc *Collection) GetSchemasForDatabase(
	ctx context.Context, txn *kv.Txn, db catalog.DatabaseDescriptor,
) (map[descpb.ID]string, error) {
	c, err := tc.GetAllSchemasInDatabase(ctx, txn, db)
	if err != nil {
		return nil, err
	}
	ret := make(map[descpb.ID]string)
	if err := c.ForEachDescriptor(func(desc catalog.Descriptor) error {
		sc, err := catalog.AsSchemaDescriptor(desc)
		if err != nil {
			return err
		}
		if sc.SchemaKind() != catalog.SchemaVirtual {
			ret[desc.GetID()] = desc.GetName()
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return ret, nil
}

// SetSyntheticDescriptors sets the provided descriptors as the synthetic
// descriptors to override all other matching descriptors during immutable
// access. An immutable copy is made if the descriptor is mutable. See the
// documentation on syntheticDescriptors.
func (tc *Collection) SetSyntheticDescriptors(descs []catalog.Descriptor) {
	tc.ResetSyntheticDescriptors()
	for _, desc := range descs {
		tc.AddSyntheticDescriptor(desc)
	}
}

// ResetSyntheticDescriptors removes all synthetic descriptors from the
// Collection.
func (tc *Collection) ResetSyntheticDescriptors() {
	tc.synthetic.reset()
}

// ResetUncommitted resets all uncommitted state in the Collection.
func (tc *Collection) ResetUncommitted(ctx context.Context) {
	tc.uncommitted.reset(ctx)
	tc.uncommittedComments.reset()
	tc.uncommittedZoneConfigs.reset()
	tc.shadowedNames = nil
	tc.validationLevels = nil
	tc.ResetSyntheticDescriptors()
	tc.deletedDescs = catalog.DescriptorIDSet{}
}

// AddSyntheticDescriptor injects a synthetic descriptor into the Collection.
// An immutable copy is made if the descriptor is mutable.
// See the documentation on syntheticDescriptors.
func (tc *Collection) AddSyntheticDescriptor(desc catalog.Descriptor) {
	tc.synthetic.add(desc)
}

func (tc *Collection) codec() keys.SQLCodec {
	return tc.cr.Codec()
}

// NotifyOfDeletedDescriptor notifies the collection of the ID of a descriptor
// which has been deleted from the system.descriptors table. This is required to
// prevent blocking on waiting until only a single version of the descriptor is
// leased at transaction commit time.
func (tc *Collection) NotifyOfDeletedDescriptor(id descpb.ID) {
	tc.deletedDescs.Add(id)
}

// SetSession sets the sqlliveness.Session for the transaction. This
// should only be called in a multi-tenant environment.
func (tc *Collection) SetSession(session sqlliveness.Session) {
	tc.sqlLivenessSession = session
}

// SetDescriptorSessionDataProvider sets a DescriptorSessionDataProvider for
// this Collection.
func (tc *Collection) SetDescriptorSessionDataProvider(dsdp DescriptorSessionDataProvider) {
	tc.temporarySchemaProvider = dsdp
	tc.validationModeProvider = dsdp
}

// GetDatabaseComment implements the scdecomp.CommentGetter interface.
func (tc *Collection) GetDatabaseComment(dbID descpb.ID) (comment string, ok bool) {
	return tc.GetComment(catalogkeys.MakeCommentKey(uint32(dbID), 0, catalogkeys.DatabaseCommentType))
}

// GetSchemaComment implements the scdecomp.CommentGetter interface.
func (tc *Collection) GetSchemaComment(schemaID descpb.ID) (comment string, ok bool) {
	return tc.GetComment(catalogkeys.MakeCommentKey(uint32(schemaID), 0, catalogkeys.SchemaCommentType))
}

// GetTableComment implements the scdecomp.CommentGetter interface.
func (tc *Collection) GetTableComment(tableID descpb.ID) (comment string, ok bool) {
	return tc.GetComment(catalogkeys.MakeCommentKey(uint32(tableID), 0, catalogkeys.TableCommentType))
}

// GetTypeComment implements the scdecomp.CommentGetter interface.
func (tc *Collection) GetTypeComment(typeID descpb.ID) (comment string, ok bool) {
	return tc.GetComment(catalogkeys.MakeCommentKey(uint32(typeID), 0, catalogkeys.TypeCommentType))
}

// GetColumnComment implements the scdecomp.CommentGetter interface.
func (tc *Collection) GetColumnComment(
	tableID descpb.ID, pgAttrNum catid.PGAttributeNum,
) (comment string, ok bool) {
	return tc.GetComment(catalogkeys.MakeCommentKey(uint32(tableID), uint32(pgAttrNum), catalogkeys.ColumnCommentType))
}

// GetIndexComment implements the scdecomp.CommentGetter interface.
func (tc *Collection) GetIndexComment(
	tableID descpb.ID, indexID catid.IndexID,
) (comment string, ok bool) {
	return tc.GetComment(catalogkeys.MakeCommentKey(uint32(tableID), uint32(indexID), catalogkeys.IndexCommentType))
}

// MaybeSetReplicationSafeTS modifies a txn to apply the replication safe timestamp,
// if we are executing against a PCR reader catalog.
func (tc *Collection) MaybeSetReplicationSafeTS(ctx context.Context, txn *kv.Txn) error {
	now := txn.DB().Clock().Now()
	desc, err := tc.leased.lm.Acquire(ctx, now, keys.SystemDatabaseID)
	if err != nil {
		return err
	}
	defer desc.Release(ctx)

	if desc.Underlying().(catalog.DatabaseDescriptor).GetReplicatedPCRVersion() == 0 {
		return nil
	}
	return txn.SetFixedTimestamp(ctx, tc.leased.lm.GetSafeReplicationTS())
}

// GetConstraintComment implements the scdecomp.CommentGetter interface.
func (tc *Collection) GetConstraintComment(
	tableID descpb.ID, constraintID catid.ConstraintID,
) (comment string, ok bool) {
	return tc.GetComment(catalogkeys.MakeCommentKey(uint32(tableID), uint32(constraintID), catalogkeys.ConstraintCommentType))
}

// ErrDescCannotBeLeased indicates that the full Get method is needed to
// lock this descriptor. This can happen if synthetic descriptor or uncommitted
// descriptors are in play. Or if the leasing layer cannot satisfy the request.
type ErrDescCannotBeLeased struct {
	id descpb.ID
}

// Error implements error.
func (e ErrDescCannotBeLeased) Error() string {
	return fmt.Sprintf("descriptor %d cannot be leased", e.id)
}

// LockDescriptorWithLease locks a descriptor within the lease manager, where the
// lease is tied to this collection. The underlying descriptor is never returned,
// since this code path skips validation and hydration required for it to be
// usable. Returns ErrDescCannotBeLeased if a full Get method is needed to fetch
// this descriptor.
func (tc *Collection) LockDescriptorWithLease(
	ctx context.Context, txn *kv.Txn, id descpb.ID,
) (uint64, error) {
	// If synthetic descriptors or uncommitted descriptors exist, always
	// use full resolution logic.
	if tc.synthetic.descs.Len() > 0 || tc.uncommitted.uncommitted.Len() > 0 {
		return 0, ErrDescCannotBeLeased{id: id}
	}
	// Handle any virtual objects first, which the lease manager won't
	// know about.
	if _, vo := tc.virtual.getObjectByID(id); vo != nil {
		return uint64(vo.Desc().GetVersion()), nil
	}
	// Otherwise, we should be able to lease the relevant object out.
	desc, shouldReadFromStore, err := tc.leased.getByID(ctx, txn, id)
	if err != nil {
		return 0, err
	}
	// If we need to read from the store, then the descriptor was not leased
	// out.
	if shouldReadFromStore {
		return 0, ErrDescCannotBeLeased{id: id}
	}
	return uint64(desc.GetVersion()), err
}

// MakeTestCollection makes a Collection that can be used for tests.
func MakeTestCollection(
	ctx context.Context, codec keys.SQLCodec, leaseManager LeaseManager,
) Collection {
	settings := cluster.MakeTestingClusterSettings()
	return Collection{
		settings: settings,
		version:  settings.Version.ActiveVersion(ctx),
		leased:   makeLeasedDescriptors(leaseManager),
		cr:       catkv.NewUncachedCatalogReader(codec),
	}
}

// InternalExecFn is the type of functions that operates using an internalExecutor.
type InternalExecFn func(ctx context.Context, txn Txn) error

// HistoricalInternalExecTxnRunnerFn callback for executing with the internal executor
// at a fixed timestamp.
type HistoricalInternalExecTxnRunnerFn = func(ctx context.Context, fn InternalExecFn) error

// HistoricalInternalExecTxnRunner is like historicalTxnRunner except it only
// passes the fn the exported Executor instead of the whole unexported
// extendedEvalContext, so it can be implemented outside pkg/sql.
type HistoricalInternalExecTxnRunner interface {
	// Exec executes the callback at a given timestamp.
	Exec(ctx context.Context, fn InternalExecFn) error
	// ReadAsOf returns the timestamp that the historical txn executor is running
	// at.
	ReadAsOf() hlc.Timestamp
}

// historicalInternalExecTxnRunner implements HistoricalInternalExecTxnRunner.
type historicalInternalExecTxnRunner struct {
	execHistoricalTxn HistoricalInternalExecTxnRunnerFn
	readAsOf          hlc.Timestamp
}

// Exec implements HistoricalInternalExecTxnRunner.Exec.
func (ht *historicalInternalExecTxnRunner) Exec(ctx context.Context, fn InternalExecFn) error {
	return ht.execHistoricalTxn(ctx, fn)
}

// ReadAsOf implements HistoricalInternalExecTxnRunner.ReadAsOf.
func (ht *historicalInternalExecTxnRunner) ReadAsOf() hlc.Timestamp {
	return ht.readAsOf
}

// NewHistoricalInternalExecTxnRunner constructs a new historical internal
// executor.
func NewHistoricalInternalExecTxnRunner(
	readAsOf hlc.Timestamp, fn HistoricalInternalExecTxnRunnerFn,
) HistoricalInternalExecTxnRunner {
	return &historicalInternalExecTxnRunner{
		execHistoricalTxn: fn,
		readAsOf:          readAsOf,
	}
}

// DecorateDescriptorError will ensure that if we have an error we will wrap
// additional context about the descriptor to aid in debugging.
func DecorateDescriptorError(desc catalog.MutableDescriptor, err error) error {
	if err != nil {
		err = errors.Wrapf(err, "adding uncommitted %s %q (%d) version %d",
			desc.DescriptorType(), desc.GetName(), desc.GetID(), desc.GetVersion())
		// If this error doesn't have a pgerror code attached to it, lets ensure
		// that it's marked as an assertion error. This ensures it gets flagged for
		// things like sentry reports.
		if pgerror.GetPGCode(err) == pgcode.Uncategorized {
			err = errors.NewAssertionErrorWithWrappedErrf(err, "unexpected error occurred")
		}
	}
	return err
}
