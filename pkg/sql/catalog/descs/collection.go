// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package descs provides abstractions for dealing with sets of descriptors.
// It is utilized during schema changes and by catalog.Accessor implementations.
package descs

import (
	"context"
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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/hydrateddesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/internal/catkv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/internal/validate"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/lease"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/nstree"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
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

	// A collection of descriptors which mirrors the descriptors committed to
	// storage. This acts as a cache by accumulating every descriptor ever read
	// from KV in the transaction affiliated with this Collection.
	stored catkv.StoredCatalog

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
	hydrated *hydrateddesc.Cache

	// skipValidationOnWrite should only be set to true during forced descriptor
	// repairs.
	skipValidationOnWrite bool

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
}

var _ catalog.Accessor = (*Collection)(nil)

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

// SkipValidationOnWrite avoids validating stored descriptors prior to
// a transaction commit.
func (tc *Collection) SkipValidationOnWrite() {
	tc.skipValidationOnWrite = true
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
}

// ReleaseAll releases all state currently held by the Collection.
// ReleaseAll calls ReleaseLeases.
func (tc *Collection) ReleaseAll(ctx context.Context) {
	tc.ReleaseLeases(ctx)
	tc.uncommitted.reset(ctx)
	tc.uncommittedComments.reset()
	tc.uncommittedZoneConfigs.reset()
	tc.stored.Reset(ctx)
	tc.ResetSyntheticDescriptors()
	tc.deletedDescs = catalog.DescriptorIDSet{}
	tc.skipValidationOnWrite = false
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
		if err != nil {
			err = errors.NewAssertionErrorWithWrappedErrf(err, "adding uncommitted %s %q (%d) version %d",
				desc.DescriptorType(), desc.GetName(), desc.GetID(), desc.GetVersion())
		}
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
	tc.stored.RemoveFromNameIndex(desc.GetID())
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
	if cachedID := tc.stored.GetCachedIDByName(e); cachedID != descpb.InvalidID {
		tc.stored.RemoveFromNameIndex(cachedID)
	}
	tc.stored.RemoveFromNameIndex(e.GetID())
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
	if cachedID := tc.stored.GetCachedIDByName(e); cachedID != descpb.InvalidID {
		tc.stored.RemoveFromNameIndex(cachedID)
	}
	tc.stored.RemoveFromNameIndex(e.GetID())
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
	if cachedID := tc.stored.GetCachedIDByName(k); cachedID != descpb.InvalidID {
		tc.stored.RemoveFromNameIndex(cachedID)
	}
	nameKey := catalogkeys.EncodeNameKey(tc.codec(), k)
	if kvTrace {
		log.VEventf(ctx, 2, "Del %s", nameKey)
	}
	b.Del(nameKey)
	return nil
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

	if descID != keys.RootNamespaceID && !keys.IsPseudoTableID(uint32(descID)) {
		return tc.AddUncommittedZoneConfig(descID, zc.ZoneConfigProto())
	}
	return nil
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

	if descID != keys.RootNamespaceID && !keys.IsPseudoTableID(uint32(descID)) {
		tc.MarkUncommittedZoneConfigDeleted(descID)
	}
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
	// First look up in-memory descriptors in collection,
	// except for leased descriptors.
	dbInMemory, err := func() (catalog.Descriptor, error) {
		flags := tree.CommonLookupFlags{
			Required:       true,
			AvoidLeased:    true,
			AvoidStorage:   true,
			IncludeOffline: true,
		}
		return tc.getDescriptorByName(
			ctx, txn, nil /* db */, nil /* sc */, dbName, flags, catalog.Database,
		)
	}()
	if errors.IsAny(err, catalog.ErrDescriptorNotFound, catalog.ErrDescriptorDropped) {
		// Swallow these errors to fall back to storage lookup.
		err = nil
	}
	if err != nil {
		return descpb.InvalidID, err
	}
	if dbInMemory != nil {
		return dbInMemory.GetID(), nil
	}
	// Look up database ID in storage if nothing was found in memory.
	return tc.stored.LookupDescriptorID(ctx, txn, keys.RootNamespaceID, keys.RootNamespaceID, dbName)
}

// LookupSchemaID returns the descriptor ID assigned to a schema name.
func (tc *Collection) LookupSchemaID(
	ctx context.Context, txn *kv.Txn, dbID descpb.ID, schemaName string,
) (descpb.ID, error) {
	// First look up in-memory descriptors in collection,
	// except for leased descriptors.
	scInMemory, err := func() (catalog.Descriptor, error) {
		flags := tree.CommonLookupFlags{
			Required:       true,
			AvoidLeased:    true,
			AvoidStorage:   true,
			IncludeOffline: true,
		}
		var parentDescs [1]catalog.Descriptor
		if err := getDescriptorsByID(ctx, tc, txn, flags, parentDescs[:], dbID); err != nil {
			return nil, err
		}
		db, err := catalog.AsDatabaseDescriptor(parentDescs[0])
		if err != nil {
			return nil, err
		}
		return tc.getDescriptorByName(
			ctx, txn, db, nil /* sc */, schemaName, flags, catalog.Schema,
		)
	}()
	if errors.IsAny(err, catalog.ErrDescriptorNotFound, catalog.ErrDescriptorDropped) {
		// Swallow these errors to fall back to storage lookup.
		err = nil
	}
	if err != nil {
		return descpb.InvalidID, err
	}
	if scInMemory != nil {
		return scInMemory.GetID(), nil
	}
	// Look up schema ID in storage if nothing was found in memory.
	return tc.stored.LookupDescriptorID(ctx, txn, dbID, keys.RootNamespaceID, schemaName)
}

// LookupObjectID returns the descriptor ID assigned to an object name.
func (tc *Collection) LookupObjectID(
	ctx context.Context, txn *kv.Txn, dbID descpb.ID, schemaID descpb.ID, objectName string,
) (descpb.ID, error) {
	// First look up in-memory descriptors in collection,
	// except for leased descriptors.
	objInMemory, err := func() (catalog.Descriptor, error) {
		flags := tree.CommonLookupFlags{
			Required:       true,
			AvoidLeased:    true,
			AvoidStorage:   true,
			IncludeOffline: true,
		}
		var parentDescs [2]catalog.Descriptor
		if err := getDescriptorsByID(ctx, tc, txn, flags, parentDescs[:], dbID, schemaID); err != nil {
			return nil, err
		}
		db, err := catalog.AsDatabaseDescriptor(parentDescs[0])
		if err != nil {
			return nil, err
		}
		sc, err := catalog.AsSchemaDescriptor(parentDescs[1])
		if err != nil {
			return nil, err
		}
		return tc.getDescriptorByName(
			ctx, txn, db, sc, objectName, flags, catalog.Any,
		)
	}()
	if errors.IsAny(err, catalog.ErrDescriptorNotFound, catalog.ErrDescriptorDropped) {
		// Swallow these errors to fall back to storage lookup.
		err = nil
	}
	if err != nil {
		return descpb.InvalidID, err
	}
	if objInMemory != nil {
		return objInMemory.GetID(), nil
	}
	// Look up ID in storage if nothing was found in memory.
	return tc.stored.LookupDescriptorID(ctx, txn, dbID, schemaID, objectName)
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

func newMutableSyntheticDescriptorAssertionError(id descpb.ID) error {
	return errors.AssertionFailedf("attempted mutable access of synthetic descriptor %d", id)
}

// GetAllDescriptorsForDatabase retrieves the complete set of descriptors
// in the requested database.
func (tc *Collection) GetAllDescriptorsForDatabase(
	ctx context.Context, txn *kv.Txn, db catalog.DatabaseDescriptor,
) (nstree.Catalog, error) {
	ns, err := tc.stored.GetAllDescriptorNamesForDatabase(ctx, txn, db)
	if err != nil {
		return ns, err
	}
	var ids catalog.DescriptorIDSet
	ids.Add(db.GetID())
	_ = ns.ForEachNamespaceEntry(func(e nstree.NamespaceEntry) error {
		if !strings.HasPrefix(e.GetName(), catconstants.PgTempSchemaName) &&
			e.GetID() != catconstants.PublicSchemaID {
			ids.Add(e.GetID())
		}
		return nil
	})
	var functionIDs catalog.DescriptorIDSet
	addSchema := func(sc catalog.SchemaDescriptor) {
		_ = sc.ForEachFunctionOverload(func(overload descpb.SchemaDescriptor_FunctionOverload) error {
			functionIDs.Add(overload.ID)
			return nil
		})
	}
	for _, f := range []func(func(descriptor catalog.Descriptor) error) error{
		tc.uncommitted.iterateUncommittedByID,
		tc.synthetic.iterateSyntheticByID,
	} {
		_ = f(func(desc catalog.Descriptor) error {
			if desc.GetParentID() == db.GetID() {
				ids.Add(desc.GetID())
			}
			if sc, isSchema := desc.(catalog.SchemaDescriptor); isSchema {
				addSchema(sc)
			}
			return nil
		})
	}
	flags := tree.CommonLookupFlags{
		AvoidLeased:    true,
		IncludeOffline: true,
		IncludeDropped: true,
	}
	// getDescriptorsByID must be used to ensure proper validation hydration etc.
	descs, err := tc.getDescriptorsByID(ctx, txn, flags, ids.Ordered()...)
	if err != nil {
		return nstree.Catalog{}, err
	}
	var ret nstree.MutableCatalog
	for _, desc := range descs {
		ret.UpsertDescriptorEntry(desc)
	}
	_ = ns.ForEachSchemaNamespaceEntryInDatabase(db.GetID(), func(
		e nstree.NamespaceEntry,
	) error {
		if sc, ok := ret.LookupDescriptorEntry(
			e.GetID(),
		).(catalog.SchemaDescriptor); ok {
			addSchema(sc)
		}

		return nil
	})

	descs, err = tc.getDescriptorsByID(ctx, txn, flags, functionIDs.Ordered()...)
	if err != nil {
		return nstree.Catalog{}, err
	}
	for _, desc := range descs {
		ret.UpsertDescriptorEntry(desc)
	}
	return ret.Catalog, nil
}

// GetAllDescriptors returns all descriptors visible by the transaction,
func (tc *Collection) GetAllDescriptors(ctx context.Context, txn *kv.Txn) (nstree.Catalog, error) {
	if err := tc.stored.EnsureAllDescriptors(ctx, txn); err != nil {
		return nstree.Catalog{}, err
	}
	var ids catalog.DescriptorIDSet
	for _, iterator := range []func(func(desc catalog.Descriptor) error) error{
		tc.stored.IterateCachedByID,
		tc.uncommitted.iterateUncommittedByID,
		tc.synthetic.iterateSyntheticByID,
		// TODO(postamar): include temporary descriptors?
	} {
		_ = iterator(func(desc catalog.Descriptor) error {
			ids.Add(desc.GetID())
			return nil
		})
	}
	flags := tree.CommonLookupFlags{
		AvoidLeased:    true,
		IncludeOffline: true,
		IncludeDropped: true,
	}
	// getDescriptorsByID must be used to ensure proper validation hydration etc.
	descs, err := tc.getDescriptorsByID(ctx, txn, flags, ids.Ordered()...)
	if err != nil {
		return nstree.Catalog{}, err
	}
	var ret nstree.MutableCatalog
	for _, desc := range descs {
		ret.UpsertDescriptorEntry(desc)
	}
	return ret.Catalog, nil
}

// GetAllDatabaseDescriptors returns all database descriptors visible by the
// transaction, ordered by name.
func (tc *Collection) GetAllDatabaseDescriptors(
	ctx context.Context, txn *kv.Txn,
) ([]catalog.DatabaseDescriptor, error) {
	if err := tc.stored.EnsureAllDatabaseDescriptors(ctx, txn); err != nil {
		return nil, err
	}
	var m nstree.NameMap
	if err := tc.stored.IterateDatabasesByName(func(db catalog.DatabaseDescriptor) error {
		m.Upsert(db, db.SkipNamespace())
		return nil
	}); err != nil {
		return nil, err
	}
	_ = tc.uncommitted.iterateUncommittedByID(func(desc catalog.Descriptor) error {
		if desc.DescriptorType() == catalog.Database {
			m.Upsert(desc, desc.SkipNamespace())
		}
		return nil
	})
	_ = tc.synthetic.iterateSyntheticByID(func(desc catalog.Descriptor) error {
		if desc.DescriptorType() == catalog.Database {
			m.Upsert(desc, desc.SkipNamespace())
		}
		return nil
	})
	var ids catalog.DescriptorIDSet
	_ = m.IterateDatabasesByName(func(entry catalog.NameEntry) error {
		ids.Add(entry.GetID())
		return nil
	})
	flags := tree.CommonLookupFlags{
		AvoidLeased:    true,
		IncludeOffline: true,
		IncludeDropped: true,
	}
	// getDescriptorsByID must be used to ensure proper validation hydration etc.
	descs, err := tc.getDescriptorsByID(ctx, txn, flags, ids.Ordered()...)
	if err != nil {
		return nil, err
	}
	// Returned slice must be ordered by name.
	m.Clear()
	dbDescs := make([]catalog.DatabaseDescriptor, 0, len(descs))
	for _, desc := range descs {
		m.Upsert(desc, desc.SkipNamespace())
	}
	_ = m.IterateDatabasesByName(func(entry catalog.NameEntry) error {
		dbDescs = append(dbDescs, entry.(catalog.DatabaseDescriptor))
		return nil
	})
	return dbDescs, nil
}

// GetAllTableDescriptorsInDatabase returns all the table descriptors visible to
// the transaction under the database with the given ID.
func (tc *Collection) GetAllTableDescriptorsInDatabase(
	ctx context.Context, txn *kv.Txn, db catalog.DatabaseDescriptor,
) ([]catalog.TableDescriptor, error) {
	all, err := tc.GetAllDescriptors(ctx, txn)
	if err != nil {
		return nil, err
	}
	// Ensure the given ID does indeed belong to a database.
	if desc := all.LookupDescriptorEntry(db.GetID()); desc == nil || desc.DescriptorType() != catalog.Database {
		return nil, sqlerrors.NewUndefinedDatabaseError(db.GetName())
	}
	dbID := db.GetID()
	var ret []catalog.TableDescriptor
	_ = all.ForEachDescriptorEntry(func(desc catalog.Descriptor) error {
		if desc.GetParentID() != dbID {
			return nil
		}
		if table, ok := desc.(catalog.TableDescriptor); ok {
			ret = append(ret, table)
		}
		return nil
	})
	return ret, nil
}

// GetSchemasForDatabase returns the schemas for a given database
// visible by the transaction.
func (tc *Collection) GetSchemasForDatabase(
	ctx context.Context, txn *kv.Txn, db catalog.DatabaseDescriptor,
) (map[descpb.ID]string, error) {
	ret, err := tc.stored.GetSchemaIDsAndNamesForDatabase(ctx, txn, db)
	if err != nil {
		return nil, err
	}
	_ = tc.uncommitted.iterateUncommittedByID(func(desc catalog.Descriptor) error {
		if desc.DescriptorType() == catalog.Schema && desc.GetParentID() == db.GetID() {
			ret[desc.GetID()] = desc.GetName()
		}
		return nil
	})
	_ = tc.synthetic.iterateSyntheticByID(func(desc catalog.Descriptor) error {
		if desc.DescriptorType() == catalog.Schema && desc.GetParentID() == db.GetID() {
			ret[desc.GetID()] = desc.GetName()
		}
		return nil
	})
	return ret, nil
}

// GetObjectNamesAndIDs returns the names and IDs of all objects in a database and schema.
func (tc *Collection) GetObjectNamesAndIDs(
	ctx context.Context,
	txn *kv.Txn,
	dbDesc catalog.DatabaseDescriptor,
	scName string,
	flags tree.DatabaseListFlags,
) (tree.TableNames, descpb.IDs, error) {
	if ok, names, ds := tc.virtual.maybeGetObjectNamesAndIDs(
		scName, dbDesc, flags,
	); ok {
		return names, ds, nil
	}

	schemaFlags := tree.SchemaLookupFlags{
		Required:       flags.Required,
		AvoidLeased:    flags.RequireMutable || flags.AvoidLeased,
		IncludeDropped: flags.IncludeDropped,
		IncludeOffline: flags.IncludeOffline,
	}
	schema, err := tc.getSchemaByName(ctx, txn, dbDesc, scName, schemaFlags)
	if err != nil {
		return nil, nil, err
	}
	if schema == nil { // required must have been false
		return nil, nil, nil
	}

	c, err := tc.stored.ScanNamespaceForSchemaObjects(ctx, txn, dbDesc, schema)
	if err != nil {
		return nil, nil, err
	}
	var tableNames tree.TableNames
	var tableIDs descpb.IDs
	_ = c.ForEachNamespaceEntry(func(e nstree.NamespaceEntry) error {
		tn := tree.MakeTableNameWithSchema(tree.Name(dbDesc.GetName()), tree.Name(scName), tree.Name(e.GetName()))
		tn.ExplicitCatalog = flags.ExplicitPrefix
		tn.ExplicitSchema = flags.ExplicitPrefix
		tableNames = append(tableNames, tn)
		tableIDs = append(tableIDs, e.GetID())
		return nil
	})
	return tableNames, tableIDs, nil
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

// AddSyntheticDescriptor injects a synthetic descriptor into the Collection.
// An immutable copy is made if the descriptor is mutable.
// See the documentation on syntheticDescriptors.
func (tc *Collection) AddSyntheticDescriptor(desc catalog.Descriptor) {
	tc.synthetic.add(desc)
}

func (tc *Collection) codec() keys.SQLCodec {
	return tc.stored.Codec()
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
	tc.stored.DescriptorValidationModeProvider = dsdp
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

// GetConstraintComment implements the scdecomp.CommentGetter interface.
func (tc *Collection) GetConstraintComment(
	tableID descpb.ID, constraintID catid.ConstraintID,
) (comment string, ok bool) {
	return tc.GetComment(catalogkeys.MakeCommentKey(uint32(tableID), uint32(constraintID), catalogkeys.ConstraintCommentType))
}

// Direct exports the catkv.Direct interface.
type Direct = catkv.Direct

// Direct provides direct access to the underlying KV-storage.
func (tc *Collection) Direct() Direct {
	return catkv.MakeDirect(tc.codec(), tc.version, tc.validationModeProvider)
}

// MakeTestCollection makes a Collection that can be used for tests.
func MakeTestCollection(ctx context.Context, leaseManager *lease.Manager) Collection {
	settings := cluster.MakeTestingClusterSettings()
	return Collection{
		settings: settings,
		version:  settings.Version.ActiveVersion(ctx),
		leased:   makeLeasedDescriptors(leaseManager),
	}
}

// InternalExecFn is the type of functions that operates using an internalExecutor.
type InternalExecFn func(ctx context.Context, txn *kv.Txn, ie sqlutil.InternalExecutor, descriptors *Collection) error

// HistoricalInternalExecTxnRunnerFn callback for executing with the internal executor
// at a fixed timestamp.
type HistoricalInternalExecTxnRunnerFn = func(ctx context.Context, fn InternalExecFn) error

// HistoricalInternalExecTxnRunner is like historicalTxnRunner except it only
// passes the fn the exported InternalExecutor instead of the whole unexported
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
