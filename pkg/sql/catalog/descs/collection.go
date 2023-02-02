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
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/hydrateddesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/internal/catkv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/internal/validate"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/lease"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/nstree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/iterutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/errors"
)

// newCollection constructs a Collection.
func newCollection(
	ctx context.Context,
	leaseMgr *lease.Manager,
	settings *cluster.Settings,
	codec keys.SQLCodec,
	hydrated *hydrateddesc.Cache,
	systemDatabase *catkv.SystemDatabaseCache,
	virtualSchemas catalog.VirtualSchemas,
	temporarySchemaProvider TemporarySchemaProvider,
	monitor *mon.BytesMonitor,
) *Collection {
	v := settings.Version.ActiveVersion(ctx)
	cr := catkv.NewCatalogReader(codec, v, systemDatabase)
	return &Collection{
		settings:    settings,
		version:     v,
		hydrated:    hydrated,
		virtual:     makeVirtualDescriptors(virtualSchemas),
		leased:      makeLeasedDescriptors(leaseMgr),
		uncommitted: makeUncommittedDescriptors(monitor),
		stored:      catkv.MakeStoredCatalog(cr, monitor),
		temporary:   makeTemporaryDescriptors(settings, codec, temporarySchemaProvider),
	}
}

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

	// temporary contains logic to access temporary schema descriptors.
	temporary temporaryDescriptors

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

// GetMaxTimestampBound returns the maximum timestamp to read schemas at.
func (tc *Collection) GetMaxTimestampBound() hlc.Timestamp {
	return tc.maxTimestampBoundDeadlineHolder.maxTimestampBound
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
	if desc.GetID() == keys.SystemDatabaseID || !desc.IsUncommittedVersion() {
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
	tc.stored.RemoveFromNameIndex(desc)
	return tc.uncommitted.upsert(ctx, desc)
}

// ValidateOnWriteEnabled is the cluster setting used to enable or disable
// validating descriptors prior to writing.
var ValidateOnWriteEnabled = settings.RegisterBoolSetting(
	settings.TenantWritable,
	"sql.catalog.descs.validate_on_write.enabled",
	"set to true to validate descriptors prior to writing, false to disable; default is true",
	true, /* defaultValue */
)

// WriteDescToBatch calls MaybeIncrementVersion, adds the descriptor to the
// collection as an uncommitted descriptor, and writes it into b.
func (tc *Collection) WriteDescToBatch(
	ctx context.Context, kvTrace bool, desc catalog.MutableDescriptor, b *kv.Batch,
) error {
	desc.MaybeIncrementVersion()
	if !tc.skipValidationOnWrite && ValidateOnWriteEnabled.Get(&tc.settings.SV) {
		if err := validate.Self(tc.version, desc); err != nil {
			return err
		}
	}
	if err := tc.AddUncommittedDescriptor(ctx, desc); err != nil {
		return err
	}
	descKey := catalogkeys.MakeDescMetadataKey(tc.codec(), desc.GetID())
	proto := desc.DescriptorProto()
	if kvTrace {
		log.VEventf(ctx, 2, "Put %s -> %s", descKey, proto)
	}
	b.Put(descKey, proto)
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

// SetTemporaryDescriptors is used in the context of the internal executor
// to override the temporary descriptors during temporary object
// cleanup.
func (tc *Collection) SetTemporaryDescriptors(provider TemporarySchemaProvider) {
	tc.temporary = makeTemporaryDescriptors(tc.settings, tc.codec(), provider)
}

// Direct exports the catkv.Direct interface.
type Direct = catkv.Direct

// Direct provides direct access to the underlying KV-storage.
func (tc *Collection) Direct() Direct {
	return catkv.MakeDirect(tc.codec(), tc.version)
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
