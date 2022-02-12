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
	"bytes"
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/hydratedtables"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/internal/validate"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/lease"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/nstree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// makeCollection constructs a Collection.
func makeCollection(
	ctx context.Context,
	leaseMgr *lease.Manager,
	settings *cluster.Settings,
	codec keys.SQLCodec,
	hydratedTables *hydratedtables.Cache,
	systemNamespace *systemDatabaseNamespaceCache,
	virtualSchemas catalog.VirtualSchemas,
	temporarySchemaProvider TemporarySchemaProvider,
) Collection {
	return Collection{
		settings:       settings,
		version:        settings.Version.ActiveVersion(ctx),
		hydratedTables: hydratedTables,
		virtual:        makeVirtualDescriptors(virtualSchemas),
		leased:         makeLeasedDescriptors(leaseMgr),
		kv:             makeKVDescriptors(codec, systemNamespace),
		temporary:      makeTemporaryDescriptors(settings, codec, temporarySchemaProvider),
		direct:         makeDirect(ctx, codec, settings),
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

	// Descriptors modified by the uncommitted transaction affiliated with this
	// Collection. This allows a transaction to see its own modifications while
	// bypassing the descriptor lease mechanism. The lease mechanism will have its
	// own transaction to read the descriptor and will hang waiting for the
	// uncommitted changes to the descriptor if this transaction is PRIORITY HIGH.
	// These descriptors are local to this Collection and their state is thus not
	// visible to other transactions.
	uncommitted uncommittedDescriptors

	// A collection of descriptors which were read from the store.
	kv kvDescriptors

	// syntheticDescriptors contains in-memory descriptors which override all
	// other matching descriptors during immutable descriptor resolution (by name
	// or by ID), but should not be written to disk. These support internal
	// queries which need to use a special modified descriptor (e.g. validating
	// non-public schema elements during a schema change). Attempting to resolve
	// a mutable descriptor by name or ID when a matching synthetic descriptor
	// exists is illegal.
	synthetic syntheticDescriptors

	// temporary contains logic to access temporary schema descriptors.
	temporary temporaryDescriptors

	// hydratedTables is node-level cache of table descriptors which utlize
	// user-defined types.
	hydratedTables *hydratedtables.Cache

	// skipValidationOnWrite should only be set to true during forced descriptor
	// repairs.
	skipValidationOnWrite bool

	// droppedDescriptors that will not need to wait for new
	// lease versions.
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

	// direct provides low-level access to descriptors via the Direct interface.
	// For the most part, it is in deprecated or testing settings.
	direct direct
}

var _ catalog.Accessor = (*Collection)(nil)

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

// SkipValidationOnWrite avoids validating uncommitted descriptors prior to
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
	tc.uncommitted.reset()
	tc.kv.reset()
	tc.synthetic.reset()
	tc.deletedDescs = catalog.DescriptorIDSet{}
	tc.skipValidationOnWrite = false
}

// HasUncommittedTables returns true if the Collection contains uncommitted
// tables.
func (tc *Collection) HasUncommittedTables() bool {
	return tc.uncommitted.hasUncommittedTables()
}

// HasUncommittedTypes returns true if the Collection contains uncommitted
// types.
func (tc *Collection) HasUncommittedTypes() bool {
	return tc.uncommitted.hasUncommittedTypes()
}

// Satisfy the linter.
var _ = (*Collection).HasUncommittedTypes

// AddUncommittedDescriptor adds an uncommitted descriptor modified in the
// transaction to the Collection. The descriptor must either be a new descriptor
// or carry the original version or carry the subsequent version to the original
// version.
//
// Subsequent attempts to resolve this descriptor mutably, either by name or ID
// will return this exact object. Subsequent attempts to resolve this descriptor
// immutably will return a copy of the descriptor in the current state. A deep
// copy is performed in this call.
func (tc *Collection) AddUncommittedDescriptor(desc catalog.MutableDescriptor) error {
	// Invalidate all the cached descriptors since a stale copy of this may be
	// included.
	tc.kv.releaseAllDescriptors()
	return tc.uncommitted.checkIn(desc)
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
	if err := tc.AddUncommittedDescriptor(desc); err != nil {
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

// GetDescriptorsWithNewVersion returns all the IDVersion pairs that have
// undergone a schema change. Returns nil for no schema changes. The version
// returned for each schema change is ClusterVersion - 1, because that's the one
// that will be used when checking for table descriptor two version invariance.
func (tc *Collection) GetDescriptorsWithNewVersion() (originalVersions []lease.IDVersion) {
	_ = tc.uncommitted.iterateNewVersionByID(func(_ catalog.NameEntry, originalVersion lease.IDVersion) error {
		originalVersions = append(originalVersions, originalVersion)
		return nil
	})
	return originalVersions
}

// GetUncommittedTables returns all the tables updated or created in the
// transaction.
func (tc *Collection) GetUncommittedTables() (tables []catalog.TableDescriptor) {
	return tc.uncommitted.getUncommittedTables()
}

func newMutableSyntheticDescriptorAssertionError(id descpb.ID) error {
	return errors.AssertionFailedf("attempted mutable access of synthetic descriptor %d", id)
}

// GetAllDescriptors returns all descriptors visible by the transaction,
// first checking the Collection's cached descriptors for validity if validate
// is set to true before defaulting to a key-value scan, if necessary.
func (tc *Collection) GetAllDescriptors(ctx context.Context, txn *kv.Txn) (nstree.Catalog, error) {
	return tc.kv.getAllDescriptors(ctx, txn, tc.version)
}

// GetAllDatabaseDescriptors returns all database descriptors visible by the
// transaction, first checking the Collection's cached descriptors for
// validity before scanning system.namespace and looking up the descriptors
// in the database cache, if necessary.
// If the argument allowMissingDesc is true, the function will return nil-s for
// missing database descriptors.
func (tc *Collection) GetAllDatabaseDescriptors(
	ctx context.Context, txn *kv.Txn,
) ([]catalog.DatabaseDescriptor, error) {
	return tc.kv.getAllDatabaseDescriptors(ctx, txn, tc.version)
}

// GetAllTableDescriptorsInDatabase returns all the table descriptors visible to
// the transaction under the database with the given ID. It first checks the
// collection's cached descriptors before defaulting to a key-value scan, if
// necessary.
func (tc *Collection) GetAllTableDescriptorsInDatabase(
	ctx context.Context, txn *kv.Txn, dbID descpb.ID,
) ([]catalog.TableDescriptor, error) {
	// Ensure the given ID does indeed belong to a database.
	found, _, err := tc.getDatabaseByID(ctx, txn, dbID, tree.DatabaseLookupFlags{
		AvoidLeased: false,
	})
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, sqlerrors.NewUndefinedDatabaseError(fmt.Sprintf("[%d]", dbID))
	}
	all, err := tc.GetAllDescriptors(ctx, txn)
	if err != nil {
		return nil, err
	}
	var ret []catalog.TableDescriptor
	for _, desc := range all.OrderedDescriptors() {
		if desc.GetParentID() == dbID {
			if table, ok := desc.(catalog.TableDescriptor); ok {
				ret = append(ret, table)
			}
		}
	}
	return ret, nil
}

// GetSchemasForDatabase returns the schemas for a given database
// visible by the transaction. This uses the schema cache locally
// if possible, or else performs a scan on kv.
func (tc *Collection) GetSchemasForDatabase(
	ctx context.Context, txn *kv.Txn, dbDesc catalog.DatabaseDescriptor,
) (map[descpb.ID]string, error) {
	return tc.kv.getSchemasForDatabase(ctx, txn, dbDesc)
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

	log.Eventf(ctx, "fetching list of objects for %q", dbDesc.GetName())
	prefix := catalogkeys.MakeObjectNameKey(tc.codec(), dbDesc.GetID(), schema.GetID(), "")
	sr, err := txn.Scan(ctx, prefix, prefix.PrefixEnd(), 0)
	if err != nil {
		return nil, nil, err
	}

	alreadySeen := make(map[string]bool)
	var tableNames tree.TableNames
	var tableIDs descpb.IDs

	for _, row := range sr {
		_, tableName, err := encoding.DecodeUnsafeStringAscending(bytes.TrimPrefix(
			row.Key, prefix), nil)
		if err != nil {
			return nil, nil, err
		}
		alreadySeen[tableName] = true
		tn := tree.MakeTableNameWithSchema(tree.Name(dbDesc.GetName()), tree.Name(scName), tree.Name(tableName))
		tn.ExplicitCatalog = flags.ExplicitPrefix
		tn.ExplicitSchema = flags.ExplicitPrefix
		tableNames = append(tableNames, tn)
		tableIDs = append(tableIDs, descpb.ID(row.ValueInt()))
	}

	return tableNames, tableIDs, nil
}

// SetSyntheticDescriptors sets the provided descriptors as the synthetic
// descriptors to override all other matching descriptors during immutable
// access. An immutable copy is made if the descriptor is mutable. See the
// documentation on syntheticDescriptors.
func (tc *Collection) SetSyntheticDescriptors(descs []catalog.Descriptor) {
	tc.synthetic.set(descs)
}

// AddSyntheticDescriptor replaces a descriptor with a synthetic
// one temporarily for a given transaction. This synthetic descriptor
// will be immutable.
func (tc *Collection) AddSyntheticDescriptor(desc catalog.Descriptor) {
	tc.synthetic.add(desc)
}

// RemoveSyntheticDescriptor removes a synthetic descriptor
// override that temporarily exists for a given transaction.
func (tc *Collection) RemoveSyntheticDescriptor(id descpb.ID) {
	tc.synthetic.remove(id)
}

func (tc *Collection) codec() keys.SQLCodec {
	return tc.kv.codec
}

// AddDeletedDescriptor is temporarily tracking descriptors that have been,
// deleted which from an add state without any intermediate steps
// Any descriptors marked as deleted will be skipped for the
// wait for one version logic inside descs.Txn, since they will no longer
// be inside storage.
// Note: that this happens, at time of writing, only when reverting an
// IMPORT or RESTORE.
func (tc *Collection) AddDeletedDescriptor(id descpb.ID) {
	tc.deletedDescs.Add(id)
}

// SetSession sets the sqlliveness.Session for the transaction. This
// should only be called in a multi-tenant environment.
func (tc *Collection) SetSession(session sqlliveness.Session) {
	tc.sqlLivenessSession = session
}
