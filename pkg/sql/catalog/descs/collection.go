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
	"sort"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/bootstrap"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/dbdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/hydratedtables"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/lease"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/resolver"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq/oid"
)

// uncommittedDescriptor is a descriptor that has been modified in the current
// transaction.
type uncommittedDescriptor struct {
	mutable   catalog.MutableDescriptor
	immutable catalog.Descriptor
}

// leasedDescriptors holds references to all the descriptors leased in the
// transaction, and supports access by name and by ID.
type leasedDescriptors struct {
	descs []lease.LeasedDescriptor
}

func (ld *leasedDescriptors) add(desc lease.LeasedDescriptor) {
	ld.descs = append(ld.descs, desc)
}

func (ld *leasedDescriptors) releaseAll() (toRelease []lease.LeasedDescriptor) {
	toRelease = append(toRelease, ld.descs...)
	ld.descs = ld.descs[:0]
	return toRelease
}

func (ld *leasedDescriptors) release(ids []descpb.ID) (toRelease []lease.LeasedDescriptor) {
	// Sort the descriptors and leases to make it easy to find the leases to release.
	leasedDescs := ld.descs
	sort.Slice(ids, func(i, j int) bool {
		return ids[i] < ids[j]
	})
	sort.Slice(leasedDescs, func(i, j int) bool {
		return leasedDescs[i].Desc().GetID() < leasedDescs[j].Desc().GetID()
	})

	filteredLeases := leasedDescs[:0] // will store the remaining leases
	idsToConsider := ids
	shouldRelease := func(id descpb.ID) (found bool) {
		for len(idsToConsider) > 0 && idsToConsider[0] < id {
			idsToConsider = idsToConsider[1:]
		}
		return len(idsToConsider) > 0 && idsToConsider[0] == id
	}
	for _, l := range leasedDescs {
		if !shouldRelease(l.Desc().GetID()) {
			filteredLeases = append(filteredLeases, l)
		} else {
			toRelease = append(toRelease, l)
		}
	}
	ld.descs = filteredLeases
	return toRelease
}

func (ld *leasedDescriptors) getByID(id descpb.ID) catalog.Descriptor {
	for i := range ld.descs {
		desc := ld.descs[i]
		if desc.Desc().GetID() == id {
			return desc.Desc()
		}
	}
	return nil
}

func (ld *leasedDescriptors) getByName(
	dbID descpb.ID, schemaID descpb.ID, name string,
) catalog.Descriptor {
	for i := range ld.descs {
		desc := ld.descs[i]
		if lease.NameMatchesDescriptor(desc.Desc(), dbID, schemaID, name) {
			return desc.Desc()
		}
	}
	return nil
}

func (ld *leasedDescriptors) numDescriptors() int {
	return len(ld.descs)
}

// MakeCollection constructs a Collection.
func MakeCollection(
	leaseMgr *lease.Manager,
	settings *cluster.Settings,
	sessionData *sessiondata.SessionData,
	hydratedTables *hydratedtables.Cache,
	virtualSchemas catalog.VirtualSchemas,
) Collection {
	return Collection{
		leaseMgr:       leaseMgr,
		settings:       settings,
		sessionData:    sessionData,
		hydratedTables: hydratedTables,
		virtualSchemas: virtualSchemas,
	}
}

// NewCollection constructs a new *Collection.
func NewCollection(
	settings *cluster.Settings,
	leaseMgr *lease.Manager,
	hydratedTables *hydratedtables.Cache,
	virtualSchemas catalog.VirtualSchemas,
) *Collection {
	tc := MakeCollection(leaseMgr, settings, nil, hydratedTables, virtualSchemas)
	return &tc
}

// Collection is a collection of descriptors held by a single session that
// serves SQL requests, or a background job using descriptors. The
// collection is cleared using ReleaseAll() which is called at the
// end of each transaction on the session, or on hitting conditions such
// as errors, or retries that result in transaction timestamp changes.
type Collection struct {
	// leaseMgr manages acquiring and releasing per-descriptor leases.
	leaseMgr *lease.Manager
	// virtualSchemas optionally holds the virtual schemas.
	virtualSchemas catalog.VirtualSchemas

	// A collection of descriptors valid for the timestamp. They are released once
	// the transaction using them is complete. If the transaction gets pushed and
	// the timestamp changes, the descriptors are released.
	// TODO (lucy): Use something other than an unsorted slice for faster lookups.
	leasedDescriptors leasedDescriptors
	// Descriptors modified by the uncommitted transaction affiliated with this
	// Collection. This allows a transaction to see its own modifications while
	// bypassing the descriptor lease mechanism. The lease mechanism will have its
	// own transaction to read the descriptor and will hang waiting for the
	// uncommitted changes to the descriptor. These descriptors are local to this
	// Collection and invisible to other transactions.
	// TODO (lucy): Replace this with a data structure for faster lookups.
	// Currently, the order in which descriptors are inserted matters, since we
	// look at the draining names to account for descriptors being renamed. Any
	// replacement data structure may have to store the name information in some
	// different, more explicit way.
	uncommittedDescriptors []*uncommittedDescriptor

	// allDescriptors is a slice of all available descriptors. The descriptors
	// are cached to avoid repeated lookups by users like virtual tables. The
	// cache is purged whenever events would cause a scan of all descriptors to
	// return different values, such as when the txn timestamp changes or when
	// new descriptors are written in the txn.
	//
	// TODO(ajwerner): This cache may be problematic in clusters with very large
	// numbers of descriptors.
	allDescriptors allDescriptors

	// allDatabaseDescriptors is a slice of all available database descriptors.
	// These are purged at the same time as allDescriptors.
	allDatabaseDescriptors []catalog.DatabaseDescriptor

	// allSchemasForDatabase maps databaseID -> schemaID -> schemaName.
	// For each databaseID, all schemas visible under the database can be
	// observed.
	// These are purged at the same time as allDescriptors.
	allSchemasForDatabase map[descpb.ID]map[descpb.ID]string

	// settings are required to correctly resolve system.namespace accesses in
	// mixed version (19.2/20.1) clusters.
	// TODO(solon): This field could maybe be removed in 20.2.
	settings *cluster.Settings

	// sessionData is the SessionData of the current session, if this Collection
	// is being used in the context of a session. It is stored so that the Collection
	// knows about state of temporary schemas (name and ID) for resolution.
	sessionData *sessiondata.SessionData

	// hydratedTables is node-level cache of table descriptors which utlize
	// user-defined types.
	hydratedTables *hydratedtables.Cache

	// syntheticDescriptors contains in-memory descriptors which override all
	// other matching descriptors during immutable descriptor resolution (by name
	// or by ID), but should not be written to disk. These support internal
	// queries which need to use a special modified descriptor (e.g. validating
	// non-public schema elements during a schema change). Attempting to resolve
	// a mutable descriptor by name or ID when a matching synthetic descriptor
	// exists is illegal.
	syntheticDescriptors []catalog.Descriptor

	// skipValidationOnWrite should only be set to true during forced descriptor
	// repairs.
	skipValidationOnWrite bool
}

var _ catalog.Accessor = (*Collection)(nil)

// allDescriptors is an abstraction to capture the complete set of descriptors
// read from the store. It is used to accelerate repeated invocations of virtual
// tables which utilize descriptors. It tends to get used to build a
// sql.internalLookupCtx.
//
// TODO(ajwerner): Memory monitoring.
// TODO(ajwerner): Unify this struct with the uncommittedDescriptors set.
// TODO(ajwerner): Unify the sql.internalLookupCtx with the descs.Collection.
type allDescriptors struct {
	descs []catalog.Descriptor
	byID  map[descpb.ID]int
}

func (d *allDescriptors) init(descriptors []catalog.Descriptor) {
	d.descs = descriptors
	d.byID = make(map[descpb.ID]int, len(descriptors))
	for i, desc := range descriptors {
		d.byID[desc.GetID()] = i
	}
}

func (d *allDescriptors) clear() {
	d.descs = nil
	d.byID = nil
}

func (d *allDescriptors) isEmpty() bool {
	return d.descs == nil
}

func (d *allDescriptors) contains(id descpb.ID) bool {
	_, exists := d.byID[id]
	return exists
}

// getLeasedDescriptorByName return a leased descriptor valid for the
// transaction, acquiring one if necessary. Due to a bug in lease acquisition
// for dropped descriptors, the descriptor may have to be read from the store,
// in which case shouldReadFromStore will be true.
func (tc *Collection) getLeasedDescriptorByName(
	ctx context.Context, txn *kv.Txn, parentID descpb.ID, parentSchemaID descpb.ID, name string,
) (desc catalog.Descriptor, shouldReadFromStore bool, err error) {
	// First, look to see if we already have the descriptor.
	// This ensures that, once a SQL transaction resolved name N to id X, it will
	// continue to use N to refer to X even if N is renamed during the
	// transaction.
	if desc = tc.leasedDescriptors.getByName(parentID, parentSchemaID, name); desc != nil {
		if log.V(2) {
			log.Eventf(ctx, "found descriptor in collection for '%s'", name)
		}
		return desc, false, nil
	}

	readTimestamp := txn.ReadTimestamp()
	ldesc, err := tc.leaseMgr.AcquireByName(ctx, readTimestamp, parentID, parentSchemaID, name)
	if err != nil {
		// Read the descriptor from the store in the face of some specific errors
		// because of a known limitation of AcquireByName. See the known
		// limitations of AcquireByName for details.
		if (catalog.HasInactiveDescriptorError(err) && errors.Is(err, catalog.ErrDescriptorDropped)) ||
			errors.Is(err, catalog.ErrDescriptorNotFound) {
			return nil, true, nil
		}
		// Lease acquisition failed with some other error. This we don't
		// know how to deal with, so propagate the error.
		return nil, false, err
	}

	expiration := ldesc.Expiration()
	if expiration.LessEq(readTimestamp) {
		log.Fatalf(ctx, "bad descriptor for T=%s, expiration=%s", readTimestamp, expiration)
	}

	tc.leasedDescriptors.add(ldesc)
	if log.V(2) {
		log.Eventf(ctx, "added descriptor '%s' to collection: %+v", name, ldesc.Desc())
	}

	// If the descriptor we just acquired expires before the txn's deadline,
	// reduce the deadline. We use ReadTimestamp() that doesn't return the commit
	// timestamp, so we need to set a deadline on the transaction to prevent it
	// from committing beyond the version's expiration time.
	err = tc.MaybeUpdateDeadline(ctx, txn)
	if err != nil {
		return nil, false, err
	}
	return ldesc.Desc(), false, nil
}

// Deadline returns the latest expiration from our leased
// descriptors which should b e the transactions deadline.
func (tc *Collection) Deadline() (deadline hlc.Timestamp, haveDeadline bool) {
	for _, l := range tc.leasedDescriptors.descs {
		expiration := l.Expiration()
		if !haveDeadline || expiration.Less(deadline) {
			haveDeadline = true
			deadline = expiration
		}
	}
	return deadline, haveDeadline
}

// MaybeUpdateDeadline updates the deadline in a given transaction
// based on the leased descriptors in this collection. This update is
// only done when a deadline exists.
func (tc *Collection) MaybeUpdateDeadline(ctx context.Context, txn *kv.Txn) (err error) {
	if deadline, haveDeadline := tc.Deadline(); haveDeadline {
		err = txn.UpdateDeadline(ctx, deadline)
	}
	return err
}

// getLeasedDescriptorByID return a leased descriptor valid for the transaction,
// acquiring one if necessary.
// We set a deadline on the transaction based on the lease expiration, which is
// the usual case, unless setTxnDeadline is false.
func (tc *Collection) getLeasedDescriptorByID(
	ctx context.Context, txn *kv.Txn, id descpb.ID, setTxnDeadline bool,
) (catalog.Descriptor, error) {
	// First, look to see if we already have the table in the shared cache.
	if desc := tc.leasedDescriptors.getByID(id); desc != nil {
		log.VEventf(ctx, 2, "found descriptor %d in cache", id)
		return desc, nil
	}

	readTimestamp := txn.ReadTimestamp()
	desc, err := tc.leaseMgr.Acquire(ctx, readTimestamp, id)
	if err != nil {
		return nil, err
	}
	expiration := desc.Expiration()
	if expiration.LessEq(readTimestamp) {
		log.Fatalf(ctx, "bad descriptor for T=%s, expiration=%s", readTimestamp, expiration)
	}

	tc.leasedDescriptors.add(desc)
	log.VEventf(ctx, 2, "added descriptor %q to collection", desc.Desc().GetName())

	if setTxnDeadline {
		err := tc.MaybeUpdateDeadline(ctx, txn)
		if err != nil {
			return nil, err
		}
	}
	return desc.Desc(), nil
}

// getDescriptorFromStore gets a descriptor from its namespace entry. It does
// not return the descriptor if the name is being drained.
func (tc *Collection) getDescriptorFromStore(
	ctx context.Context,
	txn *kv.Txn,
	parentID descpb.ID,
	parentSchemaID descpb.ID,
	name string,
	mutable bool,
) (found bool, desc catalog.Descriptor, err error) {
	// Bypass the namespace lookup from the store for system tables.
	descID := bootstrap.LookupSystemTableDescriptorID(ctx, tc.settings, tc.codec(), parentID, name)
	isSystemDescriptor := descID != descpb.InvalidID
	if !isSystemDescriptor {
		var found bool
		var err error
		found, descID, err = catalogkv.LookupObjectID(ctx, txn, tc.codec(), parentID, parentSchemaID, name)
		if err != nil || !found {
			return found, nil, err
		}
	}
	// Always pick up a mutable copy so it can be cached.
	desc, err = catalogkv.GetMutableDescriptorByID(ctx, txn, tc.codec(), descID)
	if err != nil {
		return false, nil, err
	} else if desc == nil && isSystemDescriptor {
		// This can happen during startup because we're not actually looking up the
		// system descriptor IDs in KV.
		return false, nil, errors.Wrapf(catalog.ErrDescriptorNotFound, "descriptor %d not found", descID)
	} else if desc == nil {
		// Having done the namespace lookup, the descriptor must exist.
		return false, nil, errors.AssertionFailedf("descriptor %d not found", descID)
	}
	isNamespace2 := parentID == keys.SystemDatabaseID && name == systemschema.NamespaceTableName
	// Immediately after a RENAME an old name still points to the descriptor
	// during the drain phase for the name. Do not return a descriptor during
	// draining.
	if desc.GetName() != name && !isNamespace2 {
		// Special case for the namespace table, whose name is namespace2 in its
		// descriptor and namespace entry.
		return false, nil, nil
	}
	ud, err := tc.addUncommittedDescriptor(desc.(catalog.MutableDescriptor))
	if err != nil {
		return false, nil, err
	}
	if !mutable {
		desc = ud.immutable
	}
	return true, desc, nil
}

// GetMutableDatabaseByName returns a mutable database descriptor with
// properties according to the provided lookup flags. RequireMutable is ignored.
func (tc *Collection) GetMutableDatabaseByName(
	ctx context.Context, txn *kv.Txn, name string, flags tree.DatabaseLookupFlags,
) (found bool, _ *dbdesc.Mutable, _ error) {
	flags.RequireMutable = true
	found, desc, err := tc.getDatabaseByName(ctx, txn, name, flags)
	if err != nil || !found {
		return false, nil, err
	}
	return true, desc.(*dbdesc.Mutable), nil
}

// GetImmutableDatabaseByName returns an immutable database descriptor with
// properties according to the provided lookup flags. RequireMutable is ignored.
func (tc *Collection) GetImmutableDatabaseByName(
	ctx context.Context, txn *kv.Txn, name string, flags tree.DatabaseLookupFlags,
) (found bool, _ catalog.DatabaseDescriptor, _ error) {
	flags.RequireMutable = false
	return tc.getDatabaseByName(ctx, txn, name, flags)
}

// GetDatabaseDesc implements the Accessor interface.
//
// TODO(ajwerner): This exists to support the SchemaResolver interface and
// should be removed or adjusted.
func (tc *Collection) GetDatabaseDesc(
	ctx context.Context, txn *kv.Txn, name string, flags tree.DatabaseLookupFlags,
) (desc catalog.DatabaseDescriptor, err error) {
	_, desc, err = tc.getDatabaseByName(ctx, txn, name, flags)
	return desc, err
}

// getDatabaseByName returns a database descriptor with properties according to
// the provided lookup flags.
func (tc *Collection) getDatabaseByName(
	ctx context.Context, txn *kv.Txn, name string, flags tree.DatabaseLookupFlags,
) (bool, catalog.DatabaseDescriptor, error) {
	if name == systemschema.SystemDatabaseName {
		// The system database descriptor should never actually be mutated, which is
		// why we return the same hard-coded descriptor every time. It's assumed
		// that callers of this method will check the privileges on the descriptor
		// (like any other database) and return an error.
		if flags.RequireMutable {
			return true, dbdesc.NewBuilder(systemschema.MakeSystemDatabaseDesc().DatabaseDesc()).BuildExistingMutableDatabase(), nil
		}
		return true, systemschema.MakeSystemDatabaseDesc(), nil
	}

	getDatabaseByName := func() (found bool, _ catalog.Descriptor, err error) {
		if found, refuseFurtherLookup, desc, err := tc.getSyntheticOrUncommittedDescriptor(
			keys.RootNamespaceID, keys.RootNamespaceID, name, flags.RequireMutable,
		); err != nil || refuseFurtherLookup {
			return false, nil, err
		} else if found {
			log.VEventf(ctx, 2, "found uncommitted descriptor %d", desc.GetID())
			return true, desc, nil
		}

		if flags.AvoidCached || flags.RequireMutable || lease.TestingTableLeasesAreDisabled() {
			return tc.getDescriptorFromStore(
				ctx, txn, keys.RootNamespaceID, keys.RootNamespaceID, name, flags.RequireMutable,
			)
		}

		desc, shouldReadFromStore, err := tc.getLeasedDescriptorByName(
			ctx, txn, keys.RootNamespaceID, keys.RootNamespaceID, name)
		if err != nil {
			return false, nil, err
		}
		if shouldReadFromStore {
			return tc.getDescriptorFromStore(
				ctx, txn, keys.RootNamespaceID, keys.RootNamespaceID, name, flags.RequireMutable,
			)
		}
		return true, desc, nil
	}

	found, desc, err := getDatabaseByName()
	if err != nil {
		return false, nil, err
	} else if !found {
		if flags.Required {
			return false, nil, sqlerrors.NewUndefinedDatabaseError(name)
		}
		return false, nil, nil
	}
	db, ok := desc.(catalog.DatabaseDescriptor)
	if !ok {
		if flags.Required {
			return false, nil, sqlerrors.NewUndefinedDatabaseError(name)
		}
		return false, nil, nil
	}
	if dropped, err := filterDescriptorState(db, flags.Required, flags); err != nil || dropped {
		return false, nil, err
	}
	return true, db, nil
}

// GetObjectDesc looks up an object by name and returns both its
// descriptor and that of its parent database. If the object is not
// found and flags.required is true, an error is returned, otherwise
// a nil reference is returned.
//
// TODO(ajwerner): clarify the purpose of the transaction here. It's used in
// some cases for some lookups but not in others. For example, if a mutable
// descriptor is requested, it will be utilized however if an immutable
// descriptor is requested then it will only be used for its timestamp and to
// set the deadline.
func (tc *Collection) GetObjectDesc(
	ctx context.Context, txn *kv.Txn, db, schema, object string, flags tree.ObjectLookupFlags,
) (desc catalog.Descriptor, err error) {
	if isVirtual, desc, err := tc.maybeGetVirtualObjectDesc(
		schema, object, flags, db,
	); isVirtual || err != nil {
		return desc, err
	}
	// Resolve type aliases which are usually available in the PostgreSQL as an extension
	// on the public schema.
	// TODO(ajwerner): Pull this underneath type resolution.
	if schema == tree.PublicSchema && flags.DesiredObjectKind == tree.TypeObject {
		if alias, ok := types.PublicSchemaAliases[object]; ok {
			if flags.RequireMutable {
				return nil, errors.AssertionFailedf(
					"cannot use mutable descriptor of aliased type %s.%s", schema, object)
			}
			return typedesc.MakeSimpleAlias(alias, keys.PublicSchemaID), nil
		}
	}
	// Fall back to physical descriptor access.
	switch flags.DesiredObjectKind {
	case tree.TypeObject:
		typeName := tree.MakeNewQualifiedTypeName(db, schema, object)
		_, desc, err := tc.getTypeByName(ctx, txn, &typeName, flags)
		return desc, err
	case tree.TableObject:
		tableName := tree.MakeTableNameWithSchema(tree.Name(db), tree.Name(schema), tree.Name(object))
		_, desc, err := tc.getTableByName(ctx, txn, &tableName, flags)
		return desc, err
	default:
		return nil, errors.AssertionFailedf("unknown desired object kind %d", flags.DesiredObjectKind)
	}
}

func (tc *Collection) maybeGetVirtualObjectDesc(
	schema string, object string, flags tree.ObjectLookupFlags, db string,
) (isVirtual bool, _ catalog.Descriptor, _ error) {
	if tc.virtualSchemas == nil {
		return false, nil, nil
	}
	scEntry, ok := tc.virtualSchemas.GetVirtualSchema(schema)
	if !ok {
		return false, nil, nil
	}
	desc, err := scEntry.GetObjectByName(object, flags)
	if err != nil {
		return true, nil, err
	}
	if desc == nil {
		if flags.Required {
			obj := tree.NewQualifiedObjectName(db, schema, object, flags.DesiredObjectKind)
			return true, nil, sqlerrors.NewUndefinedObjectError(obj, flags.DesiredObjectKind)
		}
		return true, nil, nil
	}
	if flags.RequireMutable {
		return true, nil, catalog.NewMutableAccessToVirtualSchemaError(scEntry, object)
	}
	return true, desc.Desc(), nil
}

func (tc *Collection) getObjectByName(
	ctx context.Context,
	txn *kv.Txn,
	catalogName, schemaName, objectName string,
	flags tree.ObjectLookupFlags,
) (found bool, _ catalog.Descriptor, err error) {

	// If we're reading the object descriptor from the store,
	// we should read its parents from the store too to ensure
	// that subsequent name resolution finds the latest name
	// in the face of a concurrent rename.
	avoidCachedForParent := flags.AvoidCached || flags.RequireMutable
	// Resolve the database.
	found, db, err := tc.GetImmutableDatabaseByName(ctx, txn, catalogName,
		tree.DatabaseLookupFlags{
			Required:       flags.Required,
			AvoidCached:    avoidCachedForParent,
			IncludeDropped: flags.IncludeDropped,
			IncludeOffline: flags.IncludeOffline,
		})
	if err != nil || !found {
		return false, nil, err
	}
	dbID := db.GetID()

	// Resolve the schema.
	foundSchema, resolvedSchema, err := tc.GetImmutableSchemaByName(ctx, txn, dbID, schemaName,
		tree.SchemaLookupFlags{
			Required:       flags.Required,
			AvoidCached:    avoidCachedForParent,
			IncludeDropped: flags.IncludeDropped,
			IncludeOffline: flags.IncludeOffline,
		})
	if err != nil || !foundSchema {
		return false, nil, err
	}
	schemaID := resolvedSchema.ID

	if found, refuseFurtherLookup, desc, err := tc.getSyntheticOrUncommittedDescriptor(
		dbID, schemaID, objectName, flags.RequireMutable,
	); err != nil || refuseFurtherLookup {
		return false, nil, err
	} else if found {
		log.VEventf(ctx, 2, "found uncommitted descriptor %d", desc.GetID())
		return true, desc, nil
	}

	// TODO(vivek): Ideally we'd avoid caching for only the
	// system.descriptor and system.lease tables, because they are
	// used for acquiring leases, creating a chicken&egg problem.
	// But doing so turned problematic and the tests pass only by also
	// disabling caching of system.eventlog, system.rangelog, and
	// system.users. For now we're sticking to disabling caching of
	// all system descriptors except role_members, role_options, and users
	// (i.e., the ones used during authn/authz flows).
	// TODO (lucy): Reevaluate the above. We have many more system tables now and
	// should be able to lease most of them.
	isAllowedSystemTable := objectName == systemschema.RoleMembersTable.GetName() ||
		objectName == systemschema.RoleOptionsTable.GetName() ||
		objectName == systemschema.UsersTable.GetName() ||
		objectName == systemschema.JobsTable.GetName() ||
		objectName == systemschema.EventLogTable.GetName()
	avoidCache := flags.AvoidCached || flags.RequireMutable || lease.TestingTableLeasesAreDisabled() ||
		(catalogName == systemschema.SystemDatabaseName && !isAllowedSystemTable)
	if avoidCache {
		return tc.getDescriptorFromStore(
			ctx, txn, dbID, schemaID, objectName, flags.RequireMutable,
		)
	}

	desc, shouldReadFromStore, err := tc.getLeasedDescriptorByName(
		ctx, txn, dbID, schemaID, objectName)
	if err != nil {
		return false, nil, err
	}
	if shouldReadFromStore {
		return tc.getDescriptorFromStore(
			ctx, txn, dbID, schemaID, objectName, flags.RequireMutable,
		)
	}
	return true, desc, nil
}

// GetMutableTableByName returns a mutable table descriptor with properties
// according to the provided lookup flags. RequireMutable is ignored.
func (tc *Collection) GetMutableTableByName(
	ctx context.Context, txn *kv.Txn, name tree.ObjectName, flags tree.ObjectLookupFlags,
) (found bool, _ *tabledesc.Mutable, _ error) {
	flags.RequireMutable = true
	found, desc, err := tc.getTableByName(ctx, txn, name, flags)
	if err != nil || !found {
		return false, nil, err
	}
	return true, desc.(*tabledesc.Mutable), nil
}

// GetImmutableTableByName returns a mutable table descriptor with properties
// according to the provided lookup flags. RequireMutable is ignored.
func (tc *Collection) GetImmutableTableByName(
	ctx context.Context, txn *kv.Txn, name tree.ObjectName, flags tree.ObjectLookupFlags,
) (found bool, _ catalog.TableDescriptor, _ error) {
	flags.RequireMutable = false
	found, desc, err := tc.getTableByName(ctx, txn, name, flags)
	if err != nil || !found {
		return false, nil, err
	}
	return true, desc, nil
}

// getTableByName returns a table descriptor with properties according to the
// provided lookup flags.
func (tc *Collection) getTableByName(
	ctx context.Context, txn *kv.Txn, name tree.ObjectName, flags tree.ObjectLookupFlags,
) (found bool, _ catalog.TableDescriptor, err error) {
	found, desc, err := tc.getObjectByName(
		ctx, txn, name.Catalog(), name.Schema(), name.Object(), flags)
	if err != nil {
		return false, nil, err
	} else if !found {
		if flags.Required {
			return false, nil, sqlerrors.NewUndefinedRelationError(name)
		}
		return false, nil, nil
	}
	table, ok := desc.(catalog.TableDescriptor)
	if !ok {
		if flags.Required {
			return false, nil, sqlerrors.NewUndefinedRelationError(name)
		}
		return false, nil, nil
	}
	if table.Adding() && table.IsUncommittedVersion() &&
		(flags.RequireMutable || flags.CommonLookupFlags.AvoidCached) {
		// Special case: We always return tables in the adding state if they were
		// created in the same transaction and a descriptor (effectively) read in
		// the same transaction is requested. What this basically amounts to is
		// resolving adding descriptors only for DDLs (etc.).
		// TODO (lucy): I'm not sure where this logic should live. We could add an
		// IncludeAdding flag and pull the special case handling up into the
		// callers. Figure that out after we clean up the name resolution layers
		// and it becomes more clear what the callers should be.
		return true, table, nil
	}
	if dropped, err := filterDescriptorState(
		table, flags.Required, flags.CommonLookupFlags,
	); err != nil || dropped {
		return false, nil, err
	}
	hydrated, err := tc.hydrateTypesInTableDesc(ctx, txn, table)
	if err != nil {
		return false, nil, err
	}

	return true, hydrated, nil
}

// GetMutableTypeByName returns a mutable type descriptor with properties
// according to the provided lookup flags. RequireMutable is ignored.
func (tc *Collection) GetMutableTypeByName(
	ctx context.Context, txn *kv.Txn, name tree.ObjectName, flags tree.ObjectLookupFlags,
) (found bool, _ *typedesc.Mutable, _ error) {
	flags.RequireMutable = true
	found, desc, err := tc.getTypeByName(ctx, txn, name, flags)
	if err != nil || !found {
		return false, nil, err
	}
	return true, desc.(*typedesc.Mutable), nil
}

// GetImmutableTypeByName returns a mutable type descriptor with properties
// according to the provided lookup flags. RequireMutable is ignored.
func (tc *Collection) GetImmutableTypeByName(
	ctx context.Context, txn *kv.Txn, name tree.ObjectName, flags tree.ObjectLookupFlags,
) (found bool, _ catalog.TypeDescriptor, _ error) {
	flags.RequireMutable = false
	return tc.getTypeByName(ctx, txn, name, flags)
}

// getTypeByName returns a type descriptor with properties according to the
// provided lookup flags.
func (tc *Collection) getTypeByName(
	ctx context.Context, txn *kv.Txn, name tree.ObjectName, flags tree.ObjectLookupFlags,
) (found bool, _ catalog.TypeDescriptor, err error) {
	found, desc, err := tc.getObjectByName(
		ctx, txn, name.Catalog(), name.Schema(), name.Object(), flags)
	if err != nil {
		return false, nil, err
	} else if !found {
		if flags.Required {
			return false, nil, sqlerrors.NewUndefinedTypeError(name)
		}
		return false, nil, nil
	}
	typ, ok := desc.(catalog.TypeDescriptor)
	if !ok {
		if flags.Required {
			return false, nil, sqlerrors.NewUndefinedTypeError(name)
		}
		return false, nil, nil
	}
	if dropped, err := filterDescriptorState(typ, flags.Required, flags.CommonLookupFlags); err != nil || dropped {
		return false, nil, err
	}
	return true, typ, nil
}

// TODO (lucy): Should this just take a database name? We're separately
// resolving the database name in lots of places where we (indirectly) call
// this.
func (tc *Collection) getUserDefinedSchemaByName(
	ctx context.Context, txn *kv.Txn, dbID descpb.ID, schemaName string, flags tree.SchemaLookupFlags,
) (catalog.SchemaDescriptor, error) {
	getSchemaByName := func() (found bool, _ catalog.Descriptor, err error) {
		if descFound, refuseFurtherLookup, desc, err := tc.getSyntheticOrUncommittedDescriptor(
			dbID, keys.RootNamespaceID, schemaName, flags.RequireMutable,
		); err != nil || refuseFurtherLookup {
			return false, nil, err
		} else if descFound {
			log.VEventf(ctx, 2, "found uncommitted descriptor %d", desc.GetID())
			return true, desc, nil
		}

		if flags.AvoidCached || flags.RequireMutable || lease.TestingTableLeasesAreDisabled() {
			return tc.getDescriptorFromStore(
				ctx, txn, dbID, keys.RootNamespaceID, schemaName, flags.RequireMutable,
			)
		}

		// Look up whether the schema is on the database descriptor and return early
		// if it's not.
		_, dbDesc, err := tc.GetImmutableDatabaseByID(
			ctx, txn, dbID, tree.DatabaseLookupFlags{Required: true},
		)
		if err != nil {
			return false, nil, err
		}
		schemaID := dbDesc.GetSchemaID(schemaName)
		if schemaID == descpb.InvalidID {
			return false, nil, nil
		}
		foundSchemaName := dbDesc.GetNonDroppedSchemaName(schemaID)
		if foundSchemaName != schemaName {
			// If there's another schema name entry with the same ID as this one, then
			// the schema has been renamed, so don't return anything.
			if foundSchemaName != "" {
				return false, nil, nil
			}
			// Otherwise, the schema has been dropped. Return early, except in the
			// specific case where flags.Required and flags.IncludeDropped are both
			// true, which forces us to look up the dropped descriptor and return it.
			if !flags.Required {
				return false, nil, nil
			}
			if !flags.IncludeDropped {
				return false, nil, catalog.NewInactiveDescriptorError(catalog.ErrDescriptorDropped)
			}
		}

		// If we have a schema ID from the database, get the schema descriptor. Since
		// the schema and database descriptors are updated in the same transaction,
		// their leased "versions" (not the descriptor version, but the state in the
		// abstract sequence of states in adding, renaming, or dropping a schema) can
		// differ by at most 1 while waiting for old leases to drain. So false
		// negatives can occur from the database lookup, in some sense, if we have a
		// lease on the latest version of the schema and on the previous version of
		// the database which doesn't reflect the changes to the schema. But this
		// isn't a problem for correctness; it can only happen on other sessions
		// before the schema change has returned results.
		desc, err := tc.getDescriptorByID(ctx, txn, schemaID, flags)
		if err != nil {
			if errors.Is(err, catalog.ErrDescriptorNotFound) ||
				errors.Is(err, catalog.ErrDescriptorDropped) {
				return false, nil, nil
			}
			return false, nil, err
		}
		return true, desc, nil
	}

	found, desc, err := getSchemaByName()
	if err != nil {
		return nil, err
	} else if !found {
		if flags.Required {
			return nil, sqlerrors.NewUndefinedSchemaError(schemaName)
		}
		return nil, nil
	}
	schema, ok := desc.(catalog.SchemaDescriptor)
	if !ok {
		if flags.Required {
			return nil, sqlerrors.NewUndefinedSchemaError(schemaName)
		}
		return nil, nil
	}
	if dropped, err := filterDescriptorState(schema, flags.Required, flags); dropped || err != nil {
		return nil, err
	}
	return schema, nil
}

// filterDescriptorState wraps the more general catalog function to swallow
// the error if the descriptor is being dropped and the descriptor is not
// required. In that case, dropped will be true. A return value of false, nil
// means this descriptor is okay given the flags.
// TODO (lucy): We would like the ByID methods to ignore the Required flag and
// unconditionally return an error for dropped descriptors if IncludeDropped is
// not set, so we can't just pass the flags passed into the methods into this
// function, hence the boolean argument. This is the only user of
// catalog.FilterDescriptorState which needs to pass in nontrivial flags, at
// time of writing, so we should clean up the interface around this bit of
// functionality.
func filterDescriptorState(
	desc catalog.Descriptor, required bool, flags tree.CommonLookupFlags,
) (dropped bool, _ error) {
	flags = tree.CommonLookupFlags{
		Required:       required,
		IncludeOffline: flags.IncludeOffline,
		IncludeDropped: flags.IncludeDropped,
	}
	if err := catalog.FilterDescriptorState(desc, flags); err != nil {
		if required || !errors.Is(err, catalog.ErrDescriptorDropped) {
			return false, err
		}
		return true, nil
	}
	return false, nil
}

// GetMutableSchemaByName resolves the schema and, if applicable, returns a
// mutable descriptor usable by the transaction. RequireMutable is ignored.
func (tc *Collection) GetMutableSchemaByName(
	ctx context.Context, txn *kv.Txn, dbID descpb.ID, schemaName string, flags tree.SchemaLookupFlags,
) (bool, catalog.ResolvedSchema, error) {
	flags.RequireMutable = true
	return tc.getSchemaByName(ctx, txn, dbID, schemaName, flags)
}

// GetImmutableSchemaByName resolves the schema and, if applicable, returns an
// immutable descriptor usable by the transaction. RequireMutable is ignored.
func (tc *Collection) GetImmutableSchemaByName(
	ctx context.Context, txn *kv.Txn, dbID descpb.ID, schemaName string, flags tree.SchemaLookupFlags,
) (bool, catalog.ResolvedSchema, error) {
	flags.RequireMutable = false
	return tc.getSchemaByName(ctx, txn, dbID, schemaName, flags)
}

// GetSchemaByName returns true and a ResolvedSchema object if the target schema
// exists under the target database.
func (tc *Collection) GetSchemaByName(
	ctx context.Context, txn *kv.Txn, dbID descpb.ID, schemaName string, flags tree.SchemaLookupFlags,
) (found bool, _ catalog.ResolvedSchema, _ error) {
	return tc.getSchemaByName(ctx, txn, dbID, schemaName, flags)
}

// getSchemaByName resolves the schema and, if applicable, returns a descriptor
// usable by the transaction.
func (tc *Collection) getSchemaByName(
	ctx context.Context, txn *kv.Txn, dbID descpb.ID, schemaName string, flags tree.SchemaLookupFlags,
) (bool, catalog.ResolvedSchema, error) {
	// Fast path public schema, as it is always found.
	if schemaName == tree.PublicSchema {
		return true, catalog.ResolvedSchema{
			ID: keys.PublicSchemaID, Kind: catalog.SchemaPublic, Name: tree.PublicSchema,
		}, nil
	}

	if tc.virtualSchemas != nil {
		if _, ok := tc.virtualSchemas.GetVirtualSchema(schemaName); ok {
			return true, catalog.ResolvedSchema{
				Kind: catalog.SchemaVirtual,
				Name: schemaName,
			}, nil
		}
	}

	// If a temp schema is requested, check if it's for the current session, or
	// else fall back to reading from the store.
	if strings.HasPrefix(schemaName, sessiondata.PgTempSchemaName) {
		if tc.sessionData != nil {
			if schemaName == sessiondata.PgTempSchemaName ||
				schemaName == tc.sessionData.SearchPath.GetTemporarySchemaName() {
				schemaID, found := tc.sessionData.GetTemporarySchemaIDForDb(uint32(dbID))
				if found {
					schema := catalog.ResolvedSchema{
						Kind: catalog.SchemaTemporary,
						Name: tc.sessionData.SearchPath.GetTemporarySchemaName(),
						ID:   descpb.ID(schemaID),
					}
					return true, schema, nil
				}
			}
		}
		exists, schemaID, err := catalogkv.ResolveSchemaID(ctx, txn, tc.codec(), dbID, schemaName)
		if err != nil {
			return false, catalog.ResolvedSchema{}, err
		} else if !exists {
			if flags.Required {
				return false, catalog.ResolvedSchema{}, sqlerrors.NewUndefinedSchemaError(schemaName)
			}
			return false, catalog.ResolvedSchema{}, nil
		}
		schema := catalog.ResolvedSchema{
			Kind: catalog.SchemaTemporary,
			Name: schemaName,
			ID:   schemaID,
		}
		return true, schema, nil
	}

	// Otherwise, the schema is user-defined. Get the descriptor.
	desc, err := tc.getUserDefinedSchemaByName(ctx, txn, dbID, schemaName, flags)
	if err != nil || desc == nil {
		return false, catalog.ResolvedSchema{}, err
	}
	return true, catalog.ResolvedSchema{
		Kind: catalog.SchemaUserDefined,
		Name: schemaName,
		ID:   desc.GetID(),
		Desc: desc,
	}, nil
}

// GetMutableDatabaseByID returns a mutable database descriptor with
// properties according to the provided lookup flags. RequireMutable is ignored.
func (tc *Collection) GetMutableDatabaseByID(
	ctx context.Context, txn *kv.Txn, dbID descpb.ID, flags tree.DatabaseLookupFlags,
) (bool, *dbdesc.Mutable, error) {
	flags.RequireMutable = true
	found, desc, err := tc.getDatabaseByID(ctx, txn, dbID, flags)
	if err != nil || !found {
		return false, nil, err
	}
	return true, desc.(*dbdesc.Mutable), nil
}

var _ = (*Collection)(nil).GetMutableDatabaseByID

// GetImmutableDatabaseByID returns an immutable database descriptor with
// properties according to the provided lookup flags. RequireMutable is ignored.
func (tc *Collection) GetImmutableDatabaseByID(
	ctx context.Context, txn *kv.Txn, dbID descpb.ID, flags tree.DatabaseLookupFlags,
) (bool, catalog.DatabaseDescriptor, error) {
	flags.RequireMutable = false
	return tc.getDatabaseByID(ctx, txn, dbID, flags)
}

func (tc *Collection) getDatabaseByID(
	ctx context.Context, txn *kv.Txn, dbID descpb.ID, flags tree.DatabaseLookupFlags,
) (bool, catalog.DatabaseDescriptor, error) {
	desc, err := tc.getDescriptorByID(ctx, txn, dbID, flags)
	if err != nil {
		if errors.Is(err, catalog.ErrDescriptorNotFound) {
			if flags.Required {
				return false, nil, sqlerrors.NewUndefinedDatabaseError(fmt.Sprintf("[%d]", dbID))
			}
			return false, nil, nil
		}
		return false, nil, err
	}
	db, ok := desc.(catalog.DatabaseDescriptor)
	if !ok {
		return false, nil, sqlerrors.NewUndefinedDatabaseError(fmt.Sprintf("[%d]", dbID))
	}
	return true, db, nil
}

// GetMutableTableByID returns a mutable table descriptor with
// properties according to the provided lookup flags. RequireMutable is ignored.
// Required is ignored, and an error is always returned if no descriptor with
// the ID exists.
func (tc *Collection) GetMutableTableByID(
	ctx context.Context, txn *kv.Txn, tableID descpb.ID, flags tree.ObjectLookupFlags,
) (*tabledesc.Mutable, error) {
	flags.RequireMutable = true
	desc, err := tc.getTableByID(ctx, txn, tableID, flags)
	if err != nil {
		return nil, err
	}
	return desc.(*tabledesc.Mutable), nil
}

// GetImmutableTableByID returns an immutable table descriptor with
// properties according to the provided lookup flags. RequireMutable is ignored.
// Required is ignored, and an error is always returned if no descriptor with
// the ID exists.
func (tc *Collection) GetImmutableTableByID(
	ctx context.Context, txn *kv.Txn, tableID descpb.ID, flags tree.ObjectLookupFlags,
) (catalog.TableDescriptor, error) {
	flags.RequireMutable = false
	desc, err := tc.getTableByID(ctx, txn, tableID, flags)
	if err != nil {
		return nil, err
	}
	return desc, nil
}

func (tc *Collection) getTableByID(
	ctx context.Context, txn *kv.Txn, tableID descpb.ID, flags tree.ObjectLookupFlags,
) (catalog.TableDescriptor, error) {
	desc, err := tc.getDescriptorByID(ctx, txn, tableID, flags.CommonLookupFlags)
	if err != nil {
		if errors.Is(err, catalog.ErrDescriptorNotFound) {
			return nil, sqlerrors.NewUndefinedRelationError(
				&tree.TableRef{TableID: int64(tableID)})
		}
		return nil, err
	}
	table, ok := desc.(catalog.TableDescriptor)
	if !ok {
		return nil, sqlerrors.NewUndefinedRelationError(
			&tree.TableRef{TableID: int64(tableID)})
	}
	hydrated, err := tc.hydrateTypesInTableDesc(ctx, txn, table)
	if err != nil {
		return nil, err
	}
	return hydrated, nil
}

func (tc *Collection) getDescriptorByID(
	ctx context.Context, txn *kv.Txn, id descpb.ID, flags tree.CommonLookupFlags,
) (catalog.Descriptor, error) {
	return tc.getDescriptorByIDMaybeSetTxnDeadline(
		ctx, txn, id, flags, false /* setTxnDeadline */)
}

// SkipValidationOnWrite avoids validating uncommitted descriptors prior to
// a transaction commit.
func (tc *Collection) SkipValidationOnWrite() {
	tc.skipValidationOnWrite = true
}

// getDescriptorByIDMaybeSetTxnDeadline returns a descriptor according to the
// provided lookup flags. Note that flags.Required is ignored, and an error is
// always returned if no descriptor with the ID exists.
func (tc *Collection) getDescriptorByIDMaybeSetTxnDeadline(
	ctx context.Context, txn *kv.Txn, id descpb.ID, flags tree.CommonLookupFlags, setTxnDeadline bool,
) (catalog.Descriptor, error) {
	readFromStore := func() (catalog.Descriptor, error) {
		// Always pick up a mutable copy so it can be cached.
		// TODO (lucy): If the descriptor doesn't exist, should we generate our
		// own error here instead of using the one from catalogkv?
		desc, err := catalogkv.MustGetMutableDescriptorByID(ctx, txn, tc.codec(), id)
		if err != nil {
			return nil, err
		}
		ud, err := tc.addUncommittedDescriptor(desc)
		if err != nil {
			return nil, err
		}
		if !flags.RequireMutable {
			return ud.immutable, nil
		}
		return desc, nil
	}
	getDescriptorByID := func() (catalog.Descriptor, error) {
		if found, sd := tc.getSyntheticDescriptorByID(id); found {
			if flags.RequireMutable {
				return nil, newMutableSyntheticDescriptorAssertionError(sd.GetID())
			}
			return sd, nil
		}
		if ud := tc.getUncommittedDescriptorByID(id); ud != nil {
			log.VEventf(ctx, 2, "found uncommitted descriptor %d", id)
			if flags.RequireMutable {
				return ud.mutable, nil
			}
			return ud.immutable, nil
		}

		if flags.AvoidCached || flags.RequireMutable || lease.TestingTableLeasesAreDisabled() {
			return readFromStore()
		}

		// If we have already read all of the descriptor, use it as a negative
		// cache to short-circuit a lookup we know will be doomed to fail.
		//
		// TODO(ajwerner): More generally leverage this set of read descriptors on
		// the resolution path.
		if !tc.allDescriptors.isEmpty() && !tc.allDescriptors.contains(id) {
			return nil, catalog.ErrDescriptorNotFound
		}

		desc, err := tc.getLeasedDescriptorByID(ctx, txn, id, setTxnDeadline)
		if err != nil {
			if errors.Is(err, catalog.ErrDescriptorNotFound) || catalog.HasInactiveDescriptorError(err) {
				return readFromStore()
			}
			return nil, err
		}
		return desc, nil
	}

	desc, err := getDescriptorByID()
	if err != nil {
		return nil, err
	}
	if dropped, err := filterDescriptorState(desc, true /* required */, flags); err != nil || dropped {
		// This is a special case for tables in the adding state: Roughly speaking,
		// we always need to resolve tables in the adding state by ID when they were
		// newly created in the transaction for DDL statements and for some
		// information queries (but not for ordinary name resolution for queries/
		// DML), but we also need to make these tables public in the schema change
		// job in a separate transaction.
		// TODO (lucy): We need something like an IncludeAdding flag so that callers
		// can specify this behavior, instead of having the collection infer the
		// desired behavior based on the flags (and likely producing unintended
		// behavior). See the similar comment on getDescriptorByName, which covers
		// the ordinary name resolution path as well as DDL statements.
		if desc.Adding() && (desc.IsUncommittedVersion() || flags.AvoidCached || flags.RequireMutable) {
			return desc, nil
		}
		return nil, err
	}
	return desc, nil
}

// GetMutableTableVersionByID is a variant of sqlbase.getTableDescFromID which returns a mutable
// table descriptor of the table modified in the same transaction.
// Deprecated in favor of GetMutableTableByID.
// TODO (lucy): Usages should be replaced with GetMutableTableByID, but this
// needs a careful look at what flags should be passed in at each call site.
func (tc *Collection) GetMutableTableVersionByID(
	ctx context.Context, tableID descpb.ID, txn *kv.Txn,
) (*tabledesc.Mutable, error) {
	return tc.GetMutableTableByID(ctx, txn, tableID, tree.ObjectLookupFlags{
		CommonLookupFlags: tree.CommonLookupFlags{
			IncludeOffline: true,
			IncludeDropped: true,
		},
	})
}

// GetMutableDescriptorByID returns a mutable implementation of the descriptor
// with the requested id. An error is returned if no descriptor exists.
// Deprecated in favor of GetMutableDescriptorByIDWithFlags.
func (tc *Collection) GetMutableDescriptorByID(
	ctx context.Context, id descpb.ID, txn *kv.Txn,
) (catalog.MutableDescriptor, error) {
	return tc.GetMutableDescriptorByIDWithFlags(ctx, txn, id, tree.CommonLookupFlags{
		IncludeOffline: true,
		IncludeDropped: true,
	})
}

// GetMutableDescriptorByIDWithFlags returns a mutable implementation of the
// descriptor with the requested id. An error is returned if no descriptor exists.
// TODO (lucy): This is meant to replace GetMutableDescriptorByID. Once it does,
// rename this function.
func (tc *Collection) GetMutableDescriptorByIDWithFlags(
	ctx context.Context, txn *kv.Txn, id descpb.ID, flags tree.CommonLookupFlags,
) (catalog.MutableDescriptor, error) {
	log.VEventf(ctx, 2, "planner getting mutable descriptor for id %d", id)
	flags.RequireMutable = true
	desc, err := tc.getDescriptorByID(ctx, txn, id, flags)
	if err != nil {
		return nil, err
	}
	return desc.(catalog.MutableDescriptor), nil
}

// GetImmutableDescriptorByID returns an immmutable implementation of the
// descriptor with the requested id. An error is returned if no descriptor exists.
// Deprecated in favor of GetMutableDescriptorByIDWithFlags.
func (tc *Collection) GetImmutableDescriptorByID(
	ctx context.Context, txn *kv.Txn, id descpb.ID, flags tree.CommonLookupFlags,
) (catalog.Descriptor, error) {
	log.VEventf(ctx, 2, "planner getting immutable descriptor for id %d", id)
	flags.RequireMutable = false
	return tc.getDescriptorByID(ctx, txn, id, flags)
}

// GetMutableSchemaByID returns a ResolvedSchema wrapping a mutable
// descriptor, if applicable. RequireMutable is ignored.
// Required is ignored, and an error is always returned if no descriptor with
// the ID exists.
func (tc *Collection) GetMutableSchemaByID(
	ctx context.Context, txn *kv.Txn, schemaID descpb.ID, flags tree.SchemaLookupFlags,
) (catalog.ResolvedSchema, error) {
	flags.RequireMutable = true
	return tc.getSchemaByID(ctx, txn, schemaID, flags)
}

var _ = (*Collection)(nil).GetMutableSchemaByID

// GetImmutableSchemaByID returns a ResolvedSchema wrapping an immutable
// descriptor, if applicable. RequireMutable is ignored.
// Required is ignored, and an error is always returned if no descriptor with
// the ID exists.
func (tc *Collection) GetImmutableSchemaByID(
	ctx context.Context, txn *kv.Txn, schemaID descpb.ID, flags tree.SchemaLookupFlags,
) (catalog.ResolvedSchema, error) {
	flags.RequireMutable = false
	return tc.getSchemaByID(ctx, txn, schemaID, flags)
}

func (tc *Collection) getSchemaByID(
	ctx context.Context, txn *kv.Txn, schemaID descpb.ID, flags tree.SchemaLookupFlags,
) (catalog.ResolvedSchema, error) {
	if schemaID == keys.PublicSchemaID {
		return catalog.ResolvedSchema{
			Kind: catalog.SchemaPublic,
			ID:   schemaID,
			Name: tree.PublicSchema,
		}, nil
	}

	// We have already considered if the schemaID is PublicSchemaID,
	// if the id appears in staticSchemaIDMap, it must map to a virtual schema.
	if scName, ok := resolver.StaticSchemaIDMap[schemaID]; ok {
		return catalog.ResolvedSchema{
			Kind: catalog.SchemaVirtual,
			ID:   schemaID,
			Name: scName,
		}, nil
	}

	// If this collection is attached to a session and the session has created
	// a temporary schema, then check if the schema ID matches.
	if tc.sessionData != nil && tc.sessionData.IsTemporarySchemaID(uint32(schemaID)) {
		return catalog.ResolvedSchema{
			Kind: catalog.SchemaTemporary,
			ID:   schemaID,
			Name: tc.sessionData.SearchPath.GetTemporarySchemaName(),
		}, nil
	}

	// Otherwise, fall back to looking up the descriptor with the desired ID.
	desc, err := tc.getDescriptorByID(ctx, txn, schemaID, flags)
	if err != nil {
		return catalog.ResolvedSchema{}, err
	}

	schemaDesc, ok := desc.(catalog.SchemaDescriptor)
	if !ok {
		return catalog.ResolvedSchema{}, pgerror.Newf(pgcode.WrongObjectType,
			"descriptor %d was not a schema", schemaID)
	}

	return catalog.ResolvedSchema{
		Kind: catalog.SchemaUserDefined,
		ID:   schemaID,
		Desc: schemaDesc,
		Name: schemaDesc.GetName(),
	}, nil
}

// hydrateTypesInTableDesc installs user defined type metadata in all types.T
// present in the input TableDescriptor. It always returns the same type of
// TableDescriptor that was passed in. It ensures that ImmutableTableDescriptors
// are not modified during the process of metadata installation. Dropped tables
// do not get hydrated.
//
// TODO(ajwerner): This should accept flags to indicate whether we can resolve
// offline descriptors.
func (tc *Collection) hydrateTypesInTableDesc(
	ctx context.Context, txn *kv.Txn, desc catalog.TableDescriptor,
) (catalog.TableDescriptor, error) {
	if desc.Dropped() {
		return desc, nil
	}
	switch t := desc.(type) {
	case *tabledesc.Mutable:
		// It is safe to hydrate directly into Mutable since it is
		// not shared. When hydrating mutable descriptors, use the mutable access
		// method to access types.
		getType := func(ctx context.Context, id descpb.ID) (tree.TypeName, catalog.TypeDescriptor, error) {
			desc, err := tc.GetMutableTypeVersionByID(ctx, txn, id)
			if err != nil {
				return tree.TypeName{}, nil, err
			}
			dbDesc, err := tc.GetMutableDescriptorByID(ctx, desc.ParentID, txn)
			if err != nil {
				return tree.TypeName{}, nil, err
			}
			sc, err := tc.getSchemaByID(
				ctx, txn, desc.ParentSchemaID,
				tree.SchemaLookupFlags{
					IncludeOffline: true,
					RequireMutable: true,
				},
			)
			if err != nil {
				return tree.TypeName{}, nil, err
			}
			name := tree.MakeNewQualifiedTypeName(dbDesc.GetName(), sc.Name, desc.Name)
			return name, desc, nil
		}

		return desc, typedesc.HydrateTypesInTableDescriptor(ctx, t.TableDesc(), typedesc.TypeLookupFunc(getType))
	case catalog.TableDescriptor:
		// ImmutableTableDescriptors need to be copied before hydration, because
		// they are potentially read by multiple threads. If there aren't any user
		// defined types in the descriptor, then return early.
		if !t.ContainsUserDefinedTypes() {
			return desc, nil
		}

		getType := typedesc.TypeLookupFunc(func(
			ctx context.Context, id descpb.ID,
		) (tree.TypeName, catalog.TypeDescriptor, error) {
			desc, err := tc.GetImmutableTypeByID(ctx, txn, id, tree.ObjectLookupFlags{})
			if err != nil {
				return tree.TypeName{}, nil, err
			}
			_, dbDesc, err := tc.GetImmutableDatabaseByID(ctx, txn, desc.GetParentID(),
				tree.DatabaseLookupFlags{Required: true})
			if err != nil {
				return tree.TypeName{}, nil, err
			}
			sc, err := tc.GetImmutableSchemaByID(
				ctx, txn, desc.GetParentSchemaID(), tree.SchemaLookupFlags{})
			if err != nil {
				return tree.TypeName{}, nil, err
			}
			name := tree.MakeNewQualifiedTypeName(dbDesc.GetName(), sc.Name, desc.GetName())
			return name, desc, nil
		})

		// Utilize the cache of hydrated tables if we have one.
		if tc.hydratedTables != nil {
			hydrated, err := tc.hydratedTables.GetHydratedTableDescriptor(ctx, t, getType)
			if err != nil {
				return nil, err
			}
			if hydrated != nil {
				return hydrated, nil
			}
			// The cache decided not to give back a hydrated descriptor, likely
			// because either we've modified the table or one of the types or because
			// this transaction has a stale view of one of the relevant descriptors.
			// Proceed to hydrating a fresh copy.
		}

		// Make a copy of the underlying descriptor before hydration.
		descBase := protoutil.Clone(t.TableDesc()).(*descpb.TableDescriptor)
		if err := typedesc.HydrateTypesInTableDescriptor(ctx, descBase, getType); err != nil {
			return nil, err
		}
		if t.IsUncommittedVersion() {
			return tabledesc.NewBuilderForUncommittedVersion(descBase).BuildImmutableTable(), nil
		}
		return tabledesc.NewBuilder(descBase).BuildImmutableTable(), nil
	default:
		return desc, nil
	}
}

// ReleaseSpecifiedLeases releases the leases for the descriptors with ids in
// the passed slice. Errors are logged but ignored.
func (tc *Collection) ReleaseSpecifiedLeases(ctx context.Context, descs []lease.IDVersion) {
	ids := make([]descpb.ID, len(descs))
	for i := range descs {
		ids[i] = descs[i].ID
	}
	toRelease := tc.leasedDescriptors.release(ids)
	for _, desc := range toRelease {
		desc.Release(ctx)
	}
}

// ReleaseLeases releases all leases. Errors are logged but ignored.
func (tc *Collection) ReleaseLeases(ctx context.Context) {
	log.VEventf(ctx, 2, "releasing %d descriptors", tc.leasedDescriptors.numDescriptors())
	for _, desc := range tc.leasedDescriptors.releaseAll() {
		desc.Release(ctx)
	}
}

// ReleaseAll releases all state currently held by the Collection.
// ReleaseAll calls ReleaseLeases.
func (tc *Collection) ReleaseAll(ctx context.Context) {
	tc.ReleaseLeases(ctx)
	tc.uncommittedDescriptors = nil
	tc.syntheticDescriptors = nil
	tc.releaseAllDescriptors()
}

// HasUncommittedTables returns true if the Collection contains uncommitted
// tables.
func (tc *Collection) HasUncommittedTables() bool {
	for _, desc := range tc.uncommittedDescriptors {
		if _, isTable := desc.immutable.(catalog.TableDescriptor); isTable {
			return true
		}
	}
	return false
}

// HasUncommittedTypes returns true if the Collection contains uncommitted
// types.
func (tc *Collection) HasUncommittedTypes() bool {
	for _, desc := range tc.uncommittedDescriptors {
		if _, isType := desc.immutable.(catalog.TypeDescriptor); isType {
			return true
		}
	}
	return false
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
	_, err := tc.addUncommittedDescriptor(desc)
	return err
}

// maybeRefreshCachedFieldsOnTypeDescriptor refreshes the cached fields on a
// Mutable if the given descriptor is a type descriptor and works as a pass
// through for all other descriptors. Mutable type descriptors are refreshed to
// reconstruct enumMetadata. This ensures that tables hydration following a
// type descriptor update (in the same txn) happens using the modified fields.
func maybeRefreshCachedFieldsOnTypeDescriptor(
	desc catalog.MutableDescriptor,
) (catalog.MutableDescriptor, error) {
	typeDesc, ok := desc.(catalog.TypeDescriptor)
	if ok {
		return typedesc.UpdateCachedFieldsOnModifiedMutable(typeDesc)
	}
	return desc, nil
}

func (tc *Collection) addUncommittedDescriptor(
	desc catalog.MutableDescriptor,
) (*uncommittedDescriptor, error) {
	version := desc.GetVersion()
	origVersion := desc.OriginalVersion()
	if version != origVersion && version != origVersion+1 {
		return nil, errors.AssertionFailedf(
			"descriptor %d version %d not compatible with cluster version %d",
			desc.GetID(), version, origVersion)
	}

	mutable, err := maybeRefreshCachedFieldsOnTypeDescriptor(desc)
	if err != nil {
		return nil, err
	}

	ud := &uncommittedDescriptor{
		mutable:   mutable,
		immutable: desc.ImmutableCopy(),
	}

	var found bool
	for i, d := range tc.uncommittedDescriptors {
		if d.mutable.GetID() == desc.GetID() {
			tc.uncommittedDescriptors[i], found = ud, true
			break
		}
	}
	if !found {
		tc.uncommittedDescriptors = append(tc.uncommittedDescriptors, ud)
	}
	tc.releaseAllDescriptors()
	return ud, nil
}

// ValidateOnWriteEnabled is the cluster setting used to enable or disable
// validating descriptors prior to writing.
var ValidateOnWriteEnabled = settings.RegisterBoolSetting(
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
		if err := catalog.ValidateSelf(desc); err != nil {
			return err
		}
	}
	if err := tc.AddUncommittedDescriptor(desc); err != nil {
		return err
	}
	return catalogkv.WriteDescToBatch(ctx, kvTrace, tc.settings, b, tc.codec(), desc.GetID(), desc)
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
func (tc *Collection) GetDescriptorsWithNewVersion() []lease.IDVersion {
	var descs []lease.IDVersion
	for _, desc := range tc.uncommittedDescriptors {
		if mut := desc.mutable; !mut.IsNew() && mut.IsUncommittedVersion() {
			descs = append(descs, lease.NewIDVersionPrev(mut.OriginalName(), mut.OriginalID(), mut.OriginalVersion()))
		}
	}
	return descs
}

// GetUncommittedTables returns all the tables updated or created in the
// transaction.
func (tc *Collection) GetUncommittedTables() (tables []catalog.TableDescriptor) {
	for _, desc := range tc.uncommittedDescriptors {
		table, ok := desc.immutable.(catalog.TableDescriptor)
		if ok && desc.immutable.IsUncommittedVersion() {
			tables = append(tables, table)
		}
	}
	return tables
}

// ValidateUncommittedDescriptors validates all uncommitted descriptors.
// Validation includes cross-reference checks. Referenced descriptors are
// read from the store unless they happen to also be part of the uncommitted
// descriptor set. We purposefully avoid using leased descriptors as those may
// be one version behind, in which case it's possible (and legitimate) that
// those are missing back-references which would cause validation to fail.
func (tc *Collection) ValidateUncommittedDescriptors(ctx context.Context, txn *kv.Txn) error {
	if tc.skipValidationOnWrite || !ValidateOnWriteEnabled.Get(&tc.settings.SV) {
		return nil
	}
	descs := make([]catalog.Descriptor, len(tc.uncommittedDescriptors))
	for i, ud := range tc.uncommittedDescriptors {
		descs[i] = ud.immutable
	}
	if len(descs) == 0 {
		return nil
	}
	bdg := catalogkv.NewOneLevelUncachedDescGetter(txn, tc.codec())
	return catalog.Validate(
		ctx,
		bdg,
		catalog.ValidationWriteTelemetry,
		catalog.ValidationLevelAllPreTxnCommit,
		descs...,
	).CombinedError()
}

// User defined type accessors.

// GetMutableTypeVersionByID is the equivalent of GetMutableTableDescriptorByID
// but for accessing types.
// Deprecated in favor of GetMutableTypeByID.
// TODO (lucy): Usages should be replaced with GetMutableTypeByID, but this
// needs a careful look at what flags should be passed in at each call site.
func (tc *Collection) GetMutableTypeVersionByID(
	ctx context.Context, txn *kv.Txn, typeID descpb.ID,
) (*typedesc.Mutable, error) {
	return tc.GetMutableTypeByID(ctx, txn, typeID, tree.ObjectLookupFlags{
		CommonLookupFlags: tree.CommonLookupFlags{
			IncludeOffline: true,
			IncludeDropped: true,
		},
	})
}

// GetMutableTypeByID returns a mutable type descriptor with
// properties according to the provided lookup flags. RequireMutable is ignored.
// Required is ignored, and an error is always returned if no descriptor with
// the ID exists.
func (tc *Collection) GetMutableTypeByID(
	ctx context.Context, txn *kv.Txn, typeID descpb.ID, flags tree.ObjectLookupFlags,
) (*typedesc.Mutable, error) {
	flags.RequireMutable = true
	desc, err := tc.getTypeByID(ctx, txn, typeID, flags)
	if err != nil {
		return nil, err
	}
	return desc.(*typedesc.Mutable), nil
}

// GetImmutableTypeByID returns an immutable type descriptor with
// properties according to the provided lookup flags. RequireMutable is ignored.
// Required is ignored, and an error is always returned if no descriptor with
// the ID exists.
func (tc *Collection) GetImmutableTypeByID(
	ctx context.Context, txn *kv.Txn, typeID descpb.ID, flags tree.ObjectLookupFlags,
) (catalog.TypeDescriptor, error) {
	flags.RequireMutable = false
	return tc.getTypeByID(ctx, txn, typeID, flags)
}

func (tc *Collection) getTypeByID(
	ctx context.Context, txn *kv.Txn, typeID descpb.ID, flags tree.ObjectLookupFlags,
) (catalog.TypeDescriptor, error) {
	desc, err := tc.getDescriptorByID(ctx, txn, typeID, flags.CommonLookupFlags)
	if err != nil {
		if errors.Is(err, catalog.ErrDescriptorNotFound) {
			return nil, pgerror.Newf(
				pgcode.UndefinedObject, "type with ID %d does not exist", typeID)
		}
		return nil, err
	}
	typ, ok := desc.(catalog.TypeDescriptor)
	if !ok {
		return nil, pgerror.Newf(
			pgcode.UndefinedObject, "type with ID %d does not exist", typeID)
	}
	return typ, nil
}

// getSyntheticOrUncommittedDescriptor attempts to look up a descriptor in the
// set of synthetic descriptors, followed by the set of uncommitted descriptors.
func (tc *Collection) getSyntheticOrUncommittedDescriptor(
	dbID descpb.ID, schemaID descpb.ID, name string, mutable bool,
) (found bool, refuseFurtherLookup bool, desc catalog.Descriptor, err error) {
	if found, sd := tc.getSyntheticDescriptorByName(
		dbID, schemaID, name); found {
		if mutable {
			return false, false, nil, newMutableSyntheticDescriptorAssertionError(sd.GetID())
		}
		return true, false, sd, nil
	}

	var ud *uncommittedDescriptor
	refuseFurtherLookup, ud = tc.getUncommittedDescriptor(dbID, schemaID, name)
	if ud == nil {
		return false, refuseFurtherLookup, nil, nil
	}
	if mutable {
		return true, false, ud.mutable, nil
	}
	return true, false, ud.immutable, nil
}

// getUncommittedDescriptor returns a descriptor for the requested name
// if the requested name is for a descriptor modified within the transaction
// affiliated with the Collection.
//
// The first return value "refuseFurtherLookup" is true when there is a known
// rename of that descriptor, so it would be invalid to miss the cache and go to
// KV (where the descriptor prior to the rename may still exist).
func (tc *Collection) getUncommittedDescriptor(
	dbID descpb.ID, schemaID descpb.ID, name string,
) (refuseFurtherLookup bool, desc *uncommittedDescriptor) {
	// Walk latest to earliest so that a DROP followed by a CREATE with the same
	// name will result in the CREATE being seen.
	for i := len(tc.uncommittedDescriptors) - 1; i >= 0; i-- {
		desc := tc.uncommittedDescriptors[i]
		mutDesc := desc.mutable
		// If a descriptor has gotten renamed we'd like to disallow using the old
		// names. The renames could have happened in another transaction but it's
		// still okay to disallow the use of the old name in this transaction
		// because the other transaction has already committed and this transaction
		// is seeing the effect of it.
		for _, drain := range mutDesc.GetDrainingNames() {
			if drain.Name == name &&
				drain.ParentID == dbID &&
				drain.ParentSchemaID == schemaID {
				return true, nil
			}
		}

		// Otherwise, if the name matches, we return it. It's up to the caller to
		// filter descriptors in non-public states.
		// TODO (lucy): Is it possible to return dropped descriptors at this point,
		// after the previous draining names check?
		if lease.NameMatchesDescriptor(mutDesc, dbID, schemaID, name) {
			return false, desc
		}
	}
	return false, nil
}

func (tc *Collection) getSyntheticDescriptorByName(
	dbID descpb.ID, schemaID descpb.ID, name string,
) (found bool, desc catalog.Descriptor) {
	for _, sd := range tc.syntheticDescriptors {
		if lease.NameMatchesDescriptor(sd, dbID, schemaID, name) {
			return true, sd
		}
	}
	return false, nil
}

func (tc *Collection) getSyntheticDescriptorByID(
	id descpb.ID,
) (found bool, desc catalog.Descriptor) {
	for _, sd := range tc.syntheticDescriptors {
		if sd.GetID() == id {
			return true, sd
		}
	}
	return false, nil
}

func newMutableSyntheticDescriptorAssertionError(id descpb.ID) error {
	return errors.AssertionFailedf("attempted mutable access of synthetic descriptor %d", id)
}

// GetUncommittedTableByID returns an uncommitted table by its ID.
func (tc *Collection) GetUncommittedTableByID(id descpb.ID) *tabledesc.Mutable {
	if ud := tc.getUncommittedDescriptorByID(id); ud != nil {
		if table, ok := ud.mutable.(*tabledesc.Mutable); ok {
			return table
		}
	}
	return nil
}

func (tc *Collection) getUncommittedDescriptorByID(id descpb.ID) *uncommittedDescriptor {
	for _, desc := range tc.uncommittedDescriptors {
		if desc.mutable.GetID() == id {
			return desc
		}
	}
	return nil
}

// GetAllDescriptors returns all descriptors visible by the transaction,
// first checking the Collection's cached descriptors for validity if validate
// is set to true before defaulting to a key-value scan, if necessary.
func (tc *Collection) GetAllDescriptors(
	ctx context.Context, txn *kv.Txn,
) ([]catalog.Descriptor, error) {
	if tc.allDescriptors.isEmpty() {
		descs, err := catalogkv.GetAllDescriptors(ctx, txn, tc.codec())
		if err != nil {
			return nil, err
		}

		// There could be tables with user defined types that need hydrating.
		if err := HydrateGivenDescriptors(ctx, descs); err != nil {
			// If we ran into an error hydrating the types, that means that we
			// have some sort of corrupted descriptor state. Rather than disable
			// uses of GetAllDescriptors, just log the error.
			log.Errorf(ctx, "%s", err.Error())
		}

		tc.allDescriptors.init(descs)
	}
	return tc.allDescriptors.descs, nil
}

// HydrateGivenDescriptors installs type metadata in the types present for all
// table descriptors in the slice of descriptors. It is exported so resolution
// on sets of descriptors can hydrate a set of descriptors (i.e. on BACKUPs).
func HydrateGivenDescriptors(ctx context.Context, descs []catalog.Descriptor) error {
	// Collect the needed information to set up metadata in those types.
	dbDescs := make(map[descpb.ID]catalog.DatabaseDescriptor)
	typDescs := make(map[descpb.ID]catalog.TypeDescriptor)
	schemaDescs := make(map[descpb.ID]catalog.SchemaDescriptor)
	for _, desc := range descs {
		switch desc := desc.(type) {
		case catalog.DatabaseDescriptor:
			dbDescs[desc.GetID()] = desc
		case catalog.TypeDescriptor:
			typDescs[desc.GetID()] = desc
		case catalog.SchemaDescriptor:
			schemaDescs[desc.GetID()] = desc
		}
	}
	// If we found any type descriptors, that means that some of the tables we
	// scanned might have types that need hydrating.
	if len(typDescs) > 0 {
		// Since we just scanned all the descriptors, we already have everything
		// we need to hydrate our types. Set up an accessor for the type hydration
		// method to look into the scanned set of descriptors.
		typeLookup := func(ctx context.Context, id descpb.ID) (tree.TypeName, catalog.TypeDescriptor, error) {
			typDesc, ok := typDescs[id]
			if !ok {
				n := tree.MakeUnresolvedName(fmt.Sprintf("[%d]", id))
				return tree.TypeName{}, nil, sqlerrors.NewUndefinedObjectError(&n,
					tree.TypeObject)
			}
			dbDesc, ok := dbDescs[typDesc.GetParentID()]
			if !ok {
				n := fmt.Sprintf("[%d]", typDesc.GetParentID())
				return tree.TypeName{}, nil, sqlerrors.NewUndefinedDatabaseError(n)
			}
			// We don't use the collection's ResolveSchemaByID method here because
			// we already have all of the descriptors. User defined types are only
			// members of the public schema or a user defined schema, so those are
			// the only cases we have to consider here.
			var scName string
			switch typDesc.GetParentSchemaID() {
			case keys.PublicSchemaID:
				scName = tree.PublicSchema
			default:
				scName = schemaDescs[typDesc.GetParentSchemaID()].GetName()
			}
			name := tree.MakeNewQualifiedTypeName(dbDesc.GetName(), scName, typDesc.GetName())
			return name, typDesc, nil
		}
		// Now hydrate all table descriptors.
		for i := range descs {
			desc := descs[i]
			// Never hydrate dropped descriptors.
			if desc.Dropped() {
				continue
			}
			tblDesc, ok := desc.(catalog.TableDescriptor)
			if ok {
				if err := typedesc.HydrateTypesInTableDescriptor(
					ctx,
					tblDesc.TableDesc(),
					typedesc.TypeLookupFunc(typeLookup),
				); err != nil {
					return err
				}
			}
		}
	}
	return nil
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
	if tc.allDatabaseDescriptors == nil {
		dbDescIDs, err := catalogkv.GetAllDatabaseDescriptorIDs(ctx, txn, tc.codec())
		if err != nil {
			return nil, err
		}
		dbDescs, err := catalogkv.GetDatabaseDescriptorsFromIDs(
			ctx, txn, tc.codec(), dbDescIDs,
		)
		if err != nil {
			return nil, err
		}
		tc.allDatabaseDescriptors = dbDescs
	}
	return tc.allDatabaseDescriptors, nil
}

// GetSchemasForDatabase returns the schemas for a given database
// visible by the transaction. This uses the schema cache locally
// if possible, or else performs a scan on kv.
func (tc *Collection) GetSchemasForDatabase(
	ctx context.Context, txn *kv.Txn, dbID descpb.ID,
) (map[descpb.ID]string, error) {
	if tc.allSchemasForDatabase == nil {
		tc.allSchemasForDatabase = make(map[descpb.ID]map[descpb.ID]string)
	}
	if _, ok := tc.allSchemasForDatabase[dbID]; !ok {
		var err error
		tc.allSchemasForDatabase[dbID], err = resolver.GetForDatabase(ctx, txn, tc.codec(), dbID)
		if err != nil {
			return nil, err
		}
	}
	return tc.allSchemasForDatabase[dbID], nil
}

// GetObjectNamesAndIDs returns the names and IDs of all objects in a database and schema.
func (tc *Collection) GetObjectNamesAndIDs(
	ctx context.Context,
	txn *kv.Txn,
	dbDesc catalog.DatabaseDescriptor,
	scName string,
	flags tree.DatabaseListFlags,
) (tree.TableNames, descpb.IDs, error) {
	if ok, names, ds := tc.maybeGetVirtualObjectNamesAndIDs(
		scName, dbDesc, flags,
	); ok {
		return names, ds, nil
	}

	schemaFlags := tree.SchemaLookupFlags{
		Required:       flags.Required,
		AvoidCached:    flags.RequireMutable || flags.AvoidCached,
		IncludeDropped: flags.IncludeDropped,
		IncludeOffline: flags.IncludeOffline,
	}
	ok, schema, err := tc.GetImmutableSchemaByName(ctx, txn, dbDesc.GetID(), scName, schemaFlags)
	if err != nil {
		return nil, nil, err
	}
	if !ok {
		if flags.Required {
			tn := tree.MakeTableNameWithSchema(tree.Name(dbDesc.GetName()), tree.Name(scName), "")
			return nil, nil, sqlerrors.NewUnsupportedSchemaUsageError(tree.ErrString(&tn.ObjectNamePrefix))
		}
		return nil, nil, nil
	}

	log.Eventf(ctx, "fetching list of objects for %q", dbDesc.GetName())
	prefix := catalogkeys.NewTableKey(dbDesc.GetID(), schema.ID, "").Key(tc.codec())
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

	// When constructing the list of entries under the `public` schema (and only
	// when constructing the list for the `public` schema), We scan both the
	// deprecated and new system.namespace table to get the complete list of
	// tables. Duplicate entries may be present in both the tables, so we filter
	// those out. If a duplicate entry is present, it doesn't matter which table
	// it is read from -- system.namespace entries are never modified, they are
	// only added/deleted. Entries are written to only one table, so duplicate
	// entries must have been copied over during migration. Thus, it doesn't
	// matter which table (newer/deprecated) the value is read from.
	//
	// It may seem counter-intuitive to read both tables if we have found data in
	// the newer version. The migration copied all entries from the deprecated
	// system.namespace and all new entries after the cluster version bump are added
	// to the new system.namespace. Why do we do this then?
	// This is to account the scenario where a table was created before
	// the cluster version was bumped, but after the older system.namespace was
	// copied into the newer system.namespace. Objects created in this window
	// will only be present in the older system.namespace. To account for this
	// scenario, we must do this filtering logic.
	// TODO(solon): This complexity can be removed in  20.2.
	if scName != tree.PublicSchema {
		return tableNames, tableIDs, nil
	}

	dprefix := catalogkeys.NewDeprecatedTableKey(dbDesc.GetID(), "").Key(tc.codec())
	dsr, err := txn.Scan(ctx, dprefix, dprefix.PrefixEnd(), 0)
	if err != nil {
		return nil, nil, err
	}

	for _, row := range dsr {
		// Decode using the deprecated key prefix.
		_, tableName, err := encoding.DecodeUnsafeStringAscending(
			bytes.TrimPrefix(row.Key, dprefix), nil)
		if err != nil {
			return nil, nil, err
		}
		if alreadySeen[tableName] {
			continue
		}
		tn := tree.MakeTableNameWithSchema(tree.Name(dbDesc.GetName()), tree.Name(scName), tree.Name(tableName))
		tn.ExplicitCatalog = flags.ExplicitPrefix
		tn.ExplicitSchema = flags.ExplicitPrefix
		tableNames = append(tableNames, tn)
		tableIDs = append(tableIDs, descpb.ID(row.ValueInt()))
	}

	return tableNames, tableIDs, nil
}

func (tc *Collection) maybeGetVirtualObjectNamesAndIDs(
	scName string, dbDesc catalog.DatabaseDescriptor, flags tree.DatabaseListFlags,
) (isVirtual bool, _ tree.TableNames, _ descpb.IDs) {
	if tc.virtualSchemas == nil {
		return false, nil, nil
	}
	entry, ok := tc.virtualSchemas.GetVirtualSchema(scName)
	if !ok {
		return false, nil, nil
	}
	names := make(tree.TableNames, 0, entry.NumTables())
	IDs := make(descpb.IDs, 0, entry.NumTables())
	schemaDesc := entry.Desc()
	entry.VisitTables(func(table catalog.VirtualObject) {
		name := tree.MakeTableNameWithSchema(
			tree.Name(dbDesc.GetName()), tree.Name(schemaDesc.GetName()), tree.Name(table.Desc().GetName()))
		name.ExplicitCatalog = flags.ExplicitPrefix
		name.ExplicitSchema = flags.ExplicitPrefix
		names = append(names, name)
		IDs = append(IDs, table.Desc().GetID())
	})
	return true, names, IDs

}

// releaseAllDescriptors releases the cached slice of all descriptors
// held by Collection.
func (tc *Collection) releaseAllDescriptors() {
	tc.allDescriptors.clear()
	tc.allDatabaseDescriptors = nil
	tc.allSchemasForDatabase = nil
}

// SetSyntheticDescriptors sets the provided descriptors as the synthetic
// descriptors to override all other matching descriptors during immutable
// access. An immutable copy is made if the descriptor is mutable. See the
// documentation on syntheticDescriptors.
func (tc *Collection) SetSyntheticDescriptors(descs []catalog.Descriptor) {
	immutableCopies := make([]catalog.Descriptor, 0, len(descs))
	for _, desc := range descs {
		if mut, ok := desc.(catalog.MutableDescriptor); ok {
			immutableCopies = append(immutableCopies, mut.ImmutableCopy())
		} else {
			immutableCopies = append(immutableCopies, desc)
		}
	}
	tc.syntheticDescriptors = immutableCopies
}

func (tc *Collection) codec() keys.SQLCodec {
	return tc.leaseMgr.Codec()
}

// LeaseManager returns the lease.Manager.
func (tc *Collection) LeaseManager() *lease.Manager {
	return tc.leaseMgr
}

// DistSQLTypeResolverFactory is an object that constructs TypeResolver objects
// that are bound under a transaction. These TypeResolvers access descriptors
// through the descs.Collection and eventually the lease.Manager. It cannot be
// used concurrently, and neither can the constructed TypeResolvers. After the
// DistSQLTypeResolverFactory is finished being used, all descriptors need to
// be released from Descriptors. It is intended to be used to resolve type
// references during the initialization of DistSQL flows.
type DistSQLTypeResolverFactory struct {
	Descriptors *Collection
	CleanupFunc func(ctx context.Context)
}

// NewTypeResolver creates a new TypeResolver that is bound under the input
// transaction. It returns a nil resolver if the factory itself is nil.
func (df *DistSQLTypeResolverFactory) NewTypeResolver(txn *kv.Txn) DistSQLTypeResolver {
	if df == nil {
		return DistSQLTypeResolver{}
	}
	return NewDistSQLTypeResolver(df.Descriptors, txn)
}

// NewSemaContext creates a new SemaContext with a TypeResolver bound to the
// input transaction.
func (df *DistSQLTypeResolverFactory) NewSemaContext(txn *kv.Txn) *tree.SemaContext {
	semaCtx := tree.MakeSemaContext()
	semaCtx.TypeResolver = df.NewTypeResolver(txn)
	return &semaCtx
}

// DistSQLTypeResolver is a TypeResolver that accesses TypeDescriptors through
// a given descs.Collection and transaction.
type DistSQLTypeResolver struct {
	descriptors *Collection
	txn         *kv.Txn
}

// NewDistSQLTypeResolver creates a new DistSQLTypeResolver.
func NewDistSQLTypeResolver(descs *Collection, txn *kv.Txn) DistSQLTypeResolver {
	return DistSQLTypeResolver{
		descriptors: descs,
		txn:         txn,
	}
}

// ResolveType implements the tree.TypeReferenceResolver interface.
func (dt DistSQLTypeResolver) ResolveType(
	context.Context, *tree.UnresolvedObjectName,
) (*types.T, error) {
	return nil, errors.AssertionFailedf("cannot resolve types in DistSQL by name")
}

// ResolveTypeByOID implements the tree.TypeReferenceResolver interface.
func (dt DistSQLTypeResolver) ResolveTypeByOID(ctx context.Context, oid oid.Oid) (*types.T, error) {
	name, desc, err := dt.GetTypeDescriptor(ctx, typedesc.UserDefinedTypeOIDToID(oid))
	if err != nil {
		return nil, err
	}
	return desc.MakeTypesT(ctx, &name, dt)
}

// GetTypeDescriptor implements the sqlbase.TypeDescriptorResolver interface.
func (dt DistSQLTypeResolver) GetTypeDescriptor(
	ctx context.Context, id descpb.ID,
) (tree.TypeName, catalog.TypeDescriptor, error) {
	flags := tree.CommonLookupFlags{
		Required: true,
	}
	desc, err := dt.descriptors.getDescriptorByIDMaybeSetTxnDeadline(
		ctx, dt.txn, id, flags, false, /* setTxnDeadline */
	)
	if err != nil {
		return tree.TypeName{}, nil, err
	}
	typeDesc, isType := desc.(catalog.TypeDescriptor)
	if !isType {
		return tree.TypeName{}, nil, pgerror.Newf(pgcode.WrongObjectType,
			"descriptor %d is a %s not a %s", id, desc.DescriptorType(), catalog.Type)
	}
	name := tree.MakeUnqualifiedTypeName(tree.Name(desc.GetName()))
	return name, typeDesc, nil
}

// HydrateTypeSlice installs metadata into a slice of types.T's.
func (dt DistSQLTypeResolver) HydrateTypeSlice(ctx context.Context, typs []*types.T) error {
	for _, t := range typs {
		if t.UserDefined() {
			name, desc, err := dt.GetTypeDescriptor(ctx, typedesc.GetTypeDescID(t))
			if err != nil {
				return err
			}
			if err := desc.HydrateTypeInfoWithName(ctx, t, &name, dt); err != nil {
				return err
			}
		}
	}
	return nil
}
