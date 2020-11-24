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
	"fmt"
	"sort"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/dbdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/hydratedtables"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/lease"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/resolver"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemadesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
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
	descs []catalog.Descriptor
}

func (ld *leasedDescriptors) add(desc catalog.Descriptor) {
	ld.descs = append(ld.descs, desc)
}

func (ld *leasedDescriptors) releaseAll() (toRelease []catalog.Descriptor) {
	toRelease = append(toRelease, ld.descs...)
	ld.descs = ld.descs[:0]
	return toRelease
}

func (ld *leasedDescriptors) release(ids []descpb.ID) (toRelease []catalog.Descriptor) {
	// Sort the descriptors and leases to make it easy to find the leases to release.
	leasedDescs := ld.descs
	sort.Slice(ids, func(i, j int) bool {
		return ids[i] < ids[j]
	})
	sort.Slice(leasedDescs, func(i, j int) bool {
		return leasedDescs[i].GetID() < leasedDescs[j].GetID()
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
		if !shouldRelease(l.GetID()) {
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
		if desc.GetID() == id {
			return desc
		}
	}
	return nil
}

func (ld *leasedDescriptors) getByName(
	dbID descpb.ID, schemaID descpb.ID, name string,
) catalog.Descriptor {
	for i := range ld.descs {
		desc := ld.descs[i]
		if lease.NameMatchesDescriptor(desc, dbID, schemaID, name) {
			return desc
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
) Collection {
	return Collection{
		leaseMgr:       leaseMgr,
		settings:       settings,
		sessionData:    sessionData,
		hydratedTables: hydratedTables,
	}
}

// NewCollection constructs a new *Collection.
func NewCollection(
	settings *cluster.Settings, leaseMgr *lease.Manager, hydratedTables *hydratedtables.Cache,
) *Collection {
	tc := MakeCollection(
		leaseMgr,
		settings,
		nil, /* sessionData */
		hydratedTables,
	)
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
	uncommittedDescriptors []uncommittedDescriptor

	// allDescriptors is a slice of all available descriptors. The descriptors
	// are cached to avoid repeated lookups by users like virtual tables. The
	// cache is purged whenever events would cause a scan of all descriptors to
	// return different values, such as when the txn timestamp changes or when
	// new descriptors are written in the txn.
	//
	// TODO(ajwerner): This cache may be problematic in clusters with very large
	// numbers of descriptors.
	allDescriptors []catalog.Descriptor

	// allDatabaseDescriptors is a slice of all available database descriptors.
	// These are purged at the same time as allDescriptors.
	allDatabaseDescriptors []*dbdesc.Immutable

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
		log.VEventf(ctx, 2, "found descriptor in collection for '%s'", name)
		return desc, false, nil
	}

	readTimestamp := txn.ReadTimestamp()
	desc, expiration, err := tc.leaseMgr.AcquireByName(ctx, readTimestamp, parentID, parentSchemaID, name)
	if err != nil {
		// Read the descriptor from the store in the face of some specific errors
		// because of a known limitation of AcquireByName. See the known
		// limitations of AcquireByName for details.
		if catalog.HasInactiveDescriptorError(err) ||
			errors.Is(err, catalog.ErrDescriptorNotFound) {
			return nil, true, nil
		}
		// Lease acquisition failed with some other error. This we don't
		// know how to deal with, so propagate the error.
		return nil, false, err
	}

	if expiration.LessEq(readTimestamp) {
		log.Fatalf(ctx, "bad descriptor for T=%s, expiration=%s", readTimestamp, expiration)
	}

	tc.leasedDescriptors.add(desc)
	log.VEventf(ctx, 2, "added descriptor '%s' to collection: %+v", name, desc)

	// If the descriptor we just acquired expires before the txn's deadline,
	// reduce the deadline. We use ReadTimestamp() that doesn't return the commit
	// timestamp, so we need to set a deadline on the transaction to prevent it
	// from committing beyond the version's expiration time.
	txn.UpdateDeadlineMaybe(ctx, expiration)
	return desc, false, nil
}

// GetMutableDatabaseDescriptor returns a mutable database descriptor.
func (tc *Collection) GetMutableDatabaseDescriptor(
	ctx context.Context, txn *kv.Txn, name string, flags tree.DatabaseLookupFlags,
) (*dbdesc.Mutable, error) {
	if log.V(2) {
		log.Infof(ctx, "reading mutable descriptor on '%s'", name)
	}
	// First try the uncommitted descriptors.
	if refuseFurtherLookup, desc, err := tc.getUncommittedDescriptor(
		keys.RootNamespaceID, keys.RootNamespaceID, name, flags,
	); refuseFurtherLookup || err != nil {
		return nil, err
	} else if mut := desc.mutable; mut != nil {
		db, ok := mut.(*dbdesc.Mutable)
		if !ok {
			return nil, nil
		}
		log.VEventf(ctx, 2, "found uncommitted descriptor %d", db.GetID())
		return db, nil
	}

	db, err := getDatabaseDesc(ctx, txn, tc.codec(), name, flags)
	if err != nil || db == nil {
		return nil, err
	}
	mutDesc, ok := db.(*dbdesc.Mutable)
	if !ok {
		// TODO (lucy): Here and elsewhere in the Collection, we return a nil
		// descriptor with a nil error if the type cast doesn't succeed, regardless
		// of whether flags.Required is true. This seems like a potential source
		// of bugs.
		return nil, nil
	}
	return mutDesc, nil
}

// GetMutableTableDescriptor returns a mutable table descriptor.
//
// If flags.required is false, GetMutableTableDescriptor() will gracefully
// return a nil descriptor and no error if the table does not exist.
// If flags.RequireMutable is false, nil will be returned.
func (tc *Collection) GetMutableTableDescriptor(
	ctx context.Context, txn *kv.Txn, tn *tree.TableName, flags tree.ObjectLookupFlags,
) (*tabledesc.Mutable, error) {
	desc, err := tc.getMutableObjectDescriptor(ctx, txn, tn, flags)
	if err != nil {
		return nil, err
	}
	mutDesc, ok := desc.(*tabledesc.Mutable)
	if !ok {
		return nil, nil
	}
	hydrated, err := tc.hydrateTypesInTableDesc(ctx, txn, mutDesc)
	if err != nil {
		return nil, err
	}
	return hydrated.(*tabledesc.Mutable), nil
}

func (tc *Collection) getMutableObjectDescriptor(
	ctx context.Context, txn *kv.Txn, name tree.ObjectName, flags tree.ObjectLookupFlags,
) (catalog.MutableDescriptor, error) {
	if log.V(2) {
		log.Infof(ctx, "reading mutable descriptor on '%s'", name)
	}

	// Resolve the database.
	db, err := tc.GetDatabaseVersion(ctx, txn, name.Catalog(),
		tree.DatabaseLookupFlags{
			Required:       flags.Required,
			AvoidCached:    flags.AvoidCached,
			IncludeDropped: flags.IncludeDropped,
			IncludeOffline: flags.IncludeOffline,
		})
	if err != nil || db == nil {
		return nil, err
	}
	dbID := db.GetID()

	// Resolve the schema to the ID of the schema.
	foundSchema, resolvedSchema, err := tc.ResolveSchema(ctx, txn, dbID, name.Schema(),
		tree.SchemaLookupFlags{
			Required:       flags.Required,
			AvoidCached:    flags.AvoidCached,
			IncludeDropped: flags.IncludeDropped,
			IncludeOffline: flags.IncludeOffline,
		})
	if err != nil || !foundSchema {
		return nil, err
	}

	if refuseFurtherLookup, desc, err := tc.getUncommittedDescriptor(
		dbID,
		resolvedSchema.ID,
		name.Object(),
		flags.CommonLookupFlags,
	); refuseFurtherLookup || err != nil {
		return nil, err
	} else if mut := desc.mutable; mut != nil {
		log.VEventf(ctx, 2, "found uncommitted descriptor %d", mut.GetID())
		return mut, nil
	}

	obj, err := getObjectDesc(
		ctx,
		txn,
		tc.settings,
		tc.codec(),
		name.Catalog(),
		name.Schema(),
		name.Object(),
		flags,
	)
	if err != nil || obj == nil {
		return nil, err
	}
	mutDesc, ok := obj.(catalog.MutableDescriptor)
	if !ok {
		return nil, nil
	}
	return mutDesc, nil
}

func (tc *Collection) getMutableUserDefinedSchemaDescriptor(
	ctx context.Context, txn *kv.Txn, dbID descpb.ID, schemaName string, flags tree.SchemaLookupFlags,
) (*schemadesc.Mutable, error) {
	log.VEventf(ctx, 2, "reading mutable descriptor on '%s'", schemaName)
	// First try the uncommitted descriptors.
	if refuseFurtherLookup, desc, err := tc.getUncommittedDescriptor(
		dbID, keys.RootNamespaceID, schemaName, flags,
	); refuseFurtherLookup || err != nil {
		return nil, err
	} else if mut := desc.mutable; mut != nil {
		schema, ok := mut.(*schemadesc.Mutable)
		if !ok {
			return nil, nil
		}
		log.VEventf(ctx, 2, "found uncommitted descriptor %d", schema.GetID())
		return schema, nil
	}

	found, schema, err := getSchema(ctx, txn, tc.codec(), dbID, schemaName, flags)
	if err != nil || !found {
		return nil, err
	}
	return schema.Desc.(*schemadesc.Mutable), nil
}

func (tc *Collection) getUserDefinedSchemaVersion(
	ctx context.Context, txn *kv.Txn, dbID descpb.ID, schemaName string, flags tree.SchemaLookupFlags,
) (*schemadesc.Immutable, error) {
	readFromStore := func() (*schemadesc.Immutable, error) {
		exists, schema, err := getSchema(ctx, txn, tc.codec(), dbID, schemaName, flags)
		if err != nil || !exists || schema.Kind != catalog.SchemaUserDefined {
			return nil, err
		}
		return schema.Desc.(*schemadesc.Immutable), nil
	}

	if refuseFurtherLookup, desc, err := tc.getUncommittedDescriptor(
		dbID,
		keys.RootNamespaceID,
		schemaName,
		flags,
	); refuseFurtherLookup || err != nil {
		return nil, err
	} else if immut := desc.immutable; immut != nil {
		log.VEventf(ctx, 2, "found uncommitted descriptor %d", immut.GetID())
		desc, ok := immut.(*schemadesc.Immutable)
		if !ok {
			if flags.Required {
				return nil, sqlerrors.NewUndefinedSchemaError(schemaName)
			}
			return nil, nil
		}
		return desc, nil
	}

	avoidCache := flags.AvoidCached || lease.TestingTableLeasesAreDisabled()
	if avoidCache {
		return readFromStore()
	}

	// Look up whether the schema is on the database descriptor and return early
	// if it's not.
	// TODO (lucy): It's unfortunate that our current API (where we look up
	// schemas by database ID and name) forces us to look up the leased database
	// descriptor here, since we'll already have done this lookup when we're
	// resolving the schema by name. Arguably we should be doing this at a higher
	// level.
	dbDesc, err := tc.GetDatabaseVersionByID(ctx, txn, dbID, tree.DatabaseLookupFlags{Required: true})
	if err != nil {
		return nil, err
	}
	schemaInfo, found := dbDesc.LookupSchema(schemaName)
	if !found {
		if flags.Required {
			return nil, sqlerrors.NewUndefinedSchemaError(schemaName)
		}
		return nil, nil
	} else if schemaInfo.Dropped {
		if flags.Required {
			return nil, pgerror.New(pgcode.InvalidSchemaName, "schema %s is being dropped")
		}
		return nil, nil
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
	desc, err := tc.getDescriptorVersionByID(ctx, txn, schemaInfo.ID, flags, true /* setTxnDeadline */)
	if err != nil {
		if errors.Is(err, catalog.ErrDescriptorNotFound) {
			return nil, sqlerrors.NewUndefinedSchemaError(schemaName)
		}
		return nil, err
	}
	schema, ok := desc.(*schemadesc.Immutable)
	if !ok {
		return nil, sqlerrors.NewUndefinedSchemaError(schemaName)
	}
	return schema, nil
}

// ResolveSchema resolves the schema and, if applicable, returns a descriptor
// usable by the transaction.
// This method departs from the pattern for the other descriptor types: there
// are no separate methods for mutable and immutable descriptors. The only
// reasons for this are that both paths require special handling for the public
// schema and temp schemas, and currently the only user of the descriptor
// collection for schemas is the CachedPhysicalAccessor, which needs access to
// both variants anyway.
func (tc *Collection) ResolveSchema(
	ctx context.Context, txn *kv.Txn, dbID descpb.ID, schemaName string, flags tree.SchemaLookupFlags,
) (bool, catalog.ResolvedSchema, error) {
	// Fast path public schema, as it is always found.
	if schemaName == tree.PublicSchema {
		return true, catalog.ResolvedSchema{
			ID: keys.PublicSchemaID, Kind: catalog.SchemaPublic, Name: tree.PublicSchema,
		}, nil
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

		exists, resolved, err := getSchema(ctx, txn, tc.codec(), dbID, schemaName, flags)
		if err != nil || !exists {
			return exists, catalog.ResolvedSchema{}, err
		}
		return exists, resolved, err
	}

	// Otherwise, the schema is user-defined. Get the descriptor.
	var desc catalog.SchemaDescriptor
	if flags.RequireMutable {
		mutDesc, err := tc.getMutableUserDefinedSchemaDescriptor(ctx, txn, dbID, schemaName, flags)
		if err != nil || mutDesc == nil {
			return false, catalog.ResolvedSchema{}, err
		}
		desc = mutDesc
	} else {
		immutDesc, err := tc.getUserDefinedSchemaVersion(ctx, txn, dbID, schemaName, flags)
		if err != nil || immutDesc == nil {
			return false, catalog.ResolvedSchema{}, err
		}
		desc = immutDesc
	}
	return true, catalog.ResolvedSchema{
		Kind: catalog.SchemaUserDefined,
		Name: schemaName,
		ID:   desc.GetID(),
		Desc: desc,
	}, nil
}

// GetDatabaseVersion returns a database descriptor with a version suitable for
// the transaction: table.ModificationTime <= txn.Timestamp < expirationTime.
// The table must be released by calling tc.ReleaseAll().
//
// If flags.required is false, GetTableVersion() will gracefully
// return a nil descriptor and no error if the table does not exist.
//
// It might also add a transaction deadline to the transaction that is
// enforced at the KV layer to ensure that the transaction doesn't violate
// the validity window of the table descriptor version returned.
func (tc *Collection) GetDatabaseVersion(
	ctx context.Context, txn *kv.Txn, name string, flags tree.DatabaseLookupFlags,
) (*dbdesc.Immutable, error) {
	readFromStore := func() (*dbdesc.Immutable, error) {
		desc, err := getDatabaseDesc(ctx, txn, tc.codec(), name, flags)
		if err != nil || desc == nil {
			return nil, err
		}
		return desc.(*dbdesc.Immutable), nil
	}

	if refuseFurtherLookup, desc, err := tc.getUncommittedDescriptor(
		keys.RootNamespaceID,
		keys.RootNamespaceID,
		name,
		flags,
	); refuseFurtherLookup || err != nil {
		return nil, err
	} else if immut := desc.immutable; immut != nil {
		log.VEventf(ctx, 2, "found uncommitted descriptor %d", immut.GetID())
		db, ok := immut.(*dbdesc.Immutable)
		if !ok {
			if flags.Required {
				return nil, sqlerrors.NewUndefinedDatabaseError(name)
			}
			return nil, nil
		}
		return db, nil
	}

	avoidCache := flags.AvoidCached || lease.TestingTableLeasesAreDisabled() ||
		name == systemschema.SystemDatabaseName
	if avoidCache {
		return readFromStore()
	}

	desc, shouldReadFromStore, err := tc.getLeasedDescriptorByName(
		ctx, txn, keys.RootNamespaceID, keys.RootNamespaceID, name)
	if err != nil {
		return nil, err
	}
	if shouldReadFromStore {
		return readFromStore()
	}
	db, ok := desc.(*dbdesc.Immutable)
	if !ok {
		if flags.Required {
			return nil, sqlerrors.NewUndefinedDatabaseError(name)
		}
		return nil, nil
	}
	return db, nil
}

// GetTableVersion returns a table descriptor with a version suitable for
// the transaction: table.ModificationTime <= txn.Timestamp < expirationTime.
// The table must be released by calling tc.ReleaseAll().
//
// If flags.required is false, GetTableVersion() will gracefully
// return a nil descriptor and no error if the table does not exist.
//
// It might also add a transaction deadline to the transaction that is
// enforced at the KV layer to ensure that the transaction doesn't violate
// the validity window of the table descriptor version returned.
//
func (tc *Collection) GetTableVersion(
	ctx context.Context, txn *kv.Txn, tn *tree.TableName, flags tree.ObjectLookupFlags,
) (*tabledesc.Immutable, error) {
	desc, err := tc.getObjectVersion(ctx, txn, tn, flags)
	if err != nil {
		return nil, err
	}
	table, ok := desc.(*tabledesc.Immutable)
	if !ok {
		if flags.Required {
			return nil, sqlerrors.NewUndefinedRelationError(tn)
		}
		return nil, nil
	}
	hydrated, err := tc.hydrateTypesInTableDesc(ctx, txn, table)
	if err != nil {
		return nil, err
	}
	return hydrated.(*tabledesc.Immutable), nil
}

func (tc *Collection) getObjectVersion(
	ctx context.Context, txn *kv.Txn, name tree.ObjectName, flags tree.ObjectLookupFlags,
) (catalog.Descriptor, error) {
	readObjectFromStore := func() (catalog.Descriptor, error) {
		return getObjectDesc(
			ctx,
			txn,
			tc.settings,
			tc.codec(),
			name.Catalog(),
			name.Schema(),
			name.Object(),
			flags,
		)
	}

	// Resolve the database.
	db, err := tc.GetDatabaseVersion(ctx, txn, name.Catalog(),
		tree.DatabaseLookupFlags{
			Required:       flags.Required,
			AvoidCached:    flags.AvoidCached,
			IncludeDropped: flags.IncludeDropped,
			IncludeOffline: flags.IncludeOffline,
		})
	if err != nil || db == nil {
		return nil, err
	}
	dbID := db.GetID()

	// Resolve the schema to the ID of the schema.
	foundSchema, resolvedSchema, err := tc.ResolveSchema(ctx, txn, dbID, name.Schema(),
		tree.SchemaLookupFlags{
			Required:       flags.Required,
			AvoidCached:    flags.AvoidCached,
			IncludeDropped: flags.IncludeDropped,
			IncludeOffline: flags.IncludeOffline,
		})
	if err != nil || !foundSchema {
		return nil, err
	}
	schemaID := resolvedSchema.ID

	// TODO(vivek): Ideally we'd avoid caching for only the
	// system.descriptor and system.lease tables, because they are
	// used for acquiring leases, creating a chicken&egg problem.
	// But doing so turned problematic and the tests pass only by also
	// disabling caching of system.eventlog, system.rangelog, and
	// system.users. For now we're sticking to disabling caching of
	// all system descriptors except the role-members-desc.
	avoidCache := flags.AvoidCached || lease.TestingTableLeasesAreDisabled() ||
		(name.Catalog() == systemschema.SystemDatabaseName && name.Object() != systemschema.RoleMembersTable.Name)

	if refuseFurtherLookup, desc, err := tc.getUncommittedDescriptor(
		dbID,
		schemaID,
		name.Object(),
		flags.CommonLookupFlags,
	); refuseFurtherLookup || err != nil {
		return nil, err
	} else if immut := desc.immutable; immut != nil {
		// If not forcing to resolve using KV, tables being added aren't visible.
		if immut.Adding() && !avoidCache {
			if !flags.Required {
				return nil, nil
			}
			return nil, catalog.FilterDescriptorState(immut, flags.CommonLookupFlags)
		}

		log.VEventf(ctx, 2, "found uncommitted descriptor %d", immut.GetID())
		return immut, nil
	}

	if avoidCache {
		return readObjectFromStore()
	}

	desc, shouldReadFromStore, err := tc.getLeasedDescriptorByName(ctx, txn, dbID, schemaID, name.Object())
	if err != nil {
		return nil, err
	}
	if shouldReadFromStore {
		return readObjectFromStore()
	}
	return desc, nil
}

// GetDatabaseVersionByID returns a database descriptor valid for the
// transaction. See GetDatabaseVersion.
func (tc *Collection) GetDatabaseVersionByID(
	ctx context.Context, txn *kv.Txn, dbID descpb.ID, flags tree.DatabaseLookupFlags,
) (*dbdesc.Immutable, error) {
	desc, err := tc.getDescriptorVersionByID(ctx, txn, dbID, flags, true /* setTxnDeadline */)
	if err != nil {
		if errors.Is(err, catalog.ErrDescriptorNotFound) {
			return nil, sqlerrors.NewUndefinedDatabaseError(fmt.Sprintf("[%d]", dbID))
		}
		return nil, err
	}
	db, ok := desc.(*dbdesc.Immutable)
	if !ok {
		return nil, sqlerrors.NewUndefinedDatabaseError(fmt.Sprintf("[%d]", dbID))
	}
	return db, nil
}

// GetTableVersionByID is a by-ID variant of GetTableVersion (i.e. uses same cache).
func (tc *Collection) GetTableVersionByID(
	ctx context.Context, txn *kv.Txn, tableID descpb.ID, flags tree.ObjectLookupFlags,
) (*tabledesc.Immutable, error) {
	desc, err := tc.getDescriptorVersionByID(ctx, txn, tableID, flags.CommonLookupFlags, true /* setTxnDeadline */)
	if err != nil {
		if errors.Is(err, catalog.ErrDescriptorNotFound) {
			return nil, sqlerrors.NewUndefinedRelationError(
				&tree.TableRef{TableID: int64(tableID)})
		}
		return nil, err
	}
	table, ok := desc.(*tabledesc.Immutable)
	if !ok {
		return nil, sqlerrors.NewUndefinedRelationError(
			&tree.TableRef{TableID: int64(tableID)})
	}
	hydrated, err := tc.hydrateTypesInTableDesc(ctx, txn, table)
	if err != nil {
		return nil, err
	}
	return hydrated.(*tabledesc.Immutable), nil
}

func (tc *Collection) getDescriptorVersionByID(
	ctx context.Context, txn *kv.Txn, id descpb.ID, flags tree.CommonLookupFlags, setTxnDeadline bool,
) (catalog.Descriptor, error) {
	if flags.AvoidCached || lease.TestingTableLeasesAreDisabled() {
		desc, err := catalogkv.GetDescriptorByID(ctx, txn, tc.codec(), id, catalogkv.Immutable,
			catalogkv.AnyDescriptorKind, true /* required */)
		if err != nil {
			return nil, err
		}
		if err := catalog.FilterDescriptorState(desc, flags); err != nil {
			return nil, err
		}
		return desc, nil
	}

	for _, ud := range tc.uncommittedDescriptors {
		if immut := ud.immutable; immut.GetID() == id {
			log.VEventf(ctx, 2, "found uncommitted descriptor %d", id)
			if immut.Dropped() {
				// TODO (lucy): This error is meant to be parallel to the error returned
				// from FilterDescriptorState, but it may be too low-level for getting
				// descriptors from the descriptor collection. In general the errors
				// being returned from this method aren't that consistent.
				return nil, catalog.NewInactiveDescriptorError(catalog.ErrDescriptorDropped)
			}
			return immut, nil
		}
	}

	// First, look to see if we already have the table in the shared cache.
	if desc := tc.leasedDescriptors.getByID(id); desc != nil {
		log.VEventf(ctx, 2, "found descriptor %d in cache", id)
		return desc, nil
	}

	readTimestamp := txn.ReadTimestamp()
	desc, expiration, err := tc.leaseMgr.Acquire(ctx, readTimestamp, id)
	if err != nil {
		return nil, err
	}

	if expiration.LessEq(readTimestamp) {
		log.Fatalf(ctx, "bad descriptor for T=%s, expiration=%s", readTimestamp, expiration)
	}

	tc.leasedDescriptors.add(desc)
	log.VEventf(ctx, 2, "added descriptor %q to collection", desc.GetName())

	if setTxnDeadline {
		// If the descriptor we just acquired expires before the txn's deadline,
		// reduce the deadline. We use ReadTimestamp() that doesn't return the commit
		// timestamp, so we need to set a deadline on the transaction to prevent it
		// from committing beyond the version's expiration time.
		txn.UpdateDeadlineMaybe(ctx, expiration)
	}
	return desc, nil
}

// GetMutableTableVersionByID is a variant of sqlbase.getTableDescFromID which returns a mutable
// table descriptor of the table modified in the same transaction.
func (tc *Collection) GetMutableTableVersionByID(
	ctx context.Context, tableID descpb.ID, txn *kv.Txn,
) (*tabledesc.Mutable, error) {
	desc, err := tc.GetMutableDescriptorByID(ctx, tableID, txn)
	if err != nil {
		return nil, err
	}
	table := desc.(*tabledesc.Mutable)
	hydrated, err := tc.hydrateTypesInTableDesc(ctx, txn, table)
	if err != nil {
		return nil, err
	}
	return hydrated.(*tabledesc.Mutable), nil
}

// GetMutableDescriptorByID returns a mutable implementation of the descriptor
// with the requested id. An error is returned if no descriptor exists.
func (tc *Collection) GetMutableDescriptorByID(
	ctx context.Context, id descpb.ID, txn *kv.Txn,
) (catalog.MutableDescriptor, error) {
	log.VEventf(ctx, 2, "planner getting mutable descriptor for id %d", id)

	if desc := tc.getUncommittedDescriptorByID(id); desc != nil {
		log.VEventf(ctx, 2, "found uncommitted descriptor %d", id)
		return desc, nil
	}
	desc, err := catalogkv.GetDescriptorByID(ctx, txn, tc.codec(), id, catalogkv.Mutable,
		catalogkv.AnyDescriptorKind, true /* required */)
	if err != nil {
		return nil, err
	}
	return desc.(catalog.MutableDescriptor), nil
}

// ResolveSchemaByID looks up a schema by ID.
//
// TODO(ajwerner): refactor this to take flags or more generally conform to the
// other resolution APIs.
func (tc *Collection) ResolveSchemaByID(
	ctx context.Context, txn *kv.Txn, schemaID descpb.ID,
) (catalog.ResolvedSchema, error) {
	return tc.resolveSchemaByID(ctx, txn, schemaID, tree.SchemaLookupFlags{
		Required: true,
	})
}

func (tc *Collection) resolveSchemaByID(
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
	var desc catalog.Descriptor
	var err error
	if flags.RequireMutable {
		// Note that this throws away the flags in general.
		desc, err = tc.GetMutableDescriptorByID(ctx, schemaID, txn)
		if err == nil {
			err = catalog.FilterDescriptorState(desc, flags)
		}
	} else {
		desc, err = tc.getDescriptorVersionByID(
			ctx, txn, schemaID, flags, true, /* setTxnDeadline */
		)
	}
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
			sc, err := tc.resolveSchemaByID(ctx, txn, desc.ParentSchemaID, tree.SchemaLookupFlags{
				Required:       true,
				RequireMutable: true,
				IncludeOffline: true,
			})
			if err != nil {
				return tree.TypeName{}, nil, err
			}
			name := tree.MakeNewQualifiedTypeName(dbDesc.GetName(), sc.Name, desc.Name)
			return name, desc, nil
		}

		return desc, typedesc.HydrateTypesInTableDescriptor(ctx, t.TableDesc(), typedesc.TypeLookupFunc(getType))
	case *tabledesc.Immutable:
		// ImmutableTableDescriptors need to be copied before hydration, because
		// they are potentially read by multiple threads. If there aren't any user
		// defined types in the descriptor, then return early.
		if !t.ContainsUserDefinedTypes() {
			return desc, nil
		}

		getType := typedesc.TypeLookupFunc(func(
			ctx context.Context, id descpb.ID,
		) (tree.TypeName, catalog.TypeDescriptor, error) {
			desc, err := tc.GetTypeVersionByID(ctx, txn, id, tree.ObjectLookupFlagsWithRequired())
			if err != nil {
				return tree.TypeName{}, nil, err
			}
			dbDesc, err := tc.GetDatabaseVersionByID(ctx, txn, desc.ParentID,
				tree.DatabaseLookupFlags{Required: true})
			if err != nil {
				return tree.TypeName{}, nil, err
			}
			sc, err := tc.ResolveSchemaByID(ctx, txn, desc.ParentSchemaID)
			if err != nil {
				return tree.TypeName{}, nil, err
			}
			name := tree.MakeNewQualifiedTypeName(dbDesc.Name, sc.Name, desc.Name)
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
		return tabledesc.NewImmutableWithIsUncommittedVersion(*descBase, t.IsUncommittedVersion()), nil
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
		if err := tc.leaseMgr.Release(desc); err != nil {
			log.Warningf(ctx, "%v", err)
		}
	}
}

// ReleaseLeases releases all leases. Errors are logged but ignored.
func (tc *Collection) ReleaseLeases(ctx context.Context) {
	log.VEventf(ctx, 2, "releasing %d descriptors", tc.leasedDescriptors.numDescriptors())
	toRelease := tc.leasedDescriptors.releaseAll()
	for _, desc := range toRelease {
		if err := tc.leaseMgr.Release(desc); err != nil {
			log.Warningf(ctx, "%v", err)
		}
	}
}

// ReleaseAll releases all state currently held by the Collection.
// ReleaseAll calls ReleaseLeases.
func (tc *Collection) ReleaseAll(ctx context.Context) {
	tc.ReleaseLeases(ctx)
	tc.uncommittedDescriptors = nil
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
// or carry the subsequent version to the original version.
func (tc *Collection) AddUncommittedDescriptor(desc catalog.MutableDescriptor) error {
	if desc.GetVersion() != desc.OriginalVersion()+1 {
		return errors.AssertionFailedf(
			"descriptor version %d not incremented from cluster version %d",
			desc.GetVersion(), desc.OriginalVersion())
	}
	tbl := uncommittedDescriptor{
		mutable:   desc,
		immutable: desc.ImmutableCopy(),
	}
	for i, d := range tc.uncommittedDescriptors {
		if d.mutable.GetID() == desc.GetID() {
			tc.uncommittedDescriptors[i] = tbl
			return nil
		}
	}
	tc.uncommittedDescriptors = append(tc.uncommittedDescriptors, tbl)
	tc.releaseAllDescriptors()
	return nil
}

// WriteDescToBatch calls MaybeIncrementVersion, adds the descriptor to the
// collection as an uncommitted descriptor, and writes it into b.
func (tc *Collection) WriteDescToBatch(
	ctx context.Context, kvTrace bool, desc catalog.MutableDescriptor, b *kv.Batch,
) error {
	desc.MaybeIncrementVersion()
	// TODO(ajwerner): Add validation here.
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
		if mut := desc.mutable; !mut.IsNew() {
			descs = append(descs, lease.NewIDVersionPrev(mut.OriginalName(), mut.OriginalID(), mut.OriginalVersion()))
		}
	}
	return descs
}

// GetUncommittedTables returns all the tables updated or created in the
// transaction.
func (tc *Collection) GetUncommittedTables() (tables []*tabledesc.Immutable) {
	for _, desc := range tc.uncommittedDescriptors {
		if table, ok := desc.immutable.(*tabledesc.Immutable); ok {
			tables = append(tables, table)
		}
	}
	return tables
}

// User defined type accessors.

// GetMutableTypeDescriptor is the equivalent of GetMutableTableDescriptor but
// for accessing types.
func (tc *Collection) GetMutableTypeDescriptor(
	ctx context.Context, txn *kv.Txn, tn *tree.TypeName, flags tree.ObjectLookupFlags,
) (*typedesc.Mutable, error) {
	desc, err := tc.getMutableObjectDescriptor(ctx, txn, tn, flags)
	if err != nil {
		return nil, err
	}
	mutDesc, ok := desc.(*typedesc.Mutable)
	if !ok {
		if flags.Required {
			return nil, sqlerrors.NewUndefinedTypeError(tn)
		}
		return nil, nil
	}
	return mutDesc, nil
}

// GetMutableTypeVersionByID is the equivalent of GetMutableTableDescriptorByID
// but for accessing types.
func (tc *Collection) GetMutableTypeVersionByID(
	ctx context.Context, txn *kv.Txn, typeID descpb.ID,
) (*typedesc.Mutable, error) {
	desc, err := tc.GetMutableDescriptorByID(ctx, typeID, txn)
	if err != nil {
		return nil, err
	}
	return desc.(*typedesc.Mutable), nil
}

// GetTypeVersion is the equivalent of GetTableVersion but for accessing types.
func (tc *Collection) GetTypeVersion(
	ctx context.Context, txn *kv.Txn, tn *tree.TypeName, flags tree.ObjectLookupFlags,
) (*typedesc.Immutable, error) {
	desc, err := tc.getObjectVersion(ctx, txn, tn, flags)
	if err != nil {
		return nil, err
	}
	typ, ok := desc.(*typedesc.Immutable)
	if !ok {
		if flags.Required {
			return nil, sqlerrors.NewUndefinedTypeError(tn)
		}
		return nil, nil
	}
	return typ, nil
}

// GetTypeVersionByID is the equivalent of GetTableVersionByID but for accessing
// types.
func (tc *Collection) GetTypeVersionByID(
	ctx context.Context, txn *kv.Txn, typeID descpb.ID, flags tree.ObjectLookupFlags,
) (*typedesc.Immutable, error) {
	desc, err := tc.getDescriptorVersionByID(ctx, txn, typeID, flags.CommonLookupFlags, true /* setTxnDeadline */)
	if err != nil {
		if errors.Is(err, catalog.ErrDescriptorNotFound) {
			return nil, pgerror.Newf(
				pgcode.UndefinedObject, "type with ID %d does not exist", typeID)
		}
		return nil, err
	}
	typ, ok := desc.(*typedesc.Immutable)
	if !ok {
		return nil, pgerror.Newf(
			pgcode.UndefinedObject, "type with ID %d does not exist", typeID)
	}
	return typ, nil
}

// getUncommittedDescriptor returns a descriptor for the requested name
// if the requested name is for a descriptor modified within the transaction
// affiliated with the LeaseCollection.
//
// The first return value "refuseFurtherLookup" is true when there is
// a known deletion of that descriptor, so it would be invalid to miss the
// cache and go to KV (where the descriptor prior to the DROP may
// still exist).
func (tc *Collection) getUncommittedDescriptor(
	dbID descpb.ID, schemaID descpb.ID, name string, flags tree.CommonLookupFlags,
) (refuseFurtherLookup bool, desc uncommittedDescriptor, err error) {
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
				// Name has gone away.
				if flags.Required {
					// If it's required here, say it doesn't exist.
					err = sqlerrors.NewUndefinedRelationError(tree.NewUnqualifiedTableName(tree.Name(name)))
				}
				// The desc collection knows better; the caller has to avoid
				// going to KV in any case: refuseFurtherLookup = true
				return true, uncommittedDescriptor{}, err
			}
		}

		// Do we know about a descriptor with this name?
		if lease.NameMatchesDescriptor(mutDesc, dbID, schemaID, name) {
			// Right state?
			if err = catalog.FilterDescriptorState(mutDesc, flags); err != nil &&
				!catalog.HasAddingTableError(err) {
				if !flags.Required {
					// If it's not required here, we simply say we don't have it.
					err = nil
				}
				// The desc collection knows better; the caller has to avoid
				// going to KV in any case: refuseFurtherLookup = true
				return true, uncommittedDescriptor{}, err
			}

			// Got a descriptor.
			return false, desc, nil
		}
	}
	return false, uncommittedDescriptor{}, nil
}

// GetUncommittedTableByID returns an uncommitted table by its ID.
func (tc *Collection) GetUncommittedTableByID(id descpb.ID) *tabledesc.Mutable {
	desc := tc.getUncommittedDescriptorByID(id)
	if desc != nil {
		if table, ok := desc.(*tabledesc.Mutable); ok {
			return table
		}
	}
	return nil
}

func (tc *Collection) getUncommittedDescriptorByID(id descpb.ID) catalog.MutableDescriptor {
	for i := range tc.uncommittedDescriptors {
		desc := &tc.uncommittedDescriptors[i]
		if desc.mutable.GetID() == id {
			return desc.mutable
		}
	}
	return nil
}

// GetAllDescriptors returns all descriptors visible by the transaction,
// first checking the Collection's cached descriptors for validity if validate
// is set to true before defaulting to a key-value scan, if necessary.
func (tc *Collection) GetAllDescriptors(
	ctx context.Context, txn *kv.Txn, validate bool,
) ([]catalog.Descriptor, error) {
	if tc.allDescriptors == nil {
		descs, err := catalogkv.GetAllDescriptors(ctx, txn, tc.codec(), validate)
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

		tc.allDescriptors = descs
	}
	return tc.allDescriptors, nil
}

// HydrateGivenDescriptors installs type metadata in the types present for all
// table descriptors in the slice of descriptors. It is exported so resolution
// on sets of descriptors can hydrate a set of descriptors (i.e. on BACKUPs).
func HydrateGivenDescriptors(ctx context.Context, descs []catalog.Descriptor) error {
	// Collect the needed information to set up metadata in those types.
	dbDescs := make(map[descpb.ID]*dbdesc.Immutable)
	typDescs := make(map[descpb.ID]*typedesc.Immutable)
	schemaDescs := make(map[descpb.ID]*schemadesc.Immutable)
	for _, desc := range descs {
		switch desc := desc.(type) {
		case *dbdesc.Immutable:
			dbDescs[desc.GetID()] = desc
		case *typedesc.Immutable:
			typDescs[desc.GetID()] = desc
		case *schemadesc.Immutable:
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
			dbDesc, ok := dbDescs[typDesc.ParentID]
			if !ok {
				n := fmt.Sprintf("[%d]", typDesc.ParentID)
				return tree.TypeName{}, nil, sqlerrors.NewUndefinedDatabaseError(n)
			}
			// We don't use the collection's ResolveSchemaByID method here because
			// we already have all of the descriptors. User defined types are only
			// members of the public schema or a user defined schema, so those are
			// the only cases we have to consider here.
			var scName string
			switch typDesc.ParentSchemaID {
			case keys.PublicSchemaID:
				scName = tree.PublicSchema
			default:
				scName = schemaDescs[typDesc.ParentSchemaID].Name
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
			if tblDesc, ok := desc.(*tabledesc.Immutable); ok {
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
	ctx context.Context, txn *kv.Txn, allowMissingDesc bool,
) ([]*dbdesc.Immutable, error) {
	if tc.allDatabaseDescriptors == nil {
		dbDescIDs, err := catalogkv.GetAllDatabaseDescriptorIDs(ctx, txn, tc.codec())
		if err != nil {
			return nil, err
		}
		dbDescs, err := catalogkv.GetDatabaseDescriptorsFromIDs(
			ctx, txn, tc.codec(), dbDescIDs, allowMissingDesc,
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

// releaseAllDescriptors releases the cached slice of all descriptors
// held by Collection.
func (tc *Collection) releaseAllDescriptors() {
	tc.allDescriptors = nil
	tc.allDatabaseDescriptors = nil
	tc.allSchemasForDatabase = nil
}

// CopyModifiedObjects copies the modified schema to the table collection. Used
// when initializing an InternalExecutor.
func (tc *Collection) CopyModifiedObjects(to *Collection) {
	if tc == nil {
		return
	}
	to.uncommittedDescriptors = tc.uncommittedDescriptors
	// Do not copy the leased descriptors because we do not want
	// the leased descriptors to be released by the "to" Collection.
	// The "to" Collection can re-lease the same descriptors.
}

// ModifiedCollectionCopier is an interface used to copy modified schema elements
// to a new Collection.
type ModifiedCollectionCopier interface {
	CopyModifiedObjects(to *Collection)
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
func (df *DistSQLTypeResolverFactory) NewTypeResolver(txn *kv.Txn) *DistSQLTypeResolver {
	if df == nil {
		return nil
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
func NewDistSQLTypeResolver(descs *Collection, txn *kv.Txn) *DistSQLTypeResolver {
	return &DistSQLTypeResolver{
		descriptors: descs,
		txn:         txn,
	}
}

// ResolveType implements the tree.TypeReferenceResolver interface.
func (dt *DistSQLTypeResolver) ResolveType(
	context.Context, *tree.UnresolvedObjectName,
) (*types.T, error) {
	return nil, errors.AssertionFailedf("cannot resolve types in DistSQL by name")
}

// ResolveTypeByOID implements the tree.TypeReferenceResolver interface.
func (dt *DistSQLTypeResolver) ResolveTypeByOID(
	ctx context.Context, oid oid.Oid,
) (*types.T, error) {
	name, desc, err := dt.GetTypeDescriptor(ctx, typedesc.UserDefinedTypeOIDToID(oid))
	if err != nil {
		return nil, err
	}
	return desc.MakeTypesT(ctx, &name, dt)
}

// GetTypeDescriptor implements the sqlbase.TypeDescriptorResolver interface.
func (dt *DistSQLTypeResolver) GetTypeDescriptor(
	ctx context.Context, id descpb.ID,
) (tree.TypeName, catalog.TypeDescriptor, error) {
	desc, err := dt.descriptors.getDescriptorVersionByID(
		ctx,
		dt.txn,
		id,
		tree.CommonLookupFlags{Required: true},
		false, /* setTxnDeadline */
	)
	if err != nil {
		return tree.TypeName{}, nil, err
	}
	name := tree.MakeUnqualifiedTypeName(tree.Name(desc.GetName()))
	return name, desc.(*typedesc.Immutable), nil
}

// HydrateTypeSlice installs metadata into a slice of types.T's.
func (dt *DistSQLTypeResolver) HydrateTypeSlice(ctx context.Context, typs []*types.T) error {
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
