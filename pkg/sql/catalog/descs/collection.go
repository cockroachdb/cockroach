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
	"sync"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/database"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/lease"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/resolver"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// UncommittedDatabase is a database that has been created/dropped
// within the current transaction using the Collection. A rename
// is a drop of the old name and creation of the new name.
type UncommittedDatabase struct {
	name    string
	id      sqlbase.ID
	dropped bool
}

// UncommittedTable is a table that has been created/dropped within the
// current transaction.
type UncommittedTable struct {
	*sqlbase.MutableTableDescriptor
	*sqlbase.ImmutableTableDescriptor
}

// MakeCollection constructs a Collection.
func MakeCollection(
	leaseMgr *lease.Manager,
	settings *cluster.Settings,
	dbCache *database.Cache,
	dbCacheSubscriber DatabaseCacheSubscriber,
) Collection {
	return Collection{
		leaseMgr:          leaseMgr,
		settings:          settings,
		databaseCache:     dbCache,
		dbCacheSubscriber: dbCacheSubscriber,
	}
}

// NewCollection constructs a new *Collection.
func NewCollection(leaseMgr *lease.Manager, settings *cluster.Settings) *Collection {
	tc := MakeCollection(leaseMgr, settings, nil, nil)
	return &tc
}

// Collection is a collection of tables held by a single session that
// serves SQL requests, or a background job using a table descriptor. The
// collection is cleared using ReleaseAll() which is called at the
// end of each transaction on the session, or on hitting conditions such
// as errors, or retries that result in transaction timestamp changes.
type Collection struct {
	// leaseMgr manages acquiring and releasing per-table leases.
	leaseMgr *lease.Manager
	// A collection of table descriptor valid for the timestamp.
	// They are released once the transaction using them is complete.
	// If the transaction gets pushed and the timestamp changes,
	// the tables are released.
	leasedTables []*sqlbase.ImmutableTableDescriptor
	// Tables modified by the uncommitted transaction affiliated
	// with this Collection. This allows a transaction to see
	// its own modifications while bypassing the table lease mechanism.
	// The table lease mechanism will have its own transaction to read
	// the table and will hang waiting for the uncommitted changes to
	// the table. These table descriptors are local to this
	// Collection and invisible to other transactions. A dropped
	// table is marked dropped.
	uncommittedTables []UncommittedTable

	// databaseCache is used as a cache for database names.
	// This field is nil when the field is initialized for an internalPlanner.
	// TODO(andrei): get rid of it and replace it with a leasing system for
	// database descriptors.
	databaseCache *database.Cache

	// schemaCache maps {databaseID, schemaName} -> (schemaID, if exists, otherwise nil).
	// TODO(sqlexec): replace with leasing system with custom schemas.
	// This is currently never cleared, because there should only be unique schemas
	// being added for each Collection as only temporary schemas can be
	// made, and you cannot read from other schema caches.
	schemaCache sync.Map

	// DatabaseCacheSubscriber is used to block until the node's database cache has been
	// updated when ReleaseAll is called.
	dbCacheSubscriber DatabaseCacheSubscriber

	// Same as uncommittedTables applying to databases modified within
	// an uncommitted transaction.
	uncommittedDatabases []UncommittedDatabase

	// allDescriptors is a slice of all available descriptors. The descriptors
	// are cached to avoid repeated lookups by users like virtual tables. The
	// cache is purged whenever events would cause a scan of all descriptors to
	// return different values, such as when the txn timestamp changes or when
	// new descriptors are written in the txn.
	//
	// TODO(ajwerner): This cache may be problematic in clusters with very large
	// numbers of descriptors.
	allDescriptors []sqlbase.DescriptorInterface

	// allDatabaseDescriptors is a slice of all available database descriptors.
	// These are purged at the same time as allDescriptors.
	allDatabaseDescriptors []*sqlbase.ImmutableDatabaseDescriptor

	// allSchemasForDatabase maps databaseID -> schemaID -> schemaName.
	// For each databaseID, all schemas visible under the database can be
	// observed.
	// These are purged at the same time as allDescriptors.
	allSchemasForDatabase map[sqlbase.ID]map[sqlbase.ID]string

	// settings are required to correctly resolve system.namespace accesses in
	// mixed version (19.2/20.1) clusters.
	// TODO(solon): This field could maybe be removed in 20.2.
	settings *cluster.Settings
}

// isSupportedSchemaName returns whether this schema name is supported.
// TODO(sqlexec): this should be deleted when we use custom schemas.
// However, this introduces an extra lookup for cases where `<database>.<table>`
// is looked up.
// See #44733.
func isSupportedSchemaName(n tree.Name) bool {
	return n == tree.PublicSchemaName || strings.HasPrefix(string(n), "pg_temp")
}

// GetMutableTableDescriptor returns a mutable table descriptor.
//
// If flags.required is false, GetMutableTableDescriptor() will gracefully
// return a nil descriptor and no error if the table does not exist.
//
func (tc *Collection) GetMutableTableDescriptor(
	ctx context.Context, txn *kv.Txn, tn *tree.TableName, flags tree.ObjectLookupFlags,
) (*sqlbase.MutableTableDescriptor, error) {
	if log.V(2) {
		log.Infof(ctx, "reading mutable descriptor on table '%s'", tn)
	}

	if !isSupportedSchemaName(tn.SchemaName) {
		return nil, nil
	}

	refuseFurtherLookup, dbID, err := tc.GetUncommittedDatabaseID(tn.Catalog(), flags.Required)
	if refuseFurtherLookup || err != nil {
		return nil, err
	}

	if dbID == sqlbase.InvalidID && tc.DatabaseCache() != nil {
		// Resolve the database from the database cache when the transaction
		// hasn't modified the database.
		dbID, err = tc.DatabaseCache().GetDatabaseID(ctx, tc.leaseMgr.DB().Txn, tn.Catalog(), flags.Required)
		if err != nil || dbID == sqlbase.InvalidID {
			// dbID can still be invalid if required is false and the database is not found.
			return nil, err
		}
	}

	// The following checks only work if the dbID is not invalid.
	if dbID != sqlbase.InvalidID {
		// Resolve the schema to the ID of the schema.
		foundSchema, schemaID, err := tc.ResolveSchemaID(ctx, txn, dbID, tn.Schema())
		if err != nil || !foundSchema {
			return nil, err
		}

		if refuseFurtherLookup, table, err := tc.getUncommittedTable(
			dbID,
			schemaID,
			tn,
			flags.Required,
		); refuseFurtherLookup || err != nil {
			return nil, err
		} else if mut := table.MutableTableDescriptor; mut != nil {
			log.VEventf(ctx, 2, "found uncommitted table %d", mut.ID)
			return mut, nil
		}
	}

	phyAccessor := catalogkv.UncachedPhysicalAccessor{}
	obj, err := phyAccessor.GetObjectDesc(
		ctx,
		txn,
		tc.settings,
		tc.codec(),
		tn.Catalog(),
		tn.Schema(),
		tn.Table(),
		flags,
	)
	if obj == nil {
		return nil, err
	}
	mutDesc, ok := obj.(*sqlbase.MutableTableDescriptor)
	if !ok {
		return nil, err
	}
	return mutDesc, nil
}

// ResolveSchemaID attempts to lookup the schema from the schemaCache if it exists,
// otherwise falling back to a database lookup.
func (tc *Collection) ResolveSchemaID(
	ctx context.Context, txn *kv.Txn, dbID sqlbase.ID, schemaName string,
) (bool, sqlbase.ID, error) {
	// Fast path public schema, as it is always found.
	if schemaName == tree.PublicSchema {
		return true, keys.PublicSchemaID, nil
	}

	type schemaCacheKey struct {
		dbID       sqlbase.ID
		schemaName string
	}

	key := schemaCacheKey{dbID: dbID, schemaName: schemaName}
	// First lookup the cache.
	if val, ok := tc.schemaCache.Load(key); ok {
		return true, val.(sqlbase.ID), nil
	}

	// Next, try lookup the result from KV, storing and returning the value.
	exists, schemaID, err := catalogkv.ResolveSchemaID(ctx, txn, tc.codec(), dbID, schemaName)
	if err != nil || !exists {
		return exists, schemaID, err
	}
	tc.schemaCache.Store(key, schemaID)
	return exists, schemaID, err
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
) (*sqlbase.ImmutableTableDescriptor, error) {
	if log.V(2) {
		log.Infof(ctx, "planner acquiring lease on table '%s'", tn)
	}

	if !isSupportedSchemaName(tn.SchemaName) {
		return nil, nil
	}

	readTableFromStore := func() (*sqlbase.ImmutableTableDescriptor, error) {
		phyAccessor := catalogkv.UncachedPhysicalAccessor{}
		obj, err := phyAccessor.GetObjectDesc(
			ctx,
			txn,
			tc.settings,
			tc.codec(),
			tn.Catalog(),
			tn.Schema(),
			tn.Table(),
			flags,
		)
		if obj == nil {
			return nil, err
		}
		tbl, ok := obj.(*sqlbase.ImmutableTableDescriptor)
		if !ok {
			return nil, err
		}
		return tbl, err
	}

	refuseFurtherLookup, dbID, err := tc.GetUncommittedDatabaseID(tn.Catalog(), flags.Required)
	if refuseFurtherLookup || err != nil {
		return nil, err
	}

	if dbID == sqlbase.InvalidID && tc.DatabaseCache() != nil {
		// Resolve the database from the database cache when the transaction
		// hasn't modified the database.
		dbID, err = tc.DatabaseCache().GetDatabaseID(ctx, tc.leaseMgr.DB().Txn, tn.Catalog(), flags.Required)
		if err != nil || dbID == sqlbase.InvalidID {
			// dbID can still be invalid if required is false and the database is not found.
			return nil, err
		}
	}

	// If at this point we have an InvalidID, we should immediately try read from store.
	if dbID == sqlbase.InvalidID {
		return readTableFromStore()
	}

	// Resolve the schema to the ID of the schema.
	foundSchema, schemaID, err := tc.ResolveSchemaID(ctx, txn, dbID, tn.Schema())
	if err != nil || !foundSchema {
		return nil, err
	}

	// TODO(vivek): Ideally we'd avoid caching for only the
	// system.descriptor and system.lease tables, because they are
	// used for acquiring leases, creating a chicken&egg problem.
	// But doing so turned problematic and the tests pass only by also
	// disabling caching of system.eventlog, system.rangelog, and
	// system.users. For now we're sticking to disabling caching of
	// all system descriptors except the role-members-table.
	avoidCache := flags.AvoidCached || lease.TestingTableLeasesAreDisabled() ||
		(tn.Catalog() == sqlbase.SystemDatabaseName && tn.ObjectName.String() != sqlbase.RoleMembersTable.Name)

	if refuseFurtherLookup, table, err := tc.getUncommittedTable(
		dbID,
		schemaID,
		tn,
		flags.Required,
	); refuseFurtherLookup || err != nil {
		return nil, err
	} else if immut := table.ImmutableTableDescriptor; immut != nil {
		// If not forcing to resolve using KV, tables being added aren't visible.
		if immut.Adding() && !avoidCache {
			if !flags.Required {
				return nil, nil
			}
			return nil, sqlbase.FilterTableState(immut.TableDesc())
		}

		log.VEventf(ctx, 2, "found uncommitted table %d", immut.ID)
		return immut, nil
	}

	if avoidCache {
		return readTableFromStore()
	}

	// First, look to see if we already have the table.
	// This ensures that, once a SQL transaction resolved name N to id X, it will
	// continue to use N to refer to X even if N is renamed during the
	// transaction.
	for _, table := range tc.leasedTables {
		if lease.NameMatchesDescriptor(table, dbID, schemaID, tn.Table()) {
			log.VEventf(ctx, 2, "found table in table collection for table '%s'", tn)
			return table, nil
		}
	}

	readTimestamp := txn.ReadTimestamp()
	desc, expiration, err := tc.leaseMgr.AcquireByName(ctx, readTimestamp, dbID, schemaID, tn.Table())
	if err != nil {
		// Read the descriptor from the store in the face of some specific errors
		// because of a known limitation of AcquireByName. See the known
		// limitations of AcquireByName for details.
		if sqlbase.HasInactiveTableError(err) ||
			errors.Is(err, sqlbase.ErrDescriptorNotFound) {
			return readTableFromStore()
		}
		// Lease acquisition failed with some other error. This we don't
		// know how to deal with, so propagate the error.
		return nil, err
	}
	table, ok := desc.(*sqlbase.ImmutableTableDescriptor)
	if !ok {
		if flags.Required {
			return nil, sqlbase.NewUndefinedRelationError(tn)
		}
		return nil, nil
	}

	if expiration.LessEq(readTimestamp) {
		log.Fatalf(ctx, "bad table for T=%s, expiration=%s", readTimestamp, expiration)
	}

	tc.leasedTables = append(tc.leasedTables, table)
	log.VEventf(ctx, 2, "added table '%s' to table collection", tn)

	// If the table we just acquired expires before the txn's deadline, reduce
	// the deadline. We use ReadTimestamp() that doesn't return the commit timestamp,
	// so we need to set a deadline on the transaction to prevent it from committing
	// beyond the table version expiration time.
	txn.UpdateDeadlineMaybe(ctx, expiration)
	return table, nil
}

// GetTableVersionByID is a by-ID variant of GetTableVersion (i.e. uses same cache).
func (tc *Collection) GetTableVersionByID(
	ctx context.Context, txn *kv.Txn, tableID sqlbase.ID, flags tree.ObjectLookupFlags,
) (*sqlbase.ImmutableTableDescriptor, error) {
	log.VEventf(ctx, 2, "planner getting table on table ID %d", tableID)

	if flags.AvoidCached || lease.TestingTableLeasesAreDisabled() {
		table, err := sqlbase.GetTableDescFromID(ctx, txn, tc.codec(), tableID)
		if err != nil {
			return nil, err
		}
		if err := sqlbase.FilterTableState(table); err != nil {
			return nil, err
		}
		return sqlbase.NewImmutableTableDescriptor(*table), nil
	}

	for _, table := range tc.uncommittedTables {
		if immut := table.ImmutableTableDescriptor; immut.ID == tableID {
			log.VEventf(ctx, 2, "found uncommitted table %d", tableID)
			if immut.Dropped() {
				return nil, sqlbase.NewUndefinedRelationError(
					tree.NewUnqualifiedTableName(tree.Name(fmt.Sprintf("<id=%d>", tableID))),
				)
			}
			return immut, nil
		}
	}

	// First, look to see if we already have the table -- including those
	// via `GetTableVersion`.
	for _, table := range tc.leasedTables {
		if table.ID == tableID {
			log.VEventf(ctx, 2, "found table %d in table cache", tableID)
			return table, nil
		}
	}

	readTimestamp := txn.ReadTimestamp()
	desc, expiration, err := tc.leaseMgr.Acquire(ctx, readTimestamp, tableID)
	if err != nil {
		if errors.Is(err, sqlbase.ErrDescriptorNotFound) {
			// Transform the descriptor error into an error that references the
			// table's ID.
			return nil, sqlbase.NewUndefinedRelationError(
				&tree.TableRef{TableID: int64(tableID)})
		}
		return nil, err
	}
	// Also return an error if the descriptor isn't a table.
	table, ok := desc.(*sqlbase.ImmutableTableDescriptor)
	if !ok {
		return nil, sqlbase.NewUndefinedRelationError(
			&tree.TableRef{TableID: int64(tableID)})
	}

	if expiration.LessEq(readTimestamp) {
		log.Fatalf(ctx, "bad table for T=%s, expiration=%s", readTimestamp, expiration)
	}

	tc.leasedTables = append(tc.leasedTables, table)
	log.VEventf(ctx, 2, "added table '%s' to table collection", table.Name)

	// If the table we just acquired expires before the txn's deadline, reduce
	// the deadline. We use ReadTimestamp() that doesn't return the commit timestamp,
	// so we need to set a deadline on the transaction to prevent it from committing
	// beyond the table version expiration time.
	txn.UpdateDeadlineMaybe(ctx, expiration)
	return table, nil
}

// GetMutableTableVersionByID is a variant of sqlbase.GetTableDescFromID which returns a mutable
// table descriptor of the table modified in the same transaction.
func (tc *Collection) GetMutableTableVersionByID(
	ctx context.Context, tableID sqlbase.ID, txn *kv.Txn,
) (*sqlbase.MutableTableDescriptor, error) {
	log.VEventf(ctx, 2, "planner getting mutable table on table ID %d", tableID)

	if table := tc.GetUncommittedTableByID(tableID).MutableTableDescriptor; table != nil {
		log.VEventf(ctx, 2, "found uncommitted table %d", tableID)
		return table, nil
	}
	return sqlbase.GetMutableTableDescFromID(ctx, txn, tc.codec(), tableID)
}

// ReleaseTableLeases releases the leases for the tables with ids in
// the passed slice. Errors are logged but ignored.
func (tc *Collection) ReleaseTableLeases(ctx context.Context, tables []lease.IDVersion) {
	// Sort the tables and leases to make it easy to find the leases to release.
	leasedTables := tc.leasedTables
	sort.Slice(tables, func(i, j int) bool {
		return tables[i].ID < tables[j].ID
	})
	sort.Slice(leasedTables, func(i, j int) bool {
		return leasedTables[i].ID < leasedTables[j].ID
	})

	filteredLeases := leasedTables[:0] // will store the remaining leases
	tablesToConsider := tables
	shouldRelease := func(id sqlbase.ID) (found bool) {
		for len(tablesToConsider) > 0 && tablesToConsider[0].ID < id {
			tablesToConsider = tablesToConsider[1:]
		}
		return len(tablesToConsider) > 0 && tablesToConsider[0].ID == id
	}
	for _, l := range leasedTables {
		if !shouldRelease(l.ID) {
			filteredLeases = append(filteredLeases, l)
		} else if err := tc.leaseMgr.Release(l); err != nil {
			log.Warningf(ctx, "%v", err)
		}
	}
	tc.leasedTables = filteredLeases
}

// ReleaseLeases releases the leases for the tables with ids in
// the passed slice. Errors are logged but ignored.
func (tc *Collection) ReleaseLeases(ctx context.Context) {
	if len(tc.leasedTables) > 0 {
		log.VEventf(ctx, 2, "releasing %d tables", len(tc.leasedTables))
		for _, table := range tc.leasedTables {
			if err := tc.leaseMgr.Release(table); err != nil {
				log.Warningf(ctx, "%v", err)
			}
		}
		tc.leasedTables = tc.leasedTables[:0]
	}
}

// ReleaseAll releases all state currently held by the Collection.
// ReleaseAll calls ReleaseLeases.
func (tc *Collection) ReleaseAll(ctx context.Context) {
	tc.ReleaseLeases(ctx)
	tc.uncommittedTables = nil
	tc.uncommittedDatabases = nil
	tc.releaseAllDescriptors()
}

// WaitForCacheToDropDatabases waits until the database cache has been updated
// to properly reflect all dropped databases, so that future commands on the
// same gateway node observe the dropped databases.
func (tc *Collection) WaitForCacheToDropDatabases(ctx context.Context) {
	for _, uc := range tc.uncommittedDatabases {
		if !uc.dropped {
			continue
		}
		// Wait until the database cache has been updated to properly
		// reflect a dropped database, so that future commands on the
		// same gateway node observe the dropped database.
		tc.dbCacheSubscriber.WaitForCacheState(
			func(dc *database.Cache) bool {
				// Resolve the database name from the database cache.
				dbID, err := dc.GetCachedDatabaseID(uc.name)
				if err != nil || dbID == sqlbase.InvalidID {
					// dbID can still be 0 if required is false and
					// the database is not found. Swallowing error here
					// because it was felt there was no value in returning
					// it to a higher layer only to be swallow there. This
					// entire codepath is only called from one place so
					// it's better to swallow it here.
					return true
				}

				// If the database name still exists but it now references another
				// db with a more recent id, we're good - it means that the database
				// name has been reused.
				return dbID > uc.id
			})
	}
}

// HasUncommittedTables returns true if the Collection contains uncommitted
// tables.
func (tc *Collection) HasUncommittedTables() bool {
	return len(tc.uncommittedTables) > 0
}

// AddUncommittedTable adds desc to the Collection.
func (tc *Collection) AddUncommittedTable(desc sqlbase.MutableTableDescriptor) error {
	if desc.Version != desc.ClusterVersion.Version+1 {
		return errors.Errorf(
			"descriptor version %d not incremented from cluster version %d",
			desc.Version, desc.ClusterVersion.Version)
	}
	tbl := UncommittedTable{
		MutableTableDescriptor:   &desc,
		ImmutableTableDescriptor: sqlbase.NewImmutableTableDescriptor(desc.TableDescriptor),
	}
	for i, table := range tc.uncommittedTables {
		if table.MutableTableDescriptor.ID == desc.ID {
			tc.uncommittedTables[i] = tbl
			return nil
		}
	}
	tc.uncommittedTables = append(tc.uncommittedTables, tbl)
	tc.releaseAllDescriptors()
	return nil
}

// GetTablesWithNewVersion returns all the idVersion pairs that have undergone a
// schema change. Returns nil for no schema changes. The version returned for
// each schema change is ClusterVersion - 1, because that's the one that will be
// used when checking for table descriptor two version invariance.
// Also returns strings representing the new <name, version> pairs
func (tc *Collection) GetTablesWithNewVersion() []lease.IDVersion {
	var tables []lease.IDVersion
	for _, table := range tc.uncommittedTables {
		if mut := table.MutableTableDescriptor; !mut.IsNewTable() {
			clusterVer := &mut.ClusterVersion
			tables = append(tables, lease.NewIDVersionPrev(clusterVer.Name, clusterVer.ID, clusterVer.Version))
		}
	}
	return tables
}

// GetTableDescsWithNewVersion is like GetTablesWithNewVersion but returns descriptors.
func (tc *Collection) GetTableDescsWithNewVersion() (
	newTables []*sqlbase.ImmutableTableDescriptor,
) {
	for _, table := range tc.uncommittedTables {
		if mut := table.MutableTableDescriptor; mut.IsNewTable() {
			newTables = append(newTables, table.ImmutableTableDescriptor)
		}
	}
	return newTables
}

// DBAction is an operation to an uncommitted database.
type DBAction bool

const (
	// DBCreated notes that the database has been created.
	DBCreated DBAction = false
	// DBDropped notes that the database has been dropped.
	DBDropped DBAction = true
)

// AddUncommittedDatabase stages the database action for the relevant database.
func (tc *Collection) AddUncommittedDatabase(name string, id sqlbase.ID, action DBAction) {
	db := UncommittedDatabase{name: name, id: id, dropped: action == DBDropped}
	tc.uncommittedDatabases = append(tc.uncommittedDatabases, db)
	tc.releaseAllDescriptors()
}

// GetUncommittedDatabaseID returns a database ID for the requested tablename
// if the requested tablename is for a database modified within the transaction
// affiliated with the LeaseCollection.
func (tc *Collection) GetUncommittedDatabaseID(
	requestedDbName string, required bool,
) (c bool, res sqlbase.ID, err error) {
	// Walk latest to earliest so that a DROP DATABASE followed by a
	// CREATE DATABASE with the same name will result in the CREATE DATABASE
	// being seen.
	for i := len(tc.uncommittedDatabases) - 1; i >= 0; i-- {
		db := tc.uncommittedDatabases[i]
		if requestedDbName == db.name {
			if db.dropped {
				if required {
					return true, sqlbase.InvalidID, sqlbase.NewUndefinedDatabaseError(requestedDbName)
				}
				return true, sqlbase.InvalidID, nil
			}
			return false, db.id, nil
		}
	}
	return false, sqlbase.InvalidID, nil
}

// getUncommittedTable returns a table for the requested tablename
// if the requested tablename is for a table modified within the transaction
// affiliated with the LeaseCollection.
//
// The first return value "refuseFurtherLookup" is true when there is
// a known deletion of that table, so it would be invalid to miss the
// cache and go to KV (where the descriptor prior to the DROP may
// still exist).
func (tc *Collection) getUncommittedTable(
	dbID sqlbase.ID, schemaID sqlbase.ID, tn *tree.TableName, required bool,
) (refuseFurtherLookup bool, table UncommittedTable, err error) {
	// Walk latest to earliest so that a DROP TABLE followed by a CREATE TABLE
	// with the same name will result in the CREATE TABLE being seen.
	for i := len(tc.uncommittedTables) - 1; i >= 0; i-- {
		table := tc.uncommittedTables[i]
		mutTbl := table.MutableTableDescriptor
		// If a table has gotten renamed we'd like to disallow using the old names.
		// The renames could have happened in another transaction but it's still okay
		// to disallow the use of the old name in this transaction because the other
		// transaction has already committed and this transaction is seeing the
		// effect of it.
		for _, drain := range mutTbl.DrainingNames {
			if drain.Name == string(tn.ObjectName) &&
				drain.ParentID == dbID &&
				drain.ParentSchemaID == schemaID {
				// Table name has gone away.
				if required {
					// If it's required here, say it doesn't exist.
					err = sqlbase.NewUndefinedRelationError(tn)
				}
				// The table collection knows better; the caller has to avoid
				// going to KV in any case: refuseFurtherLookup = true
				return true, UncommittedTable{}, err
			}
		}

		// Do we know about a table with this name?
		if lease.NameMatchesDescriptor(
			mutTbl,
			dbID,
			schemaID,
			tn.Table(),
		) {
			// Right state?
			if err = sqlbase.FilterTableState(mutTbl.TableDesc()); err != nil && !sqlbase.HasAddingTableError(err) {
				if !required {
					// If it's not required here, we simply say we don't have it.
					err = nil
				}
				// The table collection knows better; the caller has to avoid
				// going to KV in any case: refuseFurtherLookup = true
				return true, UncommittedTable{}, err
			}

			// Got a table.
			return false, table, nil
		}
	}
	return false, UncommittedTable{}, nil
}

// GetUncommittedTableByID returns an uncommitted table by its ID.
func (tc *Collection) GetUncommittedTableByID(id sqlbase.ID) UncommittedTable {
	// Walk latest to earliest so that a DROP TABLE followed by a CREATE TABLE
	// with the same name will result in the CREATE TABLE being seen.
	for i := len(tc.uncommittedTables) - 1; i >= 0; i-- {
		table := tc.uncommittedTables[i]
		if table.MutableTableDescriptor.ID == id {
			return table
		}
	}
	return UncommittedTable{}
}

// GetAllDescriptors returns all descriptors visible by the transaction,
// first checking the Collection's cached descriptors for validity
// before defaulting to a key-value scan, if necessary.
//
// TODO(ajwerner): Have this return []sqlbase.DescriptorInterface.
func (tc *Collection) GetAllDescriptors(
	ctx context.Context, txn *kv.Txn,
) ([]sqlbase.DescriptorInterface, error) {
	if tc.allDescriptors == nil {
		descs, err := catalogkv.GetAllDescriptors(ctx, txn, tc.codec())
		if err != nil {
			return nil, err
		}
		// There could be tables with user defined types that need hydrating,
		// so collect the needed information to set up metadata in those types.
		dbDescs := make(map[sqlbase.ID]*sqlbase.ImmutableDatabaseDescriptor)
		typDescs := make(map[sqlbase.ID]*sqlbase.ImmutableTypeDescriptor)
		for _, desc := range descs {
			switch desc := desc.(type) {
			case *sqlbase.ImmutableDatabaseDescriptor:
				dbDescs[desc.GetID()] = desc
			case *sqlbase.ImmutableTypeDescriptor:
				typDescs[desc.GetID()] = desc
			}
		}
		// If we found any type descriptors, that means that some of the tables we
		// scanned might have types that need hydrating.
		if len(typDescs) > 0 {
			// Since we just scanned all the descriptors, we already have everything
			// we need to hydrate our types. Set up an accessor for the type hydration
			// method to look into the scanned set of descriptors.
			typeLookup := func(id sqlbase.ID) (*tree.TypeName, sqlbase.TypeDescriptorInterface, error) {
				typDesc := typDescs[id]
				dbDesc := dbDescs[typDesc.ParentID]
				if dbDesc == nil {
					// TODO (rohany): Since DROP TYPE has not been implemented yet
					//  (see #48363), dropped databases do not yet clean up their
					//  orphaned child type descriptors. That could lead to dbDesc being
					//  nil here. Once we support drop type, this check does not need to
					//  be performed.
					return nil, nil, errors.Newf("database id %d not found", typDesc.ParentID)
				}
				schemaName, err := resolver.ResolveSchemaNameByID(ctx, txn, tc.codec(), dbDesc.GetID(), typDesc.ParentSchemaID)
				if err != nil {
					return nil, nil, err
				}
				name := tree.MakeNewQualifiedTypeName(dbDesc.GetName(), schemaName, typDesc.GetName())
				return &name, typDesc, nil
			}
			// Now hydrate all table descriptors.
			for i := range descs {
				desc := descs[i]
				if tblDesc, ok := desc.(*sqlbase.ImmutableTableDescriptor); ok {
					if err := sqlbase.HydrateTypesInTableDescriptor(tblDesc.TableDesc(), typeLookup); err != nil {
						// If we ran into an error hydrating the types, that means that we
						// have some sort of corrupted descriptor state. Rather than disable
						// uses of GetAllDescriptors, just log the error.
						log.Errorf(ctx, "%s", err.Error())
					}
				}
			}
		}

		tc.allDescriptors = descs
	}
	return tc.allDescriptors, nil
}

// GetAllDatabaseDescriptors returns all database descriptors visible by the
// transaction, first checking the Collection's cached descriptors for
// validity before scanning system.namespace and looking up the descriptors
// in the database cache, if necessary.
func (tc *Collection) GetAllDatabaseDescriptors(
	ctx context.Context, txn *kv.Txn,
) ([]*sqlbase.ImmutableDatabaseDescriptor, error) {
	if tc.allDatabaseDescriptors == nil {
		dbDescIDs, err := catalogkv.GetAllDatabaseDescriptorIDs(ctx, txn, tc.codec())
		if err != nil {
			return nil, err
		}
		dbDescs, err := catalogkv.GetDatabaseDescriptorsFromIDs(ctx, txn, tc.codec(), dbDescIDs)
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
	ctx context.Context, txn *kv.Txn, dbID sqlbase.ID,
) (map[sqlbase.ID]string, error) {
	if tc.allSchemasForDatabase == nil {
		tc.allSchemasForDatabase = make(map[sqlbase.ID]map[sqlbase.ID]string)
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
	to.uncommittedTables = tc.uncommittedTables
	to.uncommittedDatabases = tc.uncommittedDatabases
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

// DatabaseCache returns the database.Cache.
func (tc *Collection) DatabaseCache() *database.Cache {
	return tc.databaseCache
}

// ResetDatabaseCache resets the table collection's database.Cache.
func (tc *Collection) ResetDatabaseCache(dbCache *database.Cache) {
	tc.databaseCache = dbCache
}

// MigrationSchemaChangeRequiredContext flags a schema change as necessary to
// run even in a mixed-version 19.2/20.1 state where schema changes are normally
// banned, because the schema change is being run in a startup migration. It's
// the caller's responsibility to ensure that the schema change job is safe to
// run in a mixed-version state.
//
// TODO (lucy): Remove this in 20.2.
func MigrationSchemaChangeRequiredContext(ctx context.Context) context.Context {
	return context.WithValue(ctx, migrationSchemaChangeRequiredHint{}, migrationSchemaChangeRequiredHint{})
}

// MigrationSchemaChangeRequiredFromContext returns true if the context
// indicates that a schema change should be run despite a mixed 19.2/20.1
// cluster version.
func MigrationSchemaChangeRequiredFromContext(ctx context.Context) bool {
	return ctx.Value(migrationSchemaChangeRequiredHint{}) == nil
}

type migrationSchemaChangeRequiredHint struct{}

// ErrSchemaChangeDisallowedInMixedState signifies that an attempted schema
// change was disallowed from running in a mixed-version
var ErrSchemaChangeDisallowedInMixedState = errors.New("schema change cannot be initiated in this version until the version upgrade is finalized")

// DatabaseCacheSubscriber allows the connExecutor to wait for a callback.
type DatabaseCacheSubscriber interface {
	// WaitForCacheState takes a callback depending on the cache state and blocks
	// until the callback declares success. The callback is repeatedly called as
	// the cache is updated.
	WaitForCacheState(cond func(*database.Cache) bool)
}
