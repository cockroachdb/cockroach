// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

//
// This file contains routines for low-level access to stored object
// descriptors, as well as accessors for the table cache.
//
// For higher levels in the SQL layer, these interface are likely not
// suitable; consider instead schema_accessors.go and resolver.go.
//

var testDisableTableLeases bool

// TestDisableTableLeases disables table leases and returns
// a function that can be used to enable it.
func TestDisableTableLeases() func() {
	testDisableTableLeases = true
	return func() {
		testDisableTableLeases = false
	}
}

func (p *planner) getVirtualTabler() VirtualTabler {
	return p.extendedEvalCtx.VirtualSchemas
}

var errTableAdding = errors.New("table is being added")

type inactiveTableError struct {
	error
}

func filterTableState(tableDesc *sqlbase.TableDescriptor) error {
	switch tableDesc.State {
	case sqlbase.TableDescriptor_DROP:
		return inactiveTableError{errors.New("table is being dropped")}
	case sqlbase.TableDescriptor_OFFLINE:
		err := errors.Errorf("table %q is offline", tableDesc.Name)
		if tableDesc.OfflineReason != "" {
			err = errors.Errorf("table %q is offline: %s", tableDesc.Name, tableDesc.OfflineReason)
		}
		return inactiveTableError{err}
	case sqlbase.TableDescriptor_ADD:
		return errTableAdding
	case sqlbase.TableDescriptor_PUBLIC:
		return nil
	default:
		return errors.Errorf("table in unknown state: %s", tableDesc.State.String())
	}
}

// An uncommitted database is a database that has been created/dropped
// within the current transaction using the TableCollection. A rename
// is a drop of the old name and creation of the new name.
type uncommittedDatabase struct {
	name    string
	id      sqlbase.ID
	dropped bool
}

type uncommittedTable struct {
	*sqlbase.MutableTableDescriptor
	*sqlbase.ImmutableTableDescriptor
}

// TableCollection is a collection of tables held by a single session that
// serves SQL requests, or a background job using a table descriptor. The
// collection is cleared using releaseTables() which is called at the
// end of each transaction on the session, or on hitting conditions such
// as errors, or retries that result in transaction timestamp changes.
type TableCollection struct {
	// leaseMgr manages acquiring and releasing per-table leases.
	leaseMgr *LeaseManager
	// A collection of table descriptor valid for the timestamp.
	// They are released once the transaction using them is complete.
	// If the transaction gets pushed and the timestamp changes,
	// the tables are released.
	leasedTables []*sqlbase.ImmutableTableDescriptor
	// Tables modified by the uncommitted transaction affiliated
	// with this TableCollection. This allows a transaction to see
	// its own modifications while bypassing the table lease mechanism.
	// The table lease mechanism will have its own transaction to read
	// the table and will hang waiting for the uncommitted changes to
	// the table. These table descriptors are local to this
	// TableCollection and invisible to other transactions. A dropped
	// table is marked dropped.
	uncommittedTables []uncommittedTable

	// databaseCache is used as a cache for database names.
	// This field is nil when the field is initialized for an internalPlanner.
	// TODO(andrei): get rid of it and replace it with a leasing system for
	// database descriptors.
	databaseCache *databaseCache

	// dbCacheSubscriber is used to block until the node's database cache has been
	// updated when releaseTables is called.
	dbCacheSubscriber dbCacheSubscriber

	// Same as uncommittedTables applying to databases modified within
	// an uncommitted transaction.
	uncommittedDatabases []uncommittedDatabase

	// allDescriptors is a slice of all available descriptors. The descriptors
	// are cached to avoid repeated lookups by users like virtual tables. The
	// cache is purged whenever events would cause a scan of all descriptors to
	// return different values, such as when the txn timestamp changes or when
	// new descriptors are written in the txn.
	allDescriptors []sqlbase.DescriptorProto

	// allDatabaseDescriptors is a slice of all available database descriptors.
	// These are purged at the same time as allDescriptors.
	allDatabaseDescriptors []*sqlbase.DatabaseDescriptor
}

type dbCacheSubscriber interface {
	// waitForCacheState takes a callback depending on the cache state and blocks
	// until the callback declares success. The callback is repeatedly called as
	// the cache is updated.
	waitForCacheState(cond func(*databaseCache) bool)
}

// getMutableTableDescriptor returns a mutable table descriptor.
//
// If flags.required is false, getMutableTableDescriptor() will gracefully
// return a nil descriptor and no error if the table does not exist.
//
func (tc *TableCollection) getMutableTableDescriptor(
	ctx context.Context, txn *client.Txn, tn *tree.TableName, flags tree.ObjectLookupFlags,
) (*sqlbase.MutableTableDescriptor, error) {
	if log.V(2) {
		log.Infof(ctx, "reading mutable descriptor on table '%s'", tn)
	}

	if tn.SchemaName != tree.PublicSchemaName {
		if flags.Required {
			return nil, sqlbase.NewUnsupportedSchemaUsageError(tree.ErrString(tn))
		}
		return nil, nil
	}

	refuseFurtherLookup, dbID, err := tc.getUncommittedDatabaseID(tn.Catalog(), flags.Required)
	if refuseFurtherLookup || err != nil {
		return nil, err
	}

	if dbID == sqlbase.InvalidID && tc.databaseCache != nil {
		// Resolve the database from the database cache when the transaction
		// hasn't modified the database.
		dbID, err = tc.databaseCache.getDatabaseID(ctx,
			tc.leaseMgr.db.Txn, tn.Catalog(), flags.Required)
		if err != nil || dbID == sqlbase.InvalidID {
			// dbID can still be invalid if required is false and the database is not found.
			return nil, err
		}
	}

	if refuseFurtherLookup, table, err := tc.getUncommittedTable(dbID, tn, flags.Required); refuseFurtherLookup || err != nil {
		return nil, err
	} else if mut := table.MutableTableDescriptor; mut != nil {
		log.VEventf(ctx, 2, "found uncommitted table %d", mut.ID)
		return mut, nil
	}

	phyAccessor := UncachedPhysicalAccessor{}
	obj, err := phyAccessor.GetObjectDesc(ctx, txn, tn, flags)
	if obj == nil {
		return nil, err
	}
	return obj.(*sqlbase.MutableTableDescriptor), err
}

// getTableVersion returns a table descriptor with a version suitable for
// the transaction: table.ModificationTime <= txn.Timestamp < expirationTime.
// The table must be released by calling tc.releaseTables().
//
// If flags.required is false, getTableVersion() will gracefully
// return a nil descriptor and no error if the table does not exist.
//
// It might also add a transaction deadline to the transaction that is
// enforced at the KV layer to ensure that the transaction doesn't violate
// the validity window of the table descriptor version returned.
//
func (tc *TableCollection) getTableVersion(
	ctx context.Context, txn *client.Txn, tn *tree.TableName, flags tree.ObjectLookupFlags,
) (*sqlbase.ImmutableTableDescriptor, error) {
	if log.V(2) {
		log.Infof(ctx, "planner acquiring lease on table '%s'", tn)
	}

	if tn.SchemaName != tree.PublicSchemaName {
		if flags.Required {
			return nil, sqlbase.NewUnsupportedSchemaUsageError(tree.ErrString(tn))
		}
		return nil, nil
	}

	refuseFurtherLookup, dbID, err := tc.getUncommittedDatabaseID(tn.Catalog(), flags.Required)
	if refuseFurtherLookup || err != nil {
		return nil, err
	}

	if dbID == sqlbase.InvalidID && tc.databaseCache != nil {
		// Resolve the database from the database cache when the transaction
		// hasn't modified the database.
		dbID, err = tc.databaseCache.getDatabaseID(ctx,
			tc.leaseMgr.db.Txn, tn.Catalog(), flags.Required)
		if err != nil || dbID == sqlbase.InvalidID {
			// dbID can still be invalid if required is false and the database is not found.
			return nil, err
		}
	}

	// TODO(vivek): Ideally we'd avoid caching for only the
	// system.descriptor and system.lease tables, because they are
	// used for acquiring leases, creating a chicken&egg problem.
	// But doing so turned problematic and the tests pass only by also
	// disabling caching of system.eventlog, system.rangelog, and
	// system.users. For now we're sticking to disabling caching of
	// all system descriptors except the role-members-table.
	avoidCache := flags.AvoidCached || testDisableTableLeases ||
		(tn.Catalog() == sqlbase.SystemDB.Name && tn.TableName.String() != sqlbase.RoleMembersTable.Name)

	if refuseFurtherLookup, table, err := tc.getUncommittedTable(dbID, tn, flags.Required); refuseFurtherLookup || err != nil {
		return nil, err
	} else if immut := table.ImmutableTableDescriptor; immut != nil {
		// If not forcing to resolve using KV, tables being added aren't visible.
		if immut.Adding() && !avoidCache {
			err := errTableAdding
			if !flags.Required {
				err = nil
			}
			return nil, err
		}

		log.VEventf(ctx, 2, "found uncommitted table %d", immut.ID)
		return immut, nil
	}

	readTableFromStore := func() (*sqlbase.ImmutableTableDescriptor, error) {
		phyAccessor := UncachedPhysicalAccessor{}
		obj, err := phyAccessor.GetObjectDesc(ctx, txn, tn, flags)
		if obj == nil {
			return nil, err
		}
		return obj.(*sqlbase.ImmutableTableDescriptor), err
	}

	if avoidCache {
		return readTableFromStore()
	}

	// First, look to see if we already have the table.
	// This ensures that, once a SQL transaction resolved name N to id X, it will
	// continue to use N to refer to X even if N is renamed during the
	// transaction.
	for _, table := range tc.leasedTables {
		if table.Name == string(tn.TableName) &&
			table.ParentID == dbID {
			log.VEventf(ctx, 2, "found table in table collection for table '%s'", tn)
			return table, nil
		}
	}

	readTimestamp := txn.ReadTimestamp()
	table, expiration, err := tc.leaseMgr.AcquireByName(ctx, readTimestamp, dbID, tn.Table())
	if err != nil {
		// Read the descriptor from the store in the face of some specific errors
		// because of a known limitation of AcquireByName. See the known
		// limitations of AcquireByName for details.
		if _, ok := err.(inactiveTableError); ok || err == sqlbase.ErrDescriptorNotFound {
			return readTableFromStore()
		}
		// Lease acquisition failed with some other error. This we don't
		// know how to deal with, so propagate the error.
		return nil, err
	}

	if !readTimestamp.Less(expiration) {
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

// getTableVersionByID is a by-ID variant of getTableVersion (i.e. uses same cache).
func (tc *TableCollection) getTableVersionByID(
	ctx context.Context, txn *client.Txn, tableID sqlbase.ID, flags tree.ObjectLookupFlags,
) (*sqlbase.ImmutableTableDescriptor, error) {
	log.VEventf(ctx, 2, "planner getting table on table ID %d", tableID)

	if flags.AvoidCached || testDisableTableLeases {
		table, err := sqlbase.GetTableDescFromID(ctx, txn, tableID)
		if err != nil {
			return nil, err
		}
		if err := filterTableState(table); err != nil {
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
	// via `getTableVersion`.
	for _, table := range tc.leasedTables {
		if table.ID == tableID {
			log.VEventf(ctx, 2, "found table %d in table cache", tableID)
			return table, nil
		}
	}

	readTimestamp := txn.ReadTimestamp()
	table, expiration, err := tc.leaseMgr.Acquire(ctx, readTimestamp, tableID)
	if err != nil {
		if err == sqlbase.ErrDescriptorNotFound {
			// Transform the descriptor error into an error that references the
			// table's ID.
			return nil, sqlbase.NewUndefinedRelationError(
				&tree.TableRef{TableID: int64(tableID)})
		}
		return nil, err
	}

	if !readTimestamp.Less(expiration) {
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

// getMutableTableVersionByID is a variant of sqlbase.GetTableDescFromID which returns a mutable
// table descriptor of the table modified in the same transaction.
func (tc *TableCollection) getMutableTableVersionByID(
	ctx context.Context, tableID sqlbase.ID, txn *client.Txn,
) (*sqlbase.MutableTableDescriptor, error) {
	log.VEventf(ctx, 2, "planner getting mutable table on table ID %d", tableID)

	if table := tc.getUncommittedTableByID(tableID).MutableTableDescriptor; table != nil {
		log.VEventf(ctx, 2, "found uncommitted table %d", tableID)
		return table, nil
	}
	return sqlbase.GetMutableTableDescFromID(ctx, txn, tableID)
}

// releaseTableLeases releases the leases for the tables with ids in
// the passed slice. Errors are logged but ignored.
func (tc *TableCollection) releaseTableLeases(ctx context.Context, tables []IDVersion) {
	// Sort the tables and leases to make it easy to find the leases to release.
	leasedTables := tc.leasedTables
	sort.Slice(tables, func(i, j int) bool {
		return tables[i].id < tables[j].id
	})
	sort.Slice(leasedTables, func(i, j int) bool {
		return leasedTables[i].ID < leasedTables[j].ID
	})

	filteredLeases := leasedTables[:0] // will store the remaining leases
	tablesToConsider := tables
	shouldRelease := func(id sqlbase.ID) (found bool) {
		for len(tablesToConsider) > 0 && tablesToConsider[0].id < id {
			tablesToConsider = tablesToConsider[1:]
		}
		return len(tablesToConsider) > 0 && tablesToConsider[0].id == id
	}
	for _, l := range leasedTables {
		if !shouldRelease(l.ID) {
			filteredLeases = append(filteredLeases, l)
		} else if err := tc.leaseMgr.Release(l); err != nil {
			log.Warning(ctx, err)
		}
	}
	tc.leasedTables = filteredLeases
}

func (tc *TableCollection) releaseLeases(ctx context.Context) {
	if len(tc.leasedTables) > 0 {
		log.VEventf(ctx, 2, "releasing %d tables", len(tc.leasedTables))
		for _, table := range tc.leasedTables {
			if err := tc.leaseMgr.Release(table); err != nil {
				log.Warning(ctx, err)
			}
		}
		tc.leasedTables = tc.leasedTables[:0]
	}
}

// releaseTables releases all tables currently held by the TableCollection.
func (tc *TableCollection) releaseTables(ctx context.Context) {
	tc.releaseLeases(ctx)
	tc.uncommittedTables = nil
	tc.uncommittedDatabases = nil
	tc.releaseAllDescriptors()
}

// Wait until the database cache has been updated to properly
// reflect all dropped databases, so that future commands on the
// same gateway node observe the dropped databases.
func (tc *TableCollection) waitForCacheToDropDatabases(ctx context.Context) {
	for _, uc := range tc.uncommittedDatabases {
		if !uc.dropped {
			continue
		}
		// Wait until the database cache has been updated to properly
		// reflect a dropped database, so that future commands on the
		// same gateway node observe the dropped database.
		tc.dbCacheSubscriber.waitForCacheState(
			func(dc *databaseCache) bool {
				// Resolve the database name from the database cache.
				dbID, err := dc.getCachedDatabaseID(uc.name)
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

func (tc *TableCollection) hasUncommittedTables() bool {
	return len(tc.uncommittedTables) > 0
}

func (tc *TableCollection) addUncommittedTable(desc sqlbase.MutableTableDescriptor) error {
	if desc.Version != desc.ClusterVersion.Version+1 {
		return errors.Errorf(
			"descriptor version %d not incremented from cluster version %d",
			desc.Version, desc.ClusterVersion.Version)
	}
	tbl := uncommittedTable{
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

// returns all the idVersion pairs that have undergone a schema change.
// Returns nil for no schema changes. The version returned for each
// schema change is ClusterVersion - 1, because that's the one that will be
// used when checking for table descriptor two version invariance.
// Also returns strings representing the new <name, version> pairs
func (tc *TableCollection) getTablesWithNewVersion() []IDVersion {
	var tables []IDVersion
	for _, table := range tc.uncommittedTables {
		if mut := table.MutableTableDescriptor; !mut.IsNewTable() {
			tables = append(tables, NewIDVersionPrev(&mut.ClusterVersion))
		}
	}
	return tables
}

type dbAction bool

const (
	dbCreated dbAction = false
	dbDropped dbAction = true
)

func (tc *TableCollection) addUncommittedDatabase(name string, id sqlbase.ID, action dbAction) {
	db := uncommittedDatabase{name: name, id: id, dropped: action == dbDropped}
	tc.uncommittedDatabases = append(tc.uncommittedDatabases, db)
	tc.releaseAllDescriptors()
}

// getUncommittedDatabaseID returns a database ID for the requested tablename
// if the requested tablename is for a database modified within the transaction
// affiliated with the LeaseCollection.
func (tc *TableCollection) getUncommittedDatabaseID(
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
func (tc *TableCollection) getUncommittedTable(
	dbID sqlbase.ID, tn *tree.TableName, required bool,
) (refuseFurtherLookup bool, table uncommittedTable, err error) {
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
			if drain.Name == string(tn.TableName) &&
				drain.ParentID == dbID {
				// Table name has gone away.
				if required {
					// If it's required here, say it doesn't exist.
					err = sqlbase.NewUndefinedRelationError(tn)
				}
				// The table collection knows better; the caller has to avoid
				// going to KV in any case: refuseFurtherLookup = true
				return true, uncommittedTable{}, err
			}
		}

		// Do we know about a table with this name?
		if mutTbl.Name == string(tn.TableName) &&
			mutTbl.ParentID == dbID {
			// Right state?
			if err = filterTableState(mutTbl.TableDesc()); err != nil && err != errTableAdding {
				if !required {
					// If it's not required here, we simply say we don't have it.
					err = nil
				}
				// The table collection knows better; the caller has to avoid
				// going to KV in any case: refuseFurtherLookup = true
				return true, uncommittedTable{}, err
			}

			// Got a table.
			return false, table, nil
		}
	}
	return false, uncommittedTable{}, nil
}

func (tc *TableCollection) getUncommittedTableByID(id sqlbase.ID) uncommittedTable {
	// Walk latest to earliest so that a DROP TABLE followed by a CREATE TABLE
	// with the same name will result in the CREATE TABLE being seen.
	for i := len(tc.uncommittedTables) - 1; i >= 0; i-- {
		table := tc.uncommittedTables[i]
		if table.MutableTableDescriptor.ID == id {
			return table
		}
	}
	return uncommittedTable{}
}

// getAllDescriptors returns all descriptors visible by the transaction,
// first checking the TableCollection's cached descriptors for validity
// before defaulting to a key-value scan, if necessary.
func (tc *TableCollection) getAllDescriptors(
	ctx context.Context, txn *client.Txn,
) ([]sqlbase.DescriptorProto, error) {
	if tc.allDescriptors == nil {
		descs, err := GetAllDescriptors(ctx, txn)
		if err != nil {
			return nil, err
		}
		tc.allDescriptors = descs
	}
	return tc.allDescriptors, nil
}

// getAllDatabaseDescriptors returns all database descriptors visible by the
// transaction, first checking the TableCollection's cached descriptors for
// validity before scanning system.namespace and looking up the descriptors
// in the database cache, if necessary.
func (tc *TableCollection) getAllDatabaseDescriptors(
	ctx context.Context, txn *client.Txn,
) ([]*sqlbase.DatabaseDescriptor, error) {
	if tc.allDatabaseDescriptors == nil {
		dbDescIDs, err := GetAllDatabaseDescriptorIDs(ctx, txn)
		if err != nil {
			return nil, err
		}
		dbDescs := make([]*sqlbase.DatabaseDescriptor, 0, len(dbDescIDs))
		for _, dbDescID := range dbDescIDs {
			dbDesc, err := MustGetDatabaseDescByID(ctx, txn, dbDescID)
			if err != nil {
				return nil, err
			}
			dbDescs = append(dbDescs, dbDesc)
		}
		tc.allDatabaseDescriptors = dbDescs
	}
	return tc.allDatabaseDescriptors, nil
}

// releaseAllDescriptors releases the cached slice of all descriptors
// held by TableCollection.
func (tc *TableCollection) releaseAllDescriptors() {
	tc.allDescriptors = nil
	tc.allDatabaseDescriptors = nil
}

// Copy the modified schema to the table collection. Used when initializing
// an InternalExecutor.
func (tc *TableCollection) copyModifiedSchema(to *TableCollection) {
	if tc == nil {
		return
	}
	to.uncommittedTables = tc.uncommittedTables
	to.uncommittedDatabases = tc.uncommittedDatabases
	// Do not copy the leased descriptors because we do not want
	// the leased descriptors to be released by the "to" TableCollection.
	// The "to" TableCollection can re-lease the same descriptors.
}

type tableCollectionModifier interface {
	copyModifiedSchema(to *TableCollection)
}

// createOrUpdateSchemaChangeJob finalizes the current mutations in the table
// descriptor. If a schema change job in the system.jobs table has not been
// created for mutations in the current transaction, one is created. The
// identifiers of the mutations and newly-created job are written to a new
// MutationJob in the table descriptor.
//
// The job creation is done within the planner's txn. This is important - if the
// txn ends up rolling back, the job needs to go away.
//
// If a job for this table has already been created, update the job's details
// and description.
func (p *planner) createOrUpdateSchemaChangeJob(
	ctx context.Context, tableDesc *sqlbase.MutableTableDescriptor, stmt string,
) (sqlbase.MutationID, error) {
	mutationID := tableDesc.ClusterVersion.NextMutationID

	// If the table being schema changed was created in the same txn, we do not
	// want to update/create a job as we expect the schema change to be executed
	// immediately (not via the schema changer). For tables created in the same
	// txn the next mutation ID will not have been allocated and the mutationID
	// will be an invalid ID. This is fine because the mutation will be processed
	// immediately.
	if tableDesc.IsNewTable() {
		return mutationID, nil
	}

	var job *jobs.Job
	var spanList []jobspb.ResumeSpanList
	if len(tableDesc.MutationJobs) > len(tableDesc.ClusterVersion.MutationJobs) {
		// Already created a job and appended the job ID to MutationJobs.
		jobID := tableDesc.MutationJobs[len(tableDesc.MutationJobs)-1].JobID
		var err error
		job, err = p.ExecCfg().JobRegistry.LoadJobWithTxn(ctx, jobID, p.txn)
		if err != nil {
			return sqlbase.InvalidMutationID, err
		}
		spanList = job.Details().(jobspb.SchemaChangeDetails).ResumeSpanList
	}

	span := tableDesc.PrimaryIndexSpan()
	for i := len(tableDesc.ClusterVersion.Mutations) + len(spanList); i < len(tableDesc.Mutations); i++ {
		spanList = append(spanList,
			jobspb.ResumeSpanList{
				ResumeSpans: []roachpb.Span{span},
			},
		)
	}

	if job == nil {
		jobRecord := jobs.Record{
			Description:   stmt,
			Username:      p.User(),
			DescriptorIDs: sqlbase.IDs{tableDesc.GetID()},
			Details:       jobspb.SchemaChangeDetails{ResumeSpanList: spanList},
			Progress:      jobspb.SchemaChangeProgress{},
		}
		job = p.ExecCfg().JobRegistry.NewJob(jobRecord)
		if err := job.WithTxn(p.txn).Created(ctx); err != nil {
			return sqlbase.InvalidMutationID, err
		}
		tableDesc.MutationJobs = append(tableDesc.MutationJobs, sqlbase.TableDescriptor_MutationJob{
			MutationID: mutationID, JobID: *job.ID()})
	} else {
		if err := job.WithTxn(p.txn).SetDetails(
			ctx,
			jobspb.SchemaChangeDetails{ResumeSpanList: spanList},
		); err != nil {
			return sqlbase.InvalidMutationID, err
		}
		if err := job.WithTxn(p.txn).SetDescription(
			ctx,
			func(ctx context.Context, description string) (string, error) {
				return strings.Join([]string{description, stmt}, ";"), nil
			},
		); err != nil {
			return sqlbase.InvalidMutationID, err
		}
	}

	return mutationID, nil
}

// createDropTablesJob creates a schema change job in the system.jobs table.
// The identifiers of the newly-created job are written in the table descriptor.
// If no job is created (no tables were dropped), a job ID of 0 is returned.
//
// The job creation is done within the planner's txn. This is important - if the`
// txn ends up rolling back, the job needs to go away.
func (p *planner) createDropTablesJob(
	ctx context.Context,
	tableDescs []*sqlbase.MutableTableDescriptor,
	droppedDetails []jobspb.DroppedTableDetails,
	stmt string,
	drainNames bool,
	droppedDatabaseID sqlbase.ID,
) (int64, error) {

	if len(tableDescs) == 0 {
		return 0, nil
	}

	descriptorIDs := make([]sqlbase.ID, 0, len(tableDescs))

	for _, tableDesc := range tableDescs {
		descriptorIDs = append(descriptorIDs, tableDesc.ID)
	}

	detailStatus := jobspb.Status_DRAINING_NAMES
	if !drainNames {
		detailStatus = jobspb.Status_WAIT_FOR_GC_INTERVAL
	}
	for i := range droppedDetails {
		droppedDetails[i].Status = detailStatus
	}

	runningStatus := RunningStatusDrainingNames
	if !drainNames {
		runningStatus = RunningStatusWaitingGC
	}
	jobRecord := jobs.Record{
		Description:   stmt,
		Username:      p.User(),
		DescriptorIDs: descriptorIDs,
		Details:       jobspb.SchemaChangeDetails{DroppedTables: droppedDetails, DroppedDatabaseID: droppedDatabaseID},
		Progress:      jobspb.SchemaChangeProgress{},
		RunningStatus: runningStatus,
	}
	job := p.ExecCfg().JobRegistry.NewJob(jobRecord)
	if err := job.WithTxn(p.txn).Created(ctx); err != nil {
		return 0, err
	}

	if err := job.WithTxn(p.txn).Started(ctx); err != nil {
		return 0, err
	}

	for _, tableDesc := range tableDescs {
		tableDesc.DropJobID = *job.ID()
	}
	return *job.ID(), nil
}

// queueSchemaChange queues up a schema changer to process an outstanding
// schema change for the table.
func (p *planner) queueSchemaChange(
	tableDesc *sqlbase.TableDescriptor, mutationID sqlbase.MutationID,
) {
	sc := SchemaChanger{
		tableID:              tableDesc.GetID(),
		mutationID:           mutationID,
		nodeID:               p.extendedEvalCtx.NodeID,
		leaseMgr:             p.LeaseMgr(),
		jobRegistry:          p.ExecCfg().JobRegistry,
		leaseHolderCache:     p.ExecCfg().LeaseHolderCache,
		rangeDescriptorCache: p.ExecCfg().RangeDescriptorCache,
		clock:                p.ExecCfg().Clock,
		settings:             p.ExecCfg().Settings,
		execCfg:              p.ExecCfg(),
	}
	p.extendedEvalCtx.SchemaChangers.queueSchemaChanger(sc)
}

// writeSchemaChange effectively writes a table descriptor to the
// database within the current planner transaction, and queues up
// a schema changer for future processing.
func (p *planner) writeSchemaChange(
	ctx context.Context, tableDesc *sqlbase.MutableTableDescriptor, mutationID sqlbase.MutationID,
) error {
	if tableDesc.Dropped() {
		// We don't allow schema changes on a dropped table.
		return fmt.Errorf("table %q is being dropped", tableDesc.Name)
	}
	return p.writeTableDesc(ctx, tableDesc, mutationID)
}

func (p *planner) writeSchemaChangeToBatch(
	ctx context.Context,
	tableDesc *sqlbase.MutableTableDescriptor,
	mutationID sqlbase.MutationID,
	b *client.Batch,
) error {
	if tableDesc.Dropped() {
		// We don't allow schema changes on a dropped table.
		return fmt.Errorf("table %q is being dropped", tableDesc.Name)
	}
	return p.writeTableDescToBatch(ctx, tableDesc, mutationID, b)
}

func (p *planner) writeDropTable(
	ctx context.Context, tableDesc *sqlbase.MutableTableDescriptor,
) error {
	return p.writeTableDesc(ctx, tableDesc, sqlbase.InvalidMutationID)
}

func (p *planner) writeTableDesc(
	ctx context.Context, tableDesc *sqlbase.MutableTableDescriptor, mutationID sqlbase.MutationID,
) error {
	b := p.txn.NewBatch()
	if err := p.writeTableDescToBatch(ctx, tableDesc, mutationID, b); err != nil {
		return err
	}
	return p.txn.Run(ctx, b)
}

func (p *planner) writeTableDescToBatch(
	ctx context.Context,
	tableDesc *sqlbase.MutableTableDescriptor,
	mutationID sqlbase.MutationID,
	b *client.Batch,
) error {
	if tableDesc.IsVirtualTable() {
		return errors.AssertionFailedf("virtual descriptors cannot be stored, found: %v", tableDesc)
	}

	if tableDesc.IsNewTable() {
		if err := runSchemaChangesInTxn(
			ctx, p, tableDesc, p.ExtendedEvalContext().Tracing.KVTracingEnabled(),
		); err != nil {
			return err
		}
	} else {
		// Only increment the table descriptor version once in this transaction.
		if err := tableDesc.MaybeIncrementVersion(ctx, p.txn, p.execCfg.Settings); err != nil {
			return err
		}

		// Schedule a schema changer for later.
		p.queueSchemaChange(tableDesc.TableDesc(), mutationID)
	}

	if err := tableDesc.ValidateTable(); err != nil {
		return errors.AssertionFailedf("table descriptor is not valid: %s\n%v", err, tableDesc)
	}

	if err := p.Tables().addUncommittedTable(*tableDesc); err != nil {
		return err
	}

	return writeDescToBatch(ctx, p.extendedEvalCtx.Tracing.KVTracingEnabled(), p.execCfg.Settings, b, tableDesc.GetID(), tableDesc.TableDesc())
}
