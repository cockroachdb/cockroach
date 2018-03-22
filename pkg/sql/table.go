// Copyright 2015 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package sql

import (
	"context"
	"fmt"

	"github.com/pkg/errors"

	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/jobs"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
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

type namespaceKey struct {
	parentID sqlbase.ID
	name     string
}

// getAllNames returns a map from ID to namespaceKey for every entry in
// system.namespace.
func (p *planner) getAllNames(ctx context.Context) (map[sqlbase.ID]namespaceKey, error) {
	namespace := map[sqlbase.ID]namespaceKey{}
	rows, _ /* cols */, err := p.queryRows(ctx, `SELECT id, "parentID", name FROM system.namespace`)
	if err != nil {
		return nil, err
	}
	for _, r := range rows {
		id, parentID, name := tree.MustBeDInt(r[0]), tree.MustBeDInt(r[1]), tree.MustBeDString(r[2])
		namespace[sqlbase.ID(id)] = namespaceKey{
			parentID: sqlbase.ID(parentID),
			name:     string(name),
		}
	}
	return namespace, nil
}

// tableKey implements sqlbase.DescriptorKey.
type tableKey namespaceKey

func (tk tableKey) Key() roachpb.Key {
	return sqlbase.MakeNameMetadataKey(tk.parentID, tk.name)
}

func (tk tableKey) Name() string {
	return tk.name
}

// GetKeysForTableDescriptor retrieves the KV keys corresponding
// to the zone, name and descriptor of a table.
func GetKeysForTableDescriptor(
	tableDesc *sqlbase.TableDescriptor,
) (zoneKey roachpb.Key, nameKey roachpb.Key, descKey roachpb.Key) {
	zoneKey = config.MakeZoneKey(uint32(tableDesc.ID))
	nameKey = sqlbase.MakeNameMetadataKey(tableDesc.ParentID, tableDesc.GetName())
	descKey = sqlbase.MakeDescMetadataKey(tableDesc.ID)
	return
}

// A unique id for a particular table descriptor version.
type tableVersionID struct {
	id      sqlbase.ID
	version sqlbase.DescriptorVersion
}

func (p *planner) getVirtualTabler() VirtualTabler {
	return p.extendedEvalCtx.VirtualSchemas
}

var errTableDropped = errors.New("table is being dropped")
var errTableAdding = errors.New("table is being added")

func filterTableState(tableDesc *sqlbase.TableDescriptor) error {
	switch {
	case tableDesc.Dropped():
		return errTableDropped
	case tableDesc.Adding():
		return errTableAdding
	case tableDesc.State != sqlbase.TableDescriptor_PUBLIC:
		return errors.Errorf("table in unknown state: %s", tableDesc.State.String())
	}
	return nil
}

// An uncommitted database is a database that has been created/dropped
// within the current transaction using the TableCollection. A rename
// is a drop of the old name and creation of the new name.
type uncommittedDatabase struct {
	name    string
	id      sqlbase.ID
	dropped bool
}

// TableCollection is a collection of tables held by a single session that
// serves SQL requests, or a background job using a table descriptor. The
// collection is cleared using releaseTables() which is called at the
// end of each transaction on the session, or on hitting conditions such
// as errors, or retries that result in transaction timestamp changes.
type TableCollection struct {
	// The timestamp used to pick tables. The timestamp falls within the
	// validity window of every table in leasedTables.
	timestamp hlc.Timestamp

	// leaseMgr manages acquiring and releasing per-table leases.
	leaseMgr *LeaseManager
	// A collection of table descriptor valid for the timestamp.
	// They are released once the transaction using them is complete.
	// If the transaction gets pushed and the timestamp changes,
	// the tables are released.
	leasedTables []*sqlbase.TableDescriptor
	// Tables modified by the uncommitted transaction affiliated
	// with this TableCollection. This allows a transaction to see
	// its own modifications while bypassing the table lease mechanism.
	// The table lease mechanism will have its own transaction to read
	// the table and will hang waiting for the uncommitted changes to
	// the table. These table descriptors are local to this
	// TableCollection and invisible to other transactions. A dropped
	// table is marked dropped.
	uncommittedTables []*sqlbase.TableDescriptor

	// databaseCache is used as a cache for database names.
	// TODO(andrei): get rid of it and replace it with a leasing system for
	// database descriptors.
	databaseCache *databaseCache

	// dbCacheSubscriber is used to block until the node's database cache has been
	// updated when releaseTables is called.
	// Can be nil if releaseTables() is only called with the
	// dontBlockForDBCacheUpdate option.
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
}

type dbCacheSubscriber interface {
	// waitForCacheState takes a callback depending on the cache state and blocks
	// until the callback declares success. The callback is repeatedly called as
	// the cache is updated.
	waitForCacheState(cond func(*databaseCache) (bool, error)) error
}

// Check if the timestamp used so far to pick tables has changed because
// of a transaction retry.
func (tc *TableCollection) resetForTxnRetry(ctx context.Context, txn *client.Txn) {
	if tc.timestamp != (hlc.Timestamp{}) &&
		tc.timestamp != txn.OrigTimestamp() {
		if err := tc.releaseTables(ctx, dontBlockForDBCacheUpdate); err != nil {
			log.Warningf(ctx, "error releasing tables")
		}
	}
}

// getTableVersion returns a table descriptor with a version suitable for
// the transaction: table.ModificationTime <= txn.Timestamp < expirationTime.
// The table must be released by calling tc.releaseTables().
//
// If flags.required is false, getTableVersion() will gracefully
// return a nil descriptor and no error if the table does not exist.
//
// TODO(vivek): #6418 introduced a transaction deadline that is enforced at
// the KV layer, and was introduced to manager the validity window of a
// table descriptor. Since we will be checking for the valid use of a table
// descriptor here, we do not need the extra check at the KV layer. However,
// for a SNAPSHOT_ISOLATION transaction the commit timestamp of the transaction
// can change, so we have kept the transaction deadline. It's worth
// reconsidering if this is really needed.
//
// TODO(vivek): Allow cached descriptors for AS OF SYSTEM TIME queries.
func (tc *TableCollection) getTableVersion(
	ctx context.Context, tn *tree.TableName, flags ObjectLookupFlags,
) (*sqlbase.TableDescriptor, *sqlbase.DatabaseDescriptor, error) {
	if log.V(2) {
		log.Infof(ctx, "planner acquiring lease on table '%s'", tn)
	}

	if flags.allowAdding {
		return nil, nil, pgerror.NewErrorf(pgerror.CodeInternalError,
			"programming error: unsupported flags passed to getTableVersion: %+v", flags)
	}

	if tn.SchemaName != tree.PublicSchemaName {
		if flags.required {
			return nil, nil, sqlbase.NewUnsupportedSchemaUsageError(tree.ErrString(tn))
		}
		return nil, nil, nil
	}

	isSystemDB := tn.Catalog() == sqlbase.SystemDB.Name
	if isSystemDB || testDisableTableLeases {
		// We don't go through the normal lease mechanism for system
		// tables. The system.lease and system.descriptor table, in
		// particular, are problematic because they are used for acquiring
		// leases itself, creating a chicken&egg problem.
		flags.avoidCached = true
		phyAccessor := UncachedPhysicalAccessor{}
		return phyAccessor.GetObjectDesc(tn, flags)
	}

	refuseFurtherLookup, dbID, err := tc.getUncommittedDatabaseID(tn.Catalog(), flags.required)
	if refuseFurtherLookup || err != nil {
		return nil, nil, err
	}

	if dbID == 0 {
		// Resolve the database from the database cache when the transaction
		// hasn't modified the database.
		dbID, err = tc.databaseCache.getDatabaseID(ctx,
			tc.leaseMgr.execCfg.DB.Txn, tn.Catalog(), flags.required)
		if err != nil || dbID == 0 {
			// dbID can still be 0 if required is false and the database is not found.
			return nil, nil, err
		}
	}

	// If the txn has been pushed the table collection is released and
	// txn deadline is reset.
	tc.resetForTxnRetry(ctx, flags.txn)

	if refuseFurtherLookup, table, err := tc.getUncommittedTable(
		dbID, tn, flags.required); refuseFurtherLookup || err != nil {
		return nil, nil, err
	} else if table != nil {
		log.VEventf(ctx, 2, "found uncommitted table %d", table.ID)
		return table, nil, nil
	}

	// First, look to see if we already have the table.
	// This ensures that, once a SQL transaction resolved name N to id X, it will
	// continue to use N to refer to X even if N is renamed during the
	// transaction.
	for _, table := range tc.leasedTables {
		if table.Name == string(tn.TableName) &&
			table.ParentID == dbID {
			log.VEventf(ctx, 2, "found table in table collection for table '%s'", tn)
			return table, nil, nil
		}
	}

	origTimestamp := flags.txn.OrigTimestamp()
	table, expiration, err := tc.leaseMgr.AcquireByName(ctx, origTimestamp, dbID, tn.Table())
	if err != nil {
		if err == sqlbase.ErrDescriptorNotFound {
			if flags.required {
				// Transform the descriptor error into an error that references the
				// table's name.
				return nil, nil, sqlbase.NewUndefinedRelationError(tn)
			}
			// We didn't find the descriptor but it's also not required. Make no fuss.
			return nil, nil, nil
		}
		// Lease acquisition failed with some other error. This we don't
		// know how to deal with, so propagate the error.
		return nil, nil, err
	}
	tc.timestamp = origTimestamp
	tc.leasedTables = append(tc.leasedTables, table)
	log.VEventf(ctx, 2, "added table '%s' to table collection", tn)

	// If the table we just acquired expires before the txn's deadline, reduce
	// the deadline. We use OrigTimestamp() that doesn't return the commit timestamp,
	// so we need to set a deadline on the transaction to prevent it from committing
	// beyond the table version expiration time.
	flags.txn.UpdateDeadlineMaybe(ctx, expiration)
	return table, nil, nil
}

// getTableVersionByID is a by-ID variant of getTableVersion (i.e. uses same cache).
func (tc *TableCollection) getTableVersionByID(
	ctx context.Context, txn *client.Txn, tableID sqlbase.ID,
) (*sqlbase.TableDescriptor, error) {
	log.VEventf(ctx, 2, "planner getting table on table ID %d", tableID)

	if testDisableTableLeases {
		table, err := sqlbase.GetTableDescFromID(ctx, txn, tableID)
		if err != nil {
			return nil, err
		}
		if err := filterTableState(table); err != nil {
			return nil, err
		}
		return table, nil
	}

	// If the txn has been pushed the table collection is released and
	// txn deadline is reset.
	tc.resetForTxnRetry(ctx, txn)

	for _, table := range tc.uncommittedTables {
		if table.ID == tableID {
			log.VEventf(ctx, 2, "found uncommitted table %d", tableID)
			if table.Dropped() {
				return nil, sqlbase.NewUndefinedRelationError(
					tree.NewUnqualifiedTableName(tree.Name(fmt.Sprintf("<id=%d>", tableID))),
				)
			}
			return table, nil
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

	origTimestamp := txn.OrigTimestamp()
	table, expiration, err := tc.leaseMgr.Acquire(ctx, origTimestamp, tableID)
	if err != nil {
		if err == sqlbase.ErrDescriptorNotFound {
			// Transform the descriptor error into an error that references the
			// table's ID.
			return nil, sqlbase.NewUndefinedRelationError(
				&tree.TableRef{TableID: int64(tableID)})
		}
		return nil, err
	}
	tc.timestamp = origTimestamp
	tc.leasedTables = append(tc.leasedTables, table)
	log.VEventf(ctx, 2, "added table '%s' to table collection", table.Name)

	// If the table we just acquired expires before the txn's deadline, reduce
	// the deadline. We use OrigTimestamp() that doesn't return the commit timestamp,
	// so we need to set a deadline on the transaction to prevent it from committing
	// beyond the table version expiration time.
	txn.UpdateDeadlineMaybe(ctx, expiration)
	return table, nil
}

// releaseOpt specifies options for tc.releaseTables().
type releaseOpt bool

const (
	// blockForDBCacheUpdate makes releaseTables() block until the node's database
	// cache has been updated to reflect the dropped or renamed databases. If used
	// within a SQL session, this ensures that future queries on that session
	// behave correctly when trying to use the names of the recently
	// dropped/renamed databases.
	blockForDBCacheUpdate     releaseOpt = true
	dontBlockForDBCacheUpdate releaseOpt = false
)

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
func (tc *TableCollection) releaseTables(ctx context.Context, opt releaseOpt) error {
	tc.timestamp = hlc.Timestamp{}
	if len(tc.leasedTables) > 0 {
		log.VEventf(ctx, 2, "releasing %d tables", len(tc.leasedTables))
		for _, table := range tc.leasedTables {
			if err := tc.leaseMgr.Release(table); err != nil {
				log.Warning(ctx, err)
			}
		}
		tc.leasedTables = tc.leasedTables[:0]
	}
	tc.uncommittedTables = nil

	if opt == blockForDBCacheUpdate {
		for _, uc := range tc.uncommittedDatabases {
			if !uc.dropped {
				continue
			}
			err := tc.dbCacheSubscriber.waitForCacheState(
				func(dc *databaseCache) (bool, error) {
					desc, err := dc.getCachedDatabaseDesc(uc.name, false /*required*/)
					if err != nil {
						return false, err
					}
					if desc == nil {
						return true, nil
					}
					// If the database name still exists but it now references another
					// db, we're good - it means that the database name has been reused
					// within the same transaction.
					return desc.ID != uc.id, nil
				})
			if err != nil {
				return err
			}
		}
	}

	tc.uncommittedDatabases = nil
	tc.releaseAllDescriptors()
	return nil
}

func (tc *TableCollection) addUncommittedTable(desc sqlbase.TableDescriptor) {
	for i, table := range tc.uncommittedTables {
		if table.ID == desc.ID {
			tc.uncommittedTables[i] = &desc
			return
		}
	}
	tc.uncommittedTables = append(tc.uncommittedTables, &desc)
	tc.releaseAllDescriptors()
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
	// Walk latest to earliest.
	for i := len(tc.uncommittedDatabases) - 1; i >= 0; i-- {
		db := tc.uncommittedDatabases[i]
		if requestedDbName == db.name {
			if db.dropped {
				if required {
					return true, 0, sqlbase.NewUndefinedDatabaseError(requestedDbName)
				}
				return true, 0, nil
			}
			return false, db.id, nil
		}
	}
	return false, 0, nil
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
) (refuseFurtherLookup bool, table *sqlbase.TableDescriptor, err error) {
	for _, table := range tc.uncommittedTables {
		// If a table has gotten renamed we'd like to disallow using the old names.
		// The renames could have happened in another transaction but it's still okay
		// to disallow the use of the old name in this transaction because the other
		// transaction has already committed and this transaction is seeing the
		// effect of it.
		for _, drain := range table.DrainingNames {
			if drain.Name == string(tn.TableName) &&
				drain.ParentID == dbID {
				// Table name has gone away.
				if required {
					// If it's required here, say it doesn't exist.
					err = sqlbase.NewUndefinedRelationError(tn)
				}
				// The table collection knows better; the caller has to avoid
				// going to KV in any case: refuseFurtherLookup = true
				return true, nil, err
			}
		}

		// Do we know about a table with this name?
		if table.Name == string(tn.TableName) &&
			table.ParentID == dbID {
			// Can we see this table?
			if err = filterTableState(table); err != nil {
				if !required {
					// Table is being dropped or added; if it's not required here,
					// we simply say we don't have it.
					err = nil
				}
				// The table collection knows better; the caller has to avoid
				// going to KV in any case: refuseFurtherLookup = true
				return true, nil, err
			}

			// Got a table.
			return false, table, nil
		}
	}
	return false, nil, nil
}

// getAllDescriptors returns all descriptors visible by the transaction,
// first checking the TableCollection's cached descriptors for validity
// before defaulting to a key-value scan, if necessary.
func (tc *TableCollection) getAllDescriptors(
	ctx context.Context, txn *client.Txn,
) ([]sqlbase.DescriptorProto, error) {
	// If the txn has been pushed the table collection is released and txn
	// deadline is reset.
	tc.resetForTxnRetry(ctx, txn)

	if tc.allDescriptors == nil {
		descs, err := GetAllDescriptors(ctx, txn)
		if err != nil {
			return nil, err
		}
		tc.timestamp = txn.OrigTimestamp()
		tc.allDescriptors = descs
	}
	return tc.allDescriptors, nil
}

// releaseAllDescriptors releases the cached slice of all descriptors
// held by TableCollection.
func (tc *TableCollection) releaseAllDescriptors() {
	tc.allDescriptors = nil
}

// createSchemaChangeJob finalizes the current mutations in the table
// descriptor and creates a schema change job in the system.jobs table.
// The identifiers of the mutations and newly-created job are written to a new
// MutationJob in the table descriptor.
//
// The job creation is done within the planner's txn. This is important - if the
// txn ends up rolling back, the job needs to go away.
func (p *planner) createSchemaChangeJob(
	ctx context.Context, tableDesc *sqlbase.TableDescriptor, stmt string,
) (sqlbase.MutationID, error) {
	span := tableDesc.PrimaryIndexSpan()
	mutationID, err := tableDesc.FinalizeMutation()
	if err != nil {
		return sqlbase.InvalidMutationID, err
	}
	var spanList []jobs.ResumeSpanList
	for i := 0; i < len(tableDesc.Mutations); i++ {
		if tableDesc.Mutations[i].MutationID == mutationID {
			spanList = append(spanList,
				jobs.ResumeSpanList{
					ResumeSpans: []roachpb.Span{span},
				},
			)
		}
	}
	jobRecord := jobs.Record{
		Description:   stmt,
		Username:      p.User(),
		DescriptorIDs: sqlbase.IDs{tableDesc.GetID()},
		Details:       jobs.SchemaChangeDetails{ResumeSpanList: spanList},
	}
	job := p.ExecCfg().JobRegistry.NewJob(jobRecord)
	if err := job.WithTxn(p.txn).Created(ctx); err != nil {
		return sqlbase.InvalidMutationID, err
	}
	tableDesc.MutationJobs = append(tableDesc.MutationJobs, sqlbase.TableDescriptor_MutationJob{
		MutationID: mutationID, JobID: *job.ID()})
	return mutationID, nil
}

// notifySchemaChange notifies that an outstanding schema change
// exists for the table.
func (p *planner) notifySchemaChange(
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

// writeTableDesc effectively writes a table descriptor to the
// database within the current planner transaction.
func (p *planner) writeTableDesc(ctx context.Context, tableDesc *sqlbase.TableDescriptor) error {
	if isVirtualDescriptor(tableDesc) {
		return pgerror.NewErrorf(pgerror.CodeInternalError,
			"programming error: virtual descriptors cannot be stored, found: %v", tableDesc)
	}

	if err := tableDesc.ValidateTable(p.extendedEvalCtx.Settings); err != nil {
		return pgerror.NewErrorf(pgerror.CodeInternalError,
			"programming error: table descriptor is not valid: %s\n%v", err, tableDesc)
	}

	p.Tables().addUncommittedTable(*tableDesc)

	descKey := sqlbase.MakeDescMetadataKey(tableDesc.GetID())
	descVal := sqlbase.WrapDescriptor(tableDesc)
	if p.extendedEvalCtx.Tracing.KVTracingEnabled() {
		log.VEventf(ctx, 2, "Put %s -> %s", descKey, descVal)
	}
	return p.txn.Put(ctx, descKey, descVal)
}

// bumpTableVersion loads the table descriptor for 'table', calls UpVersion and persists it.
func (p *planner) bumpTableVersion(ctx context.Context, tn *tree.TableName) error {
	var tableDesc *TableDescriptor
	var err error
	p.runWithOptions(resolveFlags{skipCache: true}, func() {
		tableDesc, _, err = p.PhysicalSchemaAccessor().GetObjectDesc(tn,
			p.ObjectLookupFlags(ctx, true /*required*/))
	})
	if err != nil {
		return err
	}

	return p.saveNonmutationAndNotify(ctx, tableDesc)
}
