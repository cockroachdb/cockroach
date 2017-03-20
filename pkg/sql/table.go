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
//
// Author: Peter Mattis (peter@cockroachlabs.com)
// Author: Andrei Matei (andreimatei1@gmail.com)

package sql

import (
	"bytes"
	"fmt"

	"github.com/pkg/errors"
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/kv/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

var testDisableTableLeases bool

// TestDisableTableLeases disables table leases and returns
// a function that can be used to enable it.
func TestDisableTableLeases() func() {
	testDisableTableLeases = true
	return func() {
		testDisableTableLeases = false
	}
}

// tableKey implements sqlbase.DescriptorKey.
type tableKey struct {
	parentID sqlbase.ID
	name     string
}

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
	zoneKey = sqlbase.MakeZoneKey(tableDesc.ID)
	nameKey = sqlbase.MakeNameMetadataKey(tableDesc.ParentID, tableDesc.GetName())
	descKey = sqlbase.MakeDescMetadataKey(tableDesc.ID)
	return
}

// SchemaAccessor provides helper methods for using the SQL schema.
type SchemaAccessor interface {
	// getTableNames retrieves the list of qualified names of tables
	// present in the given database.
	getTableNames(ctx context.Context, dbDesc *sqlbase.DatabaseDescriptor) (parser.TableNames, error)

	// expandTableGlob expands wildcards from the end of `expr` and
	// returns the list of matching tables.
	expandTableGlob(ctx context.Context, expr parser.TablePattern) (parser.TableNames, error)

	// getTableOrViewDesc returns a table descriptor for either a table or view,
	// or nil if the descriptor is not found.
	getTableOrViewDesc(ctx context.Context, tn *parser.TableName) (*sqlbase.TableDescriptor, error)

	// getTableDesc returns a table descriptor for a table, or nil if the
	// descriptor is not found. If you want the "not found" condition to
	// return an error, use mustGetTableDesc() instead.
	// Returns an error if the underlying table descriptor actually
	// represents a view rather than a table.
	getTableDesc(ctx context.Context, tn *parser.TableName) (*sqlbase.TableDescriptor, error)

	// getViewDesc returns a table descriptor for a table, or nil if the
	// descriptor is not found.
	// Returns an error if the underlying table descriptor actually
	// represents a table rather than a view.
	getViewDesc(ctx context.Context, tn *parser.TableName) (*sqlbase.TableDescriptor, error)

	// mustGetTableOrViewDesc returns a table descriptor for either a table or
	// view, or an error if the descriptor is not found.
	mustGetTableOrViewDesc(
		ctx context.Context, tn *parser.TableName,
	) (*sqlbase.TableDescriptor, error)

	// mustGetTableDesc returns a table descriptor for a table, or an error if
	// the descriptor is not found.
	mustGetTableDesc(ctx context.Context, tn *parser.TableName) (*sqlbase.TableDescriptor, error)

	// mustGetViewDesc returns a table descriptor for a view, or an error if the
	// descriptor is not found.
	mustGetViewDesc(ctx context.Context, tn *parser.TableName) (*sqlbase.TableDescriptor, error)

	// NB: one can use GetTableDescFromID() to retrieve a descriptor for
	// a table from a transaction using its ID, assuming it was loaded
	// in the transaction already.

	// notifySchemaChange notifies that an outstanding schema change
	// exists for the table.
	notifySchemaChange(id sqlbase.ID, mutationID sqlbase.MutationID)

	// getTableLease acquires a lease for the specified table. The lease will be
	// released when the planner closes.
	getTableLease(ctx context.Context, tn *parser.TableName) (*sqlbase.TableDescriptor, error)

	// releaseLeases releases all leases currently held by the planner.
	releaseLeases(ctx context.Context)

	// writeTableDesc effectively writes a table descriptor to the
	// database within the current planner transaction.
	writeTableDesc(ctx context.Context, tableDesc *sqlbase.TableDescriptor) error
}

var _ SchemaAccessor = &planner{}

func (p *planner) getTableOrViewDesc(
	ctx context.Context, tn *parser.TableName,
) (*sqlbase.TableDescriptor, error) {
	return getTableOrViewDesc(ctx, p.txn, &p.session.virtualSchemas, tn)
}

func getTableOrViewDesc(
	ctx context.Context, txn *client.Txn, vt VirtualTabler, tn *parser.TableName,
) (*sqlbase.TableDescriptor, error) {
	virtual, err := vt.getVirtualTableDesc(tn)
	if err != nil || virtual != nil {
		if sqlbase.IsUndefinedTableError(err) {
			return nil, nil
		}
		return virtual, err
	}

	dbDesc, err := MustGetDatabaseDesc(ctx, txn, vt, tn.Database())
	if err != nil {
		return nil, err
	}

	desc := sqlbase.TableDescriptor{}
	found, err := getDescriptor(ctx, txn, tableKey{parentID: dbDesc.ID, name: tn.Table()}, &desc)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, nil
	}
	return &desc, nil
}

func (p *planner) getTableDesc(
	ctx context.Context, tn *parser.TableName,
) (*sqlbase.TableDescriptor, error) {
	return getTableDesc(ctx, p.txn, &p.session.virtualSchemas, tn)
}

// getTableDesc implements the SchemaAccessor interface.
func getTableDesc(
	ctx context.Context, txn *client.Txn, vt VirtualTabler, tn *parser.TableName,
) (*sqlbase.TableDescriptor, error) {
	desc, err := getTableOrViewDesc(ctx, txn, vt, tn)
	if err != nil {
		return desc, err
	}
	if desc != nil && !desc.IsTable() {
		return nil, sqlbase.NewWrongObjectTypeError(tn.String(), "table")
	}
	return desc, nil
}

// getViewDesc implements the SchemaAccessor interface.
func (p *planner) getViewDesc(
	ctx context.Context, tn *parser.TableName,
) (*sqlbase.TableDescriptor, error) {
	desc, err := p.getTableOrViewDesc(ctx, tn)
	if err != nil {
		return desc, err
	}
	if desc != nil && !desc.IsView() {
		return nil, sqlbase.NewWrongObjectTypeError(tn.String(), "view")
	}
	return desc, nil
}

// mustGetTableOrViewDesc implements the SchemaAccessor interface.
func (p *planner) mustGetTableOrViewDesc(
	ctx context.Context, tn *parser.TableName,
) (*sqlbase.TableDescriptor, error) {
	desc, err := p.getTableOrViewDesc(ctx, tn)
	if err != nil {
		return nil, err
	}
	if desc == nil {
		return nil, sqlbase.NewUndefinedTableError(tn.String())
	}
	if err := filterTableState(desc); err != nil {
		return nil, err
	}
	return desc, nil
}

// mustGetTableDesc implements the SchemaAccessor interface.
func (p *planner) mustGetTableDesc(
	ctx context.Context, tn *parser.TableName,
) (*sqlbase.TableDescriptor, error) {
	return mustGetTableDesc(ctx, p.txn, &p.session.virtualSchemas, tn)
}

func mustGetTableDesc(
	ctx context.Context, txn *client.Txn, vt VirtualTabler, tn *parser.TableName,
) (*sqlbase.TableDescriptor, error) {
	desc, err := getTableDesc(ctx, txn, vt, tn)
	if err != nil {
		return nil, err
	}
	if desc == nil {
		return nil, sqlbase.NewUndefinedTableError(tn.String())
	}
	if err := filterTableState(desc); err != nil {
		return nil, err
	}
	return desc, nil
}

// mustGetViewDesc implements the SchemaAccessor interface.
func (p *planner) mustGetViewDesc(
	ctx context.Context, tn *parser.TableName,
) (*sqlbase.TableDescriptor, error) {
	desc, err := p.getViewDesc(ctx, tn)
	if err != nil {
		return nil, err
	}
	if desc == nil {
		return nil, sqlbase.NewUndefinedViewError(tn.String())
	}
	if err := filterTableState(desc); err != nil {
		return nil, err
	}
	return desc, nil
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

// getTableLease implements the SchemaAccessor interface.
func (p *planner) getTableLease(
	ctx context.Context, tn *parser.TableName,
) (*sqlbase.TableDescriptor, error) {
	if log.V(2) {
		log.Infof(ctx, "planner acquiring lease on table '%s'", tn)
	}

	isSystemDB := tn.Database() == sqlbase.SystemDB.Name
	isVirtualDB := p.session.virtualSchemas.isVirtualDatabase(tn.Database())
	if isSystemDB || isVirtualDB || testDisableTableLeases {
		// We don't go through the normal lease mechanism for:
		// - system tables. The system.lease and system.descriptor table, in
		//   particular, are problematic because they are used for acquiring
		//   leases itself, creating a chicken&egg problem.
		// - virtual tables. These tables' descriptors are not persisted,
		//   so they cannot be leased. Instead, we simply return the static
		//   descriptor and rely on the immutability privileges set on the
		//   descriptors to cause upper layers to reject mutations statements.
		tbl, err := p.mustGetTableDesc(ctx, tn)
		if err != nil {
			return nil, err
		}
		if err := filterTableState(tbl); err != nil {
			return nil, err
		}
		return tbl, nil
	}

	dbID, err := p.getDatabaseID(ctx, tn.Database())
	if err != nil {
		return nil, err
	}

	// First, look to see if we already have a lease for this table.
	// This ensures that, once a SQL transaction resolved name N to id X, it will
	// continue to use N to refer to X even if N is renamed during the
	// transaction.
	var lease *LeaseState
	for _, l := range p.session.leases {
		if parser.ReNormalizeName(l.Name) == tn.TableName.Normalize() &&
			l.ParentID == dbID {
			lease = l
			if log.V(2) {
				log.Infof(ctx, "found lease in planner cache for table '%s'", tn)
			}
			break
		}
	}

	// If we didn't find a lease or the lease is about to expire, acquire one.
	if lease == nil || p.removeLeaseIfExpiring(ctx, lease) {
		var err error
		lease, err = p.session.leaseMgr.AcquireByName(ctx, p.txn, dbID, tn.Table())
		if err != nil {
			if err == sqlbase.ErrDescriptorNotFound {
				// Transform the descriptor error into an error that references the
				// table's name.
				return nil, sqlbase.NewUndefinedTableError(tn.String())
			}
			return nil, err
		}
		p.session.leases = append(p.session.leases, lease)
		if log.V(2) {
			log.Infof(ctx, "added lease on table '%s' to planner cache", tn)
		}
		// If the lease we just acquired expires before the txn's deadline, reduce
		// the deadline.
		p.txn.UpdateDeadlineMaybe(hlc.Timestamp{WallTime: lease.Expiration().UnixNano()})
	}
	return &lease.TableDescriptor, nil
}

// getTableLeaseByID is a by-ID variant of getTableLease (i.e. uses same cache).
func (p *planner) getTableLeaseByID(
	ctx context.Context, tableID sqlbase.ID,
) (*sqlbase.TableDescriptor, error) {
	if log.V(2) {
		log.Infof(ctx, "planner acquiring lease on table ID %d", tableID)
	}

	if testDisableTableLeases {
		table, err := sqlbase.GetTableDescFromID(ctx, p.txn, tableID)
		if err != nil {
			return nil, err
		}
		if err := filterTableState(table); err != nil {
			return nil, err
		}
		return table, nil
	}

	// First, look to see if we already have a lease for this table -- including
	// leases acquired via `getTableLease`.
	var lease *LeaseState
	for _, l := range p.session.leases {
		if l.ID == tableID {
			lease = l
			if log.V(2) {
				log.Infof(ctx, "found lease in planner cache for table %d", tableID)
			}
			break
		}
	}

	// If we didn't find a lease or the lease is about to expire, acquire one.
	if lease == nil || p.removeLeaseIfExpiring(ctx, lease) {
		var err error
		lease, err = p.session.leaseMgr.Acquire(ctx, p.txn, tableID, 0)
		if err != nil {
			if err == sqlbase.ErrDescriptorNotFound {
				// Transform the descriptor error into an error that references the
				// table's ID.
				return nil, sqlbase.NewUndefinedTableError(fmt.Sprintf("<id=%d>", tableID))
			}
			return nil, err
		}
		p.session.leases = append(p.session.leases, lease)
		// If the lease we just acquired expires before the txn's deadline, reduce
		// the deadline.
		p.txn.UpdateDeadlineMaybe(hlc.Timestamp{WallTime: lease.Expiration().UnixNano()})
	}
	return &lease.TableDescriptor, nil
}

// removeLeaseIfExpiring removes a lease and returns true if it is about to expire.
// The method also resets the transaction deadline.
func (p *planner) removeLeaseIfExpiring(ctx context.Context, lease *LeaseState) bool {
	session := p.session
	if lease == nil || lease.hasSomeLifeLeft(session.leaseMgr.clock) {
		return false
	}

	// Remove the lease from session.leases.
	idx := -1
	for i, l := range session.leases {
		if l == lease {
			idx = i
			break
		}
	}
	if idx == -1 {
		log.Warningf(ctx, "lease (%s) not found", lease)
		return false
	}
	session.leases[idx] = session.leases[len(session.leases)-1]
	session.leases[len(session.leases)-1] = nil
	session.leases = session.leases[:len(session.leases)-1]

	if err := session.leaseMgr.Release(lease); err != nil {
		log.Warning(ctx, err)
	}

	// Reset the deadline so that a new deadline will be set after the lease is acquired.
	p.txn.ResetDeadline()
	for _, l := range session.leases {
		p.txn.UpdateDeadlineMaybe(hlc.Timestamp{WallTime: l.Expiration().UnixNano()})
	}
	return true
}

// getTableNames implements the SchemaAccessor interface.
func (p *planner) getTableNames(
	ctx context.Context, dbDesc *sqlbase.DatabaseDescriptor,
) (parser.TableNames, error) {
	if e, ok := p.session.virtualSchemas.getVirtualSchemaEntry(dbDesc.Name); ok {
		return e.tableNames(), nil
	}

	prefix := sqlbase.MakeNameMetadataKey(dbDesc.ID, "")
	sr, err := p.txn.Scan(ctx, prefix, prefix.PrefixEnd(), 0)
	if err != nil {
		return nil, err
	}

	var tableNames parser.TableNames
	for _, row := range sr {
		_, tableName, err := encoding.DecodeUnsafeStringAscending(
			bytes.TrimPrefix(row.Key, prefix), nil)
		if err != nil {
			return nil, err
		}
		tn := parser.TableName{
			DatabaseName: parser.Name(dbDesc.Name),
			TableName:    parser.Name(tableName),
		}
		tableNames = append(tableNames, tn)
	}
	return tableNames, nil
}

func (p *planner) getAliasedTableName(n parser.TableExpr) (*parser.TableName, error) {
	if ate, ok := n.(*parser.AliasedTableExpr); ok {
		n = ate.Expr
	}
	table, ok := n.(*parser.NormalizableTableName)
	if !ok {
		return nil, errors.Errorf("TODO(pmattis): unsupported FROM: %s", n)
	}
	return table.NormalizeWithDatabaseName(p.session.Database)
}

// notifySchemaChange implements the SchemaAccessor interface.
func (p *planner) notifySchemaChange(id sqlbase.ID, mutationID sqlbase.MutationID) {
	sc := SchemaChanger{
		tableID:    id,
		mutationID: mutationID,
		nodeID:     p.evalCtx.NodeID,
		leaseMgr:   p.session.leaseMgr,
	}
	p.session.TxnState.schemaChangers.queueSchemaChanger(sc)
}

// releaseLeases implements the SchemaAccessor interface.
func (p *planner) releaseLeases(ctx context.Context) {
	session := p.session
	if session.leases != nil {
		if log.V(2) {
			log.Infof(ctx, "planner releasing %d leases", len(session.leases))
		}
		for _, lease := range session.leases {
			if err := session.leaseMgr.Release(lease); err != nil {
				log.Warning(ctx, err)
			}
		}
		session.leases = nil
	}
}

// writeTableDesc implements the SchemaAccessor interface.
func (p *planner) writeTableDesc(ctx context.Context, tableDesc *sqlbase.TableDescriptor) error {
	if isVirtualDescriptor(tableDesc) {
		panic(fmt.Sprintf("Virtual Descriptors cannot be stored, found: %v", tableDesc))
	}
	return p.txn.Put(
		ctx, sqlbase.MakeDescMetadataKey(tableDesc.GetID()), sqlbase.WrapDescriptor(tableDesc),
	)
}

// expandTableGlob implements the SchemaAccessor interface.
// The pattern must be already normalized using NormalizeTablePattern().
func (p *planner) expandTableGlob(
	ctx context.Context, pattern parser.TablePattern,
) (parser.TableNames, error) {
	if t, ok := pattern.(*parser.TableName); ok {
		if err := t.QualifyWithDatabase(p.session.Database); err != nil {
			return nil, err
		}
		return parser.TableNames{*t}, nil
	}

	glob := pattern.(*parser.AllTablesSelector)

	if err := glob.QualifyWithDatabase(p.session.Database); err != nil {
		return nil, err
	}

	dbDesc, err := p.mustGetDatabaseDesc(ctx, string(glob.Database))
	if err != nil {
		return nil, err
	}

	tableNames, err := p.getTableNames(ctx, dbDesc)
	if err != nil {
		return nil, err
	}
	return tableNames, nil
}

// searchAndQualifyDatabase augments the table name with the database
// where it was found. It searches first in the session current
// database, if that's defined, otherwise the search path.  The
// provided TableName is modified in-place in case of success, and
// left unchanged otherwise.
// The table name must not be qualified already.
func (p *planner) searchAndQualifyDatabase(ctx context.Context, tn *parser.TableName) error {
	t := *tn

	descFunc := p.getTableLease
	if p.avoidCachedDescriptors {
		// AS OF SYSTEM TIME queries need to fetch the table descriptor at the
		// specified time, and never lease anything. The proto transaction already
		// has its timestamps set correctly so getTableOrViewDesc will fetch with
		// the correct timestamp.
		descFunc = p.getTableOrViewDesc
	}

	if p.session.Database != "" {
		t.DatabaseName = parser.Name(p.session.Database)
		desc, err := descFunc(ctx, &t)
		if err != nil && !sqlbase.IsUndefinedTableError(err) && !sqlbase.IsUndefinedDatabaseError(err) {
			return err
		}
		if desc != nil {
			// Table was found, use this name.
			*tn = t
			return nil
		}
	}

	// Not found using the current session's database, so try
	// the search path instead.
	for _, database := range p.session.SearchPath {
		t.DatabaseName = parser.Name(database)
		desc, err := descFunc(ctx, &t)
		if err != nil && !sqlbase.IsUndefinedTableError(err) && !sqlbase.IsUndefinedDatabaseError(err) {
			return err
		}
		if desc != nil {
			// The table or view exists in this database, so return it.
			*tn = t
			return nil
		}
	}

	return sqlbase.NewUndefinedTableError(string(t.TableName))
}

// getQualifiedTableName returns the database-qualified name of the table
// or view represented by the provided descriptor.
func (p *planner) getQualifiedTableName(
	ctx context.Context, desc *sqlbase.TableDescriptor,
) (string, error) {
	dbDesc, err := sqlbase.GetDatabaseDescFromID(ctx, p.txn, desc.ParentID)
	if err != nil {
		return "", err
	}
	tbName := parser.TableName{
		DatabaseName: parser.Name(dbDesc.Name),
		TableName:    parser.Name(desc.Name),
	}
	return tbName.String(), nil
}

// findTableContainingIndex returns the name of the table containing
// an index of the given name. An error is returned if the index is
// not found or if the index name is ambiguous (i.e. exists in
// multiple tables).
func (p *planner) findTableContainingIndex(
	ctx context.Context, dbName parser.Name, idxName parser.Name,
) (result *parser.TableName, err error) {
	dbDesc, err := p.mustGetDatabaseDesc(ctx, dbName.Normalize())
	if err != nil {
		return nil, err
	}

	tns, err := p.getTableNames(ctx, dbDesc)
	if err != nil {
		return nil, err
	}

	normName := idxName.Normalize()
	result = nil
	for i := range tns {
		tn := &tns[i]
		tableDesc, err := p.mustGetTableDesc(ctx, tn)
		if err != nil {
			return nil, err
		}
		status, _, _ := tableDesc.FindIndexByNormalizedName(normName)
		if status != sqlbase.DescriptorAbsent {
			if result != nil {
				return nil, fmt.Errorf("index name %q is ambiguous (found in %s and %s)",
					normName, tn.String(), result.String())
			}
			result = tn
		}
	}
	if result == nil {
		return nil, fmt.Errorf("index %q does not exist", normName)
	}
	return result, nil
}

// expandIndexName ensures that the index name is qualified with a
// table name, and searches the table name if not yet specified. It returns
// the TableName of the underlying table for convenience.
func (p *planner) expandIndexName(
	ctx context.Context, index *parser.TableNameWithIndex,
) (*parser.TableName, error) {
	tn, err := index.Table.NormalizeWithDatabaseName(p.session.Database)
	if err != nil {
		return nil, err
	}

	if index.SearchTable {
		realTableName, err := p.findTableContainingIndex(ctx, tn.DatabaseName, tn.TableName)
		if err != nil {
			return nil, err
		}
		index.Index = tn.TableName
		index.Table.TableNameReference = realTableName
		tn = realTableName
	}
	return tn, nil
}
