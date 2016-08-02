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

	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/sql/sqlbase"
	"github.com/cockroachdb/cockroach/util/encoding"
	"github.com/cockroachdb/cockroach/util/hlc"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/pkg/errors"
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

// getKeysForTableDescriptor retrieves the KV keys corresponding
// to the zone, name and descriptor of a table.
func getKeysForTableDescriptor(
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
	getTableNames(dbDesc *sqlbase.DatabaseDescriptor) (parser.QualifiedNames, error)

	// expandTableGlob expands wildcards from the end of `expr` and
	// returns the list of matching tables.
	// `expr` is possibly modified to be qualified with the database it refers to.
	// `expr` is assumed to be of one of several forms:
	// 		database.table
	// 		table
	// 		*
	expandTableGlob(expr *parser.QualifiedName) (parser.QualifiedNames, error)

	// getTableDesc returns a table descriptor, or nil if the descriptor
	// is not found. If you want the "not found" condition to return an error,
	// use mustGetTableDesc() instead.
	getTableDesc(qname *parser.QualifiedName) (*sqlbase.TableDescriptor, error)

	// mustGetTableDesc returns a table descriptor, or an error if the descriptor
	// is not found.
	mustGetTableDesc(qname *parser.QualifiedName) (*sqlbase.TableDescriptor, error)

	// NB: one can use GetTableDescFromID() to retrieve a descriptor for
	// a table from a transaction using its ID, assuming it was loaded
	// in the transaction already.

	// notifySchemaChange notifies that an outstanding schema change
	// exists for the table.
	notifySchemaChange(id sqlbase.ID, mutationID sqlbase.MutationID)

	// getTableLease acquires a lease for the specified table. The lease will be
	// released when the planner closes.
	getTableLease(qname *parser.QualifiedName) (*sqlbase.TableDescriptor, error)

	// getAliasedTableLease looks up the table descriptor for an alias table
	// expression.
	getAliasedTableLease(n parser.TableExpr) (*sqlbase.TableDescriptor, error)

	// releaseLeases releases all leases currently held by the planner.
	releaseLeases()

	// writeTableDesc effectively writes a table descriptor to the
	// database within the current planner transaction.
	writeTableDesc(tableDesc *sqlbase.TableDescriptor) error
}

var _ SchemaAccessor = &planner{}

// getTableDesc implements the SchemaAccessor interface.
func (p *planner) getTableDesc(qname *parser.QualifiedName) (*sqlbase.TableDescriptor, error) {
	if err := qname.NormalizeTableName(p.session.Database); err != nil {
		return nil, err
	}

	virtual, err := getVirtualTableDesc(qname)
	if err != nil || virtual != nil {
		return virtual, err
	}

	dbDesc, err := p.mustGetDatabaseDesc(qname.Database())
	if err != nil {
		return nil, err
	}

	desc := sqlbase.TableDescriptor{}
	found, err := p.getDescriptor(tableKey{parentID: dbDesc.ID, name: qname.Table()}, &desc)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, nil
	}
	return &desc, nil
}

// mustGetTableDesc implements the SchemaAccessor interface.
func (p *planner) mustGetTableDesc(qname *parser.QualifiedName) (*sqlbase.TableDescriptor, error) {
	desc, err := p.getTableDesc(qname)
	if err != nil {
		return nil, err
	}
	if desc == nil {
		return nil, sqlbase.NewUndefinedTableError(qname.String())
	}
	return desc, nil
}

// getTableLease implements the SchemaAccessor interface.
func (p *planner) getTableLease(qname *parser.QualifiedName) (*sqlbase.TableDescriptor, error) {
	if log.V(2) {
		log.Infof(p.ctx(), "planner acquiring lease on table %q", qname)
	}
	if err := qname.NormalizeTableName(p.session.Database); err != nil {
		return nil, err
	}

	isSystemDB := qname.Database() == sqlbase.SystemDB.Name
	isVirtualDB := isVirtualDatabase(qname.Database())
	if isSystemDB || isVirtualDB || testDisableTableLeases {
		// We don't go through the normal lease mechanism for:
		// - system tables. The system.lease and system.descriptor table, in
		//   particular, are problematic because they are used for acquiring
		//   leases itself, creating a chicken&egg problem.
		// - virtual tables. These tables' descriptors are not persisted,
		//   so they cannot be leased. Instead, we simply return the static
		//   descriptor and rely on the immutability privileges set on the
		//   descriptors to cause upper layers to reject mutations statements.
		return p.mustGetTableDesc(qname)
	}

	dbID, err := p.getDatabaseID(qname.Database())
	if err != nil {
		return nil, err
	}

	// First, look to see if we already have a lease for this table.
	// This ensures that, once a SQL transaction resolved name N to id X, it will
	// continue to use N to refer to X even if N is renamed during the
	// transaction.
	var lease *LeaseState
	for _, l := range p.leases {
		if sqlbase.NormalizeName(l.Name) == sqlbase.NormalizeName(qname.Table()) &&
			l.ParentID == dbID {
			lease = l
			if log.V(2) {
				log.Infof(p.ctx(), "found lease in planner cache for table %q", qname)
			}
			break
		}
	}

	// If we didn't find a lease or the lease is about to expire, acquire one.
	if lease == nil || p.removeLeaseIfExpiring(lease) {
		var err error
		lease, err = p.leaseMgr.AcquireByName(p.txn, dbID, qname.Table())
		if err != nil {
			if err == sqlbase.ErrDescriptorNotFound {
				// Transform the descriptor error into an error that references the
				// table's name.
				return nil, sqlbase.NewUndefinedTableError(qname.String())
			}
			return nil, err
		}
		p.leases = append(p.leases, lease)
		// If the lease we just acquired expires before the txn's deadline, reduce
		// the deadline.
		p.txn.UpdateDeadlineMaybe(hlc.Timestamp{WallTime: lease.Expiration().UnixNano()})
	}
	return &lease.TableDescriptor, nil
}

// getTableLeaseByID is a by-ID variant of getTableLease (i.e. uses same cache).
func (p *planner) getTableLeaseByID(tableID sqlbase.ID) (*sqlbase.TableDescriptor, error) {
	if log.V(2) {
		log.Infof(p.ctx(), "planner acquiring lease on table ID %d", tableID)
	}

	// First, look to see if we already have a lease for this table -- including
	// leases acquired via `getTableLease`.
	var lease *LeaseState
	for _, l := range p.leases {
		if l.ID == tableID {
			lease = l
			if log.V(2) {
				log.Infof(p.ctx(), "found lease in planner cache for table %d", tableID)
			}
			break
		}
	}

	// If we didn't find a lease or the lease is about to expire, acquire one.
	if lease == nil || p.removeLeaseIfExpiring(lease) {
		var err error
		lease, err = p.leaseMgr.Acquire(p.txn, tableID, 0)
		if err != nil {
			if err == sqlbase.ErrDescriptorNotFound {
				// Transform the descriptor error into an error that references the
				// table's ID.
				return nil, sqlbase.NewUndefinedTableError(fmt.Sprintf("<id=%d>", tableID))
			}
			return nil, err
		}
		p.leases = append(p.leases, lease)
		// If the lease we just acquired expires before the txn's deadline, reduce
		// the deadline.
		p.txn.UpdateDeadlineMaybe(hlc.Timestamp{WallTime: lease.Expiration().UnixNano()})
	}
	return &lease.TableDescriptor, nil
}

// removeLeaseIfExpiring removes a lease and returns true if it is about to expire.
// The method also resets the transaction deadline.
func (p *planner) removeLeaseIfExpiring(lease *LeaseState) bool {
	if lease == nil || lease.hasSomeLifeLeft(p.leaseMgr.clock) {
		return false
	}

	// Remove the lease from p.leases.
	idx := -1
	for i, l := range p.leases {
		if l == lease {
			idx = i
			break
		}
	}
	if idx == -1 {
		log.Warningf(p.ctx(), "lease (%s) not found", lease)
		return false
	}
	p.leases[idx] = p.leases[len(p.leases)-1]
	p.leases[len(p.leases)-1] = nil
	p.leases = p.leases[:len(p.leases)-1]

	if err := p.leaseMgr.Release(lease); err != nil {
		log.Warning(p.ctx(), err)
	}

	// Reset the deadline so that a new deadline will be set after the lease is acquired.
	p.txn.ResetDeadline()
	for _, l := range p.leases {
		p.txn.UpdateDeadlineMaybe(hlc.Timestamp{WallTime: l.Expiration().UnixNano()})
	}
	return true
}

// getTableNames implements the SchemaAccessor interface.
func (p *planner) getTableNames(dbDesc *sqlbase.DatabaseDescriptor) (parser.QualifiedNames, error) {
	if e, ok := getVirtualSchemaEntry(dbDesc.Name); ok {
		return e.tableNames()
	}

	prefix := sqlbase.MakeNameMetadataKey(dbDesc.ID, "")
	sr, err := p.txn.Scan(prefix, prefix.PrefixEnd(), 0)
	if err != nil {
		return nil, err
	}

	var qualifiedNames parser.QualifiedNames
	for _, row := range sr {
		_, tableName, err := encoding.DecodeUnsafeStringAscending(
			bytes.TrimPrefix(row.Key, prefix), nil)
		if err != nil {
			return nil, err
		}
		qname, err := parser.NewQualifiedNameFromDBAndTable(dbDesc.Name, tableName)
		if err != nil {
			return nil, err
		}
		qualifiedNames = append(qualifiedNames, qname)
	}
	return qualifiedNames, nil
}

// getAliasedTableLease implements the SchemaAccessor interface.
func (p *planner) getAliasedTableLease(n parser.TableExpr) (*sqlbase.TableDescriptor, error) {
	ate, ok := n.(*parser.AliasedTableExpr)
	if !ok {
		return nil, errors.Errorf("TODO(pmattis): unsupported FROM: %s", n)
	}
	table, ok := ate.Expr.(*parser.QualifiedName)
	if !ok {
		return nil, errors.Errorf("TODO(pmattis): unsupported FROM: %s", n)
	}
	desc, err := p.getTableLease(table)
	if err != nil {
		return nil, err
	}
	return desc, nil
}

// notifySchemaChange implements the SchemaAccessor interface.
func (p *planner) notifySchemaChange(id sqlbase.ID, mutationID sqlbase.MutationID) {
	sc := SchemaChanger{
		tableID:    id,
		mutationID: mutationID,
		nodeID:     p.evalCtx.NodeID,
		leaseMgr:   p.leaseMgr,
	}
	p.session.TxnState.schemaChangers.queueSchemaChanger(sc)
}

// releaseLeases implements the SchemaAccessor interface.
func (p *planner) releaseLeases() {
	if log.V(2) {
		log.Infof(p.ctx(), "planner releasing leases")
	}
	if p.leases != nil {
		for _, lease := range p.leases {
			if err := p.leaseMgr.Release(lease); err != nil {
				log.Warning(p.ctx(), err)
			}
		}
		p.leases = nil
	}
}

// writeTableDesc implements the SchemaAccessor interface.
func (p *planner) writeTableDesc(tableDesc *sqlbase.TableDescriptor) error {
	if isVirtualDescriptor(tableDesc) {
		panic(fmt.Sprintf("Virtual Descriptors cannot be stored, found: %v", tableDesc))
	}
	return p.txn.Put(sqlbase.MakeDescMetadataKey(tableDesc.GetID()),
		sqlbase.WrapDescriptor(tableDesc))
}

// expandTableGlob implements the SchemaAccessor interface.
func (p *planner) expandTableGlob(expr *parser.QualifiedName) (
	parser.QualifiedNames, error) {
	if len(expr.Indirect) == 0 {
		return parser.QualifiedNames{expr}, nil
	}

	if err := expr.QualifyWithDatabase(p.session.Database); err != nil {
		return nil, err
	}
	// We must have a single indirect: either .table or .*
	if len(expr.Indirect) != 1 {
		return nil, fmt.Errorf("invalid table glob: %s", expr)
	}

	switch expr.Indirect[0].(type) {
	case parser.NameIndirection:
		return parser.QualifiedNames{expr}, nil
	case parser.StarIndirection:
		dbDesc, err := p.mustGetDatabaseDesc(string(expr.Base))
		if err != nil {
			return nil, err
		}
		tableNames, err := p.getTableNames(dbDesc)
		if err != nil {
			return nil, err
		}
		return tableNames, nil
	default:
		return nil, fmt.Errorf("invalid table glob: %s", expr)
	}
}
