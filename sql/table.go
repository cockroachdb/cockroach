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

package sql

import (
	"bytes"
	"errors"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/sql/sqlbase"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/encoding"
	"github.com/cockroachdb/cockroach/util/log"
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

var errDescriptorNotFound = errors.New("descriptor not found")

// getTableDescFromID retrieves the table descriptor for the table
// ID passed in using an existing txn. Teturns an error if the
// descriptor doesn't exist or if it exists and is not a table.
func getTableDescFromID(txn *client.Txn, id sqlbase.ID) (*sqlbase.TableDescriptor, error) {
	desc := &sqlbase.Descriptor{}
	descKey := sqlbase.MakeDescMetadataKey(id)

	if err := txn.GetProto(descKey, desc); err != nil {
		return nil, err
	}
	table := desc.GetTable()
	if table == nil {
		return nil, errDescriptorNotFound
	}
	return table, nil
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

func updateCommitBy(commitBy time.Time, lease *LeaseState) time.Time {
	if commitBy.IsZero() || commitBy.After(lease.Expiration()) {
		return lease.Expiration()
	}
	return commitBy
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
	// is not found. If you want to transform the not found condition
	// into an error, use newUndefinedTableError().
	getTableDesc(qname *parser.QualifiedName) (*sqlbase.TableDescriptor, error)

	// getTableID retrieves the table ID for the specified table. It uses the
	// descriptor cache to perform lookups, falling back to the KV store when
	// necessary.
	getTableID(qname *parser.QualifiedName) (sqlbase.ID, error)

	// NB: one can use getTableDescFromID() to retrieve a descriptor for
	// a table from a transaction using its ID, assuming it was loaded
	// in the transaction already.

	// notifySchemaChange notifies that an outstanding schema change
	// exists for the table.
	notifySchemaChange(id sqlbase.ID, mutationID sqlbase.MutationID)

	// getTableLease acquires a lease for the specified table. The lease will be
	// released when the planner closes. Note that a shallow copy of the table
	// descriptor is returned. It is safe to mutate fields of the returned
	// descriptor, but the values those fields point to should not be modified.
	getTableLease(qname *parser.QualifiedName) (sqlbase.TableDescriptor, error)

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
	dbDesc, err := p.getDatabaseDesc(qname.Database())
	if err != nil {
		return nil, err
	}
	if dbDesc == nil {
		return nil, databaseDoesNotExistError(qname.Database())
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

// getTableLease implements the SchemaAccessor interface.
func (p *planner) getTableLease(qname *parser.QualifiedName) (sqlbase.TableDescriptor, error) {
	if err := qname.NormalizeTableName(p.session.Database); err != nil {
		return sqlbase.TableDescriptor{}, err
	}

	if qname.Database() == sqlbase.SystemDB.Name || testDisableTableLeases {
		// We don't go through the normal lease mechanism for system tables. The
		// system.lease and system.descriptor table, in particular, are problematic
		// because they are used for acquiring leases itself, creating a
		// chicken&egg problem.
		desc, err := p.getTableDesc(qname)
		if err != nil {
			return sqlbase.TableDescriptor{}, err
		}
		if desc == nil {
			return sqlbase.TableDescriptor{}, newUndefinedTableError(qname.String())
		}
		return *desc, nil
	}

	tableID, err := p.getTableID(qname)
	if err != nil {
		return sqlbase.TableDescriptor{}, err
	}

	var lease *LeaseState
	var commitBy time.Time
	for _, l := range p.leases {
		commitBy = updateCommitBy(commitBy, l)
		if l.TableDescriptor.ID == tableID {
			lease = l
		}
	}
	if lease == nil {
		var err error
		lease, err = p.leaseMgr.Acquire(p.txn, tableID, 0)
		if err != nil {
			if err == errDescriptorNotFound {
				// Transform the descriptor error into an error that references the
				// table's name.
				return sqlbase.TableDescriptor{}, newUndefinedTableError(qname.String())
			}
			return sqlbase.TableDescriptor{}, err
		}
		p.leases = append(p.leases, lease)
		commitBy = updateCommitBy(commitBy, lease)
	}
	p.txn.SetDeadline(roachpb.Timestamp{WallTime: commitBy.UnixNano()})
	return lease.TableDescriptor, nil
}

// getTableID implements the SchemaAccessor interface.
func (p *planner) getTableID(qname *parser.QualifiedName) (sqlbase.ID, error) {
	if err := qname.NormalizeTableName(p.session.Database); err != nil {
		return 0, err
	}

	dbID, err := p.getDatabaseID(qname.Database())
	if err != nil {
		return 0, err
	}

	// Lookup the ID of the table in the cache. The use of the cache might cause
	// the usage of a recently renamed table, but that's a race that could occur
	// anyways.
	// TODO(andrei): remove the used of p.systemConfig as a cache for table names,
	// replace it with using the leases for resolving names, and do away with any
	// races due to renames. We'll probably have to rewrite renames to perform
	// an async schema change.
	nameKey := tableKey{dbID, qname.Table()}
	key := nameKey.Key()
	if nameVal := p.systemConfig.GetValue(key); nameVal != nil {
		id, err := nameVal.GetInt()
		return sqlbase.ID(id), err
	}

	gr, err := p.txn.Get(key)
	if err != nil {
		return 0, err
	}
	if !gr.Exists() {
		return 0, newUndefinedTableError(qname.String())
	}
	return sqlbase.ID(gr.ValueInt()), nil
}

// getTableNames implements the SchemaAccessor interface.
func (p *planner) getTableNames(dbDesc *sqlbase.DatabaseDescriptor) (parser.QualifiedNames, error) {
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
		qname := &parser.QualifiedName{
			Base:     parser.Name(dbDesc.Name),
			Indirect: parser.Indirection{parser.NameIndirection(tableName)},
		}
		if err := qname.NormalizeTableName(""); err != nil {
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
		return nil, util.Errorf("TODO(pmattis): unsupported FROM: %s", n)
	}
	table, ok := ate.Expr.(*parser.QualifiedName)
	if !ok {
		return nil, util.Errorf("TODO(pmattis): unsupported FROM: %s", n)
	}
	desc, err := p.getTableLease(table)
	if err != nil {
		return nil, err
	}
	return &desc, nil
}

// notifySchemaChange implements the SchemaAccessor interface.
func (p *planner) notifySchemaChange(id sqlbase.ID, mutationID sqlbase.MutationID) {
	sc := SchemaChanger{
		tableID:    id,
		mutationID: mutationID,
		nodeID:     p.evalCtx.NodeID,
		cfg:        p.systemConfig,
		leaseMgr:   p.leaseMgr,
	}
	p.session.TxnState.schemaChangers.queueSchemaChanger(sc)
}

// releaseLeases implements the SchemaAccessor interface.
func (p *planner) releaseLeases() {
	if p.leases != nil {
		for _, lease := range p.leases {
			if err := p.leaseMgr.Release(lease); err != nil {
				log.Warning(err)
			}
		}
		p.leases = nil
	}
}

// writeTableDesc implements the SchemaAccessor interface.
func (p *planner) writeTableDesc(tableDesc *sqlbase.TableDescriptor) error {
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
		dbDesc, err := p.getDatabaseDesc(string(expr.Base))
		if err != nil {
			return nil, err
		}
		if dbDesc == nil {
			return nil, databaseDoesNotExistError(string(expr.Base))
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
