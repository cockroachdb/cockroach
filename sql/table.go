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
	"errors"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/sql/sqlbase"
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

func tableDoesNotExistError(name string) error {
	return fmt.Errorf("table %q does not exist", name)
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

// getTableDesc returns a table descriptor, or nil if the descriptor is not
// found.
// If you want to transform the not found condition into an error, use
// tableDoesNotExistError().
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

var errDescriptorNotFound = errors.New("descriptor not found")

// get the table descriptor for the ID passed in using an existing txn.
// returns an error if the descriptor doesn't exist or if it exists and is not
// a table.
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

func getKeysForTableDescriptor(
	tableDesc *sqlbase.TableDescriptor,
) (zoneKey roachpb.Key, nameKey roachpb.Key, descKey roachpb.Key) {
	zoneKey = sqlbase.MakeZoneKey(tableDesc.ID)
	nameKey = sqlbase.MakeNameMetadataKey(tableDesc.ParentID, tableDesc.GetName())
	descKey = sqlbase.MakeDescMetadataKey(tableDesc.ID)
	return
}

// getTableLease acquires a lease for the specified table. The lease will be
// released when the planner closes. Note that a shallow copy of the table
// descriptor is returned. It is safe to mutate fields of the returned
// descriptor, but the values those fields point to should not be modified.
func (p *planner) getTableLease(qname *parser.QualifiedName) (sqlbase.TableDescriptor, error) {
	if log.V(2) {
		log.Infof("planner acquiring lease on table %q", qname)
	}
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
			return sqlbase.TableDescriptor{}, tableDoesNotExistError(qname.String())
		}
		return *desc, nil
	}

	if err := qname.NormalizeTableName(p.session.Database); err != nil {
		return sqlbase.TableDescriptor{}, err
	}

	dbID, err := p.getDatabaseID(qname.Database())
	if err != nil {
		return sqlbase.TableDescriptor{}, err
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
				log.Infof("found lease in planner cache for table %q", qname)
			}
			break
		}
	}

	// If we didn't find a lease, acquire one.
	if lease == nil {
		var err error
		lease, err = p.leaseMgr.AcquireByName(p.txn, dbID, qname.Table())
		if err != nil {
			if err == errDescriptorNotFound {
				// Transform the descriptor error into an error that references the
				// table's name.
				return sqlbase.TableDescriptor{}, tableDoesNotExistError(qname.String())
			}
			return sqlbase.TableDescriptor{}, err
		}
		p.leases = append(p.leases, lease)
		// If the lease we just acquired expires before the txn's deadline, reduce
		// the deadline.
		curDeadline := p.txn.GetDeadline()
		if curDeadline == nil || time.Unix(0, curDeadline.WallTime).After(lease.Expiration()) {
			p.txn.SetDeadline(roachpb.Timestamp{WallTime: lease.Expiration().UnixNano()})
		}
	}
	return lease.TableDescriptor, nil
}

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
