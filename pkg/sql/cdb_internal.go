// Copyright 2016 The Cockroach Authors.
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
// Author: Raphael 'kena' Poss (knz@cockroachlabs.com)

package sql

import (
	"strings"
	"time"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

var cdbInternal = virtualSchema{
	name: "cdb_internal",
	tables: []virtualSchemaTable{
		cdbInternalTablesTable,
		cdbInternalSessionsTable,
		cdbInternalTxnsTable,
		cdbInternalLeasesTable,
		cdbInternalSchemaChangesTable,
	},
}

type sessionRegistry struct {
	syncutil.Mutex
	store map[*Session]struct{}
}

func makeSessionRegistry() sessionRegistry {
	return sessionRegistry{store: make(map[*Session]struct{})}
}

func (r *sessionRegistry) register(s *Session) {
	r.Lock()
	r.store[s] = struct{}{}
	r.Unlock()
}

func (r *sessionRegistry) deregister(s *Session) {
	r.Lock()
	delete(r.store, s)
	r.Unlock()
}

var cdbInternalTablesTable = virtualSchemaTable{
	schema: `
CREATE TABLE cdb_internal.tables (
  TABLE_ID                 INT NOT NULL,
  PARENT_ID                INT NOT NULL,
  NAME                     STRING NOT NULL,
  VERSION                  INT NOT NULL,
  MODIFICATION_TIME        TIMESTAMP NOT NULL,
  MODIFICATION_TIME_L      DECIMAL NOT NULL,
  FORMAT_VERSION           STRING NOT NULL,
  STATE                    STRING NOT NULL,
  SC_LEASE_NODE_ID         INT,
  SC_LEASE_EXPIRATION_TIME TIMESTAMP
);
`,
	populate: func(p *planner, addRow func(...parser.Datum) error) error {
		descs, err := p.getAllDescriptors()
		if err != nil {
			return err
		}
		// Note: we do not use forEachTableDesc() here because we want to
		// include added and dropped descriptors.
		for _, desc := range descs {
			table, ok := desc.(*sqlbase.TableDescriptor)
			if !ok || !userCanSeeDescriptor(table, p.session.User) {
				continue
			}
			leaseNodeDatum := parser.DNull
			leaseExpDatum := parser.DNull
			if table.Lease != nil {
				leaseNodeDatum = parser.NewDInt(parser.DInt(int64(table.Lease.NodeID)))
				leaseExpDatum = parser.MakeDTimestamp(
					time.Unix(0, table.Lease.ExpirationTime), time.Nanosecond,
				)
			}
			if err := addRow(
				parser.NewDInt(parser.DInt(int64(table.ID))),
				parser.NewDInt(parser.DInt(int64(table.ParentID))),
				parser.NewDString(parser.Name(table.Name).String()),
				parser.NewDInt(parser.DInt(int64(table.Version))),
				parser.MakeDTimestamp(time.Unix(0, table.ModificationTime.WallTime), time.Microsecond),
				parser.TimestampToDecimal(table.ModificationTime),
				parser.NewDString(table.FormatVersion.String()),
				parser.NewDString(table.State.String()),
				leaseNodeDatum,
				leaseExpDatum,
			); err != nil {
				return err
			}
		}
		return nil
	},
}

var cdbInternalSchemaChangesTable = virtualSchemaTable{
	schema: `
CREATE TABLE cdb_internal.schema_changes (
  TABLE_ID      INT NOT NULL,
  PARENT_ID     INT NOT NULL,
  NAME          STRING NOT NULL,
  TYPE          STRING NOT NULL,
  TARGET_ID     INT,
  TARGET_NAME   STRING,
  STATE         STRING NOT NULL,
  DIRECTION     STRING NOT NULL,
  RESUME_KEY    BYTES NOT NULL,
  RESUME_ENDKEY BYTES NOT NULL
);
`,
	populate: func(p *planner, addRow func(...parser.Datum) error) error {
		descs, err := p.getAllDescriptors()
		if err != nil {
			return err
		}
		// Note: we do not use forEachTableDesc() here because we want to
		// include added and dropped descriptors.
		for _, desc := range descs {
			table, ok := desc.(*sqlbase.TableDescriptor)
			if !ok || !userCanSeeDescriptor(table, p.session.User) {
				continue
			}
			tableID := parser.NewDInt(parser.DInt(int64(table.ID)))
			parentID := parser.NewDInt(parser.DInt(int64(table.ParentID)))
			tableName := parser.NewDString(parser.Name(table.Name).String())
			for _, mut := range table.Mutations {
				mutType := "UNKNOWN"
				targetID := parser.DNull
				targetName := parser.DNull
				switch d := mut.Descriptor_.(type) {
				case *sqlbase.DescriptorMutation_Column:
					mutType = "COLUMN"
					targetID = parser.NewDInt(parser.DInt(int64(d.Column.ID)))
					targetName = parser.NewDString(d.Column.Name)
				case *sqlbase.DescriptorMutation_Index:
					mutType = "INDEX"
					targetID = parser.NewDInt(parser.DInt(int64(d.Index.ID)))
					targetName = parser.NewDString(d.Index.Name)
				}
				if err := addRow(
					tableID,
					parentID,
					tableName,
					parser.NewDString(mutType),
					targetID,
					targetName,
					parser.NewDString(mut.State.String()),
					parser.NewDString(mut.Direction.String()),
					parser.NewDBytes(parser.DBytes([]byte(mut.ResumeSpan.Key))),
					parser.NewDBytes(parser.DBytes([]byte(mut.ResumeSpan.EndKey))),
				); err != nil {
					return err
				}
			}
		}
		return nil
	},
}

var cdbInternalSessionsTable = virtualSchemaTable{
	schema: `
CREATE TABLE cdb_internal.sessions (
  NODE_ID                 INT NOT NULL,
  SESSION_ID              INT NOT NULL,
  CLIENT                  STRING NOT NULL,
  "USER"                  STRING NOT NULL,
  TXN_STATE               STRING NOT NULL,
  TXN_ID                  INT,
  AUTO_RETRY              BOOL NOT NULL,
  SQL_TIMESTAMP           TIMESTAMP NOT NULL,
  DEFAULT_DATABASE        STRING NOT NULL,
  DEFAULT_ISOLATION_LEVEL STRING NOT NULL,
  TIME_ZONE               STRING NOT NULL,
  MEMORY_USAGE            INT NOT NULL,
  SEARCH_PATH             STRING NOT NULL
);
`,
	populate: func(p *planner, addRow func(...parser.Datum) error) error {
		if p.session.sessionRegistry == nil {
			return nil
		}
		registry := p.session.sessionRegistry
		registry.Lock()
		defer registry.Unlock()
		nodeID := parser.NewDInt(parser.DInt(int64(p.leaseMgr.nodeID.Get())))
		for s := range registry.store {
			if s.User != p.session.User && p.session.User != security.RootUser {
				continue
			}

			txnDatum := parser.DNull
			txn := s.TxnState.txn
			if txn != nil {
				txnDatum = parser.NewDInt(parser.DInt(int64(uintptr(unsafe.Pointer(txn)))))
			}
			if err := addRow(
				nodeID,
				parser.NewDInt(parser.DInt(int64(uintptr(unsafe.Pointer(s))))),
				parser.NewDString(s.Client),
				parser.NewDString(s.User),
				parser.NewDString(s.TxnState.State.String()),
				txnDatum,
				parser.MakeDBool(parser.DBool(s.TxnState.autoRetry)),
				parser.MakeDTimestamp(s.TxnState.sqlTimestamp, time.Microsecond),
				parser.NewDString(s.Database),
				parser.NewDString(s.DefaultIsolationLevel.String()),
				parser.NewDString(s.Location.String()),
				parser.NewDInt(parser.DInt(s.mon.CurAllocated())),
				parser.NewDString(strings.Join(s.SearchPath, ",")),
			); err != nil {
				return err
			}
		}
		return nil
	},
}

var cdbInternalTxnsTable = virtualSchemaTable{
	schema: `
CREATE TABLE cdb_internal.txns (
  NODE_ID           INT NOT NULL,
  TXN_ID            INT NOT NULL,
  KV_TXN_UUID       STRING,
  KEY               BYTES,
  EPOCH             INT NOT NULL,
  TIMESTAMP         TIMESTAMP NOT NULL,
  TIMESTAMP_L       DECIMAL NOT NULL,
  PRIORITY          INT NOT NULL,
  SEQUENCE          INT NOT NULL,
  ISOLATION         STRING NOT NULL,
  NAME              STRING NOT NULL,
  STATUS            STRING NOT NULL,
  ORIG_TIMESTAMP    TIMESTAMP NOT NULL,
  ORIG_TIMESTAMP_L  DECIMAL NOT NULL,
  WRITING           BOOL NOT NULL,
  NUM_INTENTS       INT NOT NULL
);
`,
	populate: func(p *planner, addRow func(...parser.Datum) error) error {
		// TODO(knz) We probably would like this table to report on all
		// Txn objects currently active, but we do not yet have a proper
		// registry for that. So for now the virtual table reports only
		// the Txns known to Session objects.
		if p.session.sessionRegistry == nil {
			return nil
		}
		registry := p.session.sessionRegistry
		registry.Lock()
		defer registry.Unlock()
		nodeID := parser.NewDInt(parser.DInt(int64(p.leaseMgr.nodeID.Get())))
		for s := range registry.store {
			if s.User != p.session.User && p.session.User != security.RootUser {
				continue
			}

			txn := s.TxnState.txn
			if txn == nil {
				continue
			}
			id := txn.Proto.ID
			idDatum := parser.DNull
			if id != nil {
				idDatum = parser.NewDString(id.String())
			}
			key := txn.Proto.Key
			keyDatum := parser.DNull
			if key != nil {
				keyDatum = parser.NewDBytes(parser.DBytes(key))
			}
			if err := addRow(
				nodeID,
				parser.NewDInt(parser.DInt(int64(uintptr(unsafe.Pointer(txn))))),
				idDatum,
				keyDatum,
				parser.NewDInt(parser.DInt(int64(txn.Proto.Epoch))),
				parser.MakeDTimestamp(time.Unix(0, txn.Proto.Timestamp.WallTime), time.Microsecond),
				parser.TimestampToDecimal(txn.Proto.Timestamp),
				parser.NewDInt(parser.DInt(int64(txn.Proto.Priority))),
				parser.NewDInt(parser.DInt(int64(txn.Proto.Sequence))),
				parser.NewDString(txn.Proto.Isolation.String()),
				parser.NewDString(txn.Proto.Name),
				parser.NewDString(txn.Proto.Status.String()),
				parser.MakeDTimestamp(time.Unix(0, txn.Proto.OrigTimestamp.WallTime), time.Microsecond),
				parser.TimestampToDecimal(txn.Proto.OrigTimestamp),
				parser.MakeDBool(parser.DBool(txn.Proto.Writing)),
				parser.NewDInt(parser.DInt(int64(len(txn.Proto.Intents)))),
			); err != nil {
				return err
			}
		}
		return nil
	},
}

var cdbInternalLeasesTable = virtualSchemaTable{
	schema: `
CREATE TABLE cdb_internal.leases (
  NODE_ID     INT NOT NULL,
  TABLE_ID    INT NOT NULL,
  NAME        STRING NOT NULL,
  PARENT_ID   INT NOT NULL,
  EXPIRATION  TIMESTAMP NOT NULL,
  RELEASED    BOOL NOT NULL,
  DELETED     BOOL NOT NULL
);
`,
	populate: func(p *planner, addRow func(...parser.Datum) error) error {
		leaseMgr := p.leaseMgr
		nodeID := parser.NewDInt(parser.DInt(int64(leaseMgr.nodeID.Get())))

		leaseMgr.mu.Lock()
		defer leaseMgr.mu.Unlock()

		for tid, ts := range leaseMgr.tables {
			tableID := parser.NewDInt(parser.DInt(int64(tid)))

			adder := func() error {
				ts.mu.Lock()
				defer ts.mu.Unlock()

				deleted := parser.MakeDBool(parser.DBool(ts.deleted))

				for _, state := range ts.active.data {
					if !userCanSeeDescriptor(&state.TableDescriptor, p.session.User) {
						continue
					}
					expCopy := state.expiration
					if err := addRow(
						nodeID,
						tableID,
						parser.NewDString(parser.Name(state.Name).String()),
						parser.NewDInt(parser.DInt(int64(state.ParentID))),
						&expCopy,
						parser.MakeDBool(parser.DBool(state.released)),
						deleted,
					); err != nil {
						return err
					}
				}
				return nil
			}

			if err := adder(); err != nil {
				return err
			}
		}
		return nil
	},
}
