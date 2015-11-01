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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Vivek Menezes (vivek@cockroachlabs.com)

package sql

import (
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/config"
	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/security"
	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/stop"
	"github.com/gogo/protobuf/proto"
)

// TableLeaderNotification used to return table leader
// notifications from the node.
type TableLeaderNotification struct {
	Key      roachpb.Key
	IsLeader bool
}

// TableLeaderInterface used to register/unregister for table leader
// notifications from a node. A node is a table leader if it is the leader
// for the range containing the table prefix key.
type TableLeaderInterface interface {
	RegisterNotifyTableLeader(key roachpb.Key, s *stop.Stopper, report chan<- TableLeaderNotification)
	UnregisterNotifyTableLeader(key roachpb.Key)
}

// SchemaChanger create a table leader responsible for executing schema
// changes on a table.
type SchemaChanger struct {
	// System Config and mutex.
	systemConfig   config.SystemConfig
	systemConfigMu sync.RWMutex
	newConfig      chan struct{}
	// All the table descriptors with outstanding mutations.
	tables map[string]descriptorState
	// The names of all the databases. We have to recreate this because some of
	// the sql operations used in running the mutations need full table names:
	// parser.QualifiedName.
	databases map[ID]string
}

// updateSystemConfig is called whenever the system config gossip entry is updated.
func (sc *SchemaChanger) updateSystemConfig(cfg *config.SystemConfig) {
	sc.systemConfigMu.Lock()
	defer sc.systemConfigMu.Unlock()
	sc.systemConfig = *cfg
	select {
	case sc.newConfig <- struct{}{}:
	default:
	}
}

// getSystemConfig returns a pointer to the latest system config. May be nil,
// if the gossip callback has not run.
func (sc *SchemaChanger) getSystemConfig() config.SystemConfig {
	sc.systemConfigMu.RLock()
	defer sc.systemConfigMu.RUnlock()
	return sc.systemConfig
}

type descriptorState struct {
	key        string
	descriptor TableDescriptor
	// This node is the leader for this table.
	isLeader bool
	// A schema change routine is running.
	isWorking bool
	// There are no outstanding mutations on this table.
	isDeleted bool
	// last outstanding mutation.
	mutationID uint32
}

// Completed work on a table.
type mutationDone struct {
	key string
	// last mutation completed.
	mutationID uint32
}

// Start runs a loop that looks for table mutations in system configuration changes.
// It registers with node for every table with an outstanding mutation. Under the
// circumstances that it is a table leader for a table, it applies the mutations on
// the table.
func (sc *SchemaChanger) Start(s *stop.Stopper, gossip *gossip.Gossip, node TableLeaderInterface, db *client.DB, leaseMgr *LeaseManager) {
	s.RunWorker(func() {
		// Create channel that receives new system config notifications.
		// The channel has a size of 1 to prevent gossip from blocking on isc.
		sc.newConfig = make(chan struct{}, 1)
		gossip.RegisterSystemConfigCallback(sc.updateSystemConfig)
		// Create channel that receives table leader notifications.
		ch := make(chan TableLeaderNotification)
		// Create channel that receives table mutation completion notifications.
		done := make(chan mutationDone)
		sc.tables = make(map[string]descriptorState)
		sc.databases = make(map[ID]string)

		for {
			select {
			case <-sc.newConfig:
				sc.processNewConfig(s, ch, node)
				for _, d := range sc.tables {
					sc.maybeApplyMutation(s, d, db, done, leaseMgr)
				}

			case n := <-ch:
				key := string(n.Key)
				log.Info("received a leader notification for %v", key)
				if d, ok := sc.tables[key]; ok {
					if n.IsLeader != d.isLeader {
						d.isLeader = n.IsLeader
						sc.tables[key] = d
						sc.maybeApplyMutation(s, d, db, done, leaseMgr)
					}
				} else {
					log.Warningf("received leader notification for table not followed: %s", key)
				}

			case n := <-done:
				log.Infof("received work completion notification for: %s", n.key)
				if d, ok := sc.tables[n.key]; ok {
					if d.isDeleted {
						node.UnregisterNotifyTableLeader(roachpb.Key(d.key))
						delete(sc.tables, d.key)
						break
					}
					if !d.isWorking {
						panic("received work completion notification with no pending work")
					}
					d.isWorking = false
					sc.tables[d.key] = d
					// still more work to be done.
					if n.mutationID != 0 && d.mutationID > n.mutationID {
						sc.maybeApplyMutation(s, d, db, done, leaseMgr)
					}
				} else {
					panic("received work completion notification for table not followed")
				}

			case <-s.ShouldStop():
				return
			}
		}
	})
}

// Populate tables with new config.
func (sc *SchemaChanger) processNewConfig(s *stop.Stopper, ch chan TableLeaderNotification, node TableLeaderInterface) {
	log.Info("received a new config")
	// Existing tables being followed.
	delTables := make(map[string]struct{}, len(sc.tables))
	for k := range sc.tables {
		delTables[k] = struct{}{}
	}

	cfg := sc.getSystemConfig()
	for _, kv := range cfg.Values {
		if kv.Value.Tag != roachpb.ValueType_BYTES {
			continue
		}
		// Attempt to unmarshal config into a table/database descriptor
		var descriptor Descriptor
		if err := kv.Value.GetProto(&descriptor); err != nil {
			log.Warningf("unable to unmarshal descriptor %v", kv.Value)
			continue
		}
		switch union := descriptor.Union.(type) {
		case *Descriptor_Database:
			// Attempt to unmarshal config into a database descriptor.
			database := union.Database
			sc.databases[database.ID] = database.Name

		case *Descriptor_Table:
			table := union.Table
			if err := table.Validate(); err != nil {
				log.Errorf("received invalid table descriptor: %v", table)
				continue
			}
			if len(table.Mutations) > 0 {
				lastMutationID := table.Mutations[len(table.Mutations)-1].ID
				key := MakeIndexKeyPrefix(table.ID, table.PrimaryIndex.ID)
				keyString := string(key)
				d, ok := sc.tables[keyString]
				if ok {
					d.mutationID = lastMutationID
				} else {
					d = descriptorState{key: keyString, descriptor: *table, mutationID: lastMutationID}
					// Register a key for the first range of the table data for notifications.
					node.RegisterNotifyTableLeader(key, s, ch)
				}
				sc.tables[keyString] = d

				// Keep the key in tables. Remove it from delTables
				delete(delTables, keyString)
			}

		default:
			panic("bad descriptor")
		}
	}
	// Unregister all the tables left in delTables.
	for key := range delTables {
		if d, ok := sc.tables[key]; ok {
			if !d.isWorking {
				node.UnregisterNotifyTableLeader(roachpb.Key(key))
				delete(sc.tables, key)
			} else {
				d.isDeleted = true
				sc.tables[key] = d
			}
		}
	}
}

func (sc *SchemaChanger) maybeApplyMutation(s *stop.Stopper, table descriptorState, db *client.DB, done chan<- mutationDone, leaseMgr *LeaseManager) {
	if !table.isLeader || table.isWorking {
		return
	}
	if _, ok := sc.databases[table.descriptor.ParentID]; !ok {
		log.Warningf("No database entry for table %s", table.descriptor.Name)
		return
	}

	table.isWorking = true
	sc.tables[table.key] = table
	s.RunAsyncTask(func() {
		id, err := applyMutations(db, table.descriptor, sc.databases[table.descriptor.ParentID], leaseMgr)
		if err != nil {
			log.Info(err)
		}
		done <- mutationDone{key: table.key, mutationID: id}
	})
}

// applyMutations applies the next set of queued mutations with the same
// mutation id to the table. It returns the last mutation ID that it applies.
func applyMutations(db *client.DB, desc TableDescriptor, dbName string, leaseMgr *LeaseManager) (uint32, error) {
	tableName := &parser.QualifiedName{Base: parser.Name(desc.Name)}
	if err := tableName.NormalizeTableName(dbName); err != nil {
		return 0, err
	}
	tableKey := tableKey{desc.ParentID, desc.Name}
	mutationID := uint32(0)
	p := planner{user: security.RootUser, leaseMgr: leaseMgr}

	err := db.Txn(func(txn *client.Txn) error {
		p.setTxn(txn, time.Now())
		// Read the table descriptor from the database
		tableDesc := &TableDescriptor{}
		if err := p.getDescriptor(tableKey, tableDesc); err != nil {
			return err
		}
		if len(tableDesc.Mutations) == 0 {
			return nil
		}
		newTableDesc := proto.Clone(tableDesc).(*TableDescriptor)
		// Apply all mutations with the same ID.
		mutationID = newTableDesc.Mutations[0].ID
		applied := 0
		for _, mutation := range newTableDesc.Mutations {
			if mutationID != mutation.ID {
				break
			}
			applied++
			if err := newTableDesc.applyMutation(*mutation); err != nil {
				return err
			}
		}
		newTableDesc.Mutations = newTableDesc.Mutations[applied:]

		b := client.Batch{}
		if err := p.backfillBatch(&b, tableName, tableDesc, newTableDesc); err != nil {
			return err
		}
		// TODO(pmattis): This is a hack. Remove when schema change operations work
		// properly.
		p.hackNoteSchemaChange(newTableDesc)

		b.Put(MakeDescMetadataKey(newTableDesc.GetID()), wrapDescriptor(newTableDesc))
		p.txn.SetSystemDBTrigger()

		if err := p.txn.Run(&b); err != nil {
			return convertBatchError(newTableDesc, b, err)
		}

		if log.V(2) {
			log.Infof("Committing mutation: %d, desc: %v", mutationID, newTableDesc)
		}
		return nil
	})
	p.resetTxn()

	// Release leases even if above txn doesn't commisc.
	p.releaseLeases(*db)

	if err != nil {
		// Irrecoverable error; delete all future mutations.
		// TODO(vivek): Figure out a better strategy here.
		log.Infof("mutation: %d, desc: %v, failed to commit: ", mutationID, desc, err)
		if err := db.Txn(func(txn *client.Txn) error {
			p := planner{txn: txn, user: security.RootUser}
			// Read the table descriptor from the database
			tableDesc := &TableDescriptor{}
			if err := p.getDescriptor(tableKey, tableDesc); err != nil {
				return err
			}
			numMutations := len(tableDesc.Mutations)
			if numMutations == 0 {
				mutationID = 0
				return nil
			}
			mutationID = tableDesc.Mutations[numMutations-1].ID
			tableDesc.Mutations = nil
			p.txn.SetSystemDBTrigger()
			err := p.txn.Put(MakeDescMetadataKey(tableDesc.GetID()), wrapDescriptor(tableDesc))
			return err
		}); err != nil {
			log.Info(err)
			return 0, err
		}
	}
	return mutationID, nil
}
