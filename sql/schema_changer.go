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
}

// updateSystemConfig is called whenever the system config gossip entry is updated.
func (t *SchemaChanger) updateSystemConfig(cfg *config.SystemConfig) {
	t.systemConfigMu.Lock()
	defer t.systemConfigMu.Unlock()
	t.systemConfig = *cfg
	select {
	case t.newConfig <- struct{}{}:
	default:
	}
}

// getSystemConfig returns a pointer to the latest system config. May be nil,
// if the gossip callback has not run.
func (t *SchemaChanger) getSystemConfig() config.SystemConfig {
	t.systemConfigMu.RLock()
	defer t.systemConfigMu.RUnlock()
	return t.systemConfig
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
func (t *SchemaChanger) Start(s *stop.Stopper, gossip *gossip.Gossip, node TableLeaderInterface, db *client.DB, leaseMgr *LeaseManager) {
	s.RunWorker(func() {
		// Create channel that receives new system config notifications.
		// The channel has a size of 1 to prevent gossip from blocking on it.
		t.newConfig = make(chan struct{}, 1)
		gossip.RegisterSystemConfigCallback(t.updateSystemConfig)
		// Create channel that receives table leader notifications.
		ch := make(chan TableLeaderNotification)
		// Create channel that receives table mutation completion notifications.
		done := make(chan mutationDone)
		// All the table descriptors with outstanding mutations.
		tables := make(map[string]descriptorState)
		// The names of all the databases. We have to recreate this because some of
		// the sql operations used in running the mutations need full table names:
		// parser.QualifiedName.
		databases := make(map[ID]string)

		for {
			select {
			case <-t.newConfig:
				log.Info("received a new config")
				// Read config; attempt to create table leader to run
				// outstanding mutations.

				// Existing tables being followed.
				delTables := make(map[string]struct{})
				for k := range tables {
					delTables[k] = struct{}{}
				}

				cfg := t.getSystemConfig()
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
						databases[database.ID] = database.Name

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
							d, ok := tables[keyString]
							if ok {
								d.mutationID = lastMutationID
							} else {
								d = descriptorState{key: keyString, descriptor: *table, mutationID: lastMutationID}
								// Register a key for the first range of the table data for notifications.
								node.RegisterNotifyTableLeader(key, s, ch)
							}
							tables[keyString] = d

							// Keep the key in tables. Remove it from delTables
							delete(delTables, keyString)
						}

					default:
						panic("bad descriptor")
					}
				}
				// Unregister all the tables in delTables.
				for key := range delTables {
					if _, ok := tables[key]; ok {
						if !tables[key].isWorking {
							node.UnregisterNotifyTableLeader(roachpb.Key(key))
							delete(tables, key)
						} else {
							d := tables[key]
							d.isDeleted = true
							tables[key] = d
						}
					}
				}
				for k, v := range tables {
					// Possibly apply mutation.
					if v.isLeader && !v.isWorking {
						v.isWorking = true
						tables[string(k)] = v
						runApplyMutation(s, v, db, databases[v.descriptor.ParentID], done, leaseMgr)
					}
				}

			case n := <-ch:
				key := string(n.Key)
				log.Info("received a leader notification for %v", key)
				if d, ok := tables[key]; ok {
					if n.IsLeader && !d.isLeader {
						d.isLeader = true
						d.isWorking = true
						tables[key] = d
						// start schema change  goroutine.
						runApplyMutation(s, d, db, databases[d.descriptor.ParentID], done, leaseMgr)
					} else if !n.IsLeader && d.isLeader {
						// Notify table leader to stop.
						d.isLeader = false
						tables[key] = d
					}
				} else {
					log.Warningf("received leader notification for table not followed: %s", key)
				}

			case d := <-done:
				log.Infof("received work completion notification for: %s", d.key)
				if v, ok := tables[d.key]; ok {
					if !v.isWorking {
						panic("received work completion notification with no pending work")
					}
					v.isWorking = false
					tables[d.key] = v
					if v.isDeleted {
						node.UnregisterNotifyTableLeader(roachpb.Key(d.key))
						delete(tables, d.key)
					} else {
						// still more work to be done.
						if v.isLeader && v.mutationID > d.mutationID {
							v.isWorking = true
							tables[d.key] = v
							runApplyMutation(s, v, db, databases[v.descriptor.ParentID], done, leaseMgr)
						}
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

func runApplyMutation(s *stop.Stopper, table descriptorState, db *client.DB, dbName string, done chan<- mutationDone, leaseMgr *LeaseManager) {
	s.RunAsyncTask(func() {
		id, err := applyMutations(db, table.descriptor, dbName, leaseMgr)
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

	// Release leases even if above txn doesn't commit.
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
