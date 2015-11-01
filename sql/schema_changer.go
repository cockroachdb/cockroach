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
	"fmt"
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
	schemaChanges map[string]schemaChange
	// The names of all the databases, to retrieve a database name from a
	// database ID in a table descriptor. We have to recreate this
	// because some of the sql operations used in running the mutations
	// need full table names: db.table
	dbNames map[ID]string
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

type schemaChange struct {
	descriptor TableDescriptor
	// This node is the leader for this table.
	isLeader bool
	// A schema change routine is running.
	isWorking bool
	// There are no outstanding mutations on this table.
	isDeleted bool
}

// Completed work on a table.
type mutationDone struct {
	// The table's primary key prefix.
	key string
	// last mutation completed.
	mutationID MutationID
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
		tlNotify := make(chan TableLeaderNotification)
		// Create channel that receives table mutation completion notifications.
		done := make(chan mutationDone)
		sc.schemaChanges = make(map[string]schemaChange)
		sc.dbNames = make(map[ID]string)

		for {
			select {
			case <-sc.newConfig:
				sc.processNewConfig(s, tlNotify, node)
				for _, d := range sc.schemaChanges {
					sc.maybeApplyMutation(s, d, db, done, leaseMgr)
				}

			case n := <-tlNotify:
				key := string(n.Key)
				if log.V(2) {
					log.Info("received a leader notification for %s", prettyKey(n.Key, 0))
				}
				if d, ok := sc.schemaChanges[key]; ok {
					if n.IsLeader != d.isLeader {
						// Leadership has changed.
						d.isLeader = n.IsLeader
						sc.schemaChanges[key] = d
						// Apply mutations if leader.
						sc.maybeApplyMutation(s, d, db, done, leaseMgr)
					}
				} else {
					log.Warningf("received leader notification for table not followed: %s", prettyKey(n.Key, 0))
				}

			case n := <-done:
				key := prettyKey(roachpb.Key(n.key), 0)
				if log.V(2) {
					log.Infof("received work completion notification for: %s", key)
				}
				if d, ok := sc.schemaChanges[n.key]; ok {
					if d.isDeleted {
						node.UnregisterNotifyTableLeader(roachpb.Key(n.key))
						delete(sc.schemaChanges, n.key)
						break
					}
					if !d.isWorking {
						panic(fmt.Sprintf("received work completion notification with no pending work: %s, %v", key, d))
					}
					d.isWorking = false
					sc.schemaChanges[n.key] = d
					// still more work to be done.
					id := d.descriptor.Mutations[len(d.descriptor.Mutations)-1].ID
					if n.mutationID != 0 && id > n.mutationID {
						sc.maybeApplyMutation(s, d, db, done, leaseMgr)
					}
				} else {
					panic(fmt.Sprintf("received work completion notification for table not followed: %s", key))
				}

			case <-s.ShouldStop():
				return
			}
		}
	})
}

// Populate schemaChanges with new config.
func (sc *SchemaChanger) processNewConfig(s *stop.Stopper, tlNotify chan TableLeaderNotification, node TableLeaderInterface) {
	// Copy the existing schema changes into oldSchemaChanges, so
	// that old schema changes not included in the new config can
	// be safely deleted.
	oldSchemaChanges := make(map[string]struct{}, len(sc.schemaChanges))
	for k := range sc.schemaChanges {
		oldSchemaChanges[k] = struct{}{}
	}

	cfg := sc.getSystemConfig()
	if log.V(2) {
		log.Info("received a new config %v", cfg)
	}

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
			sc.dbNames[database.ID] = database.Name

		case *Descriptor_Table:
			table := union.Table
			if err := table.Validate(); err != nil {
				log.Errorf("received invalid table descriptor: %v", table)
				continue
			}
			if len(table.Mutations) > 0 {
				// The table descriptor has an outstanding mutation; follow
				// the schema change and apply it if the local node is a table
				// leader.
				key := MakeIndexKeyPrefix(table.ID, table.PrimaryIndex.ID)
				keyString := string(key)
				d, ok := sc.schemaChanges[keyString]
				if ok {
					d.descriptor = *table
				} else {
					d = schemaChange{descriptor: *table}
					// Register the schema change for table leader notifications. The schema change
					// can only be applied on a node who is the table leader for the table.
					node.RegisterNotifyTableLeader(key, s, tlNotify)
				}
				sc.schemaChanges[keyString] = d

				// Remove the schema change from oldSchemaChanges so that it
				// doesn't eventually get deleted from SchemaChanges below.
				delete(oldSchemaChanges, keyString)
			}

		default:
			panic(fmt.Sprintf("bad descriptor: %v, %v", union, descriptor.Union))
		}
	}
	// Delete all the schemaChanges left in oldSchemaChanges.
	for key := range oldSchemaChanges {
		if d, ok := sc.schemaChanges[key]; ok {
			if !d.isWorking {
				node.UnregisterNotifyTableLeader(roachpb.Key(key))
				delete(sc.schemaChanges, key)
			} else {
				d.isDeleted = true
				sc.schemaChanges[key] = d
			}
		}
	}
}

// maybeApplyMutation applied the schema change
func (sc *SchemaChanger) maybeApplyMutation(s *stop.Stopper, table schemaChange, db *client.DB, done chan<- mutationDone, leaseMgr *LeaseManager) {
	if !table.isLeader || table.isWorking {
		return
	}
	if _, ok := sc.dbNames[table.descriptor.ParentID]; !ok {
		log.Warningf("No database entry for table %s", table.descriptor.Name)
		return
	}

	table.isWorking = true
	key := MakeIndexKeyPrefix(table.descriptor.ID, table.descriptor.PrimaryIndex.ID)
	keyString := string(key)

	sc.schemaChanges[keyString] = table
	s.RunAsyncTask(func() {
		id, err := applyMutations(db, table.descriptor, sc.dbNames[table.descriptor.ParentID], sc.getSystemConfig(), leaseMgr)
		if err != nil {
			log.Info(err)
		}
		done <- mutationDone{key: keyString, mutationID: id}
	})
}

// applyMutations applies the next set of queued mutations with the same
// mutation id to the table. It returns the last mutation ID that it applies.
func applyMutations(db *client.DB, desc TableDescriptor, dbName string, cfg config.SystemConfig, leaseMgr *LeaseManager) (MutationID, error) {
	tableName := &parser.QualifiedName{Base: parser.Name(desc.Name)}
	if err := tableName.NormalizeTableName(dbName); err != nil {
		return 0, err
	}
	tableKey := tableKey{desc.ParentID, desc.Name}
	mutationID := MutationID(0)
	p := planner{user: security.RootUser, systemConfig: &cfg, leaseMgr: leaseMgr}

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
