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

package server

import (
	"time"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/config"
	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/sql"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/stop"
	"github.com/gogo/protobuf/proto"
)

type tableLeaderCreator struct {
	newConfig chan config.SystemConfig
	node      *Node
	db        *client.DB
}

// updateSystemConfig is called whenever the system config gossip entry is updated.
func (t *tableLeaderCreator) updateSystemConfig(cfg *config.SystemConfig) {
	t.newConfig <- *cfg
}

type descriptorState struct {
	descriptor sql.TableDescriptor
	// This node is the leader for this table.
	isLeader bool
	// A schema change routine is running.
	isWorking bool
	// There are no outstanding mutations on this table.
	isDeleted bool
}

// Completed work on a table.
type mutationDone struct {
	key roachpb.Key
}

func (t tableLeaderCreator) start(s *stop.Stopper, gossip *gossip.Gossip) {
	s.RunWorker(func() {
		// Create channel that receives new system configs.
		t.newConfig = make(chan config.SystemConfig)
		gossip.RegisterSystemConfigCallback(t.updateSystemConfig)
		// Create channel that receives key leader notifications.
		ch := make(chan keyLeaderNotification)
		// Create channel that receives table mutation completion notifications.
		done := make(chan mutationDone)
		// All the table descriptors with outstanding mutations.
		tables := make(map[string]descriptorState)
		// The names of all the databases. We have to recreate this because some of
		// the sql operations used in running the mutations need fully table
		// parser.QualifiedName.
		databases := make(map[sql.ID]string)
		// Wake up every once in a while to run outstanding table mutations.
		ticker := time.NewTicker(50 * time.Millisecond)

		for {
			select {
			case cfg := <-t.newConfig:
				log.Info("received a new config")
				// Read config; attempt to create table leader to run
				// outstanding mutations.

				// Existing tables being followed.
				delTables := make(map[string]struct{})
				for k := range tables {
					delTables[k] = struct{}{}
				}

				for _, kv := range cfg.Values {
					// Attempt to unmarshal config into a table descriptor.
					var descriptor sql.TableDescriptor
					if err := proto.Unmarshal(kv.Value.RawBytes, &descriptor); err != nil {
						// Attempt to unmarshal config into a database descriptor.
						var dbDescriptor sql.DatabaseDescriptor
						if err := proto.Unmarshal(kv.Value.RawBytes, &dbDescriptor); err == nil {
							databases[dbDescriptor.ID] = dbDescriptor.Name
						}
						continue
					}
					if err := descriptor.Validate(); err != nil {
						continue
					}
					// Unmarshal successful; found a table descriptor.
					if len(descriptor.Mutations) > 0 {
						key := sql.MakeIndexKeyPrefix(descriptor.ID, descriptor.PrimaryIndex.ID)
						keyString := string(key)
						if _, ok := tables[keyString]; !ok {
							tables[keyString] = descriptorState{descriptor: descriptor}
							// Register a key for the first range of the table data for notifications.
							t.node.registerNotifyKeyLeader(key, s, ch)
						}
						// Keep the key in tables. Remove it from delTables
						delete(delTables, keyString)
					}
				}
				// Unregister all the tables in delTables.
				for key := range delTables {
					if _, ok := tables[key]; ok {
						if !tables[key].isWorking {
							t.node.unregisterNotifyKeyLeader(roachpb.Key(key))
							delete(tables, key)
						} else {
							d := tables[key]
							d.isDeleted = true
							tables[key] = d
						}
					}
				}

			case n := <-ch:
				log.Info("received a leader notification")
				keyString := string(n.key)
				if d, ok := tables[keyString]; ok {
					if n.isLeader && !d.isLeader {
						d.isLeader = true
						d.isWorking = true
						tables[keyString] = d
						// start schema change  goroutine.
						go func() {
							descriptor := tables[keyString].descriptor
							err := sql.RunOneMutation(t.db, descriptor, databases[descriptor.ParentID])
							if err != nil {
								log.Info(err)
							}
							done <- mutationDone{key: n.key}
						}()
					} else if !n.isLeader && d.isLeader {
						// Notify table leader to stop.
						d.isLeader = false
						tables[keyString] = d
					}
				} else {
					log.Info("received leader notification for table not followed")
				}

			case <-ticker.C:
				for k, v := range tables {
					if v.isLeader && !v.isWorking {
						keyString := string(k)
						if !v.isWorking {
							v.isWorking = true
							tables[keyString] = v
							go func() {
								err := sql.RunOneMutation(t.db, v.descriptor, databases[v.descriptor.ParentID])
								if err != nil {
									log.Info(err)
								}
								done <- mutationDone{key: roachpb.Key(k)}
							}()
						}
					}
				}

			case d := <-done:
				keyString := string(d.key)
				if v, ok := tables[keyString]; ok {
					if !v.isWorking {
						panic("received work completion notification with no pending work")
					}
					v.isWorking = false
					// continue to work when the clock ticks next.
					tables[keyString] = v
					if v.isDeleted {
						t.node.unregisterNotifyKeyLeader(roachpb.Key(keyString))
						delete(tables, keyString)
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
