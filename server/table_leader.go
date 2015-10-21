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
	"sync"

	"github.com/cockroachdb/cockroach/config"
	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/sql"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/stop"
	"github.com/gogo/protobuf/proto"
)

type tableLeaderCreator struct {
	// System Config and mutex.
	systemConfig   *config.SystemConfig
	systemConfigMu sync.RWMutex
	newConfig      chan bool
	node           *Node
}

// updateSystemConfig is called whenever the system config gossip entry is updated.
func (t *tableLeaderCreator) updateSystemConfig(cfg *config.SystemConfig) {
	t.systemConfigMu.Lock()
	defer t.systemConfigMu.Unlock()
	t.systemConfig = cfg
	t.newConfig <- true
}

// getSystemConfig returns a pointer to the latest system config. May be nil,
// if the gossip callback has not run.
func (t *tableLeaderCreator) getSystemConfig() *config.SystemConfig {
	t.systemConfigMu.RLock()
	defer t.systemConfigMu.RUnlock()
	return t.systemConfig
}

func newTableLeaderCreator(n *Node) *tableLeaderCreator {
	return &tableLeaderCreator{newConfig: make(chan bool), node: n}
}

func (t *tableLeaderCreator) start(s *stop.Stopper, gossip *gossip.Gossip) {
	s.RunWorker(func() {
		gossip.RegisterSystemConfigCallback(t.updateSystemConfig)
		ch := make(chan keyLeaderNotification, 1000)
		changers := make(map[string]chan struct{})
		tables := make(map[sql.ID]struct{})
		for {
			select {
			case <-t.newConfig:
				// Read config; create table leader for new tables.
				cfg := t.getSystemConfig()
				for _, kv := range cfg.Values {
					// Attempt to unmarshal config into a table descriptor.
					var descriptor sql.TableDescriptor
					if err := proto.Unmarshal(kv.Value.Bytes, &descriptor); err != nil {
						continue
					}
					if err := descriptor.Validate(); err != nil {
						continue
					}
					// Unmarshal successful; found a table descriptor
					if _, ok := tables[descriptor.ID]; ok {
						continue
					}
					// Register a key from the first range of the table data for notifications.
					t.node.registerNotifyKeyLeader(sql.MakeIndexKeyPrefix(descriptor.ID, descriptor.PrimaryIndex.ID), s, ch)
				}
				// We ought to also unregister deleted tables.

			case n := <-ch:
				// Read leader notify channel.
				key := n.key.String()

				if n.isLeader {
					if _, ok := changers[key]; ok {
						// start table leader goroutine.
						changers[key] = make(chan struct{})
						execTableLeader(s, changers[key], n.key)
					}
				} else {
					// Notify table leader to stop.
					if c, ok := changers[key]; ok {
						delete(changers, key)
						c <- struct{}{}
					}
				}

			case <-s.ShouldStop():
				return
			}
		}
	})
}

func execTableLeader(s *stop.Stopper, ch chan struct{}, key roachpb.Key) {
	log.Info("Starting table leader")
	s.RunWorker(func() {
		for {
			select {
			case <-ch:
				log.Info("quiting table leader")
				break

			case <-s.ShouldStop():
				return
			}
		}
	})
}

type keyLeaderNotification struct {
	key      roachpb.Key
	isLeader bool
}
