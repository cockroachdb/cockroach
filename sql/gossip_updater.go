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
// Author: Vivek Menezes (vivek@cockroachlabs.com)

package sql

import (
	"bytes"
	"sync"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/config"
	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/stop"
)

// GossipUpdater process system config gossip updates for the SQL layer.
type GossipUpdater struct {
	// System Config and mutex.
	systemConfig   config.SystemConfig
	systemConfigMu sync.RWMutex
}

// updateSystemConfig is called whenever the system config gossip entry is updated.
func (g *GossipUpdater) updateSystemConfig(cfg config.SystemConfig) {
	g.systemConfigMu.Lock()
	defer g.systemConfigMu.Unlock()
	g.systemConfig = cfg
}

// getSystemConfig returns a pointer to the latest system config.
func (g *GossipUpdater) getSystemConfig() config.SystemConfig {
	g.systemConfigMu.RLock()
	defer g.systemConfigMu.RUnlock()
	return g.systemConfig
}

// Start starts a goroutine that refreshes the lease manager
// leases for tables received in the latest system configuration via gossip.
func (g *GossipUpdater) Start(s *stop.Stopper, db *client.DB, gossip *gossip.Gossip, leaseMgr *LeaseManager) {
	s.RunWorker(func() {
		descKeyPrefix := keys.MakeTablePrefix(uint32(DescriptorTable.ID))
		gossipUpdateC := gossip.RegisterSystemConfigChannel()
		for {
			select {
			case <-gossipUpdateC:
				cfg := *gossip.GetSystemConfig()
				g.updateSystemConfig(cfg)

				// Read all tables and their versions
				if log.V(2) {
					log.Info("received a new config %v", cfg)
				}

				// Loop through the configuration to find all the tables.
				for _, kv := range cfg.Values {
					if !bytes.HasPrefix(kv.Key, descKeyPrefix) {
						continue
					}
					// Attempt to unmarshal config into a table/database descriptor.
					var descriptor Descriptor
					if err := kv.Value.GetProto(&descriptor); err != nil {
						log.Warningf("%s: unable to unmarshal descriptor %v", kv.Key, kv.Value)
						continue
					}
					switch union := descriptor.Union.(type) {
					case *Descriptor_Table:
						table := union.Table
						if err := table.Validate(); err != nil {
							log.Errorf("%s: received invalid table descriptor: %v", kv.Key, table)
							continue
						}
						if log.V(2) {
							log.Infof("%s: refreshing lease table: %d, version: %d",
								kv.Key, table.ID, table.Version)
						}
						// Try to refresh the table lease to one >= this version.
						if err := leaseMgr.refreshLease(db, table.ID, table.Version); err != nil {
							log.Warningf("%s: %v", kv.Key, err)
						}

					case *Descriptor_Database:
						// Ignore.
					}
				}

			case <-s.ShouldStop():
				return
			}
		}
	})
}
