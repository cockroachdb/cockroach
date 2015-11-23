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
	"errors"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/storage"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/stop"
)

const checkTableLeadersInterval = 50 * time.Millisecond

// tableLeaderNotification is used to return notifications for
// registered tables.
type tableLeaderNotification struct {
	tableID ID
	// Is the local node the raft leader for the range containing
	// the primary key prefix.
	isLeader bool
}

type tableLeader struct {
	tableID        ID
	primaryIndexID IndexID
	isLeader       bool
	report         chan<- tableLeaderNotification
}

type tableLeaders struct {
	stores *storage.Stores
	s      *stop.Stopper
	mu     sync.Mutex
	// mu protects tableLeaders.
	tables map[ID]tableLeader
}

func newTableLeaders(stores *storage.Stores, s *stop.Stopper) *tableLeaders {
	return &tableLeaders{stores: stores, s: s, tables: make(map[ID]tableLeader)}
}

// registers a table for table leader notifications.
// A node is a table leader if the node is a replica leader for
// the range containing the primary key prefix for the table.
func (t *tableLeaders) register(tableID ID, primaryIndexID IndexID, report chan<- tableLeaderNotification) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if _, ok := t.tables[tableID]; ok {
		return util.Errorf("key already registered: %v", tableID)
	}
	tl := tableLeader{tableID: tableID, primaryIndexID: primaryIndexID, isLeader: false, report: report}
	if ok := t.isTableLeader(tl); ok {
		tl.isLeader = true
		notifyTableLeader(t.s, tl)
	}
	t.tables[tableID] = tl
	// Start periodic table leader checks for all registered keys.
	if len(t.tables) == 1 {
		t.periodicallyCheckTableLeaders()
	}
	return nil
}

// unregisterNotify drops a prior registration.
func (t *tableLeaders) unregister(tableID ID) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if _, ok := t.tables[tableID]; !ok {
		return util.Errorf("unregistering a key that is not being followed: %v", tableID)
	}
	delete(t.tables, tableID)
	return nil
}

// periodicallyCheckTableLeaders loops on a periodic ticker to attempt to
// select the local node as a table leader. It exits as soon as there
// are no more registered keys. It's safe for more than
// one of these routines to run concurrently.
func (t *tableLeaders) periodicallyCheckTableLeaders() {
	t.s.RunWorker(func() {
		ticker := time.NewTicker(checkTableLeadersInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				t.checkTableLeaders()
				// Check if there are any registered tables.
				if t.done() {
					return
				}
			case <-t.s.ShouldStop():
				return
			}
		}
	})
}

// checkTableLeaders checks to see if any of the tables have
// changed node leadership, and sends notifications for all leader
// changes.
func (t *tableLeaders) checkTableLeaders() {
	t.mu.Lock()
	defer t.mu.Unlock()
	for id, tl := range t.tables {
		isLeader := t.isTableLeader(tl)
		if tl.isLeader != isLeader {
			tl.isLeader = isLeader
			t.tables[id] = tl
			notifyTableLeader(t.s, tl)
		}
	}
}

func (t *tableLeaders) done() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.tables) == 0
}

var errFound = errors.New("found replica")

// testIsReplicaLeader can be set in tests.
var testIsReplicaLeader bool

// isTableLeader returns true when the local node is a leader for this table.
func (t *tableLeaders) isTableLeader(tl tableLeader) bool {
	var isLeader bool
	key := MakeIndexKeyPrefix(tl.tableID, tl.primaryIndexID)
	if err := t.stores.VisitStores(func(s *storage.Store) error {
		if rng := s.LookupReplica(keys.Addr(key), nil); rng != nil {
			if rng.IsLeader() || testIsReplicaLeader {
				isLeader = true
			}
			return errFound
		}
		return nil
	}); err != nil && err != errFound {
		panic(err)
	}
	return isLeader
}

func notifyTableLeader(s *stop.Stopper, tl tableLeader) {
	s.RunAsyncTask(func() {
		if log.V(2) {
			log.Infof("about to send leader notification: %v", tl)
		}
		tl.report <- tableLeaderNotification{tableID: tl.tableID, isLeader: tl.isLeader}
	})
}
