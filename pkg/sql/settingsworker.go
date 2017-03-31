// Copyright 2017 The Cockroach Authors.
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

package sql

import (
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// settingsRefresherMu serializes calls to fetchAndUpdate inside RefreshSettings
// -- this usually is true anyway, as there is only one RefreshSettings loop
// started by Server, so it is only interesting in testcluster.
var settingsRefresherMu syncutil.Mutex

// RefreshSettings starts a settings-changes listener.
func RefreshSettings(s *stop.Stopper, db *client.DB, leaseMgr *LeaseManager, gossip *gossip.Gossip) {
	fetchAndUpdate := func(ctx context.Context, db *client.DB, leaseMgr *LeaseManager) {
		const fetchQuery = "SELECT name, value, valueType FROM system.settings"
		ie := InternalExecutor{LeaseManager: leaseMgr}

		updater := settings.NewUpdater()

		if err := db.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
			updater.Reset()
			rows, err := ie.queryRowsInTransaction(ctx, "refresh-settings", txn, fetchQuery)
			if err != nil {
				return err
			}
			for _, row := range rows {
				k := string(parser.MustBeDString(row[0]))
				v := string(parser.MustBeDString(row[1]))
				t := "s"
				if row[2] != parser.DNull {
					t = string(parser.MustBeDString(row[2]))
				}
				if err := updater.Add(k, v, t); err != nil {
					// log and carry on -- we want to update other settings if possible.
					log.Warning(ctx, err)
				}
			}
			return nil
		}); err != nil {
			log.Warning(ctx, err)
		} else {
			updater.Apply()
		}
	}

	s.RunWorker(func() {
		ctx := context.Background()
		gossipUpdateC := gossip.RegisterSystemConfigChannel()
		for {
			select {
			case <-gossipUpdateC:
				settingsRefresherMu.Lock()
				fetchAndUpdate(ctx, db, leaseMgr)
				settingsRefresherMu.Unlock()
			case <-s.ShouldStop():
				return
			}
		}
	})
}
