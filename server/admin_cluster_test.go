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
// Author: Matt Tracy (matt@cockroachlabs.com)

package server_test

import (
	"testing"
	"time"

	"github.com/pkg/errors"

	"github.com/cockroachdb/cockroach/base"
	"github.com/cockroachdb/cockroach/server/serverpb"
	"github.com/cockroachdb/cockroach/testutils/testcluster"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/leaktest"
)

func TestAdminAPITableStats(t *testing.T) {
	defer leaktest.AfterTest(t)()
	const nodeCount = 3
	tc := testcluster.StartTestCluster(t, nodeCount, base.TestClusterArgs{
		ReplicationMode: base.ReplicationAuto,
		ServerArgs: base.TestServerArgs{
			ScanInterval:    time.Millisecond,
			ScanMaxIdleTime: time.Millisecond,
		},
	})
	defer tc.Stopper().Stop()
	if err := tc.WaitForFullReplication(); err != nil {
		t.Fatal(err)
	}
	server0 := tc.Server(0)

	// Create clients (SQL, HTTP) connected to server 0.
	db := tc.ServerConn(0)

	client, err := server0.GetHTTPClient()
	if err != nil {
		t.Fatal(err)
	}

	client.Timeout = base.NetworkTimeout * 3

	// Make a single table and insert some data. The database and test have
	// names which require escaping, in order to verify that database and
	// table names are being handled correctly.
	if _, err := db.Exec(`CREATE DATABASE "test test"`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`
		CREATE TABLE "test test"."foo foo" (
			id INT PRIMARY KEY,
			val STRING
		)`,
	); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		if _, err := db.Exec(`
			INSERT INTO "test test"."foo foo" VALUES(
				$1, $2
			)`, i, "test",
		); err != nil {
			t.Fatal(err)
		}
	}

	url := server0.AdminURL() + "/_admin/v1/databases/test test/tables/foo foo/stats"
	var tsResponse serverpb.TableStatsResponse

	// The new SQL table may not yet have split into its own range. Wait for
	// this to occur, and for full replication.
	util.SucceedsSoon(t, func() error {
		if err := util.GetJSON(client, url, &tsResponse); err != nil {
			return err
		}
		if tsResponse.RangeCount != 1 {
			return errors.Errorf("Table range not yet separated.")
		}
		if tsResponse.NodeCount != nodeCount {
			return errors.Errorf("Table range not yet replicated to %d nodes.", 3)
		}
		if a, e := tsResponse.ReplicaCount, int64(nodeCount); a != e {
			return errors.Errorf("expected %d replicas, found %d", e, a)
		}
		return nil
	})

	// These two conditions *must* be true, given that the above
	// SucceedsSoon has succeeded.
	if a, e := tsResponse.Stats.KeyCount, int64(20); a < e {
		t.Fatalf("expected at least 20 total keys, found %d", a)
	}
	if len(tsResponse.MissingNodes) > 0 {
		t.Fatalf("expected no missing nodes, found %v", tsResponse.MissingNodes)
	}

	// Kill a node, ensure it shows up in MissingNodes and that ReplicaCount is
	// lower.
	tc.StopServer(1)

	if err := util.GetJSON(client, url, &tsResponse); err != nil {
		t.Fatal(err)
	}
	if a, e := tsResponse.NodeCount, int64(nodeCount); a != e {
		t.Errorf("expected %d nodes, found %d", e, a)
	}
	if a, e := tsResponse.RangeCount, int64(1); a != e {
		t.Errorf("expected %d ranges, found %d", e, a)
	}
	if a, e := tsResponse.ReplicaCount, int64((nodeCount/2)+1); a != e {
		t.Errorf("expected %d replicas, found %d", e, a)
	}
	if a, e := tsResponse.Stats.KeyCount, int64(10); a < e {
		t.Errorf("expected at least 10 total keys, found %d", a)
	}
	if len(tsResponse.MissingNodes) != 1 {
		t.Errorf("expected one missing node, found %v", tsResponse.MissingNodes)
	}

	// Call TableStats with a very low timeout. This tests that fan-out queries
	// do not leak goroutines if the calling context is abandoned.
	// Interestingly, the call can actually sometimes succeed, despite the small
	// timeout; however, in aggregate (or in stress tests) this will suffice for
	// detecting leaks.
	client.Timeout = 1 * time.Nanosecond
	_ = util.GetJSON(client, url, &tsResponse)
}
