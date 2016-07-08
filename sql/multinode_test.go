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
// Author: David Taylor (david@cockroachlabs.com)
// Author: William Haack (will@cockroachlabs.com)

package sql_test

import (
	gosql "database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/base"
	"github.com/cockroachdb/cockroach/config"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/server"
	"github.com/cockroachdb/cockroach/storage"
	"github.com/cockroachdb/cockroach/testutils/serverutils"
	"github.com/cockroachdb/cockroach/util/leaktest"
	"github.com/cockroachdb/cockroach/util/stop"
	"github.com/cockroachdb/cockroach/util/tracing"
)

// Starts up a cluster made of up `nodes` in-memory testing servers,
// creates database `name and returns open gosql.DB connections to each
// node (to the named db), as well as a cleanup func that stops and
// cleans up all nodes and connections.
func SetupMultinodeTestCluster(
	t testing.TB, nodes int, name string,
) (MultinodeTestCluster, []*gosql.DB, *stop.Stopper) {
	if nodes < 1 {
		t.Fatal("invalid cluster size: ", nodes)
	}
	stopper := stop.NewStopper()

	// Force all ranges to be replicated everywhere. This is needed until #7297 is
	// fixed, otherwise starting a cluster takes forever.
	cfg := config.DefaultZoneConfig()
	cfg.ReplicaAttrs = make([]roachpb.Attributes, nodes)
	fn := config.TestingSetDefaultZoneConfig(cfg)
	stopper.AddCloser(stop.CloserFn(fn))

	var servers []serverutils.TestServerInterface
	var conns []*gosql.DB
	args := base.TestServerArgs{
		Stopper:       stopper,
		PartOfCluster: true,
		UseDatabase:   name,
	}
	first, conn, _ := serverutils.StartServer(t, args)
	servers = append(servers, first)
	conns = append(conns, conn)
	args.JoinAddr = first.ServingAddr()
	for i := 1; i < nodes; i++ {
		s, conn, _ := serverutils.StartServer(t, args)
		servers = append(servers, s)
		conns = append(conns, conn)
	}

	if _, err := conns[0].Exec(fmt.Sprintf(`CREATE DATABASE %s`, name)); err != nil {
		t.Fatal(err)
	}

	testCluster := MultinodeTestCluster{Servers: servers}
	return testCluster, conns, first.Stopper()
}

func TestMultinodeCockroach(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer tracing.Disable()()

	testCluster, conns, stopper := SetupMultinodeTestCluster(t, 3, "Testing")
	if err := testCluster.WaitForFullReplication(); err != nil {
		t.Fatal(err)
	}
	defer stopper.Stop()

	if _, err := conns[0].Exec(`CREATE TABLE testing (k INT PRIMARY KEY, v INT)`); err != nil {
		t.Fatal(err)
	}

	if _, err := conns[0].Exec(`INSERT INTO testing VALUES (5, 1), (4, 2), (1, 2)`); err != nil {
		t.Fatal(err)
	}

	if r, err := conns[1].Query(`SELECT * FROM testing WHERE k = 5`); err != nil {
		t.Fatal(err)
	} else if !r.Next() {
		t.Fatal("no rows")
	}

	if r, err := conns[2].Exec(`DELETE FROM testing`); err != nil {
		t.Fatal(err)
	} else if rows, err := r.RowsAffected(); err != nil {
		t.Fatal(err)
	} else if expected, actual := int64(3), rows; expected != actual {
		t.Fatalf("wrong row count deleted: expected %d actual %d", expected, actual)
	}
}

// A MultinodeTestCluster is an array of in-memory nodes connected to each other.
type MultinodeTestCluster struct {
	Servers []serverutils.TestServerInterface
}

// WaitForFullReplication waits until all of the nodes in the cluster have the
// same number of replicas.
func (tc *MultinodeTestCluster) WaitForFullReplication() error {
	// TODO (WillHaack): Optimize sleep time.
	for notReplicated := true; notReplicated; time.Sleep(100 * time.Millisecond) {
		notReplicated = false
		var numReplicas int
		err := tc.Servers[0].(*server.TestServer).Stores().VisitStores(func(s *storage.Store) error {
			numReplicas = s.ReplicaCount()
			return nil
		})
		if err != nil {
			return err
		}
		for _, s := range tc.Servers {
			err := s.(*server.TestServer).Stores().VisitStores(func(s *storage.Store) error {
				if numReplicas != s.ReplicaCount() {
					notReplicated = true
				}
				return nil
			})
			if err != nil {
				return err
			}
			if notReplicated {
				break
			}
		}
	}
	return nil
}
