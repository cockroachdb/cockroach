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

package sql_test

import (
	gosql "database/sql"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/server/testingshim"
	"github.com/cockroachdb/cockroach/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/util/leaktest"
	"github.com/cockroachdb/cockroach/util/stop"
	"github.com/cockroachdb/cockroach/util/tracing"
)

// Starts up a cluster made of up `nodes` in-memory testing servers,
// creates database `name and returns open gosql.DB connections to each
// node (to the named db), as well as a cleanup func that stops and
// cleans up all nodes and connections.
// TODO(davidt): Change zone config to actually add replication.
// TODO(davidt): Ensure that ranges are actually replicated before returning.
// Until these TODOs are resolved, the cluster returned is not particularly
// useful for benchmarking, as without replication overhead it is the same as
// single-node operation, except without the local-call optimization for the
// additional nodes.
func SetupMultinodeTestCluster(
	t testing.TB, nodes int, name string,
) ([]*gosql.DB, *stop.Stopper) {
	if nodes < 1 {
		t.Fatal("invalid cluster size: ", nodes)
	}
	var servers []testingshim.TestServerInterface
	var conns []*gosql.DB
	first, conn, _ := sqlutils.SetupServer(t, testingshim.TestServerParams{
		UseDatabase: name,
	})
	servers = append(servers, first)
	conns = append(conns, conn)
	params := testingshim.TestServerParams{}
	params.JoinAddr = first.ServingAddr()
	params.UseDatabase = name
	params.Stopper = first.Stopper()
	for i := 1; i < nodes; i++ {
		s, conn, _ := sqlutils.SetupServer(t, params)
		servers = append(servers, s)
		conns = append(conns, conn)
	}

	if _, err := conns[0].Exec(fmt.Sprintf(`CREATE DATABASE %s`, name)); err != nil {
		t.Fatal(err)
	}

	return conns, first.Stopper()
}

// NB(davidt): until `SetupMultinodeTestCluster` actually returns a cluster
// with replication configured, this is only testing adding nodes to a cluster
// and then their ability to serve SQL by talking to a remote, single-node KV.
func TestMultinodeCockroach(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer tracing.Disable()()

	t.Skip("#7450")

	conns, stopper := SetupMultinodeTestCluster(t, 3, "Testing")
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
