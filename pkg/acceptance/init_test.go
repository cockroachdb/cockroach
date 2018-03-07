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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.

package acceptance

import (
	"context"
	gosql "database/sql"
	"io/ioutil"
	"net/http"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/acceptance/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// useInitMode is an option for runTestWithCluster.
func useInitMode(mode cluster.InitMode) func(*cluster.TestConfig) {
	return func(cfg *cluster.TestConfig) {
		cfg.InitMode = mode
	}
}

func TestInitModeBootstrapNodeZero(t *testing.T) {
	s := log.Scope(t)
	defer s.Close(t)

	// TODO(tschottdorf): give LocalCluster support for the init modes and we should be able
	// to switch this to RunLocal. Ditto below.
	RunDocker(t, func(t *testing.T) {
		runTestWithCluster(t, testInitModeInner, useInitMode(cluster.INIT_BOOTSTRAP_NODE_ZERO))
	})
}

func TestInitModeCommand(t *testing.T) {
	s := log.Scope(t)
	defer s.Close(t)

	// TODO(tschottdorf): see above.
	RunDocker(t, func(t *testing.T) {
		runTestWithCluster(t, testInitModeInner, useInitMode(cluster.INIT_COMMAND))
	})
}

func testInitModeInner(
	ctx context.Context, t *testing.T, c cluster.Cluster, cfg cluster.TestConfig,
) {
	// Verify that all nodes are initialized and able to serve SQL
	for i := 0; i < c.NumNodes(); i++ {
		db := makePGClient(t, c.PGUrl(ctx, i))
		defer db.Close()

		var val int
		if err := db.QueryRow("SELECT 1").Scan(&val); err != nil {
			t.Errorf("failed to scan: %s", err)
		}
	}
}

func TestInitModeNone(t *testing.T) {
	s := log.Scope(t)
	defer s.Close(t)

	// TODO(tschottdorf): see above.
	RunDocker(t, func(t *testing.T) {
		runTestWithCluster(t, testInitModeNoneInner, useInitMode(cluster.INIT_NONE))
	})
}

func testInitModeNoneInner(
	ctx context.Context, t *testing.T, c cluster.Cluster, cfg cluster.TestConfig,
) {
	// If we can't initialize the cluster via this interface, give up.
	// TODO(bdarnell): support RunInitCommand on the Cluster interface?
	lc, ok := c.(*cluster.DockerCluster)
	if !ok {
		return
	}

	var dbs []*gosql.DB
	defer func() {
		for _, db := range dbs {
			db.Close()
		}
	}()

	for i := 0; i < c.NumNodes(); i++ {
		dbs = append(dbs, makePGClient(t, c.PGUrl(ctx, i)))
	}

	// Give the server time to bind its ports.
	// TODO(bdarnell): Make dockercluster use the systemd readiness
	// notification instead of fire-and-forget for INIT_NONE
	time.Sleep(time.Second)

	// Initially, we can connect to any node, but queries issued will hang.
	errCh := make(chan error, len(dbs))
	for _, db := range dbs {
		db := db
		go func() {
			var val int
			errCh <- db.QueryRow("SELECT 1").Scan(&val)
		}()
	}

	// Give them time to get a "connection refused" or similar error if
	// the server isn't listening.
	time.Sleep(time.Second)
	select {
	case err := <-errCh:
		t.Fatalf("query finished prematurely with err %v", err)
	default:
	}

	// Check that the /health endpoint is functional even before cluster init,
	// whereas other debug endpoints return an appropriate error.
	httpTests := []struct {
		endpoint       string
		expectedStatus int
	}{
		{"/health", http.StatusOK},
		{"/health?ready=1", http.StatusServiceUnavailable},
		{"/_status/nodes", http.StatusNotFound},
	}
	for _, tc := range httpTests {
		resp, err := cluster.HTTPClient.Get(c.URL(ctx, 0) + tc.endpoint)
		if err != nil {
			t.Fatalf("unexpected error hitting %s endpoint: %v", tc.endpoint, err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != tc.expectedStatus {
			bodyBytes, _ := ioutil.ReadAll(resp.Body)
			t.Fatalf("unexpected response code %d (expected %d) hitting %s endpoint: %v",
				resp.StatusCode, tc.expectedStatus, tc.endpoint, string(bodyBytes))
		}
	}

	// TODO(bdarnell): initialize a node other than 0. This will provide
	// a different test from what happens in TestInitModeCommand.
	// Currently, it doesn't seem to work (maybe because node 0 is the
	// --join address?)
	lc.RunInitCommand(ctx, 0)

	// Make sure that running init again returns the expected error message and
	// does not break the cluster. We have to use ExecCLI rahter than OneShot in
	// order to actually get the output from the command.
	output, _, err := c.ExecCLI(ctx, 0, []string{"init"})
	if err == nil {
		t.Fatalf("expected error running init command on initialized cluster, got output: %s", output)
	}
	if !testutils.IsError(err, "cluster has already been initialized") {
		t.Fatalf("got unexpected error when running init command on initialized cluster: %v\noutput: %s",
			err, output)
	}

	// Once initialized, the queries we started earlier will finish.
	deadline := time.After(10 * time.Second)
	for i := 0; i < len(dbs); i++ {
		select {
		case err := <-errCh:
			if err != nil {
				t.Errorf("querying node %d: %s", i, err)
			}
		case <-deadline:
			t.Errorf("timed out waiting for query %d", err)
		}
	}

	// New queries will work too.
	for i, db := range dbs {
		var val int
		if err := db.QueryRow("SELECT 1").Scan(&val); err != nil {
			t.Errorf("querying node %d: %s", i, err)
		}
	}
}
