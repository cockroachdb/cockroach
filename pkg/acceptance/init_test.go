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
	"testing"

	"github.com/cockroachdb/cockroach/pkg/acceptance/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/pkg/errors"
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
	var dbs []*gosql.DB
	defer func() {
		for _, db := range dbs {
			db.Close()
		}
	}()

	for i := 0; i < c.NumNodes(); i++ {
		dbs = append(dbs, makePGClient(t, c.PGUrl(ctx, i)))
	}

	// Initially, we can't connect to any node.
	for _, db := range dbs {
		var val int
		if err := db.QueryRow("SELECT 1").Scan(&val); err == nil {
			// TODO(bdarnell): more precise error assertions
			t.Errorf("did not get expected error")
		}
	}

	// If we can, initialize the cluster and proceed.
	// TODO(bdarnell): support RunInitCommand on the Cluster interface?
	lc, ok := c.(*cluster.DockerCluster)
	if !ok {
		return
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

	// Once initialized, we can query each node.
	testutils.SucceedsSoon(t, func() error {
		for i, db := range dbs {
			var val int
			if err := db.QueryRow("SELECT 1").Scan(&val); err != nil {
				return errors.Wrapf(err, "querying node %d", i)
			}
		}
		return nil
	})
}
