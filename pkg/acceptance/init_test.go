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
	gosql "database/sql"
	"testing"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/acceptance/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/pkg/errors"
)

// useInitMode is an option for runTestOnConfigs.
func useInitMode(mode cluster.InitMode) func(*cluster.TestConfig) {
	return func(cfg *cluster.TestConfig) {
		cfg.InitMode = mode
	}
}

func TestInitModeBootstrapNodeZero(t *testing.T) {
	s := log.Scope(t)
	defer s.Close(t)

	runTestOnConfigs(t, testInitModeInner, useInitMode(cluster.INIT_BOOTSTRAP_NODE_ZERO))
}

func TestInitModeCommand(t *testing.T) {
	s := log.Scope(t)
	defer s.Close(t)

	runTestOnConfigs(t, testInitModeInner, useInitMode(cluster.INIT_COMMAND))
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

	runTestOnConfigs(t, testInitModeNoneInner, useInitMode(cluster.INIT_NONE))
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
	lc, ok := c.(*cluster.LocalCluster)
	if !ok {
		return
	}

	// TODO(bdarnell): initialize a node other than 0. This will provide
	// a different test from what happens in TestInitModeCommand.
	// Currently, it doesn't seem to work (maybe because node 0 is the
	// --join address?)
	lc.RunInitCommand(ctx, 0)

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
