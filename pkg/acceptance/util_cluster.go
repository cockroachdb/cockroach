// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package acceptance

import (
	"context"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/acceptance/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/errors"
)

const (
	dockerTest = "runMode=docker"
)

var stopper = stop.NewStopper()

// RunDocker runs the given acceptance test using a Docker cluster.
func RunDocker(t *testing.T, testee func(t *testing.T)) {
	t.Run(dockerTest, testee)
}

// turns someTest#123 into someTest when invoked with ReplicaAllLiteralString.
// This is useful because the go test harness automatically disambiguates
// subtests in that way when they are invoked multiple times with the same name,
// and we sometimes call RunDocker multiple times in tests.
var reStripTestEnumeration = regexp.MustCompile(`#\d+$`)

// StartCluster starts a cluster from the relevant flags. All test clusters
// should be created through this command since it sets up the logging in a
// unified way.
func StartCluster(ctx context.Context, t *testing.T, cfg cluster.TestConfig) (c cluster.Cluster) {
	var completed bool
	defer func() {
		if !completed && c != nil {
			c.AssertAndStop(ctx, t)
		}
	}()

	parts := strings.Split(t.Name(), "/")
	if len(parts) < 2 {
		t.Fatal("must invoke RunDocker")
	}

	var runMode string
	for _, part := range parts[1:] {
		part = reStripTestEnumeration.ReplaceAllLiteralString(part, "")
		switch part {
		case dockerTest:
			if runMode != "" {
				t.Fatalf("test has more than one run mode: %s and %s", runMode, part)
			}
			runMode = part
		}
	}

	switch runMode {
	case dockerTest:
		logDir := *flagLogDir
		if logDir != "" {
			logDir = filepath.Join(logDir, filepath.Clean(t.Name()))
		}
		l := cluster.CreateDocker(ctx, cfg, logDir, stopper)
		l.Start(ctx)
		c = l

	default:
		t.Fatalf("unable to run in mode %q, use RunDocker", runMode)
	}

	// Don't wait for replication unless requested (usually it is).
	if !cfg.NoWait && cfg.InitMode != cluster.INIT_NONE {
		wantedReplicas := 3
		if numNodes := c.NumNodes(); numNodes < wantedReplicas {
			wantedReplicas = numNodes
		}

		// Looks silly, but we actually start zero-node clusters in the
		// reference tests.
		if wantedReplicas > 0 {
			log.Infof(ctx, "waiting for first range to have %d replicas", wantedReplicas)

			testutils.SucceedsSoon(t, func() error {
				select {
				case <-stopper.ShouldQuiesce():
					t.Fatal("interrupted")
				case <-time.After(time.Second):
				}

				// Always talk to node 0 because it's guaranteed to exist.
				db, err := c.NewDB(ctx, 0)
				if err != nil {
					t.Fatal(err)
				}
				rows, err := db.Query(`SELECT array_length(replicas, 1) FROM crdb_internal.ranges LIMIT 1`)
				if err != nil {
					// Versions <= 1.1 do not contain the crdb_internal table, which is what's used
					// to determine whether a cluster has up-replicated. This is relevant for the
					// version upgrade acceptance test. Just skip the replication check for this case.
					if testutils.IsError(err, "(table|relation) \"crdb_internal.ranges\" does not exist") {
						return nil
					}
					t.Fatal(err)
				}
				defer rows.Close()
				var foundReplicas int
				if rows.Next() {
					if err = rows.Scan(&foundReplicas); err != nil {
						t.Fatalf("unable to scan for length of replicas array: %s", err)
					}
					if log.V(1) {
						log.Infof(ctx, "found %d replicas", foundReplicas)
					}
				} else {
					return errors.Errorf("no ranges listed")
				}

				if foundReplicas < wantedReplicas {
					return errors.Errorf("expected %d replicas, only found %d", wantedReplicas, foundReplicas)
				}
				return nil
			})
		}

		// Ensure that all nodes are serving SQL by making sure a simple
		// read-only query succeeds.
		for i := 0; i < c.NumNodes(); i++ {
			testutils.SucceedsSoon(t, func() error {
				db, err := c.NewDB(ctx, i)
				if err != nil {
					return err
				}
				if _, err := db.Exec("SHOW DATABASES"); err != nil {
					return err
				}
				return nil
			})
		}
	}

	completed = true
	return c
}
