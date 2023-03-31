// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package clusterupgrade

import (
	"context"
	gosql "database/sql"
	"fmt"
	"math/rand"
	"path/filepath"
	"strconv"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/version"
)

const (
	// MainVersion is the sentinel used to represent that the binary
	// passed to roachtest should be uploaded when `version` is left
	// unspecified.
	MainVersion = ""
)

// BinaryVersion returns the binary version running on the node
// associated with the given database connection.
// NB: version means major.minor[-internal]; the patch level isn't
// returned. For example, a binary of version 19.2.4 will return 19.2.
func BinaryVersion(db *gosql.DB) (roachpb.Version, error) {
	zero := roachpb.Version{}
	var sv string
	if err := db.QueryRow(`SELECT crdb_internal.node_executable_version();`).Scan(&sv); err != nil {
		return zero, err
	}

	if len(sv) == 0 {
		return zero, fmt.Errorf("empty version")
	}

	return roachpb.ParseVersion(sv)
}

// ClusterVersion returns the cluster version active on the node
// associated with the given database connection. Note that the
// returned value might become stale due to the cluster auto-upgrading
// in the background plus gossip asynchronicity.
// NB: cluster versions are always major.minor[-internal]; there isn't
// a patch level.
func ClusterVersion(ctx context.Context, db *gosql.DB) (roachpb.Version, error) {
	zero := roachpb.Version{}
	var sv string
	if err := db.QueryRowContext(ctx, `SHOW CLUSTER SETTING version`).Scan(&sv); err != nil {
		return zero, err
	}

	return roachpb.ParseVersion(sv)
}

// UploadVersion uploads the specified crdb version to the given
// nodes. It returns the path of the uploaded binaries on the nodes,
// suitable to be used with `roachdprod start --binary=<path>`.
func UploadVersion(
	ctx context.Context,
	t test.Test,
	l *logger.Logger,
	c cluster.Cluster,
	nodes option.NodeListOption,
	newVersion string,
) (string, error) {
	binaryName := "./cockroach"
	if newVersion == MainVersion {
		if err := c.PutE(ctx, l, t.Cockroach(), binaryName, nodes); err != nil {
			return "", err
		}
	} else if binary, ok := t.VersionsBinaryOverride()[newVersion]; ok {
		// If an override has been specified for newVersion, use that binary.
		l.Printf("using binary override for version %s: %s", newVersion, binary)
		binaryName = "./cockroach-" + newVersion
		if err := c.PutE(ctx, l, binary, binaryName, nodes); err != nil {
			return "", err
		}
	} else {
		v := "v" + newVersion
		dir := v
		binaryName = filepath.Join(dir, "cockroach")
		// Check if the cockroach binary already exists.
		if err := c.RunE(ctx, nodes, "test", "-e", binaryName); err != nil {
			if err := c.RunE(ctx, nodes, "mkdir", "-p", dir); err != nil {
				return "", err
			}
			if err := c.Stage(ctx, l, "release", v, dir, nodes); err != nil {
				return "", err
			}
		}
	}
	return BinaryPathFromVersion(newVersion), nil
}

// InstallFixtures copies the previously created fixtures (in
// pkg/cmd/roachtest/fixtures) for the given version to the nodes
// passed. After this step, the corresponding binary can be started on
// the cluster and it will use that store directory.
func InstallFixtures(
	ctx context.Context, l *logger.Logger, c cluster.Cluster, nodes option.NodeListOption, v string,
) error {
	c.Run(ctx, nodes, "mkdir -p {store-dir}")
	vv := version.MustParse("v" + v)
	// The fixtures use cluster version (major.minor) but the input might be
	// a patch release.
	name := CheckpointName(
		roachpb.Version{Major: int32(vv.Major()), Minor: int32(vv.Minor())}.String(),
	)
	for _, n := range nodes {
		if err := c.PutE(ctx, l,
			"pkg/cmd/roachtest/fixtures/"+strconv.Itoa(n)+"/"+name+".tgz",
			"{store-dir}/fixture.tgz", c.Node(n),
		); err != nil {
			return err
		}
	}
	// Extract fixture. Fail if there's already an LSM in the store dir.
	c.Run(ctx, nodes, "cd {store-dir} && [ ! -f {store-dir}/CURRENT ] && tar -xf fixture.tgz")
	return nil
}

// StartWithBinary starts a cockroach binary, assumed to already be
// present in the nodes in the path given.
func StartWithBinary(
	ctx context.Context,
	l *logger.Logger,
	c cluster.Cluster,
	nodes option.NodeListOption,
	binaryPath string,
	startOpts option.StartOpts,
) {
	settings := install.MakeClusterSettings(install.BinaryOption(binaryPath))
	c.Start(ctx, l, startOpts, settings, nodes)
}

// BinaryPathFromVersion shows where the binary for the given version
// can be found on roachprod nodes. It's either `./cockroach` or the
// path to which a released binary is staged.
func BinaryPathFromVersion(v string) string {
	if v == "" {
		return "./cockroach"
	}
	return filepath.Join("v"+v, "cockroach")
}

// RestartNodesWithNewBinary uploads a given cockroach version to the
// nodes passed, and restarts the cockroach process.
func RestartNodesWithNewBinary(
	ctx context.Context,
	t test.Test,
	l *logger.Logger,
	c cluster.Cluster,
	nodes option.NodeListOption,
	startOpts option.StartOpts,
	newVersion string,
) error {
	// NB: We could technically stage the binary on all nodes before
	// restarting each one, but on Unix it's invalid to write to an
	// executable file while it is currently running. So we do the
	// simple thing and upload it serially instead.

	// Restart nodes in a random order; otherwise node 1 would be running all
	// the migrations and it probably also has all the leases.
	rand.Shuffle(len(nodes), func(i, j int) {
		nodes[i], nodes[j] = nodes[j], nodes[i]
	})
	for _, node := range nodes {
		l.Printf("restarting node %d into version %s", node, VersionMsg(newVersion))
		// Stop the cockroach process gracefully in order to drain it properly.
		// This makes the upgrade closer to how users do it in production, but
		// it's also needed to eliminate flakiness. In particular, this will
		// make sure that DistSQL draining information is dissipated through
		// gossip so that other nodes running an older version don't consider
		// this upgraded node for DistSQL plans (see #87154 for more details).
		// TODO(yuzefovich): ideally, we would also check that the drain was
		// successful since if it wasn't, then we might see flakes too.
		if err := c.StopCockroachGracefullyOnNode(ctx, l, node); err != nil {
			return err
		}

		binary, err := UploadVersion(ctx, t, l, c, c.Node(node), newVersion)
		if err != nil {
			return err
		}
		StartWithBinary(ctx, l, c, c.Node(node), binary, startOpts)

		// We have seen cases where a transient error could occur when this
		// newly upgraded node serves as a gateway for a distributed query due
		// to remote nodes not being able to dial back to the gateway for some
		// reason (investigation of it is tracked in #87634). For now, we're
		// papering over these flakes by this sleep. For more context, see
		// #87104.
		// TODO(yuzefovich): remove this sleep once #87634 is fixed.
		time.Sleep(4 * time.Second)
	}

	return nil
}

// WaitForClusterUpgrade waits for the cluster version to reach the
// first node's binary version. This function should only be called if
// every node in the cluster has been restarted to run the same binary
// version. We rely on the cluster's internal self-upgrading
// mechanism to update the underlying cluster version.
func WaitForClusterUpgrade(
	ctx context.Context, l *logger.Logger, nodes option.NodeListOption, dbFunc func(int) *gosql.DB,
) error {
	newVersion, err := BinaryVersion(dbFunc(nodes[0]))
	if err != nil {
		return err
	}

	l.Printf("waiting for cluster to auto-upgrade to %s", newVersion)
	for _, node := range nodes {
		err := retry.ForDuration(10*time.Minute, func() error {
			currentVersion, err := ClusterVersion(ctx, dbFunc(node))
			if err != nil {
				return err
			}
			if currentVersion != newVersion {
				return fmt.Errorf("%d: expected cluster version %s, got %s", node, newVersion, currentVersion)
			}
			l.Printf("%s: acked by n%d", currentVersion, node)
			return nil
		})
		if err != nil {
			return err
		}
	}

	l.Printf("all nodes (%v) are upgraded to %s", nodes, newVersion)
	return nil
}

// CheckpointName returns the expected name of the checkpoint file
// under `pkg/cmd/roachtest/fixtures/{nodeID}` for the given binary
// version.
func CheckpointName(binaryVersion string) string {
	return "checkpoint-v" + binaryVersion
}

// VersionMsg returns a version string to be displayed in logs. It's
// either the version given, or the "<current>" string to represent
// the latest cockroach version, typically built off the branch being
// tested.
func VersionMsg(v string) string {
	if v == MainVersion {
		return "<current>"
	}

	return v
}
