// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tests

import (
	"context"
	gosql "database/sql"
	"fmt"
	"math/rand"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil/clusterupgrade"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil/mixedversion"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/errors"
)

func runDecommissionMixedVersions(ctx context.Context, t test.Test, c cluster.Cluster) {
	// NB: The suspect duration must be at least 10s, as versions 23.2 and
	// beyond will reset to the default of 30s if it fails validation, even if
	// set by a previous version.
	const suspectDuration = 10 * time.Second

	mvt := mixedversion.NewTest(ctx, t, t.L(), c, c.All(),
		// We test only upgrades from 23.2 in this test because it uses
		// the `workload fixtures import` command, which is only supported
		// reliably multi-tenant mode starting from that version.
		mixedversion.MinimumSupportedVersion("v23.2.0"),
	)
	n1 := 1
	n2 := 2

	mvt.OnStartup(
		"set suspect duration",
		func(ctx context.Context, l *logger.Logger, rng *rand.Rand, h *mixedversion.Helper) error {
			return h.System.Exec(
				rng,
				"SET CLUSTER SETTING server.time_after_store_suspect = $1",
				suspectDuration.String(),
			)
		})

	mvt.OnStartup(
		"preload data",
		func(ctx context.Context, l *logger.Logger, rng *rand.Rand, h *mixedversion.Helper) error {
			node, db := h.RandomDB(rng)
			cmd := `./cockroach workload fixtures import tpcc --warehouses=100 {pgurl:1}`
			if err := c.RunE(ctx, option.WithNodes(c.Node(node)), cmd); err != nil {
				return errors.Wrap(err, "failed to import fixtures")
			}

			return errors.Wrapf(
				roachtestutil.WaitFor3XReplication(ctx, l, db),
				"error waiting for 3x replication",
			)
		})

	mvt.InMixedVersion(
		"test decommission",
		func(ctx context.Context, l *logger.Logger, rng *rand.Rand, h *mixedversion.Helper) error {
			n1Version, _ := h.System.NodeVersion(n1) // safe to ignore error as n1 is part of the cluster
			n2Version, _ := h.System.NodeVersion(n2) // safe to ignore error as n1 is part of the cluster
			db1 := h.System.Connect(n1)
			db2 := h.System.Connect(n2)

			l.Printf("checking membership via n%d (%s)", n1, n1Version)
			if err := newLivenessInfo(db1).membershipNotEquals("active").eventuallyEmpty(); err != nil {
				return err
			}

			sleepDur := 2 * suspectDuration
			l.Printf("sleeping for %s", sleepDur)
			sleepCtx(ctx, sleepDur)

			l.Printf("partially decommissioning n1 (%s) from n2 (%s)", n1Version, n2Version)
			if err := partialDecommission(ctx, c, n1, n2, clusterupgrade.CockroachPathForVersion(t, n2Version)); err != nil {
				return err
			}

			l.Printf("verifying n1 is decommissioning via n2 (%s)", n2Version)
			err := newLivenessInfo(db2).
				membershipEquals("decommissioning").
				isDecommissioning().
				eventuallyOnlyNode(n1)
			if err != nil {
				return err
			}

			l.Printf("recommissioning all nodes via n1 (%s)", n1Version)
			if err := recommissionNodes(ctx, c, c.All(), n1, clusterupgrade.CockroachPathForVersion(t, n1Version)); err != nil {
				return err
			}

			l.Printf("verifying no node is decommissioning")
			if err := newLivenessInfo(db1).isDecommissioning().eventuallyEmpty(); err != nil {
				return err
			}

			l.Printf("verifying all nodes are active")
			if err := newLivenessInfo(db1).membershipNotEquals("active").eventuallyEmpty(); err != nil {
				return err
			}

			return nil
		})

	mvt.AfterUpgradeFinalized(
		"fully decommission on last upgrade",
		func(ctx context.Context, l *logger.Logger, rng *rand.Rand, h *mixedversion.Helper) error {
			sleepDur := 2 * suspectDuration
			l.Printf("sleeping for %s", sleepDur)
			sleepCtx(ctx, sleepDur)

			if h.Context().ToVersion.IsCurrent() {
				l.Printf("fully decommissioning n1 via n2")

				n1Version, _ := h.System.NodeVersion(n1) // safe to ignore error as n1 is part of the cluster
				return fullyDecommission(ctx, c, n1, n2, clusterupgrade.CockroachPathForVersion(t, n1Version))
			} else {
				l.Printf("skipping -- still more upgrades to go through")
				return nil
			}
		})

	mvt.Run()
}

// partialDecommission runs `cockroach node decommission --wait=none`
// from a given node, targeting another. It uses the specified binary
// to run the command.
func partialDecommission(
	ctx context.Context, c cluster.Cluster, target, from int, cockroachPath string,
) error {
	cmd := roachtestutil.NewCommand("%s node decommission %d", cockroachPath, target).
		WithEqualsSyntax().
		Flag("wait", "none").
		Flag("port", fmt.Sprintf("{pgport:%d}", from)).
		Flag("certs-dir", install.CockroachNodeCertsDir).
		String()

	return c.RunE(ctx, option.WithNodes(c.Node(from)), cmd)
}

// recommissionNodes runs `cockroach node recommission` from a given
// node, targeting the `nodes` in the cluster. It uses the specified
// binary to run the command.
func recommissionNodes(
	ctx context.Context,
	c cluster.Cluster,
	nodes option.NodeListOption,
	from int,
	cockroachPath string,
) error {
	cmd := roachtestutil.NewCommand("%s node recommission %s", cockroachPath, nodes.NodeIDsString()).
		Flag("port", fmt.Sprintf("{pgport:%d}", from)).
		Flag("certs-dir", install.CockroachNodeCertsDir).
		String()

	return c.RunE(ctx, option.WithNodes(c.Node(from)), cmd)
}

// fullyDecommission is like partialDecommission, except it uses
// `--wait=all`.
func fullyDecommission(
	ctx context.Context, c cluster.Cluster, target, from int, cockroachPath string,
) error {
	cmd := roachtestutil.NewCommand("%s node decommission %d", cockroachPath, target).
		WithEqualsSyntax().
		Flag("wait", "all").
		Flag("port", fmt.Sprintf("{pgport:%d}", from)).
		Flag("certs-dir", install.CockroachNodeCertsDir).
		String()

	return c.RunE(ctx, option.WithNodes(c.Node(from)), cmd)
}

// gossipLiveness is a helper struct that allows callers to verify
// that the liveness data (`crdb_internal.gossip_liveness`) eventually
// reaches a desired state.
//
// Typical usage:
//
//	newLivenessInfo(db).membershipEquals("decommissioned").eventuallyOnlyNode(n1)
//
// In this example, we assert that eventually only node `n1` has its
// membership status equal to `decommissioned`. This could be used
// after a `decommission` command.
type gossipLiveness struct {
	node            int
	decommissioning bool
	membership      string
}

type livenessInfo struct {
	db       *gosql.DB
	filters  []func(gossipLiveness) bool
	liveness []gossipLiveness
}

func newLivenessInfo(db *gosql.DB) *livenessInfo {
	return &livenessInfo{db: db}
}

// addFilter adds a filter to be applied to the liveness records when
// checking for a property. Some filters are already predefined, such
// as `membershipEquals`, `isDecommissioning`, etc.
func (l *livenessInfo) addFilter(f func(gossipLiveness) bool) {
	l.filters = append(l.filters, f)
}

// membershipEquals adds a filter so that we only look at records
// where the `membership` column matches the value passed.
func (l *livenessInfo) membershipEquals(membership string) *livenessInfo {
	l.addFilter(func(rec gossipLiveness) bool {
		return rec.membership == membership
	})

	return l
}

// membershipNotEquals adds a filter so that we only look at records
// where the `membership` column is *different* from the value passed.
func (l *livenessInfo) membershipNotEquals(membership string) *livenessInfo {
	l.addFilter(func(rec gossipLiveness) bool {
		return rec.membership != membership
	})

	return l
}

// isDecommissioning adds a filter so that we only look at records
// where the `decommissioning` column is `true`.
func (l *livenessInfo) isDecommissioning() *livenessInfo {
	l.addFilter(func(rec gossipLiveness) bool {
		return rec.decommissioning
	})

	return l
}

// eventuallyEmpty asserts that, eventually, the number of records in
// `crdb_internal.gossip_liveness` that match the filters used is zero.
func (l *livenessInfo) eventuallyEmpty() error {
	return l.eventually(func(records []gossipLiveness) error {
		if len(records) > 0 {
			return errors.Newf("expected no matches, found: %#v", records)
		}

		return nil
	})
}

// eventuallyOnlyNode asserts that, eventually, only the liveness
// record for the given `node` matches the filters used.
func (l *livenessInfo) eventuallyOnlyNode(node int) error {
	return l.eventually(func(records []gossipLiveness) error {
		if len(records) != 1 {
			return errors.Newf("expected one liveness record, found: %#v", records)
		}

		if records[0].node != node {
			return errors.Newf("expected to match n%d, found n%d", node, records[0].node)
		}

		return nil
	})
}

// eventually asserts that, eventually, the `predicate` given returns
// true when called with the records that match all the filters used.
func (l *livenessInfo) eventually(predicate func([]gossipLiveness) error) error {
	return testutils.SucceedsSoonError(func() error {
		if err := l.refreshLiveness(); err != nil {
			return errors.Wrap(err, "refreshing liveness info")
		}

		var filtered []gossipLiveness
		for _, record := range l.liveness {
			match := true
			for _, filter := range l.filters {
				if !filter(record) {
					match = false
					break
				}
			}

			if match {
				filtered = append(filtered, record)
			}
		}

		return predicate(filtered)
	})
}

func (l *livenessInfo) refreshLiveness() error {
	rows, err := l.db.Query(
		"SELECT node_id, decommissioning, membership FROM crdb_internal.gossip_liveness",
	)
	if err != nil {
		return err
	}

	var records []gossipLiveness
	for rows.Next() {
		var record gossipLiveness
		if err := rows.Scan(&record.node, &record.decommissioning, &record.membership); err != nil {
			return errors.Wrap(err, "failed to scan liveness row")
		}

		records = append(records, record)
	}

	if err := rows.Err(); err != nil {
		return errors.Wrap(err, "failed to read liveness rows")
	}

	l.liveness = records
	return nil
}

func sleepCtx(ctx context.Context, duration time.Duration) {
	select {
	case <-ctx.Done():
	case <-time.After(duration):
	}
}
