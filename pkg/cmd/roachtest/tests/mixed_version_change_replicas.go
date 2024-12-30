// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil/clusterupgrade"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil/mixedversion"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/errors"
)

func registerChangeReplicasMixedVersion(r registry.Registry) {
	r.Add(registry.TestSpec{
		Name:             "change-replicas/mixed-version",
		Owner:            registry.OwnerKV,
		Cluster:          r.MakeClusterSpec(4),
		CompatibleClouds: registry.AllExceptAWS,
		Suites:           registry.Suites(registry.MixedVersion, registry.Nightly),
		Randomized:       true,
		Run:              runChangeReplicasMixedVersion,
		Timeout:          60 * time.Minute,
	})
}

var v232 = clusterupgrade.MustParseVersion("v23.2.0")

// runChangeReplicasMixedVersion is a regression test for
// https://github.com/cockroachdb/cockroach/issues/94834. It runs replica config
// changes (moves replicas around) in mixed-version clusters, both explicitly
// with ALTER RANGE RELOCATE and implicitly via zone configs and the replicate
// queue.
func runChangeReplicasMixedVersion(ctx context.Context, t test.Test, c cluster.Cluster) {
	// createTable creates a test table, and splits and scatters it.
	createTable := func(
		ctx context.Context, l *logger.Logger, r *rand.Rand, h *mixedversion.Helper,
	) error {
		l.Printf("creating table")
		if err := h.Exec(r, `CREATE TABLE test (id INT PRIMARY KEY)`); err != nil {
			return err
		}
		_, db := h.RandomDB(r)
		if err := roachtestutil.WaitFor3XReplication(ctx, l, db); err != nil {
			return err
		}

		// Enable split/scatter on tenants if necessary.
		if err := enableTenantSplitScatter(l, r, h); err != nil {
			return err
		}

		l.Printf("splitting table")
		const ranges = 100
		err := h.Exec(r, `ALTER TABLE test SPLIT AT SELECT i FROM generate_series(1, $1) AS g(i)`,
			ranges-1)
		if err != nil {
			return err
		}

		l.Printf("scattering table")
		if err := h.Exec(r, `ALTER TABLE test SCATTER`); err != nil {
			return err
		}
		return nil
	}

	// evacuateNodeUsingZoneConfig moves replicas off of a node using a zone
	// config.
	evacuateNodeUsingZoneConfig := func(
		ctx context.Context,
		l *logger.Logger,
		r *rand.Rand,
		h *mixedversion.Helper,
		node int,
	) error {
		l.Printf("moving replicas off of n%d using zone config", node)
		// Enable necessary features on tenant deployments if running on a
		// version where they are not enabled by default.
		if !h.Context().FromVersion.AtLeast(mixedversion.TenantsAndSystemAlignedSettingsVersion) {
			for _, name := range []string{
				"sql.virtual_cluster.feature_access.zone_configs.enabled",
				"sql.virtual_cluster.feature_access.zone_configs_unrestricted.enabled",
			} {
				if err := setTenantSetting(l, r, h, name, true); err != nil {
					return errors.Wrapf(err, "setting %s", name)
				}
			}
		}

		err := h.Exec(r, fmt.Sprintf(
			`ALTER TABLE test CONFIGURE ZONE USING constraints = '[-node%d]'`, node))
		if err != nil {
			return err
		}

		var rangeCount int
		for i := 0; i < 60; i++ {
			err := h.QueryRow(r, `SELECT count(*) FROM `+
				`[SHOW RANGES FROM TABLE test] WHERE $1::int = ANY(replicas)`, node).Scan(&rangeCount)
			if err != nil {
				return err
			}
			l.Printf("%d replicas on n%d", rangeCount, node)
			if rangeCount == 0 {
				break
			}
			time.Sleep(3 * time.Second)
		}
		if rangeCount > 0 {
			return errors.Errorf("n%d still has %d replicas", node, rangeCount)
		}

		if err = h.Exec(r, `ALTER TABLE test CONFIGURE ZONE USING constraints = '[]'`); err != nil {
			return err
		}

		return nil
	}

	// evacuateNodeUsingRelocate moves replicas off of a node using ALTER TABLE
	// RELOCATE.
	evacuateNodeUsingRelocate := func(
		ctx context.Context,
		l *logger.Logger,
		r *rand.Rand,
		h *mixedversion.Helper,
		node int,
	) error {
		setReplicateQueueEnabled := func(enabled bool) error {
			for _, node := range c.All() {
				_, err := h.System.Connect(node).ExecContext(ctx,
					`SELECT crdb_internal.kv_set_queue_active('replicate', $1)`, enabled)
				if err != nil {
					return err
				}
			}
			return nil
		}

		// Relocate replicas to other nodes which don't already have one.
		for _, target := range c.All() {
			if target == node {
				continue
			}

			var rangeErrors map[int]string
			for i := 0; i < 30; i++ {
				l.Printf("moving replicas from n%d to n%d using ALTER TABLE RELOCATE", node, target)

				// Disable the replicate queue, to avoid interference.
				if err := setReplicateQueueEnabled(false); err != nil {
					return err
				}

				// Relocate ranges.
				rows, err := h.Query(r, `ALTER RANGE RELOCATE FROM $1::int TO $2::int FOR `+
					`SELECT range_id FROM [SHOW RANGES FROM TABLE test] `+
					`WHERE $1::int = ANY(replicas) AND $2::int != ALL(replicas)`, node, target)
				if err != nil {
					return err
				}

				var relocated int
				rangeErrors = map[int]string{}
				for rows.Next() {
					var rangeID int
					var pretty, result string
					if err := rows.Scan(&rangeID, &pretty, &result); err != nil {
						return err
					} else if result != "ok" {
						rangeErrors[rangeID] = result
					} else {
						relocated++
					}
				}
				if err := rows.Err(); err != nil {
					return err
				}

				l.Printf("%d replicas relocated, %d errors", relocated, len(rangeErrors))

				// Re-enable the replicate queue. This is needed not only to reset the
				// state for the test, but also to fix any conflicts with ongoing
				// configuration changes made by the replicate queue before the next
				// retry.
				if err := setReplicateQueueEnabled(true); err != nil {
					return err
				}

				// If all ranges were relocated, move onto the next target.
				if len(rangeErrors) == 0 {
					break
				}

				time.Sleep(3 * time.Second)
			}

			// If ranges still failed after exhausting retries, give up.
			if len(rangeErrors) > 0 {
				for rangeID, result := range rangeErrors {
					l.Printf("failed to move r%d from n%d to n%d: %s",
						rangeID, node, target, result)
				}
				return errors.Errorf("failed to move %d replicas from n%d to n%d",
					len(rangeErrors), node, target)
			}
		}

		return nil
	}

	// moveReplicas moves replicas around between nodes randomly.
	moveReplicas := func(
		ctx context.Context, l *logger.Logger, r *rand.Rand, h *mixedversion.Helper,
	) error {
		// Skip this step if we are in multitenant mode and we are not
		// running at least 23.2 yet. This function is not supported in this
		// scenario as we can't configure zones.
		if h.IsMultitenant() && !h.Context().FromVersion.AtLeast(v232) {
			l.Printf("multitenant deployment running an unsupported version; skipping")
			return nil
		}

		// First, scatter the range.
		l.Printf("scattering table")
		if err := h.Exec(r, `ALTER TABLE test SCATTER`); err != nil {
			return err
		}

		// Evacuate each node, in random order, using a random mechanism.
		nodes := append(option.NodeListOption{}, c.All()...)
		r.Shuffle(len(nodes), func(i, j int) {
			nodes[i], nodes[j] = nodes[j], nodes[i]
		})

		for _, node := range nodes {
			if r.Float64() < 0.5 {
				if err := evacuateNodeUsingZoneConfig(ctx, l, r, h, node); err != nil {
					return err
				}
			} else {
				if err := evacuateNodeUsingRelocate(ctx, l, r, h, node); err != nil {
					return err
				}
			}
		}

		// Finally, scan the table to ensure all ranges are functional. We don't
		// expect any rows, since the table is empty, but this still requires a scan
		// across each range.
		l.Printf("scanning table")
		var count int
		row := h.QueryRow(r, `SELECT count(*) FROM test`)
		if err := row.Scan(&count); err != nil {
			return err
		} else if count != 0 {
			return errors.Errorf("unexpected row count %d", count)
		}

		return nil
	}

	// Set up and run test.
	mvt := mixedversion.NewTest(
		ctx, t, t.L(), c, c.All(), mixedversion.ClusterSettingOption(
			// Speed up the queues.
			install.EnvOption{"COCKROACH_SCAN_MAX_IDLE_TIME=10ms"},
		),
		// Avoid repeatedly running into #114549 on earlier minor versions.
		// TODO(kvoli): Remove in 24.2.
		mixedversion.AlwaysUseLatestPredecessors,
	)

	mvt.OnStartup("create test table", createTable)
	mvt.InMixedVersion("move replicas", moveReplicas)
	mvt.AfterUpgradeFinalized("move replicas", moveReplicas)
	mvt.Run()
}

// enableTenantSplitScatter updates cluster settings that allow
// tenants to use SPLIT AT and SCATTER. This is only performed if the
// mixedversion test is running on a multitenant deployment, and only
// if required by the active version.
func enableTenantSplitScatter(l *logger.Logger, r *rand.Rand, h *mixedversion.Helper) error {
	// Note that although TenantsAndSystemAlignedSettingsVersion generally refers to
	// shared process deployments, the defaults for SPLIT and SCATTER were also changed
	// for separate process in the same version.
	if h.Context().FromVersion.AtLeast(mixedversion.TenantsAndSystemAlignedSettingsVersion) {
		return nil
	}

	settings := []string{
		"sql.split_at.allow_for_secondary_tenant.enabled",
		"sql.scatter.allow_for_secondary_tenant.enabled",
	}

	for _, s := range settings {
		// Only enable the relevant settings if they are not already
		// enabled by default.
		if err := setTenantSetting(l, r, h, s, true); err != nil {
			return errors.Wrapf(err, "failed to set cluster setting %s", s)
		}
	}

	return nil
}

// setTenantSetting sets the cluster setting of the given name on
// the tenant created in for the mixedversion test. After setting it
// via the system tenant, it also waits until the update is visible to
// the actual tenant, making sure that statements that need the
// setting to be enabled can run successfully.
//
// It is a no-op to call this function on single tenant (system-only)
// deployments.
func setTenantSetting(
	l *logger.Logger, r *rand.Rand, h *mixedversion.Helper, name string, value bool,
) error {
	if !h.IsMultitenant() {
		return nil
	}

	if err := h.System.Exec(
		r,
		fmt.Sprintf(`ALTER TENANT $1 SET CLUSTER SETTING %s = $2`, name),
		h.Tenant.Descriptor.Name, value,
	); err != nil {
		return errors.Wrapf(err, "failed to set %s", name)
	}

	// Wait for the setting to be visible to all nodes in the tenant.
	for _, n := range h.Tenant.Descriptor.Nodes {
		db := h.Tenant.Connect(n)
		if err := testutils.SucceedsSoonError(func() error {
			var currentValue bool
			if err := db.QueryRow(fmt.Sprintf("SHOW CLUSTER SETTING %s", name)).Scan(&currentValue); err != nil {
				return errors.Wrapf(err, "failed to retrieve setting %s", name)
			}

			if currentValue != value {
				err := fmt.Errorf(
					"waiting for setting %s: current (%t) != expected (%t)", name, currentValue, value,
				)
				l.Printf("%v", err)
				return err
			}

			return nil
		}); err != nil {
			return err
		}
	}
	return nil
}
