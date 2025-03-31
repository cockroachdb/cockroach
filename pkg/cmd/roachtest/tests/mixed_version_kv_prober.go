// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	"math/rand"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil/mixedversion"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/errors"
)

func registerKVProberMixedVersion(r registry.Registry) {
	r.Add(registry.TestSpec{
		Name:             "kv-prober/mixed-version",
		Owner:            registry.OwnerObservability,
		Cluster:          r.MakeClusterSpec(5, spec.WorkloadNode()),
		CompatibleClouds: registry.AllClouds,
		Suites:           registry.Suites(registry.MixedVersion, registry.Nightly),
		Run:              runKVProberMixedVersion,
	})
}

// runKVProber tests that accessing crdb_internal_probe_ranges works across mixed version clusters.
func runKVProberMixedVersion(ctx context.Context, t test.Test, c cluster.Cluster) {
	mvt := mixedversion.NewTest(ctx, t, t.L(), c,
		c.CRDBNodes(),
		// We test only upgrades from ?? because it is earliest supported version.
		mixedversion.MinimumSupportedVersion("v23.2.0"),
	)

	// Not sure if using a kv heavy workload is necessary for this...
	initWorkload := roachtestutil.NewCommand("./cockroach workload init tpcc").
		Arg("{pgurl%s}", c.CRDBNodes())
	runWorkload := roachtestutil.NewCommand("./cockroach workload run tpcc").
		Arg("{pgurl%s}", c.CRDBNodes()).
		Option("tolerate-errors")

	kvProbeRead := func(ctx context.Context, l *logger.Logger, r *rand.Rand, h *mixedversion.Helper) error {
		var kv_errors int
		if err := h.QueryRow(r, `select COUNT(error) from crdb_internal.probe_ranges(INTERVAL '1s', 'read') WHERE error != ''`).Scan(&kv_errors); err != nil {
			return errors.Wrap(err, "querying crdb_internal.probe_ranges(INTERVAL '1s', 'read')")
		}
		l.Printf("Successfully queried crdb_internal.probe_ranges(INTERVAL '1s', 'read')")
		return nil
	}

	kvProbeWrite := func(ctx context.Context, l *logger.Logger, r *rand.Rand, h *mixedversion.Helper) error {
		var kv_errors int
		if err := h.QueryRow(r, `select COUNT(error) from crdb_internal.probe_ranges(INTERVAL '1s', 'write') WHERE error != ''`).Scan(&kv_errors); err != nil {
			return errors.Wrap(err, "querying crdb_internal.probe_ranges(INTERVAL '1s', 'write')")
		}
		l.Printf("Successfully queried crdb_internal.probe_ranges(INTERVAL '1s', 'write')")
		return nil
	}

	stopTpcc := mvt.Workload("tpcc", c.WorkloadNode(), initWorkload, runWorkload)
	defer stopTpcc()

	mvt.InMixedVersion("check kv prober reads", kvProbeRead)
	mvt.InMixedVersion("check kv prober writes", kvProbeWrite)

	mvt.Run()
}
