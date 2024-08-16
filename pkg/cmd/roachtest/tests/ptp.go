// Copyright 2021 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/stretchr/testify/require"
	"strings"
)

func registerPTP(r registry.Registry) {

	r.Add(registry.TestSpec{
		Name:             "ptp",
		Owner:            registry.OwnerServer, // TODO(tbg): KV?
		Cluster:          r.MakeClusterSpec(1), // TODO(tbg): 3?
		Leases:           registry.MetamorphicLeases,
		Suites:           registry.Suites(registry.Weekly),
		CompatibleClouds: registry.OnlyGCE, // TODO(tbg)
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			require.False(t, c.IsLocal())
			// TODO(tbg): do some verification in each step, i.e. at least a few SQL queries
			// but probably a minute of running kv95 or something like that while using PTP.
			// Or, emit proper benchmark artifact from the PTP run so we can see the overhead
			// of using a ptp device vs vanilla.

			firstOpt := install.DefaultRunOptions()
			firstOpt.Nodes = c.Nodes(1).InstallNodes()
			details, err := c.RunWithDetails(ctx, t.L(), firstOpt, "uname -r")
			uname := strings.TrimSpace(details[0].Stdout)

			_, err = c.RunWithDetails(ctx, t.L(), install.DefaultRunOptions(),
				"sudo apt-get update -y && sudo apt-get install linux-modules-extra-"+uname+" && sudo modprobe ptp_kvm")
			require.NoError(t, err)

			// Start vanilla once, which should work. This is mostly to get
			// any cluster init code that may not be idempotent enough to be
			// retried if we started with badOpts right away.
			{
				c.Start(ctx, t.L(), option.DefaultStartOpts(), install.MakeClusterSettings(), c.All())
				c.Stop(ctx, t.L(), option.DefaultStopOpts(), c.All())
			}

			// Start with an invalid (not-found) PTP device. This should fail to even start,
			// thus proving (to a degree) that we at least try to do something with the ptp
			// device.
			{
				badOpts := option.DefaultStartOpts()
				badOpts.RoachprodOpts.ExtraArgs = append(badOpts.RoachprodOpts.ExtraArgs,
					"--clock-device=/does/not/exist",
				)
				shouldErr := c.StartE(ctx, t.L(), badOpts, install.MakeClusterSettings(), c.All())
				require.Error(t, shouldErr)
				// TODO(tbg): assert something more in error if possible.
				// At present it doesn't contain anything specific, just:
				// COMMAND_PROBLEM: exit status 1.
				t.L().Printf("got expected error starting with invalid ptp device: %s", shouldErr)
			}

			// Run against the real PTP device. This should work.
			{
				goodOpts := option.DefaultStartOpts()
				goodOpts.RoachprodOpts.ExtraArgs = append(goodOpts.RoachprodOpts.ExtraArgs,
					"--clock-device=/dev/ptp0",
				)
				c.Start(ctx, t.L(), goodOpts, install.MakeClusterSettings(), c.All())
				c.Stop(ctx, t.L(), option.DefaultStopOpts(), c.All())

				// TODO(tbg): it's difficult to tell that we're actually using the ptp device.
			}

			// Run against vanilla again, serving as a sanity check that we didn't pick
			// up really strange timestamps while using PTP.
			{
				c.Start(ctx, t.L(), option.DefaultStartOpts(), install.MakeClusterSettings(), c.All())
				c.Stop(ctx, t.L(), option.DefaultStopOpts(), c.All())
			}
		},
	})
}
