// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	spec2 "github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func registerPTP(r registry.Registry) {
	r.Add(registry.TestSpec{
		Name:  "ptp",
		Owner: registry.OwnerKV,
		// Don't reuse this cluster after this test, since we're installing kernel modules etc.
		// In practice I don't see how this could cause problems but it's not worth any risk.
		Cluster: r.MakeClusterSpec(3, spec2.ReuseNone()),
		Leases:  registry.MetamorphicLeases,
		// We're unlikely to break PTP support on the regular and this test does not meaningfully
		// exercise anything else.
		Suites: registry.Suites(registry.Weekly),
		// It can possibly work on other clouds too, but this has not been tested. It will
		// depend on the linux distribution and perhaps kernel version.
		CompatibleClouds: registry.OnlyGCE,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			require.False(t, c.IsLocal())
			nodes := c.CRDBNodes()

			// delta.sh shows the difference between the system clock and the PTP clock, in seconds.
			deltaDotSH := `#!/usr/bin/env bash
set -euo pipefail
# Example output:
# phc_ctl[3247.110]: clock time is 1724228402.794777005 or Wed Aug 21 08:20:02 2024
datesec="$(date -u '+%s')"
ptpsec="$(phc_ctl /dev/ptp0 get | sed -nr 's/.*clock time is ([0-9]+).*/\1/p')"
delta=$(($datesec-$ptpsec))
echo $delta`
			require.NoError(t, c.PutString(ctx, deltaDotSH, "delta.sh", 0700, nodes))
			setOffset := func(ctx context.Context, add time.Duration) error {
				ns := option.WithNodes(nodes)
				// Stop the skewer if it's running. Note that the clock skew persists once it has exited;
				// the skewer is really more like a clock daemon that makes sure the configuration is
				// continuously upheld, even if in this case the clock wouldn't really drift over time.
				// There doesn't seem to be a way to have it eagerly exit after making the initial adjustment.
				if err := c.RunE(
					ctx, ns, `! sudo systemctl is-active clock-skewer || sudo systemctl stop clock-skewer`,
				); err != nil {
					return err
				}
				if err := c.RunE(ctx, ns,
					// -m: log to stdout
					// -l: log level
					// -s: source clock
					// -c: target clock (to which offset will be added)
					// -O: offset in seconds
					fmt.Sprintf(`sudo systemd-run --unit clock-skewer -- phc2sys -m -l 6 -s /dev/ptp0 -c CLOCK_REALTIME -O "%d"`,
						int64(add.Seconds())),
				); err != nil {
					return err
				}

				wantedSecs := int64(add.Seconds())
				if err := testutils.SucceedsSoonError(func() error {
					res, err := c.RunWithDetails(ctx, t.L(), ns, `./delta.sh`)
					if err != nil {
						return err
					}
					for i, r := range res {
						secs, err := strconv.ParseInt(strings.TrimSpace(r.Stdout), 10, 64)
						if err != nil {
							return err
						}

						if math.Abs(float64(secs-wantedSecs)) > 1 {
							// The observed and desired offset are more than a second apart, so we're
							// not satisfied (yet). We tolerate one second because we're making two
							// separate measurements but it really oughtn't matter.
							return errors.Errorf("%d: actual offset %ds and desired offset %ds haven't converged",
								i+1, secs, wantedSecs)
						}
					}
					return nil
				}); err != nil {
					return err
				}

				if add == 0 {
					// If we're resetting the offset, we have now restored it but the skewer is still running.
					// This should do no harm since we're tolerant of it running in the next call, but it's good
					// form to remove it.
					if err := c.RunE(ctx, ns, `sudo systemctl stop clock-skewer`); err != nil {
						return err
					}
				}
				t.L().Printf("system clock now offset from ptp clock by %ds", wantedSecs)
				return nil
			}

			wln := option.WithNodes(c.Node(1)) // light workload only, so no need for dedicated workload node
			workload := func(ctx context.Context) error {
				t.L().Printf("running short workload")
				if err := c.RunE(
					ctx, wln,
					`./cockroach workload init kv --drop --splits 100 {pgurl:1}`); err != nil {
					return err
				}

				return c.RunE(ctx, wln, fmt.Sprintf(
					"./cockroach workload run kv --max-rate 100 --read-percent=50 --duration=60s {pgurl%s}",
					nodes,
				))
			}

			details, err := c.RunWithDetails(ctx, t.L(), option.WithNodes(c.Nodes(1)), "uname -r")
			require.NoError(t, err)
			uname := strings.TrimSpace(details[0].Stdout)

			t.L().Printf("installing ptp")
			_, err = c.RunWithDetails(ctx, t.L(), option.WithNodes(nodes),
				strings.Replace(
					`sudo apt-get update -yyq && sudo apt-get install -yyq linuxptp linux-modules-extra-$uname && sudo modprobe ptp_kvm && sudo chmod o+rw /dev/ptp0`,
					"$uname", uname, -1,
				))
			require.NoError(t, err)

			// Start vanilla once, which should work. This is mostly to get past
			// any cluster init code that may not be idempotent enough to be
			// retried if we started with badOpts right away.
			{
				t.L().Printf("initializing cluster (regular clock)")
				c.Start(ctx, t.L(), option.DefaultStartOpts(), install.MakeClusterSettings(), nodes)
				c.Stop(ctx, t.L(), option.DefaultStopOpts(), nodes)
			}

			// Start with an invalid (not-found) PTP device. This should fail to even start,
			// thus proving (to a degree) that we at least try to do something with the ptp
			// device.
			{
				t.L().Printf("checking that CRDB fails to start with invalid PTP path")
				badOpts := option.DefaultStartOpts()
				badOpts.RoachprodOpts.ExtraArgs = append(badOpts.RoachprodOpts.ExtraArgs,
					"--clock-device=/does/not/exist",
				)
				shouldErr := c.StartE(ctx, t.L(), badOpts, install.MakeClusterSettings(), nodes)
				require.Error(t, shouldErr)
				// TODO(tbg): assert something more in error if possible.
				// At present it doesn't contain anything specific, just:
				// COMMAND_PROBLEM: exit status 1.
				t.L().Printf("got expected error starting with invalid ptp device: %s", shouldErr)
			}

			// Run against the real PTP device. This should work. It should continue working
			// if we mess with the system clock half-way through, since we're not supposed to
			// rely on that clock.
			{
				t.L().Printf("checking that CRDB can run against correct PTP device")
				goodOpts := option.DefaultStartOpts()
				goodOpts.RoachprodOpts.ExtraArgs = append(goodOpts.RoachprodOpts.ExtraArgs,
					"--clock-device=/dev/ptp0",
					"--disable-max-offset-check", // see https://github.com/cockroachdb/cockroach/issues/134760
				)
				c.Start(ctx, t.L(), goodOpts, install.MakeClusterSettings(), nodes)
				require.NoError(t, workload(ctx))
				// Move the real clock back by an hour.
				conn := sqlutils.MakeSQLRunner(c.Conn(ctx, t.L(), 1))
				var tPre time.Time
				conn.QueryRow(t, `SELECT now()`).Scan(&tPre)
				t.L().Printf("rewinding system clock by an hour")
				require.NoError(t, setOffset(ctx, -3600*time.Second))

				// HACK: As discussed in [1], we aren't using the ptp device throughout,
				// and it's also possible that gRPC and other libraries have dependencies
				// on the system clock that cause a hiccup when we suddenly move the sys
				// clock back by an hour. To make this test stable without boiling many
				// an ocean, give the system 60s to recover.
				time.Sleep(60 * time.Second)

				// Now we assume the cluster is back on its feet.
				var tPost time.Time
				conn.QueryRow(t, `SELECT now()`).Scan(&tPost)
				// It's normal for there to be a little difference, since changing the offset also takes
				// time. But there shouldn't be an hour of difference and in particular the timestamps
				// should retain the ordering in which they were taken, which would not be the case if
				// the second now() picked up the regressed system clock.
				t.L().Printf("now() returned %s (before changing system clock) and %s (after), post-pre=%ds",
					tPre, tPost, int64(tPost.Sub(tPre).Seconds()))
				if tPost.Before(tPre) {
					// When tPre was taken, the system and ptp clock were aligned.
					// When tPost was taken, the system clock was trailing the PTP
					// by about an hour.
					// We really shouldn't be using the system clock at all here and
					// tPost should be later than tPre.
					// This is assuming we don't see clock adjustments on the PTP clock
					// though, so if this ever fails by a tiny margin that's probably
					// what happened.
					t.Fatalf("now() regressed, possibly using system clock instead of PTP: %s -> %s", tPre, tPost)
				}
				require.NoError(t, workload(ctx))
				c.Stop(ctx, t.L(), option.DefaultStopOpts(), nodes)
			}

			// Run against vanilla again, serving as a sanity check that we didn't pick
			// up really strange timestamps while using PTP.
			{
				t.L().Printf("restoring system clock and running CRDB against it once more")
				require.NoError(t, setOffset(ctx, 0)) // this also stops the clock-skewer service
				opts := option.DefaultStartOpts()
				opts.RoachprodOpts.ExtraArgs = []string{
					"--disable-max-offset-check", // see https://github.com/cockroachdb/cockroach/issues/134760
				}
				c.Start(ctx, t.L(), opts, install.MakeClusterSettings(), nodes)
				require.NoError(t, workload(ctx))
				c.Stop(ctx, t.L(), option.DefaultStopOpts(), nodes)
			}
		},
	})
}
