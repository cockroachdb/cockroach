// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/stretchr/testify/require"
)

// registerProcessLock registers the process lock test.
func registerProcessLock(r registry.Registry) {
	const runDuration = 5 * time.Minute
	r.Add(registry.TestSpec{
		Name:             "process-lock",
		Owner:            registry.OwnerStorage,
		Cluster:          r.MakeClusterSpec(4, spec.WorkloadNode(), spec.ReuseNone()),
		CompatibleClouds: registry.AllExceptAWS,
		Suites:           registry.Suites(registry.Nightly),
		// Encryption is implemented within the virtual filesystem layer,
		// just like disk-health monitoring. It's important to exercise
		// encryption-at-rest to ensure we don't corrupt the encryption-at-rest
		// data during a concurrent process start.
		EncryptionSupport: registry.EncryptionMetamorphic,
		Leases:            registry.MetamorphicLeases,
		Timeout:           2 * runDuration,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			startOpts := option.DefaultStartOpts()
			startSettings := install.MakeClusterSettings()
			startSettings.Env = append(startSettings.Env, "COCKROACH_AUTO_BALLAST=false")

			t.Status("starting cluster")
			c.Start(ctx, t.L(), startOpts, startSettings, c.CRDBNodes())

			// Wait for upreplication.
			conn := c.Conn(ctx, t.L(), 2)
			defer conn.Close()
			require.NoError(t, conn.PingContext(ctx))
			require.NoError(t, roachtestutil.WaitFor3XReplication(ctx, t.L(), conn))

			c.Run(ctx, option.WithNodes(c.WorkloadNode()), `./cockroach workload init kv --splits 1000 {pgurl:1}`)

			seed := int64(1666467482296309000)
			rng := randutil.NewTestRandWithSeed(seed)

			t.Status("starting workload")
			m := c.NewMonitor(ctx, c.CRDBNodes())
			m.Go(func(ctx context.Context) error {
				c.Run(ctx, option.WithNodes(c.WorkloadNode()), fmt.Sprintf(`./cockroach workload run kv --read-percent 0 `+
					`--duration %s --concurrency 512 --max-rate 4096 --tolerate-errors `+
					` --min-block-bytes=1024 --max-block-bytes=1024 `+
					`{pgurl:1-3}`, runDuration.String()))
				return nil
			})
			m.Go(func(ctx context.Context) error {
				// Query /proc/ on each of the nodes to retrieve the exact
				// command used to start the node. This is more future proof
				// than trying to reconstruct the command deposited into the
				// start script.
				var startCommands [3]string
				var storeFlags [3]string
				var enterpriseEncryption [3]string
				for n := 1; n <= 3; n++ {
					// On each node, find the cockroach process's pid by
					// grepping for `./cockroach start` in ps's output, and
					// grabbing the first field after stripping leading
					// whitespace. Then, use this pid cat /proc/<pid>/cmdline.
					result, err := c.RunWithDetailsSingleNode(ctx, t.L(), option.WithNodes(c.Node(n)),
						"cat /proc/`ps -eo pid,args | grep -E '([0-9]+) ./cockroach start' |  sed 's/^ *//' | cut -d ' ' -f 1`/cmdline")
					if err != nil {
						return err
					}
					// Arguments in /proc/<pid>/cmdline are NUL-delimited.
					args := strings.Split(result.Stdout, "\x00")
					var cmd bytes.Buffer
					fmt.Fprint(&cmd, args[0])
					for i := 1; i < len(args); i++ {
						if strings.ContainsRune(args[i], ' ') {
							// Quote any arguments that contain spaces.
							fmt.Fprintf(&cmd, " \"%s\"", args[i])
						} else {
							fmt.Fprintf(&cmd, " %s", args[i])
						}

						// Save the store flags that we'll need to run manual
						// store commands.
						if args[i] == "--store" {
							storeFlags[n-1] = args[i] + " " + args[i+1]
						}
						if args[i] == "--enterprise-encryption" {
							enterpriseEncryption[n-1] = args[i] + " " + args[i+1]
						}
					}
					startCommands[n-1] = cmd.String()
					t.L().PrintfCtx(ctx, "Retrieved n%d's start command: %s", n, startCommands[n-1])
				}

				done := time.After(runDuration)
				ticker := time.NewTicker(time.Second)
				defer ticker.Stop()
				for {
					select {
					case <-done:
						return nil
					case <-ctx.Done():
						return ctx.Err()
					case <-ticker.C:
						// Pick a random node.
						n := randutil.RandIntInRange(rng, 1, 4)

						// Pick a random operation to run.
						ops := []func(){
							func() {
								// Try to start the node again.
								err := c.RunE(ctx, option.WithNodes(c.Node(n)), startCommands[n-1])
								t.L().PrintfCtx(ctx, "Attempt to start cockroach process on n%d while another cockroach process is still running; error expected: %v", n, err)
							},
							func() {
								// Try to perform a manual compaction.
								err := c.RunE(ctx, option.WithNodes(c.Node(n)), fmt.Sprintf("./cockroach debug compact /mnt/data1/cockroach %s", enterpriseEncryption[n-1]))
								t.L().PrintfCtx(ctx, "Attempt to manual compact store on n%d while another cockroach process is still running; error expected: %v", n, err)
							},
							func() {
								// Restart the node. This verifies that the
								// other operations that were performed
								// concurrently with the running process did not
								// corrupt the on-disk state.
								m.ExpectDeath()
								c.Stop(ctx, t.L(), option.DefaultStopOpts(), c.Node(n))
								c.Start(ctx, t.L(), startOpts, startSettings, c.Node(n))
								m.ResetDeaths()
							},
						}
						ops[randutil.RandIntInRange(rng, 0, len(ops))]()
					}
				}
			})

			m.Wait()
		},
	})
}
