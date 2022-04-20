package tests

import (
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/stretchr/testify/require"
)

func registerRollingRestart(r registry.Registry) {
	spec := r.MakeClusterSpec(6)
	r.Add(registry.TestSpec{
		Name:              "rolling-restart",
		Owner:             registry.OwnerKV,
		Tags:              []string{`default`},
		Cluster:           spec,
		NonReleaseBlocker: true,
		Run:               runRollingRestart,
	})
}

func runRollingRestart(ctx context.Context, t test.Test, c cluster.Cluster) {
	cockroachNodes := c.Range(1, c.Spec().NodeCount-1)
	workloadNode := c.Node(c.Spec().NodeCount)
	c.Put(ctx, t.Cockroach(), "./cockroach", c.All())
	pgUrl := fmt.Sprintf("{pgurl:1-%d}", c.Spec().NodeCount-1)

	startOpts := option.DefaultStartOpts()
	settings := install.MakeClusterSettings()
	c.Start(ctx, t.L(), startOpts, settings, cockroachNodes)

	db := c.Conn(ctx, t.L(), 1)
	defer db.Close()
	_, err := db.Exec("SET CLUSTER SETTING sql.trace.stmt.enable_threshold = '5s'")
	require.NoError(t, err, "failed to change cluster settings")

	// Run workload
	c.Run(ctx, workloadNode, "./cockroach workload init kv", pgUrl)
	t.L().Printf("workload initialized")
	go func() {
		c.Run(ctx, workloadNode,
			"./cockroach workload run kv --max-block-bytes 4096 --min-block-bytes 1024 --read-percent 10 --tolerate-errors --concurrency 64 --batch 10 --max-rate 350 --tolerate-errors",
			pgUrl)
	}()

	// Wait for wl to soak in.
	<-time.After(5*time.Minute)

	t.L().Printf("starting rolling restart")

	// Continuous rolling restart
	for {
		for _, node := range cockroachNodes {
			// Killing node
			t.L().Printf("restarting node %d", node)
			if err := c.StopE(ctx, t.L(), option.StopOpts{
				RoachprodOpts: roachprod.StopOpts{
					ProcessTag: "",
					Sig:        15,
					Wait:       true,
				},
				RoachtestOpts: struct {
					Worker bool
				}{true},
			}, c.Node(node)); err != nil {
				t.L().Printf("failed to stop node %d: %s", node, err)
			} else {
				// Restart node
				c.Start(ctx, t.L(), startOpts, settings, c.Node(node))
			}
			// Pause
			<-time.After(1 * time.Minute)
		}
	}
}
