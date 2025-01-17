// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package perturbation

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

// backfill will create a backfill table during the startup and an index on it
// during the perturbation. The table and index are configured to always have
// one replica on the target node, but no leases. This stresses replication
// admission control.
type backfill struct{}

var _ perturbation = backfill{}

func (b backfill) setup() variations {
	// TODO(#139262): Track down why this test causes stalls and drop the value
	// to something more reasonable (like 5) once this is done.
	v := setup(b, math.Inf(1))
	return v
}

func (b backfill) setupMetamorphic(rng *rand.Rand) variations {
	v := b.setup()
	// TODO(#133114): The backfill test can cause OOM with low memory
	// configurations.
	v = v.randomize(rng)
	if v.mem == spec.Low {
		v.mem = spec.Standard
	}

	// TODO(#139319): This can be removed once we stop testing the non "full"
	// mode. Without full AC, these tests can OOM.
	if v.numNodes >= 30 && (v.acMode == elasticOnlyBoth || v.acMode == fullNormalElasticRepl) {
		v.acMode = fullBoth
	}

	// TODO(#136848): The backfill test will cause WAL failover resulting in
	// OOMs even with high memory configurations. This test passes without WAL
	// failover enabled or with more vCPUs per node.
	if v.disks > 1 && v.numNodes >= 30 && v.vcpu <= 8 {
		v.vcpu = 16
	}
	return v
}

// startTargetNode starts the target node and creates the backfill table.
func (b backfill) startTargetNode(ctx context.Context, t test.Test, v variations) {
	v.startNoBackup(ctx, t, v.targetNodes())

	// Create enough splits to start with one replica on each store.
	numSplits := v.vcpu * v.disks
	// TODO(baptist): Handle multiple target nodes.
	target := v.targetNodes()[0]
	initCmd := fmt.Sprintf("./cockroach workload init kv --db backfill --splits %d {pgurl:1}", numSplits)
	v.Run(ctx, option.WithNodes(v.Node(1)), initCmd)
	db := v.Conn(ctx, t.L(), 1)
	defer db.Close()

	cmd := fmt.Sprintf(`ALTER DATABASE backfill CONFIGURE ZONE USING constraints='{"+node%d":1}', lease_preferences='[[-node%d]]', num_replicas=3`, target, target)
	_, err := db.ExecContext(ctx, cmd)
	require.NoError(t, err)

	t.L().Printf("waiting for replicas to be in place")
	v.waitForRebalanceToStop(ctx, t)

	// Create and fill the backfill kv database before the test starts. We don't
	// want the fill to impact the test throughput. We use a larger block size
	// to create a lot of SSTables and ranges in a short amount of time.
	runCmd := fmt.Sprintf(
		"./cockroach workload run kv --db backfill --duration=%s --max-block-bytes=%d --min-block-bytes=%d --concurrency=100 {pgurl%s}",
		v.perturbationDuration, 10_000, 10_000, v.stableNodes())
	v.Run(ctx, option.WithNodes(v.workloadNodes()), runCmd)

	t.L().Printf("waiting for io overload to end")
	v.waitForIOOverloadToEnd(ctx, t)
	v.waitForRebalanceToStop(ctx, t)
}

// startPerturbation creates the index for the table.
func (b backfill) startPerturbation(ctx context.Context, t test.Test, v variations) time.Duration {
	db := v.Conn(ctx, t.L(), 1)
	defer db.Close()
	startTime := timeutil.Now()
	cmd := "CREATE INDEX backfill_index ON backfill.kv (k, v)"
	_, err := db.ExecContext(ctx, cmd)
	require.NoError(t, err)
	return timeutil.Since(startTime)
}

// endPerturbation does nothing as the backfill database is already created.
func (b backfill) endPerturbation(ctx context.Context, t test.Test, v variations) time.Duration {
	waitDuration(ctx, v.validationDuration)
	return v.validationDuration
}
